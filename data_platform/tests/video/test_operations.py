import io
import json
import subprocess
import sys
from pathlib import Path
from unittest.mock import Mock
from urllib.error import HTTPError

import pytest
from scripts import check_video_sources as check, migrate_cdc_stack as migration, sonar_video as sonar


def response(data):
    return io.BytesIO(json.dumps(data).encode())


@pytest.fixture
def sources(monkeypatch):
    data = {}
    for name, prefix, connector_class in [
        ("abassurance-sqlserver", "abassurance", "SqlServerConnector"),
        ("assureplus-oracle-v2", "assureplus_v2", "OracleConnector"),
    ]:
        data[name] = {
            "info": {"config": {"topic.prefix": prefix, "connector.class": "test." + connector_class}},
            "status": {"connector": {"state": "RUNNING"}, "tasks": [{"state": "RUNNING"}]},
        }
    monkeypatch.setattr(check, "urlopen", lambda *a, **k: response(data))
    return data


def test_source_health(sources, monkeypatch):
    run = Mock()
    monkeypatch.setattr(check.subprocess, "run", run)
    check.check()
    assert "kafka-get-offsets.sh" in run.call_args.args[0][3]


@pytest.mark.parametrize("fault,message", [
    ("missing", "Missing connector"), ("prefix", "Unexpected topic prefix"),
    ("class", "Unexpected connector class"), ("connector", "Connector is not ready"),
    ("empty tasks", "Connector is not ready"), ("failed task", "Connector is not ready"),
])
def test_invalid_source_stops_before_offsets(sources, monkeypatch, fault, message):
    name = "assureplus-oracle-v2"
    detail = sources[name]
    if fault == "missing":
        del sources[name]
    elif fault == "prefix":
        detail["info"]["config"]["topic.prefix"] = "different"
    elif fault == "class":
        detail["info"]["config"]["connector.class"] = "test.SqlServerConnector"
    elif fault == "connector":
        detail["status"]["connector"]["state"] = "FAILED"
    else:
        detail["status"]["tasks"] = [] if fault == "empty tasks" else [{"state": "FAILED"}]
    run = Mock()
    monkeypatch.setattr(check.subprocess, "run", run)
    with pytest.raises(RuntimeError, match=message + ": " + name):
        check.check()
    run.assert_not_called()


def test_sonar_request_and_pagination(monkeypatch, capsys):
    monkeypatch.setenv("SONAR_TOKEN", "fixture")
    monkeypatch.setattr(sys, "argv", ["report", "example", "--branch", "develop"])
    def open_request(req, timeout):
        assert req.get_header("Authorization").startswith("Basic ")
        assert "branch=develop" in req.full_url
        if "project_status" in req.full_url:
            return response({"projectStatus": {"status": "ERROR", "conditions": [
                {"metricKey": "new_coverage", "status": "ERROR", "actualValue": "12"}]}})
        return response({"issues": [{"key": "test", "rule": "example", "component": "f", "message": "details"}],
                         "paging": {"total": 101}})
    monkeypatch.setattr(sonar, "urlopen", open_request)
    sonar.main()
    assert capsys.readouterr().out.count("details") == 2
    def reject(req, timeout):
        raise HTTPError(req.full_url, 400, "Bad request", {}, io.BytesIO(b'bad scope'))
    monkeypatch.setattr(sonar, "urlopen", reject)
    with pytest.raises(RuntimeError, match="bad scope"):
        sonar.query("issues/search")


def test_cold_restore_contract(tmp_path, monkeypatch):
    names = ["test-oracle", "test-sql", "test-kafka", "test-connect"]
    monkeypatch.setattr(migration, "NAMES", names)
    state = {"Name": names[0], "State": {"Status": "exited", "OOMKilled": False, "ExitCode": 0},
             "Config": {"Hostname": "original"}, "NetworkSettings": {"Networks": {"old": {"Aliases": ["broker"]}}},
             "Mounts": [{"Type": "volume", "Destination": "/data", "RW": True}],
             "HostConfig": {"PortBindings": {"9092/tcp": [{"HostPort": "9192"}]}}}
    monkeypatch.setattr(migration, "inspect", lambda name: state)
    calls = []
    def docker(*args):
        calls.append(args)
        if args[0] == "cp":
            Path(args[-1]).mkdir()
            (Path(args[-1]) / "row").write_bytes(b"persisted")
    monkeypatch.setattr(migration, "docker", docker)
    root = tmp_path / "archive"
    with pytest.raises(ValueError, match="Incomplete"):
        migration.restore(root)
    state["State"]["Status"] = "running"
    with pytest.raises(ValueError, match="Stop"):
        migration.backup(root)
    state["State"]["Status"] = "exited"
    state["Mounts"][0]["Type"] = "tmpfs"
    with pytest.raises(ValueError, match="Unsupported"):
        migration.backup(root)
    state["Mounts"][0]["Type"] = "volume"
    migration.backup(root)
    monkeypatch.setattr(subprocess, "check_output", lambda *a, **k: names[0])
    with pytest.raises(ValueError, match="unused"):
        migration.restore(root)
    monkeypatch.setattr(subprocess, "check_output", lambda *a, **k: "")
    migration.restore(root)
    assert [call[1] for call in calls if call[0] == "start"] == names
    for name in names:
        assert (tmp_path / "fema-restored" / name / "0/row").read_bytes() == b"persisted"
    assert "127.0.0.1:9192:9092/tcp" in next(c for c in calls if c[0] == "create")
    with pytest.raises(FileExistsError):
        migration.restore(root)


def test_cli_entry_points(monkeypatch, tmp_path):
    import runpy
    import os

    root = Path(__file__).resolve().parents[3]
    monkeypatch.setattr(sys, "argv", ["demo", "--cycles", "-1"])
    with pytest.raises(SystemExit) as error:
        runpy.run_path(str(root / "data_platform/demo/demo_cdc_generator.py"), run_name="__main__")
    assert error.value.code == 2
    monkeypatch.setattr(sys, "argv", ["migration", "restore", str(tmp_path)])
    monkeypatch.setattr(os, "geteuid", lambda: 1000)
    with pytest.raises(SystemExit) as error:
        runpy.run_path(str(root / "scripts/migrate_cdc_stack.py"), run_name="__main__")
    assert error.value.code == 2
    monkeypatch.setattr(os, "geteuid", lambda: 0)
    old = os.umask(0o077)
    try:
        with pytest.raises(ValueError, match="Incomplete"):
            runpy.run_path(str(root / "scripts/migrate_cdc_stack.py"), run_name="__main__")
    finally:
        os.umask(old)

    import urllib.request
    monkeypatch.setattr(urllib.request, "urlopen", lambda *a, **k: response({}))
    with pytest.raises(RuntimeError, match="Missing connector: abassurance-sqlserver"):
        runpy.run_path(str(root / "scripts/check_video_sources.py"), run_name="__main__")
    monkeypatch.setattr(sys, "argv", ["sonar", "project", "--pull-request", "57"])
    def sonar_response(req, **kwargs):
        data = {"projectStatus": {"status": "OK", "conditions": []}} if "project_status" in req.full_url else {
            "issues": [], "paging": {"total": 0}}
        return response(data)
    monkeypatch.setattr(urllib.request, "urlopen", sonar_response)
    runpy.run_path(str(root / "scripts/sonar_video.py"), run_name="__main__")


def test_inspect_and_docker_errors_propagate(monkeypatch):
    monkeypatch.setattr(subprocess, "check_output", lambda args: b'[{"Name":"cdc"}]')
    assert migration.inspect("cdc")["Name"] == "cdc"
    def failed(args, check):
        assert check is True and args == ["docker", "start", "cdc"]
        raise subprocess.CalledProcessError(1, args)
    monkeypatch.setattr(subprocess, "run", failed)
    with pytest.raises(subprocess.CalledProcessError):
        migration.docker("start", "cdc")

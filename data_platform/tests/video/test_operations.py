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


@pytest.mark.parametrize("state,tasks", [("RUNNING", ["RUNNING"]), ("FAILED", ["RUNNING"]), ("RUNNING", [])])
def test_source_health(monkeypatch, state, tasks):
    data = {e: {"info": {"config": {"connector.class": "test." + e}},
                "status": {"connector": {"state": state}, "tasks": [{"state": t} for t in tasks]}}
            for e in ["SqlServerConnector", "OracleConnector", "Other"]}
    monkeypatch.setattr(check, "urlopen", lambda *a, **k: response(data))
    run = Mock()
    monkeypatch.setattr(check.subprocess, "run", run)
    if tasks and state == "RUNNING":
        check.check()
        assert "kafka-get-offsets.sh" in run.call_args.args[0][3]
        del data["OracleConnector"]
    with pytest.raises(RuntimeError):
        check.check()


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

"""Unit tests for Debezium REST operations without contacting Kafka Connect."""
import importlib.util
import io
import json
import urllib.error
from pathlib import Path
from unittest.mock import patch

import pytest


ROOT = Path(__file__).resolve().parents[3]
CONNECTOR_DIR = ROOT / "infrastructure" / "debezium"


def load_module(filename, name):
    spec = importlib.util.spec_from_file_location(name, CONNECTOR_DIR / filename)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def register():
    return load_module("register_connector.py", "register_connector_test")


@pytest.fixture
def validate():
    return load_module("validate_connector.py", "validate_connector_test")


@pytest.fixture
def configuration(tmp_path):
    path = tmp_path / "connector.json"
    path.write_text(json.dumps({
        "name": "assureplus-test",
        "config": {
            "connector.class": "io.debezium.connector.oracle.OracleConnector",
            "database.password": "sensitive-test-password",
        },
    }), encoding="utf-8")
    return path


def response(data):
    return io.BytesIO(json.dumps(data).encode("utf-8"))


def missing_connector():
    return urllib.error.HTTPError("http://connect/connectors/test", 404, "Not Found", {}, None)


def test_existing_connector_does_not_register_again(register, configuration, monkeypatch, capsys):
    monkeypatch.setattr(register, "CONFIG_FILE", configuration)
    with patch.object(register.urllib.request, "urlopen", return_value=response({})) as call:
        register.register_connector()
    assert call.call_count == 1
    assert "already exists" in capsys.readouterr().out


def test_missing_connector_posts_json(register, configuration, monkeypatch, capsys):
    monkeypatch.setattr(register, "CONFIG_FILE", configuration)
    captured = []

    def fake_open(request, timeout):
        captured.append((request, timeout))
        if len(captured) == 1:
            raise missing_connector()
        return response({"name": "assureplus-test"})

    with patch.object(register.urllib.request, "urlopen", side_effect=fake_open):
        register.register_connector()

    request, timeout = captured[1]
    assert request.get_method() == "POST"
    assert timeout == 60
    assert json.loads(request.data) == json.loads(configuration.read_text())
    assert "Connector registered: assureplus-test" in capsys.readouterr().out



@pytest.mark.parametrize("invalid_name", [
    "../admin",
    "..",
    "/connectors",
    "test/other",
    "test?query=1",
    "test#fragment",
    "",
    42,
    None,
])
def test_register_rejects_unsafe_connector_names_before_http(
    register, configuration, monkeypatch, invalid_name
):
    payload = json.loads(configuration.read_text())
    payload["name"] = invalid_name
    configuration.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setattr(register, "CONFIG_FILE", configuration)

    with patch.object(register.urllib.request, "urlopen") as urlopen:
        with pytest.raises(ValueError, match="Invalid Kafka Connect connector name"):
            register.register_connector()

    urlopen.assert_not_called()


def test_register_allows_safe_connector_name(register, configuration, monkeypatch):
    payload = json.loads(configuration.read_text())
    payload["name"] = "assureplus.oracle_v2-1"
    configuration.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setattr(register, "CONFIG_FILE", configuration)

    with patch.object(register.urllib.request, "urlopen", return_value=response({})) as urlopen:
        register.register_connector()

    urlopen.assert_called_once_with(
        "http://localhost:18083/connectors/assureplus.oracle_v2-1",
        timeout=10,
    )


def test_existing_connector_server_error_is_not_ignored(register, configuration, monkeypatch):
    monkeypatch.setattr(register, "CONFIG_FILE", configuration)
    error = urllib.error.HTTPError("http://connect", 500, "Server Error", {}, None)
    with patch.object(register.urllib.request, "urlopen", side_effect=error):
        with pytest.raises(urllib.error.HTTPError) as exc:
            register.register_connector()
    assert exc.value.code == 500


def test_register_post_error_exits_nonzero(register, configuration, monkeypatch, capsys):
    monkeypatch.setattr(register, "CONFIG_FILE", configuration)
    post_error = urllib.error.HTTPError("http://connect", 409, "Conflict", {}, None)
    with patch.object(register.urllib.request, "urlopen", side_effect=[missing_connector(), post_error]):
        with pytest.raises(SystemExit) as exc:
            register.register_connector()
    assert exc.value.code == 1
    assert "409" in capsys.readouterr().out


def test_validate_success_posts_config_to_plugin_endpoint(validate, configuration, monkeypatch):
    monkeypatch.setattr(validate, "CONFIG_FILE", configuration)
    captured = {}

    def fake_open(request, timeout):
        captured["request"] = request
        captured["timeout"] = timeout
        return response({"error_count": 0, "configs": []})

    with patch.object(validate.urllib.request, "urlopen", side_effect=fake_open):
        assert validate.validate_connector() is True

    request = captured["request"]
    assert request.get_method() == "PUT"
    assert request.full_url.endswith("/connector-plugins/io.debezium.connector.oracle.OracleConnector/config/validate")
    assert json.loads(request.data)["name"] == "assureplus-test"
    assert captured["timeout"] == 60


def test_validation_failure_masks_password(validate, configuration, monkeypatch, capsys):
    monkeypatch.setattr(validate, "CONFIG_FILE", configuration)
    result = {
        "error_count": 1,
        "configs": [{"value": {
            "name": "database.password",
            "errors": ["invalid: sensitive-test-password"],
        }}],
    }
    with patch.object(validate.urllib.request, "urlopen", return_value=response(result)):
        assert validate.validate_connector() is False
    output = capsys.readouterr().out
    assert "sensitive-test-password" not in output
    assert "[REDACTED]" in output


@pytest.mark.parametrize("error", [
    urllib.error.HTTPError("http://connect", 400, "Bad Request", {}, None),
    urllib.error.URLError("connection refused"),
])
def test_validation_network_errors_return_false(validate, configuration, monkeypatch, error):
    monkeypatch.setattr(validate, "CONFIG_FILE", configuration)
    with patch.object(validate.urllib.request, "urlopen", side_effect=error):
        assert validate.validate_connector() is False

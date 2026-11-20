
import json
import sys
import urllib.error
import urllib.request
from pathlib import Path

CONNECT_URL = "http://localhost:18083"
CONFIG_FILE = Path(__file__).with_name("assureplus-oracle.json")

def validate_connector() -> bool:
    data = json.loads(CONFIG_FILE.read_text())

    config = data["config"].copy()
    config["name"] = data["name"]

    connector_class = config["connector.class"]
    url = (
        f"{CONNECT_URL}/connector-plugins/"
        f"{connector_class}/config/validate"
    )
    request = urllib.request.Request(
        url,
        data=json.dumps(config).encode(),
        headers={"Content-Type": "application/json"},
        method="PUT",
    )

    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            result = json.load(response)
    except urllib.error.HTTPError as exc:
        print(f"Kafka Connect HTTP error: {exc.code}")
        return False
    except urllib.error.URLError as exc:
        print(f"Kafka Connect unavailable: {exc.reason}")
        return False

    error_count = result["error_count"]
    print(f"Configuration errors: {error_count}")

    for item in result["configs"]:
        name = item["value"]["name"]
        for error in item["value"].get("errors", []):
            password = config.get("database.password", "")
            if password:
                error = error.replace(password, "[REDACTED]")
            print(f"{name}: {error}")

    return error_count == 0


if __name__ == "__main__":
    sys.exit(0 if validate_connector() else 1)

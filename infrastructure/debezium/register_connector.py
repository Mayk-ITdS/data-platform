
import json
import re
import urllib.error
import urllib.request
from pathlib import Path

CONNECT_URL = "http://localhost:18083"
CONFIG_FILE = Path(__file__).with_name("assureplus-oracle.json")


def register_connector():
    data = json.loads(CONFIG_FILE.read_text())
    name = data["name"]
    if not isinstance(name, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", name):
        raise ValueError("Invalid Kafka Connect connector name")

    try:
        with urllib.request.urlopen(
            f"{CONNECT_URL}/connectors/{name}",
            timeout=10,
        ):
            print(f"Connector {name} already exists.")
            return
    except urllib.error.HTTPError as exc:
        if exc.code != 404:
            raise

    request = urllib.request.Request(
        f"{CONNECT_URL}/connectors",
        data=json.dumps(data).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            result = json.load(response)
            print("Connector registered:", result["name"])
    except urllib.error.HTTPError as exc:
        print("Kafka Connect HTTP error:", exc.code)
        print("Check connect-fema logs for details.")
        raise SystemExit(1)


if __name__ == "__main__":
    register_connector()

"""Print connector names, task states and Kafka end offsets before filming."""
import json
import os
import subprocess
from urllib.request import urlopen


def check():
    url = os.getenv("FEMA_CONNECT_URL", "http://127.0.0.1:18083").rstrip("/")
    with urlopen(url + "/connectors?expand=status&expand=info", timeout=15) as response:
        connectors = json.load(response)
    engines = set()
    expected = {
        "abassurance-sqlserver": ("SqlServerConnector", "abassurance"),
        "assureplus-oracle-v2": ("OracleConnector", "assureplus_v2"),
    }
    for name, (expected_kind, expected_prefix) in expected.items():
        if name not in connectors:
            raise RuntimeError("Missing connector: " + name)
        detail = connectors[name]
        config = detail["info"]["config"]
        if config.get("topic.prefix") != expected_prefix:
            raise RuntimeError("Unexpected topic prefix: " + name)
        if config["connector.class"].split(".")[-1] != expected_kind:
            raise RuntimeError("Unexpected connector class: " + name)
        kind = detail["info"]["config"]["connector.class"].split(".")[-1]
        if kind not in {"OracleConnector", "SqlServerConnector"}:
            continue
        status = detail["status"]
        tasks = [t["state"] for t in status["tasks"]]
        print(name, kind, status["connector"]["state"], "tasks", tasks, flush=True)
        if status["connector"]["state"] != "RUNNING" or not tasks or set(tasks) != {"RUNNING"}:
            raise RuntimeError("Connector is not ready: " + name)
        engines.add(kind)
    if engines != {"OracleConnector", "SqlServerConnector"}:
        raise RuntimeError("Expected both Oracle and SQL Server source connectors")
    subprocess.run(["docker", "exec", "kafka-fema", "/opt/kafka/bin/kafka-get-offsets.sh",
                    "--bootstrap-server", "kafka-fema:9092", "--topic",
                    "(abassurance|assureplus_v2)\\..*", "--time", "latest"], check=True)


if __name__ == "__main__":
    check()

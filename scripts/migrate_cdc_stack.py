"""Cold-copy four stopped Docker containers and their mounts to a new host."""
import argparse
import json
import os
from pathlib import Path
import subprocess

NAMES = ["oracle-fema-v2", "sqlserver-fema", "kafka-fema", "connect-fema"]


def docker(*args):
    subprocess.run(["docker", *map(str, args)], check=True)


def inspect(name):
    return json.loads(subprocess.check_output(["docker", "inspect", name]))[0]


def backup(root):
    states = [inspect(name) for name in NAMES]
    for state in states:
        if state["State"]["Status"] != "exited" or state["State"]["OOMKilled"] or state["State"]["ExitCode"] == 137:
            raise ValueError("Stop all source containers cleanly before backup: " + state["Name"])
        if any(m["Type"] not in ("volume", "bind") for m in state["Mounts"]):
            raise ValueError("Unsupported mount type: " + state["Name"])
    root.mkdir(mode=0o700, parents=True, exist_ok=False)
    for name, state in zip(NAMES, states):
        folder = root / name
        folder.mkdir()
        image = "fema-migration/" + name + ":cold"
        docker("commit", name, image)
        docker("save", "-o", folder / "image.tar", image)
        for i, mount in enumerate(state["Mounts"]):
            docker("cp", "-a", name + ":" + mount["Destination"], folder / f"mount-{i}")
        (folder / "inspect.json").write_text(json.dumps(state))
    (root / "complete").touch()


def restore(root):
    if not (root / "complete").is_file():
        raise ValueError("Incomplete backup")
    existing = subprocess.check_output(["docker", "ps", "-a", "--format", "{{.Names}}"], text=True).splitlines()
    if set(existing).intersection(NAMES):
        raise ValueError("Destination container names must be unused")
    network = "fema-cdc"
    if network not in subprocess.check_output(["docker","network","ls","--format","{{.Name}}"], text=True).splitlines():
        docker("network", "create", network)
    for name in NAMES:
        folder = root / name
        state = json.loads((folder / "inspect.json").read_text())
        cfg = state["HostConfig"]
        docker("load", "-i", folder / "image.tar")
        args = ["create", "--network", network, "--name", name, "--hostname", state["Config"]["Hostname"]]
        for net in state["NetworkSettings"]["Networks"].values():
            for alias in net.get("Aliases") or []:
                args.extend(["--network-alias", alias])
        for port, bindings in (cfg.get("PortBindings") or {}).items():
            for binding in bindings or []:
                args.extend(["-p", "127.0.0.1:" + binding["HostPort"] + ":" + port])
        for i, m in enumerate(state["Mounts"]):
            # Use a separate restored copy; retain the archive for recovery.
            dest = root.parent / "fema-restored" / name / str(i)
            dest.parent.mkdir(parents=True, exist_ok=True)
            if dest.exists():
                raise FileExistsError(dest)
            subprocess.run(["cp","-a","--reflink=auto",folder / f"mount-{i}",dest], check=True)
            args.extend(["-v", str(dest) + ":" + m["Destination"] + (":ro" if not m["RW"] else ":rw")])
        docker(*args, "fema-migration/" + name + ":cold")
    for name in NAMES:
        docker("start", name)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["backup", "restore"])
    parser.add_argument("directory", type=Path)
    args = parser.parse_args()
    if os.geteuid() != 0:
        parser.error("Root is required to preserve database file owners")
    os.umask(0o077)
    (backup if args.action == "backup" else restore)(args.directory.resolve())

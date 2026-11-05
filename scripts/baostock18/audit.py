"""Read-only host process and runner-state audit. No SQL, locks or signals."""

# ruff: noqa: T201
import os
import sys


def readonly(event, args):
    """Reject writes, signals, network access and subprocess creation in this probe."""
    if event == "open":
        path, mode, flags = args
        if (mode and any(c in mode for c in "wax+")) or flags & (
            os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC | os.O_APPEND
        ):
            raise RuntimeError("phase0 forbids filesystem writes")
    if event in ("os.kill", "os.killpg", "socket.connect", "subprocess.Popen"):
        raise RuntimeError("phase0 read-only audit")


sys.addaudithook(readonly)
import datetime
import json
import pathlib
import time

t = time.monotonic()
db = pathlib.Path(sys.argv[1])
r = {
    "observed_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    "states": {},
    "holders": [],
    "errors": [],
    "process_count": 0,
    "daily_processes": [],
    "sql_queries": 0,
}
for p in pathlib.Path(sys.argv[2]).glob("*.json"):
    try:
        r["states"][p.stem] = json.loads(p.read_text())
    except (OSError, ValueError) as exc:
        r["errors"].append(str(exc))
identities = {}
for p in [db, db.with_suffix(".market.lock"), db.with_suffix(".publisher.lock"), db.with_name(db.name + ".wal")]:
    try:
        s = p.stat()
        identities[(s.st_dev, s.st_ino)] = str(p)
    except FileNotFoundError:
        if p.suffix != ".wal":
            r["errors"].append("missing: " + str(p))
for p in pathlib.Path("/proc").glob("[0-9]*"):
    try:
        r["process_count"] += 1
        status = dict(line.split(":", 1) for line in (p / "status").read_text().splitlines())
        cgroup = (p / "cgroup").read_text()
        cmd = (p / "cmdline").read_bytes().replace(b"\0", b" ").decode(errors="replace")
        item = {
            "pid": int(p.name),
            "nspid": status.get("NSpid", "").strip(),
            "comm": (p / "comm").read_text().strip(),
            "cgroup": cgroup,
            "fds": [],
        }
        for f in (p / "fd").iterdir():
            try:
                s = f.stat()
                target = os.readlink(f)
                if (s.st_dev, s.st_ino) in identities or any(
                    x in target
                    for x in [
                        "dq_quant_invest_data.duckdb",
                        "dq_quant_invest_data.market.lock",
                        "dq_quant_invest_data.publisher.lock",
                    ]
                ):
                    item["fds"].append(
                        {
                            "fd": f.name,
                            "target": target,
                            "device": s.st_dev,
                            "inode": s.st_ino,
                            "fdinfo": (p / "fdinfo" / f.name).read_text(),
                        }
                    )
            except (FileNotFoundError, ProcessLookupError):
                pass
            except PermissionError as exc:
                r["errors"].append(str(exc))
        if item["fds"]:
            item["status"] = {k: status[k].strip() for k in ("VmHWM", "VmRSS", "State") if k in status}
            item["io"] = (p / "io").read_text()
            r["holders"].append(item)
        if ("daily-stocks" in cmd or "daily_stock" in cmd) and int(p.name) != os.getpid():
            r["daily_processes"].append({**item, "cmdline": cmd, "state": status["State"].strip()})
    except (FileNotFoundError, ProcessLookupError):
        pass
    except PermissionError as exc:
        r["errors"].append(str(exc))
lock_ids = {str(ino) for dev, ino in identities}
r["locks"] = [
    line
    for line in pathlib.Path("/proc/locks").read_text().splitlines()
    if any((":" + ino + " ") in line for ino in lock_ids)
]
daily = r["states"].get("daily-stocks", {})
r["heartbeat_age_seconds"] = time.time() - daily.get("heartbeat", 0)
r["ready"] = bool(
    daily
    and daily.get("phase") != "running"
    and "pid" in daily
    and daily["pid"] is None
    and 0 <= r["heartbeat_age_seconds"] < 120
    and not r["holders"]
    and not r["locks"]
    and not r["errors"]
)
r["elapsed_seconds"] = time.monotonic() - t
print(json.dumps(r), flush=True)

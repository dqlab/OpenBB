"""Parameterized NAS window orchestration around the already rehearsed driver.

The admission/migration/backup/import/visibility implementation is exclusively
``openbb_duckdb.baostock18.driver`` from the exact rehearsed wheel. This wrapper
only inventories, drains, transports the packet, and restores recorded services.
"""

# ruff: noqa: S108, S603, S608, T201
import argparse
import base64
import json
import shlex
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path


def now():
    """Return an observed wall time for receipts."""
    return datetime.now(timezone.utc).isoformat()


class Window:
    """Record every external action and always restore the initial running set."""

    def __init__(self, config):
        self.c = config
        self.root = Path(config["output"])
        self.root.mkdir(mode=0o700)
        self.seq = 0
        self.before = None
        self.stopped = False
        self.created = False
        self.driver_launched = False
        self.driver_finished = False

    def save(self, name, value):
        """Retain exclusive evidence, including failures."""
        with (self.root / name).open("x") as stream:
            json.dump(value, stream, indent=2, ensure_ascii=False)

    def run(self, args, *, stdin=None, check=True):
        """Use argument-list SSH quoting and save each result."""
        started = time.monotonic()
        p = subprocess.run(self.c["ssh"] + [shlex.join(args)], input=stdin, capture_output=True, text=True, check=False)
        self.seq += 1
        result = {
            "at": now(),
            "args": args,
            "seconds": time.monotonic() - started,
            "returncode": p.returncode,
            "stdout": p.stdout,
            "stderr": p.stderr,
        }
        self.save(f"command-{self.seq:05d}.json", result)
        if check and p.returncode:
            raise RuntimeError(f"Command {self.seq} failed: {p.stderr[-2500:]}")
        return result

    def docker(self, *args, **kwargs):
        """Invoke the configured Docker binary."""
        return self.run([self.c["docker"], *args], **kwargs)

    def states(self, names=None):
        """Read all preexisting containers, including exited containers."""
        if names is None:
            names = self.docker("ps", "-aq")["stdout"].split()
        values = [
            json.loads(line) for line in self.docker("inspect", "--format", "{{json .}}", *names)["stdout"].splitlines()
        ]
        return [{k: x[k] for k in ("Id", "Name", "State", "Image", "Mounts")} for x in values]

    def probe(self, code, *, container=None, python=None, args=(), check=True):
        """Run an explicit remote Python probe without shell expansion."""
        return self.docker(
            "exec",
            "-i",
            container or self.c["maintenance"],
            python or self.c["python"],
            "-B",
            "-",
            *args,
            stdin=code,
            check=check,
        )

    def audit(self):
        """Audit host process handles and scheduler state with a read-only script."""
        result = self.probe(
            Path(self.c["audit_script"]).read_text(),
            container=self.c["audit_container"],
            args=[self.c["driver"]["target"], self.c["runner_state"]],
        )
        return json.loads(result["stdout"])

    def wait(self):
        """Observe natural native completion; never send a job signal."""
        started = time.monotonic()
        while True:
            audit = self.audit()
            daily = audit["states"].get("daily-stocks", {})
            print(
                json.dumps(
                    {
                        "at": now(),
                        "stage": "wait-native",
                        "daily": daily,
                        "ready": audit["ready"],
                        "holders": len(audit["holders"]),
                        "errors": audit["errors"],
                    }
                ),
                flush=True,
            )
            idle = all(
                audit["states"].get(s, {}).get("phase") != "running"
                and audit["states"].get(s, {}).get("pid", "unknown") is None
                for s in self.c["native_keys"]
            )
            if audit["ready"] and idle:
                self.save("natural-completion.json", {"seconds": time.monotonic() - started, **audit})
                return
            if time.monotonic() - started >= self.c["max_wait_seconds"]:
                raise RuntimeError("Natural completion wait bound reached; no window opened")
            time.sleep(self.c["poll_seconds"])

    def fetch(self, path):
        """Fetch a small remote JSON receipt without altering it."""
        code = "from pathlib import Path\nprint(Path(" + repr(path) + ").read_text())\n"
        return json.loads(self.probe(code)["stdout"])

    def install(self):
        """Transport only reviewed immutable bytes into a new maintenance directory."""
        files = {
            "approved.whl": Path(self.c["wheel"]).read_bytes(),
            "baseline.json": Path(self.c["baseline"]).read_bytes(),
        }
        payload = {k: base64.b64encode(v).decode() for k, v in files.items()}
        code = (
            """import base64,hashlib,pathlib,subprocess,sys,json
w=pathlib.Path("""
            + repr(self.c["mount"])
            + """)
files="""
            + repr(payload)
            + """
for name,body in files.items():
 with (w/name).open('xb') as stream:stream.write(base64.b64decode(body))
assert hashlib.sha256((w/'approved.whl').read_bytes()).hexdigest()=="""
            + repr(self.c["wheel_sha256"])
            + """
assert hashlib.sha256((w/'baseline.json').read_bytes()).hexdigest()=="""
            + repr(self.c["baseline_sha256"])
            + """
p=w/'openbb_duckdb-1.2.1-py3-none-any.whl'
(w/'approved.whl').rename(p)
subprocess.run([sys.executable,'-m','pip','install','--no-index','--no-deps','--target',str(w/'provider'),str(p)],check=True)
"""
        )
        self.probe(code)

    def wait_driver(self):
        """Wait for an exit certificate before recovery, including after SSH loss."""
        path = self.c["mount"] + "/driver-exit.json"
        while True:
            code = (
                "from pathlib import Path\np=Path(" + repr(path) + ")\nprint(p.read_text() if p.exists() else 'null')\n"
            )
            r = self.probe(code, check=False)
            if r["returncode"] == 0:
                value = json.loads(r["stdout"])
                if value is not None:
                    self.driver_finished = True
                    self.save("driver-exit.json", value)
                    return value
            print(json.dumps({"at": now(), "stage": "wait-driver-exit", "probe_returncode": r["returncode"]}), flush=True)
            time.sleep(self.c["poll_seconds"])

    def launch_driver(self):
        """Run a durable local supervisor; no remote SSH lifetime controls the writer."""
        code = (
            """import pathlib,subprocess,sys,json,os,time
w=pathlib.Path("""
            + repr(self.c["mount"])
            + """)
started=time.monotonic()
with (w/'driver.stdout').open('x') as out,(w/'driver.stderr').open('x') as err:
 p=subprocess.run([sys.executable,'-B','-m','openbb_duckdb.baostock18.driver','--config',str(w/'driver-config.json')],stdout=out,stderr=err,check=False)
with (w/'driver-exit.json').open('x') as stream:
 json.dump({'returncode':p.returncode,'seconds':time.monotonic()-started},stream);stream.flush();os.fsync(stream.fileno())
"""
        )
        self.docker(
            "exec",
            "-d",
            "-e",
            "PYTHONPATH=" + self.c["mount"] + "/provider",
            self.c["maintenance"],
            self.c["python"],
            "-B",
            "-c",
            code,
        )
        self.driver_launched = True
        return self.wait_driver()

    def restore(self):
        """Restore only initially running targets, data-api then api then native."""
        failures, started = [], time.monotonic()
        for short in self.c["recovery_order"]:
            name = self.c["services"][short]
            old = next(x for x in self.before if x["Name"] == "/" + name)
            if not old["State"]["Running"]:
                continue
            current = self.states([name])[0]
            if not current["State"]["Running"]:
                r = self.docker("start", name, check=False)
                if r["returncode"]:
                    failures.append("start " + name)
                    continue
            if short in self.c["health_urls"]:
                healthy = False
                for _ in range(self.c["health_attempts"]):
                    r = self.run(
                        [
                            "curl",
                            "--noproxy",
                            "*",
                            "--insecure",
                            "--fail",
                            "--silent",
                            "--show-error",
                            "--max-time",
                            str(self.c["health_timeout_seconds"]),
                            self.c["health_urls"][short],
                        ],
                        check=False,
                    )
                    if r["returncode"] == 0 and json.loads(r["stdout"]).get("status") == "ok":
                        healthy = True
                        break
                    time.sleep(self.c["health_poll_seconds"])
                if not healthy:
                    failures.append("health " + name)
                if short == "data-api" and healthy:
                    query = (
                        """import duckdb,json,threading,time
started=time.monotonic()
config={'enable_external_access':False,'threads':2,'memory_limit':'512MB'}
with duckdb.connect("""
                        + repr(self.c["driver"]["target"])
                        + """,read_only=True,config=config) as c:
 timer=threading.Timer("""
                        + repr(self.c["driver"]["max_verify_seconds"])
                        + """,c.interrupt);timer.daemon=True;timer.start()
 try:rows=c.execute('SELECT snapshot_id,symbol,date,close FROM a_share.bar_revision LIMIT 3').fetchall()
 finally:timer.cancel()
assert len(rows)==3
print(json.dumps({'read_only':True,'rows':rows,'seconds':time.monotonic()-started},default=str))
"""
                    )
                    r = self.probe(query, container=name, python=self.c["data_api_python"], check=False)
                    if r["returncode"]:
                        failures.append("bounded query " + name)
            for _ in range(self.c["health_attempts"]):
                current = self.states([name])[0]
                if current["State"].get("Health", {}).get("Status") == "healthy":
                    break
                time.sleep(self.c["health_poll_seconds"])
            if (
                old["State"].get("Health", {}).get("Status") == "healthy"
                and current["State"].get("Health", {}).get("Status") != "healthy"
            ):
                failures.append("container health " + name)
        after = self.states([x["Id"] for x in self.before])
        targets = set(self.c["services"].values())
        comparison = [
            {
                "name": b["Name"],
                "same_id": a["Id"] == b["Id"],
                "running_matches": a["State"]["Running"] == b["State"]["Running"],
                "non_target_unchanged": b["Name"].lstrip("/") in targets
                or all(a["State"][k] == b["State"][k] for k in ("Status", "Pid", "StartedAt")),
            }
            for a, b in zip(after, self.before)
        ]
        self.save("services-after.json", after)
        result = {
            "seconds": time.monotonic() - started,
            "failures": failures,
            "comparison": comparison,
            "status": "passed"
            if not failures
            and all(x["same_id"] and x["running_matches"] and x["non_target_unchanged"] for x in comparison)
            else "failed",
        }
        self.save("recovery.json", result)
        self.save("native-after-recovery.json", self.audit())
        return result

    def catalog_readback(self):
        """Record current visibility and legacy counts before restarting writers."""
        code = (
            """import duckdb,json,pathlib,threading
p=pathlib.Path("""
            + repr(self.c["driver"]["target"])
            + """)
result={'read_only':True,'wal_exists':p.with_name(p.name+'.wal').exists()}
with duckdb.connect(str(p),read_only=True,config={'enable_external_access':False,'threads':2,'memory_limit':'2GB'}) as c:
 timer=threading.Timer("""
            + repr(self.c["driver"]["max_verify_seconds"])
            + """,c.interrupt);timer.daemon=True;timer.start()
 try:
  tables=c.execute("SELECT table_name FROM duckdb_tables() "
                   "WHERE schema_name='baostock18_admission' AND NOT internal").fetchall()
  result['admission_physical_tables']=len(tables)
  names=('basis_admission','visibility_event','dataset_pointer')
  result['visibility_counts']={t:c.execute('SELECT count(*) FROM baostock18_admission.'+t).fetchone()[0]
                               for (t,) in tables if t in names}
  result['visible_rows']=c.execute('SELECT count(*) FROM baostock18_admission.consumable').fetchone()[0] if tables else 0
  result['a_share_counts']={t:c.execute('SELECT count(*) FROM a_share.'+t).fetchone()[0] for t in """
            + repr(
                tuple(
                    (
                        "attempt",
                        "bar_revision",
                        "bar_snapshot",
                        "calendar_day",
                        "factor_revision",
                        "instrument_version",
                        "job",
                        "reference_snapshot",
                    )
                )
            )
            + """}
 finally:timer.cancel()
print(json.dumps(result))
"""
        )
        result = self.probe(code, check=False)
        self.save("catalog-final-readback.json", result)

    def execute(self):
        """Open a window only after an exact-wheel successful local chain receipt."""
        receipt = json.loads(Path(self.c["rehearsal_receipt"]).read_text())
        if receipt["status"] != "passed":
            raise RuntimeError("Local E2E receipt is not green")
        import hashlib

        if hashlib.sha256(Path(self.c["wheel"]).read_bytes()).hexdigest() != self.c["wheel_sha256"]:
            raise RuntimeError("Rehearsed wheel changed")
        result = {"started_at": now(), "status": "aborted"}
        try:
            self.before = self.states()
            self.save("services-initial.json", self.before)
            self.wait()
            # Refresh immediately before any stop; preserve all original running/exited states.
            fresh = self.states([x["Id"] for x in self.before])
            if any(a["State"]["Running"] != b["State"]["Running"] for a, b in zip(fresh, self.before)):
                raise RuntimeError("Preexisting container running state changed while waiting")
            audit = self.audit()
            if not audit["ready"]:
                raise RuntimeError("Native completion gate changed before window")
            self.run(["mkdir", "-m", "700", self.c["host_window"]])
            self.docker(
                "run",
                "-d",
                "--pull",
                "never",
                "--name",
                self.c["maintenance"],
                "--network",
                "none",
                "--read-only",
                "--user",
                self.c["user"],
                "--cpus",
                str(self.c["cpus"]),
                "--memory",
                self.c["memory"],
                "--memory-swap",
                self.c["memory"],
                "--pids-limit",
                str(self.c["pids_limit"]),
                "--tmpfs",
                "/tmp:rw,size=" + self.c["tmpfs_size"],
                "--mount",
                "type=bind,src=" + self.c["host_home"] + ",dst=" + self.c["container_home"] + ",readonly",
                "--mount",
                "type=bind,src=" + self.c["host_catalog"] + ",dst=" + str(Path(self.c["driver"]["target"]).parent),
                "--mount",
                "type=bind,src=" + self.c["host_window"] + ",dst=" + self.c["mount"],
                "--entrypoint",
                self.c["python"],
                self.c["image"],
                "-B",
                "-c",
                "import signal;signal.pause()",
            )
            self.created = True
            self.install()
            config = dict(self.c["driver"], window_started_at=now())
            self.save("driver-config.json", config)
            self.probe(
                "import json,pathlib\np=pathlib.Path("
                + repr(self.c["mount"] + "/driver-config.json")
                + ')\nwith p.open("x") as stream:json.dump('
                + repr(config)
                + ",stream,indent=2)\n"
            )
            self.docker(
                "exec",
                "-e",
                "PYTHONPATH=" + self.c["mount"] + "/provider",
                self.c["maintenance"],
                self.c["python"],
                "-B",
                "-m",
                "openbb_duckdb.baostock18.driver",
                "--config",
                self.c["mount"] + "/driver-config.json",
                "--check-input-references",
                "--output",
                self.c["mount"] + "/candidate-reference-precheck.json",
            )
            for short in self.c["stop_order"]:
                name = self.c["services"][short]
                old = next(x for x in self.before if x["Name"] == "/" + name)
                if old["State"]["Running"]:
                    self.stopped = True
                    self.docker("stop", "--time", "-1", name)
            if any(x["State"]["Running"] for x in self.states(list(self.c["services"].values()))):
                raise RuntimeError("Target services did not stop")
            audit = self.audit()
            if audit["holders"] or audit["locks"] or audit["errors"]:
                raise RuntimeError("Catalog handles/locks remain after drain")
            # The exact same installed module that passed the local copy drives all catalog operations.
            r = self.launch_driver()
            driver = self.fetch(config["output"] + "/receipt.json")
            self.save("driver-receipt.json", driver)
            if r["returncode"] or driver["status"] != "passed":
                raise RuntimeError("Shared production driver aborted: " + str(driver.get("error")))
            result["status"] = "passed"
        except BaseException as exc:
            result["error"] = repr(exc)
        finally:
            if self.stopped:
                try:
                    if self.driver_launched and not self.driver_finished:
                        self.wait_driver()
                    self.catalog_readback()
                    recovery = self.restore()
                    result["recovery"] = recovery["status"]
                    if recovery["status"] != "passed":
                        result["status"] = "aborted"
                except BaseException as exc:
                    result["recovery_error"] = repr(exc)
                    result["status"] = "aborted"
            else:
                result["recovery"] = "not_needed_no_services_stopped"
            result["completed_at"] = now()
            self.save("receipt.json", result)
        print(json.dumps(result), flush=True)
        return result

    def precheck_only(self):
        """Use a running maintenance container for input checks without a window.

        Only a new artifact directory is created in its existing recovery mount.
        No target catalog connection, container start/stop or CHECKPOINT occurs.
        """
        import hashlib

        receipt = json.loads(Path(self.c["rehearsal_receipt"]).read_text())
        if receipt["status"] != "passed":
            raise RuntimeError("Local E2E receipt is not green")
        if hashlib.sha256(Path(self.c["wheel"]).read_bytes()).hexdigest() != self.c["wheel_sha256"]:
            raise RuntimeError("Rehearsed wheel changed")
        before = self.states()
        self.save("services-before.json", before)
        existing = next(x for x in before if x["Name"] == "/" + self.c["maintenance"])
        if not existing["State"]["Running"]:
            raise RuntimeError("Precheck requires an already-running maintenance container")
        self.probe("from pathlib import Path\nPath(" + repr(self.c["mount"]) + ").mkdir(mode=0o700)\n")
        self.install()
        config = dict(self.c["driver"], window_started_at=now())
        self.probe(
            "import pathlib,json\nwith pathlib.Path("
            + repr(self.c["mount"] + "/driver-config.json")
            + ').open("x") as stream:json.dump('
            + repr(config)
            + ",stream,indent=2)\n"
        )
        r = self.docker(
            "exec",
            "-e",
            "PYTHONPATH=" + self.c["mount"] + "/provider",
            self.c["maintenance"],
            self.c["python"],
            "-B",
            "-m",
            "openbb_duckdb.baostock18.driver",
            "--config",
            self.c["mount"] + "/driver-config.json",
            "--check-input-references",
            "--output",
            self.c["mount"] + "/candidate-reference-precheck.json",
            check=False,
        )
        precheck = self.fetch(self.c["mount"] + "/candidate-reference-precheck.json")
        self.save("candidate-reference-precheck.json", precheck)
        after = self.states([x["Id"] for x in before])
        self.save("services-after.json", after)
        unchanged = all(
            a["Id"] == b["Id"] and all(a["State"][k] == b["State"][k] for k in ("Running", "Pid", "StartedAt", "Status"))
            for a, b in zip(after, before)
        )
        result = {
            "status": "passed" if r["returncode"] == 0 and unchanged else "aborted",
            "reason": "candidate reference gate",
            "precheck": precheck,
            "all_preexisting_service_states_unchanged": unchanged,
            "target_service_stops": 0,
            "target_service_starts": 0,
            "checkpoint_executions": 0,
            "target_catalog_connections": 0,
            "recovery": "not_needed_no_services_stopped",
            "completed_at": now(),
        }
        self.save("receipt.json", result)
        print(json.dumps(result), flush=True)
        return result


def main():
    """Load a deployment packet containing no implicit production paths."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
    parser.add_argument("--precheck-only", action="store_true")
    args = parser.parse_args()
    window = Window(json.loads(Path(args.config).read_text()))
    result = window.precheck_only() if args.precheck_only else window.execute()
    if result["status"] != "passed":
        sys.exit(1)


if __name__ == "__main__":
    main()

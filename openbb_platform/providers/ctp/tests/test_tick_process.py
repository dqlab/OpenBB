"""Run the actual native CLI with an injected, socket-free MdApi."""

# ruff: noqa: D103, S106, S603
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

from dq_live_market_data_collector.tick_file import inspect_file


def test_native_process_signals(tmp_path):
    instruments = tmp_path / "instruments.json"
    instruments.write_text('["test"]')
    env = dict(os.environ, CTP_USER="synthetic", CTP_PASSWORD="synthetic")
    env["PYTHONPATH"] = str(Path(__file__).parent) + os.pathsep + env.get("PYTHONPATH", "")
    for sig in (signal.SIGTERM, signal.SIGKILL):
        root = tmp_path / str(sig)
        proc = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "dq_live_market_data_collector.tick_cli",
                "native",
                "--root",
                str(root),
                "--front",
                "synthetic",
                "--broker",
                "synthetic",
                "--environment",
                "synthetic",
                "--instruments",
                str(instruments),
                "--duration",
                "30",
                "--interval",
                "0.01",
                "--md-module",
                "fake_md",
            ],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        try:
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                status = list(root.rglob("status.json"))
                if status and json.loads(status[0].read_text())["ingress"]["written_seq"]:
                    break
                assert proc.poll() is None
                time.sleep(0.02)
            else:
                raise AssertionError("no written snapshot")
            proc.send_signal(sig)
            out, err = proc.communicate(timeout=10)
            if sig == signal.SIGTERM:
                assert proc.returncode == 0, (out, err)
                assert list(root.rglob("*.jsonl"))
                assert not list(root.rglob("*.jsonl.open"))
            else:
                assert proc.returncode == -signal.SIGKILL
                paths = list(root.rglob("*.jsonl.open"))
                assert len(paths) == 1
                before = paths[0].read_bytes()
                report = inspect_file(paths[0])
                assert report["unread_bytes"] >= 0
                assert paths[0].read_bytes() == before
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.wait()


def test_reconnect_and_hanging_release_subprocess(tmp_path):
    instruments = tmp_path / "instruments.json"
    instruments.write_text('["test"]')
    env = dict(os.environ, CTP_USER="synthetic", CTP_PASSWORD="synthetic")
    env["PYTHONPATH"] = str(Path(__file__).parent) + os.pathsep + env.get("PYTHONPATH", "")
    for mode in ("FAKE_RECONNECT", "FAKE_HANG_RELEASE", "FAKE_HANG_INIT", "FAKE_GIL_RELEASE"):
        root = tmp_path / mode
        env[mode] = "1"
        start = time.monotonic()
        proc = subprocess.run([
            sys.executable, "-m", "dq_live_market_data_collector.tick_cli", "native",
            "--root", str(root), "--front", "synthetic", "--broker", "synthetic",
            "--environment", "synthetic", "--instruments", str(instruments),
            "--duration", "0.7", "--interval", "0.01", "--shutdown-timeout",
            "3" if mode == "FAKE_RECONNECT" else "0.2",
            "--startup-timeout", "1",
            "--md-module", "fake_md",
        ], env=env, capture_output=True, timeout=8, check=False)
        del env[mode]
        assert time.monotonic() - start < 8
        if mode in ("FAKE_HANG_INIT", "FAKE_GIL_RELEASE"):
            assert proc.returncode != 0
            assert b"exceeded deadline" in proc.stderr
        elif mode == "FAKE_HANG_RELEASE":
            assert proc.returncode != 0
            assert b"source_close_failed" in next(root.rglob("events.jsonl")).read_bytes()
        else:
            assert proc.returncode == 0, proc.stderr
            events = [json.loads(x) for x in next(root.rglob("events.jsonl")).read_text().splitlines()]
            assert any(x["event"] == "disconnected" for x in events)
            assert len([x for x in events if x["event"] == "ack" and x["tracked"]]) == 2
            records = [json.loads(x) for p in root.rglob("part-*.jsonl") for x in p.read_text().splitlines()]
            assert [r["local_seq"] for r in records] == [1, 2, 3]
            assert [r["record_type"] for r in records].count("rejected_callback") == 1
            verify = subprocess.run([sys.executable, "-m", "dq_live_market_data_collector.tick_cli",
                                     "verify", str(root)], env=env, capture_output=True, check=False)
            assert verify.returncode == 0, verify.stdout
            assert sum(r["records"] for r in json.loads(verify.stdout)) == 3
            status = json.loads(next(root.rglob("status.json")).read_text())["ingress"]
            assert status["received"] == 3 and status["rejected"] == 1
            assert status["rejection_reasons"] == {"retired_generation": 1}
            assert status["coverage_complete"] is False


def test_startup_allowance_and_hard_budget(tmp_path):
    instruments = tmp_path / "instruments.json"
    instruments.write_text('["test"]')
    env = dict(os.environ, CTP_USER="synthetic", CTP_PASSWORD="synthetic", FAKE_IMPORT_DELAY="1.3")
    env["PYTHONPATH"] = str(Path(__file__).parent) + os.pathsep + env.get("PYTHONPATH", "")
    for allowance, success in ((3, True), (0.1, False)):
        root = tmp_path / str(allowance)
        start = time.monotonic()
        result = subprocess.run([
            sys.executable, "-m", "dq_live_market_data_collector.tick_cli", "native",
            "--root", str(root), "--front", "synthetic", "--broker", "synthetic",
            "--environment", "synthetic", "--instruments", str(instruments),
            "--duration", "0.3", "--shutdown-timeout", "0.5", "--startup-timeout",
            str(allowance), "--interval", "0.01", "--md-module", "fake_md",
        ], env=env, capture_output=True, timeout=8, check=False)
        elapsed = time.monotonic() - start
        assert elapsed < allowance + 0.3 + 0.5 + 1.5
        if success:
            assert result.returncode == 0, result.stderr
            events = [json.loads(line)["event"] for line in next(root.rglob("events.jsonl")).read_text().splitlines()]
            assert events.index("session_start") < events.index("session_stopping") < events.index("session_end")
        else:
            assert result.returncode != 0
            assert b"exceeded deadline" in result.stderr


def test_nonfinite_hard_budget_is_rejected(tmp_path):
    instruments = tmp_path / "instruments.json"
    instruments.write_text('["test"]')
    for duration, startup in (("1", "nan"), ("1e308", "1e308")):
        result = subprocess.run([
            sys.executable, "-m", "dq_live_market_data_collector.tick_cli", "native",
            "--root", str(tmp_path / "out"), "--front", "synthetic", "--broker", "synthetic",
            "--environment", "synthetic", "--instruments", str(instruments),
            "--duration", duration, "--startup-timeout", startup,
        ], capture_output=True, timeout=5, check=False)
        assert result.returncode != 0
        assert b"finite and positive" in result.stderr


def test_subprocess_overflow_and_disk_failure(tmp_path):
    instruments = tmp_path / "instruments.json"
    instruments.write_text('["test"]')
    env = dict(os.environ, CTP_USER="synthetic", CTP_PASSWORD="synthetic")
    env["PYTHONPATH"] = str(Path(__file__).parent) + os.pathsep + env.get("PYTHONPATH", "")
    for mode in ("FAKE_OVERFLOW", "FAKE_DISK_FAIL"):
        root = tmp_path / mode
        env[mode] = "1"
        proc = subprocess.run([
            sys.executable, "-m", "dq_live_market_data_collector.tick_cli", "native",
            "--root", str(root), "--front", "synthetic", "--broker", "synthetic",
            "--environment", "synthetic", "--instruments", str(instruments),
            "--duration", "3", "--interval", "0.01", "--capacity", "1",
            "--md-module", "fake_md",
        ], env=env, capture_output=True, timeout=10, check=False)
        del env[mode]
        assert proc.returncode != 0
        status = json.loads(next(root.rglob("status.json")).read_text())["ingress"]
        if mode == "FAKE_OVERFLOW":
            assert status["rejected"] > 0 and "ingress failure" in status["error"]
        else:
            assert "synthetic disk full" in status["error"]
            assert b"synthetic disk full" in proc.stderr


def test_two_native_instances_one_fails_other_survives(tmp_path):
    instruments = tmp_path / "instruments.json"
    instruments.write_text('["test"]')
    env = dict(os.environ, CTP_USER="synthetic", CTP_PASSWORD="synthetic")
    env["PYTHONPATH"] = str(Path(__file__).parent) + os.pathsep + env.get("PYTHONPATH", "")
    cmd = [
        sys.executable, "-m", "dq_live_market_data_collector.tick_cli", "native",
        "--root", str(tmp_path / "shared"), "--front", "synthetic", "--broker", "synthetic",
        "--environment", "synthetic", "--instruments", str(instruments), "--duration", "0.8",
        "--interval", "0.01", "--md-module", "fake_md",
    ]
    bad_env = dict(env, FAKE_DISK_FAIL="1")
    healthy = subprocess.Popen(cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    failed = subprocess.Popen(cmd, env=bad_env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        good_out, good_err = healthy.communicate(timeout=10)
        bad_out, bad_err = failed.communicate(timeout=10)
        assert healthy.returncode == 0, (good_out, good_err)
        assert failed.returncode != 0, (bad_out, bad_err)
        statuses = [json.loads(p.read_text()) for p in (tmp_path / "shared").rglob("status.json")]
        assert len(statuses) == 2
        assert len({s["session_id"] for s in statuses}) == 2
        assert any(s["ingress"]["durable_seq"] == 1 for s in statuses)
        assert any("synthetic disk full" in (s["ingress"]["error"] or "") for s in statuses)
        paths = list((tmp_path / "shared").rglob("part-*.jsonl"))
        assert len(paths) == 1 and inspect_file(paths[0])["records"] == 1
    finally:
        for proc in (healthy, failed):
            if proc.poll() is None:
                proc.kill()
                proc.wait()


def test_output_paths_enospc_reports_failure_and_evidence(tmp_path):
    instruments = tmp_path / "instruments.json"
    instruments.write_text('["test"]')
    env = dict(os.environ, CTP_USER="synthetic", CTP_PASSWORD="synthetic")
    env["PYTHONPATH"] = str(Path(__file__).parent) + os.pathsep + env.get("PYTHONPATH", "")
    for mode in ("tick", "events", "status", "tick,events,status"):
        root = tmp_path / mode.replace(",", "_")
        env["FAKE_ENOSPC"] = mode
        result = subprocess.run([
            sys.executable, "-m", "dq_live_market_data_collector.tick_cli", "native",
            "--root", str(root), "--front", "synthetic", "--broker", "synthetic",
            "--environment", "synthetic", "--instruments", str(instruments), "--duration", "0.3",
            "--interval", "0.01", "--shutdown-timeout", "1", "--md-module", "fake_md",
        ], env=env, capture_output=True, timeout=5, check=False)
        assert result.returncode != 0, (mode, result.stderr)
        counts = {kind: result.stderr.count(f"ENOSPC_HIT:{kind}\n".encode())
                  for kind in ("tick", "events", "status")}
        assert counts["events" if "," in mode else mode] > 0, (mode, counts, result.stderr)
        assert b"synthetic " in result.stderr
        if mode in ("events", "tick,events,status"):
            assert counts["tick"] == counts["status"] == 0
            assert not list(root.rglob("status.json"))
            assert not list(root.rglob("part-*.jsonl"))
        if mode == "tick":
            statuses = list(root.rglob("status.json"))
            if statuses:
                status = json.loads(statuses[0].read_text())["ingress"]
                assert "synthetic tick full" in status["error"]
                assert status["coverage_complete"] is False
        if mode == "status":
            assert not list(root.rglob("status.json"))


def test_final_status_enospc_cannot_return_success(tmp_path):
    instruments = tmp_path / "instruments.json"
    instruments.write_text('["test"]')
    env = dict(os.environ, CTP_USER="synthetic", CTP_PASSWORD="synthetic",
               FAKE_ENOSPC="status", FAKE_STATUS_FINAL="1")
    env["PYTHONPATH"] = str(Path(__file__).parent) + os.pathsep + env.get("PYTHONPATH", "")
    result = subprocess.run([
        sys.executable, "-m", "dq_live_market_data_collector.tick_cli", "native",
        "--root", str(tmp_path / "output"), "--front", "synthetic", "--broker", "synthetic",
        "--environment", "synthetic", "--instruments", str(instruments), "--duration", "0.3",
        "--interval", "0.01", "--shutdown-timeout", "3", "--md-module", "fake_md",
    ], env=env, capture_output=True, timeout=8, check=False)
    assert result.returncode != 0
    assert result.stderr.count(b"ENOSPC_HIT:status\n") == 1
    assert json.loads(result.stdout)["ok"] is False
    assert list((tmp_path / "output").rglob("status.json"))  # Earlier snapshot may be stale.


def test_login_rejection_subprocess(tmp_path):
    instruments = tmp_path / "instruments.json"
    instruments.write_text('["test"]')
    env = dict(os.environ, CTP_USER="synthetic", CTP_PASSWORD="synthetic", FAKE_LOGIN_REJECT="1")
    env["PYTHONPATH"] = str(Path(__file__).parent) + os.pathsep + env.get("PYTHONPATH", "")
    result = subprocess.run([
        sys.executable, "-m", "dq_live_market_data_collector.tick_cli", "native",
        "--root", str(tmp_path / "out"), "--front", "synthetic", "--broker", "synthetic",
        "--environment", "synthetic", "--instruments", str(instruments), "--duration", "1",
        "--md-module", "fake_md",
    ], env=env, capture_output=True, timeout=5, check=False)
    assert result.returncode != 0
    events = next((tmp_path / "out").rglob("events.jsonl")).read_text()
    assert '"login_rejected"' in events
    assert not list((tmp_path / "out").rglob("part-*.jsonl"))


def test_one_second_startup_allowance_under_cpu_load(tmp_path):
    """The configurable startup allowance works with competing CPU work."""
    import shutil

    import pytest

    taskset = shutil.which("taskset")
    if not hasattr(os, "sched_getaffinity") or not taskset:
        pytest.skip("requires Linux CPU affinity and taskset")
    cpu = str(min(os.sched_getaffinity(0)))
    instruments = tmp_path / "instruments.json"
    instruments.write_text('["test"]')
    env = dict(os.environ, CTP_USER="synthetic", CTP_PASSWORD="synthetic")
    env["PYTHONPATH"] = str(Path(__file__).parent) + os.pathsep + env.get("PYTHONPATH", "")
    hogs = []
    try:
        for _ in range(2):
            hogs.append(subprocess.Popen(
                [taskset, "-c", cpu, sys.executable, "-c", "while True: pass"],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            ))
        root = tmp_path / "out"
        result = subprocess.run([
            taskset, "-c", cpu, sys.executable, "-m",
            "dq_live_market_data_collector.tick_cli", "native", "--root", str(root),
            "--front", "synthetic", "--broker", "synthetic", "--environment", "synthetic",
            "--instruments", str(instruments), "--duration", "0.7", "--shutdown-timeout", "0.5",
            "--startup-timeout", "1", "--interval", "0.01", "--md-module", "fake_md",
        ], env=env, capture_output=True, timeout=8, check=False)
        assert result.returncode == 0, result.stderr
        events = [json.loads(line)["event"] for line in next(root.rglob("events.jsonl")).read_text().splitlines()]
        assert events.index("session_start") < events.index("session_stopping") < events.index("session_end")
    finally:
        for hog in hogs:
            hog.kill()
            hog.wait()


def test_supervisor_death_kills_native_child(tmp_path):
    """A killed supervisor must not leave its acquisition process running."""
    import pytest

    if not sys.platform.startswith("linux"):
        pytest.skip("Linux parent-death signal and procfs required")
    instruments = tmp_path / "instruments.json"
    instruments.write_text('["test"]')
    env = dict(os.environ, CTP_USER="synthetic", CTP_PASSWORD="synthetic")
    env["PYTHONPATH"] = str(Path(__file__).parent) + os.pathsep + env.get("PYTHONPATH", "")
    proc = subprocess.Popen(
        [sys.executable, "-m", "dq_live_market_data_collector.tick_cli", "native",
         "--root", str(tmp_path / "out"), "--front", "synthetic", "--broker", "synthetic",
         "--environment", "synthetic", "--instruments", str(instruments),
         "--duration", "30", "--interval", "0.01", "--md-module", "fake_md"],
        env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    try:
        children = Path(f"/proc/{proc.pid}/task/{proc.pid}/children")
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            pids = children.read_text().split()
            if pids and list((tmp_path / "out").rglob("status.json")):
                break
            assert proc.poll() is None
            time.sleep(0.02)
        else:
            raise AssertionError("native child did not start")
        child = Path(f"/proc/{pids[0]}/stat")
        proc.kill()
        proc.wait(timeout=5)
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            try:
                state = child.read_text().rsplit(") ", 1)[1][0]
            except FileNotFoundError:
                break
            if state == "Z":
                break
            time.sleep(0.02)
        else:
            raise AssertionError("native child survived supervisor death")
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait()


def test_supervisor_reaps_child_if_handler_registration_fails(monkeypatch):
    """An exception after spawn cannot strand the real child process."""
    from types import SimpleNamespace

    import pytest
    from dq_live_market_data_collector import tick_cli

    spawned = []
    popen = subprocess.Popen

    def record_child(*args, **kwargs):
        child = popen(*args, **kwargs)
        spawned.append(child)
        return child

    def fail_signal(*_args):
        raise RuntimeError("handler registration failed")

    monkeypatch.setattr(tick_cli.subprocess, "Popen", record_child)
    monkeypatch.setattr(tick_cli.signal, "signal", fail_signal)
    with pytest.raises(RuntimeError, match="handler registration failed"):
        tick_cli.supervise_native(
            SimpleNamespace(duration=1, startup_timeout=1, shutdown_timeout=1),
            ["native"],
        )
    assert len(spawned) == 1
    assert spawned[0].poll() is not None


def test_supervisor_starts_with_an_active_thread(tmp_path, monkeypatch):
    """Launch the real native child while another parent thread holds a lock."""
    import threading

    from dq_live_market_data_collector import tick_cli

    instruments = tmp_path / "instruments.json"
    instruments.write_text('["test"]')
    monkeypatch.setenv("CTP_USER", "synthetic")
    monkeypatch.setenv("CTP_PASSWORD", "synthetic")
    monkeypatch.setenv(
        "PYTHONPATH", str(Path(__file__).parent) + os.pathsep + os.environ.get("PYTHONPATH", "")
    )
    ready, release = threading.Event(), threading.Event()
    lock = threading.Lock()

    def hold_lock():
        with lock:
            ready.set()
            release.wait(10)

    thread = threading.Thread(target=hold_lock)
    thread.start()
    try:
        assert ready.wait(2)
        assert tick_cli.main([
            "native", "--root", str(tmp_path / "out"), "--front", "synthetic",
            "--broker", "synthetic", "--environment", "synthetic", "--instruments",
            str(instruments), "--duration", "0.3", "--interval", "0.01",
            "--md-module", "fake_md",
        ]) == 0
    finally:
        release.set()
        thread.join(timeout=2)
    assert not thread.is_alive()

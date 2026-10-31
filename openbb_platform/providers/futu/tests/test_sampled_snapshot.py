"""Offline checks for raw REST snapshot evidence."""

import base64
import contextlib
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest
from openbb_futu.client import FutuError, raw_snapshot
from openbb_futu.sampled_snapshot import run


@pytest.fixture
def server(monkeypatch, tmp_path):
    """Serve controlled local responses."""

    class Handler(BaseHTTPRequestHandler):
        status = 200
        delay = 0
        drip = False
        requests = 0
        body = b'{"ret_code":0,"data":{"snapshot_list":[{"code":"HK.00700"}]}}'

        def do_POST(self):
            import time

            self.rfile.read(int(self.headers["Content-Length"]))
            type(self).requests += 1
            time.sleep(self.delay)
            self.send_response(self.status)
            self.send_header("Authorization", "secret-header")
            self.end_headers()
            with contextlib.suppress(BrokenPipeError):
                if self.drip:
                    for byte in self.body:
                        self.wfile.write(bytes([byte]))
                        self.wfile.flush()
                        time.sleep(0.08)
                else:
                    self.wfile.write(self.body)

        def log_message(self, *args):
            pass

    http = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=http.serve_forever, daemon=True)
    thread.start()
    monkeypatch.setenv("FUTU_TEST_HTTP_BASE", f"http://127.0.0.1:{http.server_port}")
    monkeypatch.setenv("FUTU_COORDINATION_DIR", str(tmp_path / "coord"))
    yield Handler
    http.shutdown()
    thread.join()


def test_raw_preserves_failure_and_rejects_secret(server):
    """Preserve safe failure bytes and reject secrets."""
    server.status = 403
    server.body = b'{"error":"denied"}'
    result = raw_snapshot(["HK.00700"], "test-token")
    assert result.body == server.body and result.status == 403
    assert result.request_id and result.started_at_utc
    server.body = b'{"access_token":"test-token"}'
    with pytest.raises(FutuError, match="sensitive_response"):
        raw_snapshot(["HK.00700"], "test-token")


def test_partial_coverage_and_durable_archive(server, tmp_path):
    """Partial responses only prove synced observed symbols."""
    result = run(
        {
            "symbols": [{"asset": "equity", "symbol": "HK.00700"}, {"asset": "etf", "symbol": "HK.02800"}],
            "max_polls": 1,
            "interval_seconds": 1,
        },
        "test-token",
        tmp_path / "out",
    )
    assert result["coverage"]["HK.00700"] == "returned_code"
    assert result["coverage"]["HK.02800"] == "missing"
    assert result["manifest"]["durable_seq"] == 1
    path = result["manifest"]["files"][0]["file"]
    record = json.loads(Path(path).read_text(encoding="utf-8").splitlines()[0])
    assert record["transport"] == "rest" and record["data_kind"] == "sampled_snapshot"
    assert base64.b64decode(record["raw_payload"]) == server.body
    assert "secret-header" not in json.dumps(record)


@pytest.mark.parametrize("status", [403, 429])
def test_http_failure_is_archived(server, tmp_path, status):
    """Archive safe error responses."""
    server.status = status
    server.body = b'{"error":"denied"}'
    result = run({"symbols": [{"asset": "future", "symbol": "US.TEST"}], "max_polls": 1}, "test-token", tmp_path / "out")
    assert result["failure"] == "provider_failure"
    assert result["coverage"]["US.TEST"] == "unknown"
    assert result["manifest"]["durable_seq"] == 1
    record = json.loads(Path(result["manifest"]["files"][0]["file"]).read_text(encoding="utf-8").splitlines()[0])
    assert record["http_status"] == status
    assert base64.b64decode(record["raw_payload"]) == server.body


def test_writer_rejection_is_failure(server, tmp_path, monkeypatch):
    """Rejected ingress cannot produce coverage."""
    from openbb_futu.archive import Archive

    monkeypatch.setattr(Archive, "accept_record", lambda *_: False)
    result = run({"symbols": [{"asset": "option", "symbol": "US.TEST"}]}, "test-token", tmp_path / "out")
    assert result["failure"] == "archive_failure"
    assert result["coverage"]["US.TEST"] == "unknown"


def test_cli_failure_and_lock(server, tmp_path):
    """Exercise the real CLI process with a rejected HTTP response and held lock."""
    import os
    import subprocess
    import sys

    from openbb_futu.coordination import Owner, coordination_directory

    server.status = 403
    server.body = b'{ "error": "denied", "raw": "\\u0041" }'
    config = tmp_path / "config.json"
    config.write_text(json.dumps({"symbols": [{"asset": "equity", "symbol": "HK.00700"}]}))
    token = tmp_path / "token.json"
    token.write_text(
        json.dumps(
            {"client_id": "client", "access_token": "test-token", "refresh_token": "refresh", "expires_at": 9999999999}
        )
    )
    token.chmod(0o600)
    command = [
        sys.executable,
        "-m",
        "openbb_futu.sampled_snapshot",
        "--config",
        str(config),
        "--token-file",
        str(token),
        "--output",
        str(tmp_path / "out"),
    ]
    with Owner(coordination_directory()):
        locked = subprocess.run(command, capture_output=True, text=True, timeout=10, env=os.environ.copy(), check=False)  # noqa: S603
    assert locked.returncode != 0
    result = subprocess.run(command, capture_output=True, text=True, timeout=10, env=os.environ.copy(), check=False)  # noqa: S603
    assert result.returncode != 0 and "provider_failure" in result.stderr
    records = list((tmp_path / "out").glob("source_id=futu/recv_date=*/session=*/*.jsonl"))
    assert len(records) == 1
    record = json.loads(records[0].read_text())
    assert record["http_status"] == 403
    assert base64.b64decode(record["raw_payload"]) == server.body
    server.status = 429
    limited = subprocess.run(command, capture_output=True, text=True, timeout=10, env=os.environ.copy(), check=False)  # noqa: S603
    assert limited.returncode != 0 and "provider_failure" in limited.stderr
    server.status = 200
    server.body = b'{"ret_code":0,"data":{"snapshot_list":[]}}'
    partial = subprocess.run(command, capture_output=True, text=True, timeout=10, env=os.environ.copy(), check=False)  # noqa: S603
    assert partial.returncode == 0
    reports = [json.loads(path.read_text()) for path in (tmp_path / "out" / "futu-sampled-coverage").glob("*.json")]
    assert any(item["coverage"]["HK.00700"] == "missing" for item in reports)


def test_timeout_and_sigterm(server, tmp_path):
    """Timeout fails and SIGTERM drains a completed first poll."""
    import os
    import signal
    import subprocess
    import sys
    import time

    server.delay = 0.3
    result = run(
        {"symbols": [{"asset": "equity", "symbol": "HK.00700"}], "timeout_seconds": 0.05},
        "test-token",
        tmp_path / "timeout",
    )
    assert result["failure"] == "timeout"
    attempt = result["attempts"][0]
    assert attempt["request_id"] and attempt["started_at_utc"]
    assert attempt["body"] is None and attempt["durability"] == "unknown"
    assert result["coverage"]["HK.00700"] == "unknown"
    server.delay = 0
    config = tmp_path / "config.json"
    config.write_text(
        json.dumps({"symbols": [{"asset": "equity", "symbol": "HK.00700"}], "max_polls": 10, "interval_seconds": 30})
    )
    token = tmp_path / "token.json"
    token.write_text(
        json.dumps(
            {"client_id": "client", "access_token": "test-token", "refresh_token": "refresh", "expires_at": 9999999999}
        )
    )
    token.chmod(0o600)
    command = [
        sys.executable,
        "-m",
        "openbb_futu.sampled_snapshot",
        "--config",
        str(config),
        "--token-file",
        str(token),
        "--output",
        str(tmp_path / "out"),
    ]
    before = server.requests
    proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=os.environ.copy())  # noqa: S603
    try:
        deadline = time.monotonic() + 10
        while server.requests == before and time.monotonic() < deadline:
            time.sleep(0.02)
        assert server.requests > before
        time.sleep(0.1)
        proc.send_signal(signal.SIGTERM)
        proc.communicate(timeout=10)
        assert proc.returncode == 0
        reports = list((tmp_path / "out" / "futu-sampled-coverage").glob("*.json"))
        assert json.loads(reports[0].read_text())["manifest"]["durable_seq"] == 1
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.communicate()


def test_cli_queue_full_and_timeout(server, tmp_path):
    """A real bounded queue and an HTTP timeout fail in the CLI process."""
    import os
    import subprocess
    import sys

    config = tmp_path / "config.json"
    token = tmp_path / "token.json"
    token.write_text(json.dumps({"client_id": "client-secret", "access_token": "test-token",
                                 "refresh_token": "refresh-secret", "expires_at": 9999999999}))
    token.chmod(0o600)
    config.write_text(json.dumps({"symbols": [{"asset": "equity", "symbol": "HK.00700"}],
                                  "max_polls": 5, "interval_seconds": 0.1, "queue_capacity": 1}))
    inject = tmp_path / "inject"
    inject.mkdir()
    (inject / "sitecustomize.py").write_text(
        "import time\nfrom dq_live_market_data_collector.tick_file import TickWriter\n"
        "original = TickWriter.append\n"
        "def slow(self, record):\n    time.sleep(1)\n    return original(self, record)\n"
        "TickWriter.append = slow\n"
    )
    env = os.environ.copy()
    env["PYTHONPATH"] = str(inject) + os.pathsep + env.get("PYTHONPATH", "")
    command = [sys.executable, "-m", "openbb_futu.sampled_snapshot", "--config", str(config),
               "--token-file", str(token), "--output", str(tmp_path / "queue")]
    result = subprocess.run(command, env=env, capture_output=True, text=True, timeout=10, check=False)  # noqa: S603
    assert result.returncode != 0 and "archive_failure" in result.stderr
    reports = list((tmp_path / "queue" / "futu-sampled-coverage").glob("*.json"))
    report = json.loads(reports[0].read_text())
    assert report["manifest"]["rejected_frames"] > 0
    assert report["manifest"]["durable_seq"] < report["manifest"]["received_frames"]
    assert all(secret not in result.stdout + result.stderr
               for secret in ("test-token", "refresh-secret", "client-secret"))
    server.delay = 0.5
    config.write_text(json.dumps({"symbols": [{"asset": "equity", "symbol": "HK.00700"}], "timeout_seconds": 0.05}))
    command[-1] = str(tmp_path / "timeout-cli")
    result = subprocess.run(command, env=os.environ.copy(), capture_output=True, text=True, timeout=10, check=False)  # noqa: S603
    assert result.returncode != 0 and "timeout" in result.stderr
    report = json.loads(next((tmp_path / "timeout-cli" / "futu-sampled-coverage").glob("*.json")).read_text())
    assert report["attempts"][0]["request_id"] and report["attempts"][0]["durability"] == "unknown"
    assert report["coverage"]["HK.00700"] == "unknown"


def test_multi_asset_partial_independent_of_ws(server, tmp_path):
    """REST missing is per requested symbol, not a WebSocket disconnect gap."""
    entries = [{"asset": a, "symbol": s} for a, s in (
        ("equity", "US.AAPL"), ("etf", "US.SPY"), ("option", "US.OPT"), ("future", "US.FUT")
    )]
    server.body = b'{"ret_code":0,"data":{"snapshot_list":[{"code":"US.AAPL"},{"code":"US.OPT"}]}}'
    result = run({"symbols": entries}, "test-token", tmp_path / "out")
    assert result["coverage"] == {"US.AAPL": "returned_code", "US.SPY": "missing",
                                  "US.OPT": "returned_code", "US.FUT": "missing"}
    assert all(v["quote_observed"] == "unknown" and v["permission"] == "unknown"
               for v in result["quote_evidence"].values())
    assert result["failure"] is None and result["manifest"]["durable_seq"] == 1
    assert result["transport"] == "rest" and "gaps" not in result


def test_no_response_connection_metadata(server, tmp_path, monkeypatch):
    """Connection errors retain sanitized request evidence without inventing a body."""
    import socket

    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    monkeypatch.setenv("FUTU_TEST_HTTP_BASE", f"http://127.0.0.1:{port}")
    result = run({"symbols": [{"asset": "future", "symbol": "US.FUT"}]}, "test-token", tmp_path / "out")
    attempt = result["attempts"][0]
    assert result["failure"] == "transport" and attempt["error"] == "transport"
    assert attempt["request_id"] and attempt["started_at_utc"]
    assert attempt["http_status"] is None and attempt["body"] is None and attempt["durability"] == "unknown"
    assert result["manifest"]["durable_seq"] == 0 and result["coverage"]["US.FUT"] == "unknown"
    assert "secret DNS detail" not in json.dumps(result)


def test_drip_cli_deadline_and_signal(server, tmp_path):
    """A real drip response cannot defeat the parent deadline or SIGTERM."""
    import signal
    import sqlite3
    import subprocess
    import sys
    import time

    from openbb_futu.coordination import Owner, RequestBudget, coordination_directory

    server.drip = True
    config = tmp_path / "drip.json"
    token = tmp_path / "private.json"
    token.write_text(json.dumps({"client_id": "private-client", "access_token": "private-access",
                                 "refresh_token": "private-refresh", "expires_at": 9999999999}))
    token.chmod(0o600)
    config.write_text(json.dumps({"symbols": [{"asset": "equity", "symbol": "HK.00700"}],
                                  "timeout_seconds": 0.6, "max_polls": 3, "interval_seconds": 0.1}))
    command = [sys.executable, "-m", "openbb_futu.sampled_snapshot", "--config", str(config),
               "--token-file", str(token), "--output", str(tmp_path / "drip")]
    start = time.monotonic()
    result = subprocess.run(command, capture_output=True, text=True, timeout=5, check=False)  # noqa: S603
    assert time.monotonic() - start < 3 and result.returncode != 0
    report = json.loads(next((tmp_path / "drip" / "futu-sampled-coverage").glob("*.json")).read_text())
    assert report["failure"] == "timeout" and report["attempts"][0]["send_status"] == "unknown"
    assert report["attempts"][0]["request_id"] and server.requests == 1
    with Owner(coordination_directory()):
        budget = RequestBudget(coordination_directory())
        with sqlite3.connect(budget.path) as db:
            assert db.execute("SELECT COUNT(*) FROM requests").fetchone()[0] == 1
    for path in (tmp_path / "drip").rglob("*"):
        if path.is_file():
            assert all(secret not in path.read_bytes() for secret in
                       (b"private-access", b"private-refresh", b"private-client"))
    assert all(secret not in result.stdout + result.stderr for secret in
               ("private-access", "private-refresh", "private-client"))
    config.write_text(json.dumps({"symbols": [{"asset": "equity", "symbol": "HK.00700"}],
                                  "timeout_seconds": 10, "max_polls": 3}))
    command[-1] = str(tmp_path / "signal")
    before = server.requests
    proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)  # noqa: S603
    try:
        deadline = time.monotonic() + 5
        while server.requests == before and time.monotonic() < deadline:
            time.sleep(0.02)
        assert server.requests == before + 1
        start = time.monotonic()
        proc.send_signal(signal.SIGTERM)
        out, err = proc.communicate(timeout=3)
        assert time.monotonic() - start < 2 and b"private-access" not in out + err
        report = json.loads(next((tmp_path / "signal" / "futu-sampled-coverage").glob("*.json")).read_text())
        assert report["failure"] == "stopped" and report["attempts"][0]["request_id"]
        assert report["attempts"][0]["send_status"] == "unknown"
        assert server.requests == before + 1
        with Owner(coordination_directory()):
            pass
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.communicate()


def test_spawn_budget_exhaustion_prevents_http(server, tmp_path):
    """A full parent budget fails before a child can make an HTTP request."""
    from openbb_futu.coordination import RequestBudget, coordination_directory

    budget = RequestBudget(coordination_directory())
    for _ in range(70):
        assert budget.acquire() == 0
    result = run({"symbols": [{"asset": "equity", "symbol": "HK.00700"}], "max_polls": 3},
                 "private-access", tmp_path / "budget")
    assert result["failure"] == "local_budget_exhausted" and server.requests == 0


def test_writer_failure_interrupts_drip(server, tmp_path, monkeypatch):
    """A writer failure interrupts an in-flight child and prevents another poll."""
    import time

    from openbb_futu.archive import Archive
    from openbb_futu.coordination import Owner, coordination_directory

    server.drip = True
    original = Archive.__init__

    def inject(self, *args, **kwargs):
        original(self, *args, **kwargs)

        def fail_writer():
            deadline = time.monotonic() + 5
            while not server.requests and time.monotonic() < deadline:
                time.sleep(0.01)
            self.error = "injected writer failure"

        threading.Thread(target=fail_writer, daemon=True).start()

    monkeypatch.setattr(Archive, "__init__", inject)
    start = time.monotonic()
    result = run({"symbols": [{"asset": "equity", "symbol": "HK.00700"}], "max_polls": 3,
                  "timeout_seconds": 5}, "private-access", tmp_path / "writer")
    assert time.monotonic() - start < 3
    assert result["failure"] == "archive_failure" and server.requests == 1
    assert result["attempts"][0]["request_id"] and result["attempts"][0]["send_status"] == "unknown"
    with Owner(coordination_directory()):
        pass


def test_cli_empty_quote_and_writer_enospc(server, tmp_path):
    """The CLI preserves empty quote items and stops polling on writer failure."""
    import os
    import subprocess
    import sys

    entries = [{"asset": "equity", "symbol": "US.AAPL"}, {"asset": "option", "symbol": "US.OPT"}]
    server.body = b'{"ret_code":0,"data":{"snapshot_list":[{"code":"US.AAPL"},{"code":"US.OPT"}]}}'
    config = tmp_path / "config.json"
    config.write_text(json.dumps({"symbols": entries, "max_polls": 3, "interval_seconds": 0.2}))
    token = tmp_path / "token.json"
    token.write_text(json.dumps({"client_id": "client", "access_token": "test-token",
                                 "refresh_token": "refresh", "expires_at": 9999999999}))
    token.chmod(0o600)
    command = [sys.executable, "-m", "openbb_futu.sampled_snapshot", "--config", str(config),
               "--token-file", str(token), "--output", str(tmp_path / "normal")]
    result = subprocess.run(command, capture_output=True, timeout=15, check=False)  # noqa: S603
    assert result.returncode == 0
    report = json.loads(next((tmp_path / "normal" / "futu-sampled-coverage").glob("*.json")).read_text())
    assert set(report["coverage"].values()) == {"returned_code"}
    assert all(v["quote_observed"] == "unknown" for v in report["quote_evidence"].values())
    records = list((tmp_path / "normal").glob("source_id=futu/recv_date=*/session=*/*.jsonl"))
    assert all(base64.b64decode(json.loads(line)["raw_payload"]) == server.body
               for path in records for line in path.read_text().splitlines())
    inject = tmp_path / "inject_enospc"
    inject.mkdir()
    (inject / "sitecustomize.py").write_text(
        "from dq_live_market_data_collector.tick_file import TickWriter\n"
        "def fail(self, record):\n    raise OSError(28, 'injected')\n"
        "TickWriter.append = fail\n"
    )
    env = os.environ.copy()
    env["PYTHONPATH"] = str(inject) + os.pathsep + env.get("PYTHONPATH", "")
    command[-1] = str(tmp_path / "failed")
    before = server.requests
    result = subprocess.run(command, env=env, capture_output=True, timeout=15, check=False)  # noqa: S603
    assert result.returncode != 0 and server.requests == before + 1


def test_uninterpreted_fields_and_duplicate_items(server, tmp_path):
    """Unknown and negative values survive unchanged and do not establish validity."""
    server.body = (b'{"ret_code":0,"data":{"snapshot_list":['
                   b'{"code":"US.AAPL","price":-1,"unknown_field":null},'
                   b'{"code":"US.AAPL","price":12.5}]}}')
    result = run({"symbols": [{"asset": "equity", "symbol": "US.AAPL"}]}, "test-token", tmp_path / "out")
    assert result["coverage"] == {"US.AAPL": "returned_code"}
    evidence = result["quote_evidence"]["US.AAPL"]
    assert evidence["quote_fields_valid"] == evidence["quote_observed"] == "unknown"
    record = json.loads(Path(result["manifest"]["files"][0]["file"]).read_text())
    assert base64.b64decode(record["raw_payload"]) == server.body
    from openbb_futu.sampled_snapshot import rebuild_rest_coverage

    manifest = tmp_path / "out" / "futu-manifests" / (result["manifest"]["session_id"] + ".json")
    assert rebuild_rest_coverage(manifest, ["US.AAPL"]) == result["coverage"]


def test_rest_cli_kill_recovery(server, tmp_path):
    """A killed sampler leaves recoverable evidence, not a durable coverage claim."""
    import subprocess
    import sys
    import time

    config = tmp_path / "config.json"
    config.write_text(json.dumps({"symbols": [{"asset": "equity", "symbol": "HK.00700"}],
                                  "max_polls": 3, "interval_seconds": 10}))
    token = tmp_path / "token.json"
    token.write_text(json.dumps({"client_id": "client", "access_token": "test-token",
                                 "refresh_token": "refresh", "expires_at": 9999999999}))
    token.chmod(0o600)
    root = tmp_path / "out"
    command = [sys.executable, "-m", "openbb_futu.sampled_snapshot", "--config", str(config),
               "--token-file", str(token), "--output", str(root)]
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)  # noqa: S603
    try:
        deadline = time.monotonic() + 8
        while server.requests < 1 and time.monotonic() < deadline:
            time.sleep(0.02)
        assert server.requests == 1
        time.sleep(0.5)
        process.kill()
        process.communicate(timeout=5)
    finally:
        if process.poll() is None:
            process.kill()
            process.communicate()
    assert not list((root / "futu-sampled-coverage").glob("*.json"))
    config.write_text(json.dumps({"symbols": [{"asset": "equity", "symbol": "HK.00700"}]}))
    restarted = subprocess.run(command, capture_output=True, timeout=15, check=False)  # noqa: S603
    assert restarted.returncode == 0
    recovery = list((root / "futu-recovery").glob("*/recovery.json"))
    assert recovery and json.loads(recovery[0].read_text())["gap"] == "unresolved_after_process_interruption"
    report = json.loads(next((root / "futu-sampled-coverage").glob("*.json")).read_text())
    assert report["quote_evidence"]["HK.00700"]["quote_observed"] == "unknown"


@pytest.mark.parametrize("first_present", [True, False])
def test_returned_code_across_poll_order(server, tmp_path, first_present):
    """A durable returned code persists regardless of whether omission came first."""
    import threading
    import time

    present = b'{"ret_code":0,"data":{"snapshot_list":[{"code":"HK.00700"}]}}'
    missing = b'{"ret_code":0,"data":{"snapshot_list":[]}}'
    server.body = present if first_present else missing

    def change_response():
        deadline = time.monotonic() + 5
        while server.requests < 1 and time.monotonic() < deadline:
            time.sleep(0.01)
        time.sleep(0.1)
        server.body = missing if first_present else present

    thread = threading.Thread(target=change_response)
    thread.start()
    try:
        result = run({"symbols": [{"asset": "equity", "symbol": "HK.00700"}],
                      "max_polls": 2, "interval_seconds": 0.5}, "test-token", tmp_path / "out")
    finally:
        thread.join()
    assert server.requests == 2
    assert result["coverage"]["HK.00700"] == "returned_code"
    assert result["quote_evidence"]["HK.00700"]["quote_observed"] == "unknown"


@pytest.mark.parametrize(
    ("key", "value"),
    [("max_polls", True), ("max_polls", 0), ("max_polls", 1001),
     ("interval_seconds", True), ("interval_seconds", 0.099),
     ("interval_seconds", float("nan")), ("timeout_seconds", False),
     ("timeout_seconds", 0), ("timeout_seconds", float("inf"))],
)
def test_polling_bounds_reject_invalid_values_before_creating_output(tmp_path, key, value):
    """Invalid numerical bounds fail before archive or network setup."""
    config = {"symbols": [{"asset": "equity", "symbol": "HK.00700"}], key: value}
    output = tmp_path / "output"
    with pytest.raises(ValueError, match="invalid polling bounds"):
        run(config, "unused", output)
    assert not output.exists()

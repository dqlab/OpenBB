"""Exercise durable sampled request evidence."""

import errno
import hashlib
import json
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
from openbb_futu import (
    coordination,
    sampled_snapshot as s,
)


@pytest.fixture
def service(tmp_path, monkeypatch):
    """Verify sampled request behavior."""
    calls = []

    class H(BaseHTTPRequestHandler):
        def do_POST(self):
            calls.append(self.rfile.read(int(self.headers["Content-Length"])))
            body = b'{"ret_code":0,"data":{"snapshot_list":[]}}'
            self.send_response(200)
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *a):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), H)
    t = threading.Thread(target=server.serve_forever)
    t.start()
    monkeypatch.setenv("FUTU_TEST_HTTP_BASE", f"http://127.0.0.1:{server.server_port}")
    monkeypatch.setenv("FUTU_COORDINATION_DIR", str(tmp_path / "coord"))
    monkeypatch.setattr(coordination, "BUDGET_WINDOW", 0.02)
    yield calls
    server.shutdown()
    t.join()
    server.server_close()


def run(out):
    """Verify sampled request behavior."""
    return s.run({"symbols": [{"asset": "equity", "symbol": "US.AAPL"}]}, "fake-token", out)


def test_success_and_restart(tmp_path, service):
    """Verify sampled request behavior."""
    out = tmp_path / "out"
    result = run(out)
    assert result["failure"] is None
    files = list(out.glob("futu-sampled-event-*"))
    assert len(files) == 2
    events = [json.loads(p.read_text()) for p in files]
    attempt = next(x for x in events if x["event"] == "attempt")
    response = next(x for x in events if x["event"] == "result")
    assert attempt["status"] == "unknown" and attempt["session_id"] == result["run_id"]
    assert response["raw_payload"] == "eyJyZXRfY29kZSI6MCwiZGF0YSI6eyJzbmFwc2hvdF9saXN0IjpbXX19"
    before = {p: hashlib.sha256(p.read_bytes()).hexdigest() for p in files}
    run(out)
    assert len(service) == 2
    assert all(hashlib.sha256(p.read_bytes()).hexdigest() == h for p, h in before.items())
    assert all(
        b"fake-token" not in p.read_bytes() for p in out.rglob("*") if p.is_file()
    )


def test_failure_event(tmp_path, service, monkeypatch):
    """Verify sampled request behavior."""
    # Failure after a real worker starts is exercised via a refused loopback port.
    monkeypatch.setenv("FUTU_TEST_HTTP_BASE", "http://127.0.0.1:1")
    out = tmp_path / "out"
    result = run(out)
    assert result["failure"] == "transport"
    events = [json.loads(p.read_text()) for p in out.glob("futu-sampled-event-*")]
    assert {e["event"] for e in events} == {"attempt", "result"}
    assert next(e for e in events if e["event"] == "result")["status"] == "failure"
    assert len(service) == 0


@pytest.mark.parametrize("fault", ["write", "file_fsync", "dir_fsync"])
def test_attempt_io_failure_no_http(tmp_path, service, monkeypatch, fault):
    """Verify sampled request behavior."""
    out = tmp_path / "out"
    out.mkdir(mode=0o700)
    original = s.RootHandle.publish

    def inject(self, name, content, **kw):
        if name.endswith("-attempt.json"):
            if fault == "write":

                class Bad:
                    def __enter__(self):
                        return self

                    def __exit__(self, *a):
                        os.close(self.fd)

                    def write(self, _):
                        raise OSError(errno.EIO, "injected write")

                def bad(fd, *a, **k):
                    x = Bad()
                    x.fd = fd
                    return x

                with monkeypatch.context() as m:
                    m.setattr(os, "fdopen", bad)
                    return original(self, name, content, **kw)
            sync = os.fsync

            def fail(fd):
                import stat

                isdir = stat.S_ISDIR(os.fstat(fd).st_mode)
                if isdir == (fault == "dir_fsync"):
                    raise OSError(errno.EIO, "injected fsync")
                return sync(fd)

            with monkeypatch.context() as m:
                m.setattr(os, "fsync", fail)
                return original(self, name, content, **kw)
        return original(self, name, content, **kw)

    monkeypatch.setattr(s.RootHandle, "publish", inject)
    with pytest.raises(OSError):
        run(out)
    assert not service


def test_unresolved_blocks_and_preserves_evidence(tmp_path, service):
    """Verify sampled request behavior."""
    out = tmp_path / "out"
    with s.RootHandle.create(out) as root:
        s._event(
            root,
            "attempt",
            "a" * 32,
            {
                "session_id": "b" * 32,
                "scope": [{"asset": "equity", "symbol": "US.AAPL"}],
                "started_at_utc": "2026-01-01T00:00:00+00:00",
                "status": "unknown",
            },
        )
    path = next(out.glob("futu-sampled-event-*"))
    before = hashlib.sha256(path.read_bytes()).hexdigest()
    with pytest.raises(RuntimeError, match="sampled_unresolved:1"):
        run(out)
    assert not service and hashlib.sha256(path.read_bytes()).hexdigest() == before


@pytest.mark.parametrize("credential_index", [0, 1, 2, 3])
def test_credential_target_rejected_before_output(tmp_path, service, credential_index):
    """Reject target overlap with every supplied credential before side effects."""
    credentials = ["access-value", "refresh-value", "client-value", "secret-value"]
    credentials[credential_index] = "US.private-token-123"
    out = tmp_path / "out"
    with pytest.raises(ValueError, match="target conflicts"):
        s.run(
            {"symbols": [{"asset": "equity", "symbol": credentials[credential_index]}]},
            credentials[0],
            out,
            secrets=credentials[1:],
        )
    assert not service
    assert not out.exists()


def test_sigkill_restart_preserves_attempt(tmp_path):
    """A killed sender leaves durable evidence and restart cannot send again."""
    import subprocess
    import sys

    arrived = threading.Event()
    release = threading.Event()
    calls = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            calls.append(self.rfile.read(int(self.headers["Content-Length"])))
            arrived.set()
            release.wait(10)
            try:
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b'{"ret_code":0,"data":{"snapshot_list":[]}}')
            except OSError:
                pass

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    env = dict(os.environ, FUTU_COORDINATION_DIR=str(tmp_path / "coord"))
    env["FUTU_TEST_HTTP_BASE"] = f"http://127.0.0.1:{server.server_port}"
    code = (
        "import openbb_futu.coordination as c; c.BUDGET_WINDOW=.02; "
        "from openbb_futu.sampled_snapshot import run; import sys; "
        "run({'symbols':[{'asset':'equity','symbol':'US.AAPL'}]},'fake-token',sys.argv[1])"
    )
    out = tmp_path / "out"
    process = subprocess.Popen([sys.executable, "-c", code, str(out)], env=env)  # noqa: S603
    try:
        assert arrived.wait(8)
        process.kill()
        process.wait(timeout=5)
        assert process.returncode == -9
        files = [p for p in out.rglob("*") if p.is_file()]
        before = {p: hashlib.sha256(p.read_bytes()).digest() for p in files}
        assert any(p.name.endswith("-attempt.json") for p in files)
        release.set()
        deadline = time.monotonic() + 8
        while True:
            restarted = subprocess.run(  # noqa: S603
                [sys.executable, "-c", code, str(out)], env=env,
                capture_output=True, timeout=15, check=False,
            )
            if b"sampled_unresolved:1" in restarted.stderr or time.monotonic() > deadline:
                break
            time.sleep(0.05)
        assert restarted.returncode != 0
        assert b"sampled_unresolved:1" in restarted.stderr
        assert len(calls) == 1
        assert all(p.exists() and hashlib.sha256(p.read_bytes()).digest() == h for p, h in before.items())
    finally:
        release.set()
        if process.poll() is None:
            process.kill()
            process.wait()
        server.shutdown()
        thread.join()
        server.server_close()

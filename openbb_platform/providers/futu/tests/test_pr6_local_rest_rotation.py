"""Local HTTP byte evidence and ordinary token rotation failures."""

import hashlib
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
from openbb_futu.client import FutuError, OAuthTokens
from openbb_futu.collector import TokenStore
from openbb_futu.discovery import discover


def test_discovery_local_http_records_exact_bytes(tmp_path, monkeypatch):
    """Capture the exact success and HTTP failure bodies from loopback."""
    monkeypatch.setattr("openbb_futu.coordination.BUDGET_WINDOW", 0.02)
    bodies = [b'{ "ret_code": 0, "data": {"option_chain": [{"code":"US.OPT"}]} }\n',
              b'{"ret_code":-9,"detail":"denied"}\n']
    requests = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            requests.append((self.path, self.headers.get("Authorization")))
            index = len(requests) - 1
            self.send_response((200, 403)[index])
            self.send_header("Content-Length", str(len(bodies[index])))
            self.end_headers()
            self.wfile.write(bodies[index])

        def log_message(self, *_args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    monkeypatch.setenv("FUTU_TEST_HTTP_BASE", f"http://127.0.0.1:{server.server_port}")
    monkeypatch.setenv("FUTU_COORDINATION_DIR", str(tmp_path / "coord"))
    try:
        report = discover([{"asset": "option", "symbol": "US.SPY"},
                           {"asset": "option", "symbol": "HK.00700"}], "local-test-secret", tmp_path / "raw")
    finally:
        server.shutdown()
        thread.join()
        server.server_close()
    assert [x[0] for x in requests] == ["/api/v1.0/quote/US.SPY/option-chain", "/api/v1.0/quote/HK.00700/option-chain"]
    assert all(x[1] == "Bearer local-test-secret" for x in requests)
    assert report["requests"][0]["symbols"] == ["US.OPT"]
    assert report["requests"][1]["error"] == "provider_failure"
    for item, body in zip(report["requests"], bodies):
        from pathlib import Path
        assert Path(item["raw_file"]).read_bytes() == body
        assert item["sha256"] == hashlib.sha256(body).hexdigest()
        assert item["bytes"] == len(body)
    assert "local-test-secret" not in json.dumps(report)
    assert set(report["target_status"].values()) == {"unknown"}


def test_rotation_success_and_failed_refresh_preserve_marker(tmp_path, monkeypatch):
    """A failed exchange preserves the prior token and uncertainty marker."""
    token = tmp_path / "token.json"
    original = {"client_id": "client", "access_token": "old", "refresh_token": "old-refresh", "expires_at": 0}
    token.write_text(json.dumps(original))
    token.chmod(0o600)
    store = TokenStore(token)
    monkeypatch.setattr("openbb_futu.collector.refresh_tokens", lambda *_a, **_k: OAuthTokens("new", "new-refresh", 60))
    assert store.rotate(None)["refresh_token"] == "new-refresh"  # noqa: S105
    assert store.read()["access_token"] == "new"  # noqa: S105
    assert not store.marker.exists()
    before = token.read_bytes()
    def fail(*_args, **_kwargs):
        raise FutuError("transport")
    monkeypatch.setattr("openbb_futu.collector.refresh_tokens", fail)
    with pytest.raises(FutuError, match="transport"):
        store.rotate(None)
    assert token.read_bytes() == before
    assert store.marker.exists()
    with pytest.raises(FutuError, match="token_rotation_uncertain"):
        store.read()


def test_rotation_replace_failure_retains_old_token_and_marker(tmp_path, monkeypatch):
    """A local publication error retains both the prior token and the pending marker."""
    token = tmp_path / "token.json"
    token.write_text(json.dumps(
        {"client_id": "client", "access_token": "old", "refresh_token": "old-refresh", "expires_at": 0}
    ))
    token.chmod(0o600)
    before = token.read_bytes()
    store = TokenStore(token)
    monkeypatch.setattr("openbb_futu.collector.refresh_tokens", lambda *_a, **_k: OAuthTokens("new", "new-refresh", 60))

    def fail_replace(*_args, **_kwargs):
        raise OSError("synthetic publication failure")

    monkeypatch.setattr("openbb_futu.collector.os.replace", fail_replace)
    with pytest.raises(OSError, match="synthetic publication failure"):
        store.rotate(None)
    assert token.read_bytes() == before
    assert store.marker.exists()
    with pytest.raises(FutuError, match="token_rotation_uncertain"):
        store.read()


def test_discovery_rejects_http_debug_before_request(tmp_path, monkeypatch, capsys):
    """HTTP debug output must never print the discovery bearer token."""
    import http.client

    monkeypatch.setattr(http.client.HTTPConnection, "debuglevel", 1)
    with pytest.raises(FutuError, match="unsafe_http_debug"):
        discover([{"asset": "option", "symbol": "US.SPY"}], "local-test-secret", tmp_path / "raw")
    captured = capsys.readouterr()
    assert "local-test-secret" not in captured.out + captured.err
    assert not (tmp_path / "raw").exists()


def test_sample_worker_rejected_independently(tmp_path, monkeypatch):
    """The enabled sampler captures loopback bytes independently of discovery."""
    import base64
    from pathlib import Path

    from openbb_futu.sampled_snapshot import run

    body = b'{"ret_code":0,"data":{"snapshot_list":[{"code":"US.SPY"}]}}'
    received = []
    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            received.append(self.rfile.read(int(self.headers["Content-Length"])))
            self.send_response(200)
            self.end_headers()
            self.wfile.write(body)
        def log_message(self, *_args):
            pass
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    monkeypatch.setattr("openbb_futu.coordination.BUDGET_WINDOW", 0.02)
    monkeypatch.setenv("FUTU_TEST_HTTP_BASE", f"http://127.0.0.1:{server.server_port}")
    monkeypatch.setenv("FUTU_COORDINATION_DIR", str(tmp_path / "coord"))
    try:
        result = run({"symbols": [{"asset": "equity", "symbol": "US.SPY"}]}, "test-token", tmp_path / "out")
    finally:
        server.shutdown()
        thread.join()
        server.server_close()
    assert len(received) == 1
    assert result["failure"] is None and result["coverage"]["US.SPY"] == "returned_code"
    record = json.loads(Path(result["manifest"]["files"][0]["file"]).read_text())
    assert base64.b64decode(record["raw_payload"]) == body

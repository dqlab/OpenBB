"""Real loopback checks for the owner-scoped synchronous REST paths."""
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
from openbb_futu import client, coordination, discovery


class Handler(BaseHTTPRequestHandler):
    """Serve synthetic REST responses on loopback."""

    calls = []
    delay = 0
    def log_message(self, *_args):
        """Suppress HTTP logs in the test."""
        pass
    def do_POST(self):
        """Record and answer a synthetic POST."""
        self.rfile.read(int(self.headers.get("Content-Length", 0)))
        self.calls.append(self.path)
        time.sleep(self.delay)
        if self.path == "/oauth2/token":
            body = {"access_token": "new", "expires_in": 3600}
        else:
            body = {"ret_code": 0, "data": {"snapshot_list": [{"code": "US.A"}]}}
        self.reply(body)
    def do_GET(self):
        """Record and answer a synthetic GET."""
        self.calls.append(self.path)
        time.sleep(self.delay)
        self.reply({"ret_code": 0, "data": {"option_chain": [{"code": "US.O"}]}})
    def reply(self, body):
        """Send a JSON response."""
        data = json.dumps(body).encode()
        self.send_response(200)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


def test_sync_real_loopback(tmp_path, monkeypatch):
    """Exercise refresh, snapshots and discovery over loopback."""
    monkeypatch.setattr(coordination, "BUDGET_WINDOW", 0.02)
    monkeypatch.setenv("FUTU_COORDINATION_DIR", str(tmp_path / "coord"))
    Handler.calls = []
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    monkeypatch.setenv("FUTU_TEST_HTTP_BASE", f"http://127.0.0.1:{server.server_port}")
    try:
        assert client.refresh_tokens("client", "refresh").access_token == "new"  # noqa: S105 -- synthetic response
        assert client.snapshot(["US.A"], "token")[0]["code"] == "US.A"
        assert client.raw_snapshot(["US.A"], "token").parsed["ret_code"] == 0
        result = discovery.discover([{"asset": "option", "symbol": "US.A"}], "token", tmp_path / "evidence")
        assert result["requests"][0]["symbols"] == ["US.O"]
        assert len(Handler.calls) == 4
    finally:
        server.shutdown()
        thread.join()
        server.server_close()


@pytest.mark.parametrize("operation", ["post", "raw", "discovery"])
def test_delayed_request_remains_charged(tmp_path, monkeypatch, operation):
    """A response delayed beyond the window still consumes a shared slot."""
    monkeypatch.setattr(coordination, "BUDGET_WINDOW", 0.1)
    monkeypatch.setattr(coordination, "BUDGET_LIMIT", 1)
    monkeypatch.setenv("FUTU_COORDINATION_DIR", str(tmp_path / "coord"))
    monkeypatch.setattr(Handler, "delay", 0.2)
    Handler.calls = []
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    monkeypatch.setenv("FUTU_TEST_HTTP_BASE", f"http://127.0.0.1:{server.server_port}")
    try:
        with coordination.Owner(tmp_path / "coord") as owner:
            time.sleep(0.12)
            if operation == "post":
                client.snapshot(["US.A"], "token", owner=owner)
            elif operation == "raw":
                client.raw_snapshot(["US.A"], "token", owner=owner)
            else:
                discovery.discover([{"asset": "option", "symbol": "US.A"}], "token", tmp_path / "evidence", owner=owner)
            with pytest.raises(client.FutuError, match="local_budget_exhausted"):
                client.snapshot(["US.A"], "token", owner=owner)
            assert len(Handler.calls) == 1
    finally:
        server.shutdown()
        thread.join()
        server.server_close()

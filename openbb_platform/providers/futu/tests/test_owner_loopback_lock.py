"""Real loopback regression for a lock handed to an external process."""
import fcntl
import json
import subprocess
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
from openbb_futu import client, coordination


def test_refresh_after_external_lock_handover_cannot_send(tmp_path, monkeypatch):
    """Only the current lock holder may send a refresh request."""
    calls = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            self.rfile.read(int(self.headers["Content-Length"]))
            calls.append(self.path)
            body = json.dumps({"access_token": "test", "expires_in": 10}).encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    monkeypatch.setenv("FUTU_COORDINATION_DIR", str(tmp_path))
    monkeypatch.setenv("FUTU_TEST_HTTP_BASE", f"http://127.0.0.1:{server.server_port}")
    monkeypatch.setattr(coordination, "BUDGET_WINDOW", 0.01)
    try:
        with coordination.Owner(tmp_path) as owner:
            time.sleep(0.02)
            assert client.refresh_tokens("id", "refresh", owner=owner).access_token == "test"  # noqa: S105
            fcntl.flock(owner._fd, fcntl.LOCK_UN)
            process = subprocess.Popen(  # noqa: S603
                [sys.executable, "-c", "import fcntl,sys; f=open(sys.argv[1], 'r+'); "
                 "fcntl.flock(f, fcntl.LOCK_EX); print('locked', flush=True); sys.stdin.read()",
                 str(tmp_path / "upstream.lock")],
                stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True,
            )
            try:
                assert process.stdout.readline().strip() == "locked"
                with pytest.raises(client.FutuError, match="coordination"):
                    client.refresh_tokens("id", "refresh", owner=owner)
                assert calls == ["/oauth2/token"]
            finally:
                process.stdin.close()
                assert process.wait(timeout=10) == 0
                process.stdout.close()
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()

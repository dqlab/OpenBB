"""Regression checks for redirect and sealed-evidence boundaries."""

# ruff: noqa: D103
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest
from openbb_futu.client import FutuError, refresh_tokens, snapshot
from openbb_futu.coverage import prepare
from test_offline_cli import invoke


@pytest.mark.parametrize("same_origin", [False, True])
def test_redirect_never_sends_credentials(tmp_path, monkeypatch, same_origin):
    received = []

    class Sink(BaseHTTPRequestHandler):
        def do_POST(self):
            received.append(
                (self.headers.get("Authorization"), self.rfile.read(int(self.headers.get("Content-Length", 0))))
            )
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"{}")

        def log_message(self, *args):
            pass

    sink = ThreadingHTTPServer(("127.0.0.1", 0), Sink)

    class Redirect(Sink):
        def do_POST(self):
            if self.path == "/sink":
                return super().do_POST()
            self.rfile.read(int(self.headers.get("Content-Length", 0)))
            self.send_response(307)
            port = origin.server_port if same_origin else sink.server_port
            self.send_header("Location", f"http://127.0.0.1:{port}/sink")
            self.end_headers()

    origin = ThreadingHTTPServer(("127.0.0.1", 0), Redirect)
    for server in (sink, origin):
        threading.Thread(target=server.serve_forever, daemon=True).start()
    monkeypatch.setenv("FUTU_TEST_HTTP_BASE", f"http://127.0.0.1:{origin.server_port}")
    monkeypatch.setenv("FUTU_COORDINATION_DIR", str(tmp_path / "coord"))
    try:
        for call in (lambda: refresh_tokens("client", "refresh-secret"), lambda: snapshot(["US.A"], "bearer-secret")):
            with pytest.raises(FutuError, match="redirect"):
                call()
        assert received == []
    finally:
        for server in (origin, sink):
            server.shutdown()
            server.server_close()


def sealed(tmp_path):
    root = tmp_path / "archive"
    source = tmp_path / "input"
    source.write_text('{"type":"QUOTE","symbol":"US.A","data":{"v":1}}\n')
    result = invoke("replay", "--input", source, "--output", root)
    assert result.returncode == 0, result.stderr
    manifest = json.loads(result.stdout)
    plan, _ = prepare(
        {"dimensions": [{"asset": "equity", "symbol": "US.A", "channel": "quote"}], "subscription_budget": 1}
    )
    (root / f"futu-plan-{manifest['run_id']}.json").write_text(json.dumps(plan))
    return root, manifest


def test_coverage_rejects_failed_manifest(tmp_path):
    root, manifest = sealed(tmp_path)
    path = Path(manifest["manifest"])
    data = json.loads(path.read_text())
    data.update(error="incomplete_receive", written_seq=0, durable_seq=1)
    path.write_text(json.dumps(data))
    assert invoke("verify", "--manifest", path).returncode != 0
    result = invoke("coverage", "--output", root, "--run-id", manifest["run_id"])
    assert result.returncode != 0
    assert json.loads(result.stdout)["dimensions"][0]["synced_rows"] == 0


def test_verify_detects_unlisted_entire_session(tmp_path):
    _, manifest = sealed(tmp_path)
    path = Path(manifest["manifest"])
    data = json.loads(path.read_text())
    data.update(files=[], received_frames=0, enqueued_frames=0, written_seq=0, durable_seq=0)
    path.write_text(json.dumps(data))
    assert invoke("verify", "--manifest", path).returncode != 0


def test_empty_and_multisegment_and_session_isolation(tmp_path):
    from openbb_futu.archive import Archive

    root = tmp_path / "archive"
    empty = Archive(root, run_id="empty").close()
    assert invoke("verify", "--manifest", empty["manifest"]).returncode == 0
    archive = Archive(root, run_id="multi", max_bytes=1)
    for _ in range(2):
        assert archive.accept('{"type":"QUOTE","data":{"v":1}}', "c")
    multi = archive.close()
    assert len(multi["files"]) == 2
    assert invoke("verify", "--manifest", multi["manifest"]).returncode == 0
    assert invoke("verify", "--manifest", empty["manifest"]).returncode == 0
    path = Path(multi["manifest"])
    data = json.loads(path.read_text())
    data["files"][0]["file"] = str(Path(empty["manifest"]).parent / ".." / "outside.jsonl")
    path.write_text(json.dumps(data))
    assert invoke("verify", "--manifest", path).returncode != 0


def test_symlink_segment_rejected(tmp_path):
    _, manifest = sealed(tmp_path)
    segment = Path(manifest["files"][0]["file"])
    target = tmp_path / "elsewhere"
    segment.rename(target)
    segment.symlink_to(target)
    assert invoke("verify", "--manifest", manifest["manifest"]).returncode != 0

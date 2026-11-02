"""Regression cases exercised through the REST sampler and local HTTP server."""

import base64
import gzip
import http.client
import json
import threading
from contextlib import suppress
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest
from openbb_futu.sampled_snapshot import run
from test_sampled_snapshot import server as local_server  # noqa: F401


def test_gzip_application_payload_records_content_encoding(monkeypatch, tmp_path):
    """The real sampler identifies the decoded body and the source encoding."""
    payload = b'{"ret_code":0,"data":{"snapshot_list":[{"code":"HK.00700"}]}}'

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):  # noqa: N802, D102
            self.rfile.read(int(self.headers["Content-Length"]))
            self.send_response(200)
            self.send_header("Content-Encoding", "gzip")
            self.end_headers()
            self.wfile.write(gzip.compress(payload))

        def log_message(self, *args):
            pass

    http = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=http.serve_forever, daemon=True)
    thread.start()
    monkeypatch.setenv("FUTU_TEST_HTTP_BASE", f"http://127.0.0.1:{http.server_port}")
    monkeypatch.setenv("FUTU_COORDINATION_DIR", str(tmp_path / "coord"))
    try:
        result = run({"symbols": [{"asset": "equity", "symbol": "HK.00700"}]}, "test-token", tmp_path / "out")
        record = json.loads(Path(result["manifest"]["files"][0]["file"]).read_text())
        assert record["http_content_encoding"] == "gzip"
        assert record["payload_representation"] == "http_content_decoded"
        assert base64.b64decode(record["raw_payload"]) == payload
    finally:
        http.shutdown()
        thread.join()


def test_sensitive_response_retains_request_metadata(local_server, tmp_path):  # noqa: F811
    """Reject a sensitive body while retaining non-secret request evidence."""
    local_server.body = b'{"access_token":"test-token"}'
    result = run({"symbols": [{"asset": "equity", "symbol": "HK.00700"}]}, "test-token", tmp_path / "out")
    assert result["failure"] == "sensitive_response"
    attempt = result["attempts"][0]
    assert attempt["request_id"] and attempt["started_at_utc"]
    assert attempt["http_status"] == 200 and attempt["received_bytes"] == len(local_server.body)
    assert attempt["body"] is None and attempt["durability"] == "unknown"
    assert "test-token" not in json.dumps(result)


def test_http_debug_does_not_disclose_token(local_server, tmp_path, capfd):  # noqa: F811
    """Refuse to send credentials when the global HTTP debugger is enabled."""
    original = http.client.HTTPConnection.debuglevel
    try:
        http.client.HTTPConnection.debuglevel = 1
        result = run({"symbols": [{"asset": "equity", "symbol": "HK.00700"}]}, "test-token", tmp_path / "out")
    finally:
        http.client.HTTPConnection.debuglevel = original
    out, err = capfd.readouterr()
    assert "test-token" not in out + err
    assert result["failure"] == "unsafe_http_debug"
    assert local_server.requests == 0


def test_rest_coverage_rebuild_rejects_tampering(local_server, tmp_path):  # noqa: F811
    """Recompute from sealed REST bytes and reject a modified segment."""
    from openbb_futu.sampled_snapshot import rebuild_rest_coverage

    result = run({"symbols": [{"asset": "equity", "symbol": "HK.00700"}]}, "test-token", tmp_path / "out")
    manifest = tmp_path / "out" / "futu-manifests" / (result["manifest"]["session_id"] + ".json")
    assert rebuild_rest_coverage(manifest, ["HK.00700"]) == {"HK.00700": "returned_code"}
    segment = Path(result["manifest"]["files"][0]["file"])
    segment.write_bytes(segment.read_bytes() + b" ")
    with pytest.raises(ValueError, match="manifest"):
        rebuild_rest_coverage(manifest, ["HK.00700"])


@pytest.mark.parametrize(
    ("encoding", "entity", "failure"),
    [
        ("identity", b'{"x":"' + b"a" * 1048576 + b'"}', "encoded_response_too_large"),
        ("gzip", gzip.compress(b'{"x":"' + b"a" * 1048576 + b'"}'), "decoded_response_too_large"),
        ("gzip", b"not gzip", "invalid_content_encoding"),
        ("gzip", gzip.compress(b"{}")[:-2], "invalid_content_encoding"),
        ("gzip", gzip.compress(b"{}") + gzip.compress(b"{}"), "invalid_content_encoding"),
        ("gzip", gzip.compress(b"{}") + b"extra", "invalid_content_encoding"),
        ("gzip, identity", b"{}", "unsupported_content_encoding"),
        ("br", b"{}", "unsupported_content_encoding"),
    ],
)
def test_stream_limits_and_invalid_encodings(monkeypatch, tmp_path, encoding, entity, failure):
    """Failures retain bounded byte evidence without publishing response bodies."""
    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):  # noqa: N802, D102
            self.rfile.read(int(self.headers["Content-Length"]))
            self.send_response(200)
            self.send_header("Content-Encoding", encoding)
            self.send_header("Content-Length", str(len(entity)))
            self.end_headers()
            try:
                for offset in range(0, len(entity), 4093):
                    self.wfile.write(entity[offset:offset + 4093])
            except (BrokenPipeError, ConnectionResetError):
                pass

        def log_message(self, *args):
            pass

    http = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=http.serve_forever, daemon=True)
    thread.start()
    monkeypatch.setenv("FUTU_TEST_HTTP_BASE", f"http://127.0.0.1:{http.server_port}")
    monkeypatch.setenv("FUTU_COORDINATION_DIR", str(tmp_path / "coord"))
    try:
        result = run({"symbols": [{"asset": "equity", "symbol": "HK.00700"}]}, "test-token", tmp_path / "out")
        assert result["failure"] == failure
        attempt = result["attempts"][0]
        assert attempt["request_id"] and attempt["http_status"] == 200
        assert attempt["encoded_bytes"] is not None and attempt["decoded_bytes"] is not None
        assert attempt["body"] is None and attempt["durability"] == "unknown"
        assert result["coverage"]["HK.00700"] == "unknown"
        assert "test-token" not in json.dumps(result)
    finally:
        http.shutdown()
        thread.join()


@pytest.mark.parametrize("chunk_size", [1, 17, 65536])
@pytest.mark.parametrize("encoding", ["identity", "gzip"])
def test_chunked_entity_preserves_payload(monkeypatch, tmp_path, chunk_size, encoding):
    """Transfer chunk boundaries do not change the exact decoded archived bytes."""
    payload = b'{ "ret_code": 0, "data": {"snapshot_list": [{"code": "HK.00700"}]}}'
    entity = gzip.compress(payload) if encoding == "gzip" else payload

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):  # noqa: N802, D102
            self.rfile.read(int(self.headers["Content-Length"]))
            self.send_response(200)
            self.send_header("Content-Encoding", encoding)
            self.send_header("Transfer-Encoding", "chunked")
            self.end_headers()
            for offset in range(0, len(entity), chunk_size):
                chunk = entity[offset:offset + chunk_size]
                self.wfile.write(f"{len(chunk):x}\r\n".encode() + chunk + b"\r\n")
            self.wfile.write(b"0\r\n\r\n")

        def log_message(self, *args):
            pass

    http = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=http.serve_forever, daemon=True)
    thread.start()
    monkeypatch.setenv("FUTU_TEST_HTTP_BASE", f"http://127.0.0.1:{http.server_port}")
    monkeypatch.setenv("FUTU_COORDINATION_DIR", str(tmp_path / "coord"))
    try:
        result = run({"symbols": [{"asset": "equity", "symbol": "HK.00700"}]}, "test-token", tmp_path / "out")
        assert result["failure"] is None
        record = json.loads(Path(result["manifest"]["files"][0]["file"]).read_text())
        assert base64.b64decode(record["raw_payload"]) == payload
        assert record["http_content_encoding"] == encoding
    finally:
        http.shutdown()
        thread.join()


@pytest.mark.parametrize("oversize", [False, True])
def test_sampler_redirect_does_not_forward_credentials(monkeypatch, tmp_path, oversize):
    """The spawned HTTP client never follows a redirect carrying its bearer token."""
    hits = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):  # noqa: N802, D102
            hits.append((self.path, self.headers.get("Authorization")))
            self.rfile.read(int(self.headers["Content-Length"]))
            self.send_response(307)
            self.send_header("Location", f"http://127.0.0.1:{self.server.server_port}/leak")
            self.end_headers()
            with suppress(BrokenPipeError, ConnectionResetError):
                self.wfile.write(b"x" * 1048577 if oversize else b"{}")

        def log_message(self, *args):
            pass

    http = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=http.serve_forever, daemon=True)
    thread.start()
    monkeypatch.setenv("FUTU_TEST_HTTP_BASE", f"http://127.0.0.1:{http.server_port}")
    monkeypatch.setenv("FUTU_COORDINATION_DIR", str(tmp_path / "coord"))
    try:
        result = run({"symbols": [{"asset": "equity", "symbol": "HK.00700"}]}, "test-token", tmp_path / "out")
        assert result["failure"] == ("encoded_response_too_large" if oversize else "provider_failure")
        assert hits == [("/api/v1.0/quote/snapshot", "Bearer test-token")]
        assert "test-token" not in json.dumps(result)
    finally:
        http.shutdown()
        thread.join()

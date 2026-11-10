"""Real HTTP regressions for credential handling and raw evidence."""

import base64
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest
from openbb_futu import coordination, sampled_snapshot


@pytest.mark.parametrize("status", [200, 403])
@pytest.mark.parametrize("optional", ["", "A", "a", "long-synthetic-refresh-credential"])
def test_safe_body_preserved(tmp_path, monkeypatch, status, optional):
    """Keep safe response bytes and omit credentials from evidence."""
    body = (b'{"ret_code":0,"data":{"snapshot_list":[{"code":"US.AAPL",'
            b'"name":"Apple"}]}}' if status == 200 else b'{"error":"access denied"}')
    calls = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            calls.append(self.rfile.read(int(self.headers["Content-Length"])))
            self.send_response(status)
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    monkeypatch.setenv("FUTU_TEST_HTTP_BASE", f"http://127.0.0.1:{server.server_port}")
    monkeypatch.setenv("FUTU_COORDINATION_DIR", str(tmp_path / "coord"))
    monkeypatch.setattr(coordination, "BUDGET_WINDOW", 0.02)
    try:
        out = tmp_path / "out"
        token = "synthetic-access-token-7349"  # noqa: S105
        result = sampled_snapshot.run(
            {"symbols": [{"asset": "equity", "symbol": "US.AAPL"}]}, token, out, secrets=(optional,)
        )
        assert result["failure"] == (None if status == 200 else "provider_failure")
        assert len(calls) == 1
        record = json.loads(Path(result["manifest"]["files"][0]["file"]).read_text().splitlines()[0])
        assert base64.b64decode(record["raw_payload"]) == body
        for path in out.rglob("*"):
            if path.is_file():
                data = path.read_bytes()
                assert token.encode() not in data
                if len(optional) >= 8:
                    assert optional.encode() not in data
    finally:
        server.shutdown()
        thread.join()
        server.server_close()


@pytest.mark.parametrize("symbol", ["US.AAPL/../../secret", "US.AAPL\n", "US.secret token", "US.AAPL?token=x"])
def test_invalid_target_no_output(tmp_path, symbol):
    """Reject invalid symbols before creating evidence."""
    out = tmp_path / "out"
    with pytest.raises(ValueError, match="invalid symbol"):
        sampled_snapshot.run({"symbols": [{"asset": "equity", "symbol": symbol}]}, "synthetic-secret-token", out)
    assert not out.exists()


def test_credential_equal_to_target_rejected(tmp_path):
    """Reject an exact conflict before creating evidence."""
    out = tmp_path / "out"
    with pytest.raises(ValueError, match="target conflicts"):
        sampled_snapshot.run({"symbols": [{"asset": "equity", "symbol": "US.AAPL"}]}, "US.AAPL", out)
    assert not out.exists()


@pytest.mark.parametrize("status", [200, 403])
@pytest.mark.parametrize("secret,numeric", [("A", False), ("long-synthetic-secret-58391", False), ("123456789", True)])
def test_secret_response_is_blocked(tmp_path, monkeypatch, status, secret, numeric):
    """Never persist a response containing a detected secret."""
    body = json.dumps({"message": int(secret) if numeric else secret}).encode()
    calls = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            calls.append(self.rfile.read(int(self.headers["Content-Length"])))
            self.send_response(status)
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    monkeypatch.setenv("FUTU_TEST_HTTP_BASE", f"http://127.0.0.1:{server.server_port}")
    monkeypatch.setenv("FUTU_COORDINATION_DIR", str(tmp_path / "coord"))
    monkeypatch.setattr(coordination, "BUDGET_WINDOW", 0.02)
    try:
        out = tmp_path / "out"
        result = sampled_snapshot.run(
            {"symbols": [{"asset": "equity", "symbol": "US.AAPL"}]},
            "synthetic-access-token-7349", out, secrets=(secret,),
        )
        assert len(calls) == 1
        assert result["failure"] == "sensitive_response"
        assert result["manifest"]["durable_seq"] == 0
        for path in out.rglob("*"):
            if path.is_file():
                assert body not in path.read_bytes()
                if len(secret) >= 8:
                    assert secret.encode() not in path.read_bytes()
    finally:
        server.shutdown()
        thread.join()
        server.server_close()


def test_long_secret_embedded_in_targets_rejected(tmp_path):
    """Neither sampling nor discovery may persist a secret in target metadata."""
    from openbb_futu.discovery import discover

    secret = "SYNTHETICSECRET"  # noqa: S105
    symbol = "US." + secret
    for action, asset in ((sampled_snapshot.run, "equity"), (discover, "option")):
        out = tmp_path / asset
        entries = [{"asset": asset, "symbol": symbol}]
        with pytest.raises(ValueError, match="target conflicts"):
            if asset == "equity":
                action({"symbols": entries}, secret, out)
            else:
                action(entries, secret, out)
        assert not out.exists()


def test_websocket_credential_fields_and_short_values():
    """Protect optional client secrets while keeping ordinary short data."""
    from openbb_futu.contracts import business_frame

    def wrap(data, secrets):
        return business_frame(json.dumps({"type": "QUOTE", "data": data}), run_id="r",
                              connection_id="c", local_seq=1, active_secrets=secrets)

    assert wrap({"name": "Apple"}, ("A",))
    with pytest.raises(ValueError, match="credential material"):
        wrap({"client_secret": "value"}, ())
    with pytest.raises(ValueError, match="credential material"):
        wrap({"other": 123456789}, ("123456789",))

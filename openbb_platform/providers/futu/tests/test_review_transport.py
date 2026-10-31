"""Offline transport checks using an actual loopback WebSocket."""

import asyncio
import json
import logging
import time

from openbb_futu.collector import Collector
from websockets.asyncio.client import connect
from websockets.asyncio.server import serve


def test_debug_logging_and_buffered_shutdown(tmp_path, monkeypatch, caplog):
    """Exercise the reader, close handshake and all propagating DEBUG loggers."""
    monkeypatch.setenv("FUTU_COORDINATION_DIR", str(tmp_path / "coord"))
    secret = "synthetic-token-never-log-8765"  # noqa: S105 -- synthetic credential
    caplog.set_level(logging.DEBUG)

    class Store:
        def read(self):
            return {
                "access_token": secret,
                "refresh_token": "synthetic-refresh",
                "client_id": "synthetic-client",
                "expires_at": time.time() + 3600,
            }

    async def run():
        async def handler(ws):
            async for raw in ws:
                msg = json.loads(raw)
                if msg["action"] == "auth":
                    await ws.send(json.dumps({"id": msg["id"], "session_id": "session"}))
                elif msg["action"] == "subscribe":
                    await ws.send(json.dumps({"id": msg["id"], "code": 0}))
                    for i in range(25):
                        await ws.send(json.dumps({"type": "QUOTE", "data": {"n": i}}))
                    collector.stop.set()
                    await ws.wait_closed()

        # Suppress the server protocol's own DEBUG frame logging: it receives the
        # credential, unlike client-side libraries under test.
        server_logger = logging.Logger("test.server", level=logging.CRITICAL + 1)
        server_logger.disabled = True
        async with serve(handler, "127.0.0.1", 0, logger=server_logger) as server:
            port = server.sockets[0].getsockname()[1]

            def factory(url, **kwargs):
                assert url == "wss://webapi-quote.futunn.com/ws"
                return connect(f"ws://127.0.0.1:{port}", **kwargs)

            collector = Collector(
                {"channels": {"quote": ["US.TEST"]}}, Store(), tmp_path / "out", connection_factory=factory
            )
            status = await collector.run()
            return status

    assert asyncio.run(run()) == 0
    manifest = json.loads(next((tmp_path / "out" / "futu-manifests").glob("*.json")).read_text())
    assert manifest["received_frames"] == manifest["durable_seq"] == 25
    assert secret not in caplog.text
    events = next((tmp_path / "out").glob("futu-events-*.jsonl")).read_text()
    assert '"status": "unknown"' in events

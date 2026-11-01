"""Real loopback collector and persistent archive, through a test-only entry."""

# ruff: noqa: D103
import asyncio
import json
import time
from pathlib import Path

from openbb_futu.collector import Collector
from openbb_futu.offline import verify
from websockets.asyncio.client import connect
from websockets.asyncio.server import serve


def test_collector_loopback_persists_duplicate_callbacks(tmp_path, monkeypatch):
    monkeypatch.setenv("FUTU_COORDINATION_DIR", str(tmp_path / "coord"))
    monkeypatch.setattr("openbb_futu.coordination.BUDGET_WINDOW", 0.05)

    class Store:
        def read(self):
            return {"access_token": "synthetic", "refresh_token": "refresh",
                    "client_id": "client", "expires_at": time.time() + 3600}

    async def run():
        async def handler(ws):
            async for raw in ws:
                command = json.loads(raw)
                if command["action"] == "auth":
                    await ws.send(json.dumps({"id": command["id"], "session_id": "s"}))
                elif command["action"] == "subscribe":
                    await ws.send(json.dumps({"id": command["id"], "code": 0}))
                    frame = json.dumps({"type": "QUOTE", "symbol": "US.A", "data": {"price": 42}})
                    await ws.send(frame)
                    await ws.send(frame)
                    await asyncio.sleep(0.1)
                    collector.stop.set()
                    await ws.wait_closed()

        async with serve(handler, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            def factory(url, **kwargs):
                return connect(f"ws://127.0.0.1:{port}", **kwargs)
            collector = Collector._isolated_for_test(
                {"channels": {"quote": ["US.A"]}}, Store(), tmp_path / "out",
                connection_factory=factory,
            )
            return await collector.run(), collector

    status, collector = asyncio.run(run())
    assert status == 0
    event_path = tmp_path / "out" / f"futu-events-{collector.run_id}.jsonl"
    events = [json.loads(line) for line in event_path.read_text().splitlines()]
    assert any(e["event"] == "stopped" for e in events)
    manifest = next((tmp_path / "out" / "futu-manifests").glob("*.json"))
    records = []
    assert verify(manifest, on_record=records.append)["status"] == "verified"
    assert len(records) == 2
    assert records[0]["raw_payload"] == records[1]["raw_payload"]
    assert [record["local_seq"] for record in records] == [1, 2]
    assert Path(next(e["manifest"] for e in events if e["event"] == "archive_closed")).is_absolute() is False


def test_event_log_rejects_existing_symlink(tmp_path):
    import pytest
    from openbb_futu.collector import _EventLog
    from openbb_futu.root_handle import RootHandle

    target = tmp_path / "target"
    target.write_text("unchanged")
    (tmp_path / ("futu-events-" + "a" * 32 + ".jsonl")).symlink_to(target)
    with RootHandle(tmp_path) as root, pytest.raises(OSError):
        _EventLog(root, "a" * 32)
    assert target.read_text() == "unchanged"


def test_event_log_poisoned_after_fsync_failure(tmp_path, monkeypatch):
    import errno
    import os

    import pytest
    from openbb_futu.collector import _EventLog
    from openbb_futu.root_handle import RootHandle

    with RootHandle(tmp_path) as root:
        log = _EventLog(root, "b" * 32)
        original = os.fsync
        def fail(fd):
            if fd == log.fd:
                raise OSError(errno.EIO, "injected")
            return original(fd)
        monkeypatch.setattr(os, "fsync", fail)
        with pytest.raises(OSError):
            log.append({"event": "first"})
        with pytest.raises(RuntimeError):
            log.append({"event": "second"})
        log.close()
    lines = (tmp_path / ("futu-events-" + "b" * 32 + ".jsonl")).read_text().splitlines()
    assert [json.loads(line)["event"] for line in lines] == ["first"]


def test_event_log_parent_replacement_fails_closed(tmp_path):
    import pytest
    from openbb_futu.collector import _EventLog
    from openbb_futu.read_budget import EvidenceChanged
    from openbb_futu.root_handle import RootHandle

    directory = tmp_path / "root"
    directory.mkdir()
    with RootHandle(directory) as root:
        log = _EventLog(root, "c" * 32)
        directory.rename(tmp_path / "moved")
        directory.mkdir()
        with pytest.raises(EvidenceChanged):
            log.append({"event": "should_not_write"})
        log.close()
    assert not list(directory.iterdir())
    assert next((tmp_path / "moved").iterdir()).read_bytes() == b""

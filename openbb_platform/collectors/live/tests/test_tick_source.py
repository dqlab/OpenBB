"""Contract tests for finite push sources."""

import json
import threading
import time

from dq_live_market_data_collector.tick_source import ReplaySource


def test_replay_preserves_identical_callbacks_and_eof(tmp_path):
    fixture = tmp_path / "input.jsonl"
    row = {"InstrumentID": "a", "UpdateMillisec": 7}
    fixture.write_text((json.dumps(row) + "\n") * 3)
    rows, events = [], []
    source = ReplaySource(fixture, lambda row: rows.append(row) or True,
                          lambda kind, data: events.append(kind))
    source.start()
    assert source.done.wait(2)
    assert source.stop_and_status()["state"] == "eof"
    assert source.close()
    assert rows == [row] * 3
    assert source.status()["callbacks"] == 3
    assert events == ["source_started", "source_end"]


def test_replay_failure_and_stop_are_consistent(tmp_path):
    fixture = tmp_path / "input.jsonl"
    fixture.write_text('{}\ninvalid\n')
    source = ReplaySource(fixture, lambda _: True, lambda *_: None)
    source.start()
    assert source.done.wait(2)
    assert source.failed.is_set()
    assert source.stop_and_status()["state"] == "failed"
    assert source.close()

    fixture.write_text('{}\n' * 100)
    entered, release = threading.Event(), threading.Event()
    def callback(_):
        entered.set()
        release.wait(2)
        return True
    source = ReplaySource(fixture, callback, lambda *_: None)
    source.start()
    assert entered.wait(2)
    assert source.stop_and_status()["state"] == "stopping"
    release.set()
    assert source.close()
    assert source.status()["state"] == "stopped"


def test_blocked_callback_close_deadline_and_distinct_records(tmp_path):
    fixture = tmp_path / "input.jsonl"
    fixture.write_text('{"value": 1}\n{"value": 2}\n')
    entered, release = threading.Event(), threading.Event()
    rows = []

    def callback(row):
        rows.append(row)
        entered.set()
        release.wait()
        return True

    source = ReplaySource(fixture, callback, lambda *_: None)
    source.start()
    assert entered.wait(2)
    try:
        before = time.monotonic()
        assert not source.close(0.05)
        assert time.monotonic() - before < 0.3
        assert source.failed.is_set()
        assert source.status()["state"] == "failed"
        assert source.status()["callbacks"] == 0
        assert not source.close(0.01)
    finally:
        release.set()
    assert source.done.wait(2)
    assert source.close(1)
    assert source.status()["state"] == "failed"
    assert source.status()["callbacks"] == 1
    assert rows == [{"value": 1}]


def test_replay_delivers_distinct_objects(tmp_path):
    fixture = tmp_path / "input.jsonl"
    fixture.write_text('{"value": 1}\n{"value": 1}\n')
    rows = []
    source = ReplaySource(fixture, lambda row: rows.append(row) or True, lambda *_: None)
    source.start()
    assert source.done.wait(2)
    assert source.close()
    assert len(rows) == 2 and rows[0] == rows[1] and rows[0] is not rows[1]


def test_source_registry_rejects_missing_and_duplicate(monkeypatch):
    import pytest

    from dq_live_market_data_collector.tick_source import load_source

    monkeypatch.setattr("importlib.metadata.entry_points", lambda **_: [])
    with pytest.raises(RuntimeError, match="exactly one"):
        load_source("replay")
    monkeypatch.setattr("importlib.metadata.entry_points", lambda **_: [object(), object()])
    with pytest.raises(RuntimeError, match="exactly one"):
        load_source("ctp")

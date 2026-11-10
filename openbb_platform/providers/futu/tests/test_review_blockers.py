# ruff: noqa: D103
"""Regression checks for conservative failure boundaries."""

import asyncio
import json

import pytest
from openbb_futu.archive import Archive, recover_open_files
from openbb_futu.collector import Collector
from openbb_futu.contracts import business_frame


def test_secrets_in_unknown_business_values_fail_closed():
    for message in ("Bearer example", "prefix active-secret suffix"):
        raw = json.dumps({"type": "UNKNOWN", "data": {"message": message}})
        with pytest.raises(ValueError):
            business_frame(raw, run_id="r", connection_id="c", local_seq=1, active_secrets=("active-secret",))
    raw = json.dumps({"type": "UNKNOWN", "data": {"message": "ordinary"}})
    assert business_frame(raw, run_id="r", connection_id="c", local_seq=1)["raw_payload"] == raw


def test_sealed_without_manifest_is_discovered(tmp_path):
    archive = Archive(tmp_path, run_id="interrupted")
    assert archive.accept('{"type":"QUOTE","data":{}}', "connection")
    archive.stop.set()
    archive.thread.join(5)
    paths = list(tmp_path.glob("source_id=futu/recv_date=*/session=*/*.jsonl"))
    original = paths[0].read_bytes()
    report = recover_open_files(tmp_path)[0]
    assert report["run_ids"] == ["interrupted"]
    assert report["gap"] == "unresolved_after_process_interruption"
    assert paths[0].read_bytes() == original


def test_stop_during_backoff_fails_and_reports(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("FUTU_COORDINATION_DIR", str(tmp_path / "coord"))
    collector = Collector({"backoff_base": 60}, None, tmp_path / "out")

    async def fail(owner):
        raise OSError("connection failed")

    collector.session = fail

    async def run():
        task = asyncio.create_task(collector.run())
        for _ in range(100):
            await asyncio.sleep(0.01)
            if collector.gap:
                break
        collector.stop.set()
        return await task

    assert asyncio.run(run()) == 1
    assert "gap_unresolved" in (tmp_path / "out" / f"futu-events-{collector.run_id}.jsonl").read_text()
    assert "failed" in capsys.readouterr().err


def test_event_write_failure_still_reports_stderr(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("FUTU_COORDINATION_DIR", str(tmp_path / "coord"))
    collector = Collector({}, None, tmp_path / "out")

    def disk_full(*args, **kwargs):
        raise OSError(28, "disk full")

    collector.event = disk_full

    async def fail(owner):
        raise OSError(28, "disk full")

    collector.session = fail
    assert asyncio.run(collector.run()) != 0
    assert "collector failed" in capsys.readouterr().err

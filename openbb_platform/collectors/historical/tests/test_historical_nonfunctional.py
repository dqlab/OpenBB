import json
import time
import tracemalloc
from datetime import UTC, date, datetime, timedelta

import pytest
import yaml

from dq_historical_market_data_collector.config import load_config
from dq_historical_market_data_collector.runtime import Collector
from dq_historical_market_data_collector.storage import Journal


def test_scheduled_small_budget_makes_progress(config, client, now):
    config.collections["daily"].chunk_days = 1
    config.collections["daily"].refresh_days = 30
    config.max_chunks_per_session = 1
    with Journal(config.storage) as journal:
        collector = Collector(config, journal, client, clock=lambda: now)
        reports = [
            collector.collect(
                as_of=now.date(),
                scheduled_day=now.date(),
                refresh_recent=True,
            )
            for _ in range(3)
        ]
        assert [report["completed_chunks"] for report in reports] == [1, 1, 1]
        assert reports[-1]["status"] == "complete"
        assert len(client.calls) == 3


def test_reset_reloads_valid_config_and_rejects_storage_change(config_dict, client, now, tmp_path):
    path = tmp_path / "collector.yaml"
    path.write_text(yaml.safe_dump(config_dict))
    config = load_config(path)
    with Journal(config.storage) as journal:
        collector = Collector(config, journal, client, clock=lambda: now, config_path=path)
        collector.reset_if_due()
        config_dict["max_chunks_per_session"] = 7
        path.write_text(yaml.safe_dump(config_dict))
        collector.clock = lambda: now + timedelta(days=7)
        collector.reset_if_due()
        collector.reload_if_pending()
        assert collector.config.max_chunks_per_session == 7
        assert journal.get_meta("reload_pending") == "0"
        config_dict["storage"]["root"] = str(tmp_path / "different-data")
        path.write_text(yaml.safe_dump(config_dict))
        journal.set_meta("reload_pending", "1")
        with pytest.raises(ValueError):
            collector.reload_if_pending()
        assert collector.config.storage == config.storage


def test_bounded_batch_memory_throughput_and_export(config, client, now, record_property):
    # Two complete 24-hour minute days: bounded synthetic data, no provider connection.
    config.collections["daily"].frequency = "1m"
    config.collections["daily"].secondary = []
    config.collections["daily"].end_date = date(2026, 9, 3)
    config.instruments["aapl"].timezone = "UTC"
    config.storage.export_batch_size = 100
    start = datetime(2026, 9, 1, tzinfo=UTC)
    client.rows = [
        {
            "date": (start + timedelta(minutes=i)).isoformat(),
            "open": 100,
            "high": 105,
            "low": 98,
            "close": 103,
            "volume": 10,
        }
        for i in range(2880)
    ]
    tracemalloc.start()
    started = time.monotonic()
    try:
        with Journal(config.storage) as journal:
            report = Collector(config, journal, client, clock=lambda: now).collect()
            elapsed = time.monotonic() - started
            _, peak = tracemalloc.get_traced_memory()
            assert report["status"] == "complete"
            assert report["reconciliation"]["accepted"] == 2880
            assert journal.pending_count() == 0
            assert peak < 128 * 1024 * 1024 and elapsed < 60
            assert (
                max(
                    len(path.read_text().splitlines())
                    for path in (journal.root / "records").glob("*.jsonl")
                )
                <= 100
            )
            record_property("elapsed_seconds", elapsed)
            record_property("peak_python_bytes", peak)
            record_property("rows", 2880)
    finally:
        tracemalloc.stop()


def test_report_write_failure_replayed_after_restart(config, client, now, monkeypatch):
    with Journal(config.storage) as journal:
        collector = Collector(config, journal, client, clock=lambda: now)
        original = journal.write_reports

        def fail_pending_report():
            if journal.db.execute("SELECT 1 FROM sessions WHERE report_pending=1").fetchone():
                raise OSError("disk")
            original()

        monkeypatch.setattr(journal, "write_reports", fail_pending_report)
        with pytest.raises(OSError):
            collector.collect()
        assert (
            journal.db.execute("SELECT count(*) FROM sessions WHERE report_pending=1").fetchone()[0]
            == 1
        )
    with Journal(config.storage) as journal:
        journal.recover()
        assert (
            journal.db.execute("SELECT count(*) FROM sessions WHERE report_pending=1").fetchone()[0]
            == 0
        )
        reports = list((journal.root / "reports").glob("*.json"))
        assert len(reports) == 1 and json.loads(reports[0].read_text())["status"] == "complete"

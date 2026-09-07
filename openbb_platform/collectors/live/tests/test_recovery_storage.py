import csv
import json
from datetime import UTC, datetime, timedelta

import pytest
from conftest import Clock, FakeProvider

from dq_live_market_data_collector.config import CollectorConfig
from dq_live_market_data_collector.service import Collector
from dq_live_market_data_collector.storage import Journal, read_status


@pytest.mark.parametrize("output_format", ["sqlite", "jsonl", "csv", "parquet", "duckdb"])
def test_all_sinks_roundtrip_and_replay(raw_config, output_format):
    if output_format in {"parquet", "duckdb"}:
        pytest.importorskip({"parquet": "pyarrow", "duckdb": "duckdb"}[output_format])
    raw_config["storage"]["format"] = output_format
    config = CollectorConfig.model_validate(raw_config)
    clock = Clock()
    provider = FakeProvider(clock)
    with Journal(config.storage) as journal:
        service = Collector(config, journal, provider=provider, clock=clock)
        service.step()
        assert journal.db.execute("SELECT SUM(exported) FROM observations").fetchone()[0] == 2
        assert journal.export_pending() == 0
        if output_format == "sqlite":
            rows = [
                json.loads(r[0]) for r in journal.db.execute("SELECT payload FROM observations")
            ]
        elif output_format == "duckdb":
            import duckdb

            with duckdb.connect(str(journal.root / "observations.duckdb"), read_only=True) as db:
                rows = [
                    json.loads(r[0])
                    for r in db.execute("SELECT payload FROM observations").fetchall()
                ]
        else:
            paths = list(journal.root.glob(f"observations/*/*.{output_format}"))
            assert len(paths) == 1
            if output_format == "jsonl":
                rows = [json.loads(line) for line in paths[0].read_text().splitlines()]
            elif output_format == "csv":
                with paths[0].open() as stream:
                    rows = list(csv.DictReader(stream))
            else:
                import pyarrow.parquet as pq

                rows = pq.read_table(paths[0]).to_pylist()
        assert {row["symbol"] for row in rows} == {"AAPL", "SPY"}


def test_output_crash_replays_stable_batch_without_duplicate_files(raw_config, monkeypatch):
    raw_config["storage"]["format"] = "jsonl"
    config = CollectorConfig.model_validate(raw_config)
    clock = Clock()
    provider = FakeProvider(clock)
    with Journal(config.storage) as journal:
        service = Collector(config, journal, provider=provider, clock=clock)
        original = journal._materialize

        def crash_after_write(*args):
            original(*args)
            raise OSError("simulated crash before export acknowledgement")

        monkeypatch.setattr(journal, "_materialize", crash_after_write)
        service.step()
        assert len(list(journal.root.glob("observations/*/*.jsonl"))) == 1
        assert read_status(journal.root)["pending_output_records"] == 2
    with Journal(config.storage) as journal:
        service = Collector(config, journal, provider=provider, clock=clock)
        clock.advance(10)
        service.step()
        rows = [
            json.loads(line)
            for path in journal.root.glob("observations/*/*.jsonl")
            for line in path.read_text().splitlines()
        ]
        assert len(rows) == len({row["record_id"] for row in rows}) == 4
        assert read_status(journal.root)["pending_output_records"] == 0


def test_another_writer_is_rejected_and_lock_released(rig):
    config, _, _, journal, _ = rig
    with pytest.raises(RuntimeError, match="Another collector"):
        Journal(config.storage)
    journal.close()
    with Journal(config.storage):
        pass


def test_storage_format_change_does_not_reinterpret_old_outbox(rig):
    config, _, _, journal, service = rig
    service.step()
    journal.close()
    config.storage.format = "jsonl"
    with pytest.raises(ValueError, match="new storage root"):
        Journal(config.storage)


def test_outage_creates_missing_session_report_without_live_backfill(rig):
    _, clock, provider, journal, service = rig
    service.step()
    clock.advance(86400 + 60)
    reports = service.step()
    assert len(provider.calls) == 2
    assert len(reports) == 2
    assert sum(report["totals"]["expected_polls"] for report in reports) == 24
    assert sum(report["totals"]["missed_polls"] for report in reports) == 22
    assert sum(report["totals"]["input_rows"] for report in reports) == 2


@pytest.mark.parametrize(
    "period,after",
    [
        ("weekly", datetime(2026, 9, 14, 13, 30, tzinfo=UTC)),
        ("monthly", datetime(2026, 10, 1, 13, 30, tzinfo=UTC)),
    ],
)
def test_periodic_resets_preserve_data_and_are_persisted(rig, period, after):
    config, clock, provider, journal, service = rig
    config.reset.period = period
    service.step()
    clock.value = after
    service.step()
    assert provider.resets == 1
    assert journal.db.execute("SELECT COUNT(*) FROM observations").fetchone()[0] == 4
    state = json.loads(journal.get_meta("lifecycle"))
    assert state["generation"] == 1
    replacement = Collector(config, journal, provider=provider, clock=clock)
    replacement.step()
    assert provider.resets == 1
    assert len(list(journal.root.glob("resets/*.json"))) == 1


def test_reset_during_overnight_session_keeps_poll_checkpoints(raw_config):
    raw_config["collections"]["quotes"]["schedule"] = {
        "timezone": "UTC",
        "weekdays": list(range(7)),
        "start": "18:00",
        "end": "17:00",
    }
    raw_config["reset"] = {"period": "monthly", "timezone": "UTC"}
    config = CollectorConfig.model_validate(raw_config)
    clock = Clock(datetime(2026, 9, 30, 23, 59, 55, tzinfo=UTC))
    provider = FakeProvider(clock)
    with Journal(config.storage) as journal:
        service = Collector(config, journal, provider=provider, clock=clock)
        service.step()
        session_id = journal.running_sessions()[0]["id"]
        clock.advance(10)
        service.step()
        assert provider.resets == 1
        assert journal.running_sessions()[0]["id"] == session_id
        assert journal.db.execute("SELECT COUNT(*) FROM polls").fetchone()[0] == 4


def test_config_change_closes_old_session_without_discarding_history(rig):
    config, clock, provider, journal, service = rig
    service.step()
    config.collections["quotes"].frequency_seconds = 20
    replacement = Collector(config, journal, provider=provider, clock=clock)
    reports = replacement.step()
    assert reports[0]["reason"] == "configuration_changed"
    assert reports[0]["session_status"] == "interrupted"
    assert journal.db.execute("SELECT COUNT(*) FROM sessions").fetchone()[0] == 2


def test_long_outage_recovery_has_a_resumable_31_day_cursor(rig):
    _, clock, provider, journal, service = rig
    service.step()
    first = clock()
    clock.advance(100 * 86400)
    service.step()
    cursor = json.loads(journal.get_meta("calendar_cursor"))
    assert datetime.fromisoformat(cursor["at"]) == first + timedelta(days=31)
    assert journal.db.execute("SELECT COUNT(*) FROM sessions").fetchone()[0] <= 25


def test_stop_reports_and_closes_provider(rig):
    _, clock, provider, journal, service = rig
    service.step()
    clock.advance(1)
    service.shutdown()
    service.shutdown()
    assert provider.closed
    assert not journal.running_sessions()
    report = json.loads((journal.root / "reports" / "latest.json").read_text())
    assert report["session_status"] == "interrupted"
    assert report["totals"]["accepted"] == 2

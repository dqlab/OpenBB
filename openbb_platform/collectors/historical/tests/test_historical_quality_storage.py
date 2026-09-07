import json
from copy import deepcopy
from datetime import date, timedelta

import pytest

from dq_historical_market_data_collector.config import Calendar
from dq_historical_market_data_collector.planner import Chunk
from dq_historical_market_data_collector.quality import digest, event_time, normalize
from dq_historical_market_data_collector.runtime import Collector
from dq_historical_market_data_collector.storage import (
    Journal,
    atomic_json,
    read_records,
    read_status,
)


def normalized(config, rows, now):
    return normalize(
        rows,
        "yahoo",
        config.sources["yahoo"],
        config.instruments["aapl"],
        config.collections["daily"],
        Chunk("daily", "aapl", date(2026, 9, 1), date(2026, 9, 4)),
        now.isoformat(),
        "test_raw_hash",
    )


@pytest.mark.parametrize(
    "change,reason",
    [
        ({"close": None}, "missing_required_field"),
        ({"close": float("nan")}, "nonfinite_number"),
        ({"close": True}, "invalid_number"),
        ({"high": 90}, "invalid_ohlc"),
        ({"symbol": "MSFT"}, "symbol_mismatch"),
        ({"currency": "EUR"}, "currency_mismatch"),
        ({"date": "bad"}, "invalid_row"),
        ({"date": None}, "missing_timestamp"),
    ],
)
def test_invalid_rows_quarantined(config, rows, now, change, reason):
    rows[0].update(change)
    accepted, rejected, outside, coverage = normalized(config, rows, now)
    assert len(accepted) == 2 and rejected[0]["reason"] == reason and outside == 0
    assert coverage["status"] == "unknown"


def test_negative_future_prices_and_duplicate_conflicts(config, rows, now):
    config.instruments["aapl"].asset_type = "future"
    rows[0].update(open=-3, high=-1, low=-5, close=-2)
    rows += [dict(rows[0]), {**rows[0], "close": -4}, {**rows[0], "date": "2026-08-31"}]
    accepted, rejected, outside, _ = normalized(config, rows, now)
    assert len(accepted) == 4 and outside == 1
    assert rejected[0]["reason"] == "conflicting_response_duplicate"


def test_timezone_ambiguity_and_daily_dates(config):
    instrument = config.instruments["aapl"]
    source = config.sources["yahoo"]
    assert event_time("20260901", instrument, source, "1d") == "2026-09-01"
    assert event_time("2026-09-01T09:30:00", instrument, source, "1m") == (
        "2026-09-01T13:30:00+00:00"
    )
    for stamp in ("2026-11-01T01:30:00", "2026-03-08T02:30:00"):
        with pytest.raises(ValueError):
            event_time(stamp, instrument, source, "1m")


def test_calendar_missing_rows(config, rows, now):
    config.collections["daily"].calendar = Calendar()
    accepted, _, _, coverage = normalized(config, rows[:1], now)
    assert len(accepted) == 1
    assert coverage["missing"] == 2 and coverage["status"] == "gaps"


@pytest.mark.parametrize("fmt", ["jsonl", "csv", "parquet", "sqlite", "duckdb"])
def test_storage_round_trip_and_replay(config, client, now, fmt):
    if fmt in {"parquet", "duckdb"}:
        pytest.importorskip({"parquet": "pyarrow", "duckdb": "duckdb"}[fmt])
    config.storage.format = fmt
    with Journal(config.storage) as journal:
        report = Collector(config, journal, client, clock=lambda: now).collect()
        assert report["status"] == "complete"
        assert journal.pending_count() == 0
        assert journal.db.execute("SELECT count(*) FROM observations").fetchone()[0] == 3
        journal.export_pending()
        if fmt == "jsonl":
            exported = [
                json.loads(line)
                for path in (journal.root / "records").glob("*.jsonl")
                for line in path.read_text().splitlines()
            ]
            assert len(exported) == 3
        elif fmt == "parquet":
            import pyarrow.parquet as pq

            assert (
                sum(pq.read_table(p).num_rows for p in (journal.root / "records").glob("*.parquet"))
                == 3
            )
        elif fmt == "csv":
            import csv

            assert (
                sum(
                    len(list(csv.DictReader(p.read_text().splitlines())))
                    for p in (journal.root / "records").glob("*.csv")
                )
                == 3
            )
        elif fmt == "duckdb":
            import duckdb

            with duckdb.connect(str(journal.root / "historical.duckdb")) as db:
                assert db.execute("SELECT count(*) FROM observations").fetchone()[0] == 3
    assert len(read_records(config.storage.root, instrument_id="aapl", as_of=now)) == 3


def test_corrections_and_reversions_preserve_asof(config, client, now):
    with Journal(config.storage) as journal:
        collector = Collector(config, journal, client, clock=lambda: now)
        collector.collect()
        client.rows[0]["close"] = 104
        collector.clock = lambda: now + timedelta(hours=1)
        collector.collect(refresh=True)
        client.rows[0]["close"] = 103
        collector.clock = lambda: now + timedelta(hours=2)
        collector.collect(refresh=True)
        collector.collect(refresh=True)
        assert journal.db.execute("SELECT count(*) FROM observations").fetchone()[0] == 5
    for offset, expected in ((0, 103), (1, 104), (2, 103)):
        result = read_records(
            config.storage.root,
            instrument_id="aapl",
            as_of=now + timedelta(hours=offset),
        )
        assert result[0]["values"]["close"] == expected
    assert (
        read_records(config.storage.root, instrument_id="aapl", as_of=now - timedelta(seconds=1))
        == []
    )


def test_export_failure_recovers_without_refetch(config, client, now, monkeypatch):
    with Journal(config.storage) as journal:
        collector = Collector(config, journal, client, clock=lambda: now)
        original = journal._write_batch
        monkeypatch.setattr(
            journal, "_write_batch", lambda *_: (_ for _ in ()).throw(OSError("disk"))
        )
        report = collector.collect()
        assert report["status"] == "storage_or_runtime_error"
        assert journal.pending_count() == 3
        monkeypatch.setattr(journal, "_write_batch", original)
    with Journal(config.storage) as journal:
        report = Collector(config, journal, client, clock=lambda: now).collect()
        assert report["resumed_chunks"] == 1 and journal.pending_count() == 0
        assert len(client.calls) == 1


def test_atomic_evidence_and_lock(config):
    with Journal(config.storage) as journal:
        payload = {"rows": [{"date": "2026-09-01"}]}
        raw_hash = journal.archive(payload)
        assert raw_hash == digest(payload)
        assert journal.archive(deepcopy(payload)) == raw_hash
        with pytest.raises(RuntimeError):
            Journal(config.storage)
        path = journal.root / "immutable.json"
        atomic_json(path, {"a": 1}, True)
        with pytest.raises(ValueError):
            atomic_json(path, {"a": 2}, True)
    config.storage.format = "sqlite"
    with pytest.raises(ValueError):
        Journal(config.storage)


def test_status_is_bounded_and_readonly(config, client, now):
    assert read_status(config.storage.root) == []
    assert not config.storage.root.exists()
    with Journal(config.storage) as journal:
        report = Collector(config, journal, client, clock=lambda: now).collect()
        assert report["reconciliation"] == {
            "input_rows": 3,
            "accepted": 3,
            "duplicates": 0,
            "outside": 0,
            "rejected": 0,
        }
    assert len(read_status(config.storage.root, 1)) == 1
    with pytest.raises(ValueError):
        read_status(config.storage.root, 1001)
    with pytest.raises(ValueError):
        read_records(config.storage.root, instrument_id="aapl", as_of=now, limit=0)

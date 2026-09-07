from datetime import UTC, date, datetime, time

import pytest

from dq_historical_market_data_collector.config import Calendar
from dq_historical_market_data_collector.planner import Chunk
from dq_historical_market_data_collector.providers import request_parameters
from dq_historical_market_data_collector.quality import normalize
from dq_historical_market_data_collector.runtime import Collector
from dq_historical_market_data_collector.storage import Journal


def test_overnight_intraday_request_covers_next_calendar_day(config):
    collection = config.collections["daily"]
    collection.frequency = "1h"
    collection.primary = "ibkr"
    collection.secondary = []
    collection.calendar = Calendar(start=time(22), end=time(2))
    instrument = config.instruments["aapl"]
    instrument.timezone = "UTC"
    source = config.sources["ibkr"]
    chunk = Chunk("daily", "aapl", date(2026, 9, 1), date(2026, 9, 2))
    params = request_parameters("ibkr", source, instrument, collection, chunk)
    assert params["end_datetime"] == "2026-09-02T02:00:00+00:00"
    rows = [
        {"date": stamp, "open": 1, "high": 2, "low": 1, "close": 2}
        for stamp in (
            "2026-09-01T22:00:00+00:00",
            "2026-09-01T23:00:00+00:00",
            "2026-09-02T00:00:00+00:00",
            "2026-09-02T01:00:00+00:00",
        )
    ]
    accepted, rejected, outside, coverage = normalize(
        rows,
        "ibkr",
        source,
        instrument,
        collection,
        chunk,
        "2026-09-03T00:00:00+00:00",
        "raw",
    )
    assert len(accepted) == 4 and not rejected and not outside
    assert coverage["status"] == "complete"


def test_after_close_schedule_can_include_current_session(config, client):
    now = datetime(2026, 9, 7, 21, tzinfo=UTC)
    config.schedule.at = time(16, 30)
    config.schedule.include_current_session = True
    collection = config.collections["daily"]
    collection.start_date = date(2026, 9, 7)
    collection.end_date = None
    client.rows = [{"date": "2026-09-07", "open": 1, "high": 2, "low": 1, "close": 2}]
    with Journal(config.storage) as journal:
        collector = Collector(config, journal, client, clock=lambda: now)
        collector.run(max_seconds=0.2)
        assert len(client.calls) == 1
        assert client.calls[0][2].end == date(2026, 9, 8)
        with pytest.raises(ValueError):
            collector.collect(as_of=date(2026, 9, 8))


def test_local_scheduler_date_ahead_of_utc(config, client):
    config.schedule.timezone = "Asia/Tokyo"
    now = datetime(2026, 9, 7, 23, tzinfo=UTC)
    with Journal(config.storage) as journal:
        collector = Collector(config, journal, client, clock=lambda: now)
        report = collector.collect(as_of=date(2026, 9, 8))
        assert report["status"] == "complete"


def test_parquet_nullable_metadata_types_stay_stable(config, client, now):
    pq = pytest.importorskip("pyarrow.parquet")

    config.storage.format = "parquet"
    with Journal(config.storage) as journal:
        collector = Collector(config, journal, client, clock=lambda: now)
        collector.collect()
        config.instruments["aapl"].con_id = 265598
        collector.collect(refresh=True)
        schemas = [pq.read_schema(path) for path in (journal.root / "records").glob("*.parquet")]
        assert all(
            schema.field("con_id").type == schemas[0].field("con_id").type for schema in schemas
        )
        assert all(
            str(schema.field("historical_published_at").type) == "string" for schema in schemas
        )


@pytest.mark.parametrize(
    "row",
    [
        {"date": "2026-09-01", "high": 1, "low": 2},
        {"date": "2026-09-01", "high": 2, "low": -1},
        {"date": "2026-09-01", "high": 2, "low": 1, "asset_type": "option"},
    ],
)
def test_partial_ohlc_and_security_type_checks(config, now, row):
    collection = config.collections["daily"]
    collection.fields = collection.required_fields = ["high", "low"]
    accepted, rejected, _, _ = normalize(
        [row],
        "yahoo",
        config.sources["yahoo"],
        config.instruments["aapl"],
        collection,
        Chunk("daily", "aapl", date(2026, 9, 1), date(2026, 9, 4)),
        now.isoformat(),
        "raw",
    )
    assert not accepted and len(rejected) == 1


def test_matching_conid_cannot_hide_conflicting_symbol(config, now):
    config.instruments["aapl"].con_id = 265598
    accepted, rejected, _, _ = normalize(
        [
            {
                "date": "2026-09-01",
                "symbol": "SPY",
                "con_id": 265598,
                "open": 1,
                "high": 2,
                "low": 1,
                "close": 2,
            }
        ],
        "ibkr",
        config.sources["ibkr"],
        config.instruments["aapl"],
        config.collections["daily"],
        Chunk("daily", "aapl", date(2026, 9, 1), date(2026, 9, 4)),
        now.isoformat(),
        "raw",
    )
    assert not accepted and rejected[0]["reason"] == "symbol_mismatch"


def test_changed_provider_model_creates_a_distinct_observation_contract(config, rows, now):
    chunk = Chunk("daily", "aapl", date(2026, 9, 1), date(2026, 9, 4))
    source = config.sources["yahoo"]
    args = (config.instruments["aapl"], config.collections["daily"], chunk, now.isoformat(), "raw")
    first = normalize(rows, "yahoo", source, *args)[0]
    changed = source.model_copy(update={"model": "CustomHistorical"})
    second = normalize(rows, "yahoo", changed, *args)[0]
    assert first[0]["values"] == second[0]["values"]
    assert first[0]["observation_key"] != second[0]["observation_key"]

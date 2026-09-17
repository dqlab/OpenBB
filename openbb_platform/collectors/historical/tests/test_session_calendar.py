from datetime import UTC, date, datetime

from openbb_collector_core.delivery import DeliveryConfig
from openbb_collector_core.incremental import heartbeat

from dq_historical_market_data_collector.config import Calendar, CollectorConfig
from dq_historical_market_data_collector.planner import Chunk
from dq_historical_market_data_collector.quality import normalize
from dq_historical_market_data_collector.runtime import Collector
from dq_historical_market_data_collector.storage import Journal


def test_weekends_are_checkpointed_without_fetching(config_dict, client):
    config_dict["collections"]["daily"].update(
        start_date="2026-09-12",
        end_date="2026-09-14",
        frequency="1m",
        calendar={"require_all_bars": False},
        chunk_days=1,
        secondary=[],
    )
    config = CollectorConfig.model_validate(config_dict)
    with Journal(config.storage) as journal:
        service = Collector(
            config, journal, client, clock=lambda: datetime(2026, 9, 15, tzinfo=UTC)
        )
        report = service.collect()
        assert report["status"] == "complete"
        assert client.calls == []
        assert report["diagnostics"] == {"calendar_closed": 2}
        assert journal.db.execute("SELECT count(*) FROM checkpoints").fetchone()[0] == 2
        service.collect()
        assert client.calls == []
        assert journal.db.execute("SELECT count(*) FROM attempts").fetchone()[0] == 2


def test_session_filter_accepts_sparse_current_bars_without_claiming_complete_coverage(config):
    collection = config.collections["daily"]
    collection.frequency = "1m"
    collection.calendar = Calendar(
        require_all_bars=False, overrides={"2026-11-27": {"end": "13:00"}}
    )
    source = config.sources["yahoo"]
    rows = [
        dict(date=f"2026-11-27T{stamp}:00-05:00", open=10, high=11, low=9, close=10, volume=1)
        for stamp in ["09:29", "09:30", "12:59", "13:00"]
    ]
    accepted, rejected, outside, coverage = normalize(
        rows,
        "yahoo",
        source,
        config.instruments["aapl"],
        collection,
        Chunk("daily", "aapl", date(2026, 11, 27), date(2026, 11, 28)),
        "2026-11-27T18:15:00+00:00",
        "raw",
    )
    assert len(accepted) == 2 and not rejected and outside == 2
    assert coverage == {
        "status": "unknown",
        "expected": None,
        "missing": 0,
        "missing_sample": [],
        "basis": "session_filter_only",
    }


def test_historical_heartbeat_uses_bounded_instrument_lookups(config, client, now, tmp_path):
    config.delivery = DeliveryConfig(
        collector_id="history",
        transport="local",
        destination_root=str(tmp_path / "master"),
        incremental=True,
    )
    with Journal(config.storage) as journal:
        Collector(config, journal, client, clock=lambda: now).collect()
        journal.set_meta("central_heartbeat_sent_at", "2000-01-01T00:00:00+00:00")
        queries = []
        journal.db.set_trace_callback(queries.append)
        heartbeat(config, journal, "historical", now)
        journal.db.set_trace_callback(None)
        assert not any("GROUP BY" in sql for sql in queries)
        plan = " ".join(
            row[3]
            for row in journal.db.execute(
                "EXPLAIN QUERY PLAN SELECT max(available_at) FROM observations "
                "WHERE instrument_id=?",
                ("aapl",),
            )
        )
        assert "COVERING INDEX" in plan

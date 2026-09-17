"""Intraday refresh resumes bounded cycles and retries empty contracts fairly."""

import json
from datetime import timedelta

import pytest

from dq_historical_market_data_collector.config import CollectorConfig
from dq_historical_market_data_collector.runtime import Collector
from dq_historical_market_data_collector.storage import Journal


def repeating_config(config_dict, now):
    config_dict["instruments"]["msft"] = {
        "symbol": "MSFT",
        "asset_type": "stock",
        "currency": "USD",
    }
    config_dict["collections"]["daily"].update(
        instrument_ids=["aapl", "msft"],
        secondary=[],
        start_date=str(now.date()),
        end_date=None,
        chunk_days=1,
        refresh_days=1,
    )
    config_dict["schedule"].update(
        include_current_session=True,
        repeat_seconds=60,
        repeat_until="16:30",
        retry_seconds=1,
    )
    config_dict["max_chunks_per_session"] = 1
    return CollectorConfig.model_validate(config_dict)


def test_partial_cycle_survives_restart_and_failure_does_not_starve_other_symbols(
    config_dict, client, now
):
    config = repeating_config(config_dict, now)
    client.rows = [{"date": str(now.date()), "open": 100, "high": 105, "low": 98, "close": 103}]
    client.failures = {"AAPL"}
    clock = [now]
    with Journal(config.storage) as journal:
        Collector(config, journal, client, clock=lambda: clock[0]).run_due(clock[0])
        first = json.loads(journal.get_meta("repeat_cycle"))
        assert "finished_at" not in first
        assert first["failed_chunks"] == 1
    clock[0] += timedelta(seconds=2)
    with Journal(config.storage) as journal:
        service = Collector(config, journal, client, clock=lambda: clock[0])
        service.run_due(clock[0])
        completed = json.loads(journal.get_meta("repeat_cycle"))
        assert completed["id"] == first["id"]
        assert completed["outcome"] == "partial"
        assert [item[1] for item in client.calls] == ["AAPL", "MSFT"]
        assert journal.db.execute("SELECT count(*) FROM observations").fetchone()[0] == 1
        clock[0] += timedelta(seconds=30)
        service.run_due(clock[0])
        assert len(client.calls) == 2
        clock[0] += timedelta(seconds=31)
        service.run_due(clock[0])
        assert json.loads(journal.get_meta("repeat_cycle"))["id"] != first["id"]
        clock[0] += timedelta(seconds=2)
        service.run_due(clock[0])
        assert [item[1] for item in client.calls] == ["AAPL", "MSFT", "AAPL", "MSFT"]
        assert journal.db.execute("SELECT count(*) FROM observations").fetchone()[0] == 1
        assert journal.db.execute("SELECT sum(duplicates) FROM attempts").fetchone()[0] == 1


def test_repeat_window_stops_new_cycles_after_cutoff(config_dict, client, now):
    config = repeating_config(config_dict, now)
    config.max_chunks_per_session = 10
    client.rows = [{"date": str(now.date()), "open": 100, "high": 105, "low": 98, "close": 103}]
    clock = [now]
    with Journal(config.storage) as journal:
        service = Collector(config, journal, client, clock=lambda: clock[0])
        service.run_due(clock[0])
        assert len(client.calls) == 2
        clock[0] = now.replace(hour=22)
        service.run_due(clock[0])
        assert len(client.calls) == 2


def test_repeat_requires_current_session(config_dict):
    config_dict["schedule"]["repeat_seconds"] = 60
    with pytest.raises(ValueError, match="include_current_session"):
        CollectorConfig.model_validate(config_dict)

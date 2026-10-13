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


def test_current_day_cycle_stops_old_requests_after_midnight(config_dict, client, now):
    config = repeating_config(config_dict, now)
    config.collections['daily'].current_day_only = True
    config.collections['daily'].start_date = now.date() - timedelta(days=30)
    config.max_chunks_per_session = 10
    clock = [now]
    requested = []
    with Journal(config.storage) as journal:
        service = Collector(config, journal, client, clock=lambda: clock[0])

        def fetch(session, key, chunk):
            requested.append(chunk)
            clock[0] = now + timedelta(days=1)
            return True

        service._collect_chunk = fetch
        service.run_due(now)
        assert len(requested) == 1
        assert requested[0].start == now.date()
        service.run_due(clock[0])
        assert len(requested) == 3
        assert all(c.start == clock[0].date() for c in requested[1:])


def test_cancelled_request_is_retried_after_restart_in_same_cycle(config_dict, client, now):
    from dq_historical_market_data_collector.providers import ProviderFailure

    config = repeating_config(config_dict, now)
    client.rows = [{"date": str(now.date()), "open": 100, "high": 105, "low": 98, "close": 103}]
    clock = [now]
    original_fetch = client.fetch
    with Journal(config.storage) as journal:
        first_service = Collector(config, journal, client, clock=lambda: clock[0])

        def cancelled(source_id, source, instrument, collection, chunk):
            client.calls.append((source_id, instrument.symbol, chunk))
            first_service.stop.set()
            raise ProviderFailure("cancelled")

        client.fetch = cancelled
        first_service.run_due(clock[0])
        first = json.loads(journal.get_meta("repeat_cycle"))
        assert "finished_at" not in first
    client.fetch = original_fetch
    clock[0] += timedelta(seconds=2)
    with Journal(config.storage) as journal:
        restarted = Collector(config, journal, client, clock=lambda: clock[0])
        restarted.run_due(clock[0])
        assert [item[1] for item in client.calls] == ["AAPL", "AAPL"]
        assert journal.db.execute("SELECT count(*) FROM observations").fetchone()[0] == 1
        clock[0] += timedelta(seconds=2)
        restarted.run_due(clock[0])
        assert [item[1] for item in client.calls] == ["AAPL", "AAPL", "MSFT"]
        assert json.loads(journal.get_meta("repeat_cycle"))["id"] == first["id"]

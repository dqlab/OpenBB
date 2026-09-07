import json
import threading
from datetime import date, timedelta

from dq_historical_market_data_collector.config import Calendar
from dq_historical_market_data_collector.runtime import Collector
from dq_historical_market_data_collector.storage import Journal


def test_primary_failure_fallback_and_isolation(config, client, now):
    client.failures = {"yahoo", "MSFT"}
    config.instruments["msft"] = config.instruments["aapl"].model_copy(update={"symbol": "MSFT"})
    config.collections["daily"].instrument_ids.append("msft")
    with Journal(config.storage) as journal:
        report = Collector(config, journal, client, clock=lambda: now).collect()
        assert report["status"] == "partial"
        assert report["completed_chunks"] == 1 and report["failed_chunks"] == 1
        assert len(client.calls) == 4
        assert report["sources"][0]["source"] == "ibkr"
        assert report["sources"][0]["fallback_attempts"] == 2
        assert "test_provider_failed" in report["diagnostics"]


def test_empty_or_partial_data_does_not_complete(config, client, now):
    client.rows = []
    with Journal(config.storage) as journal:
        collector = Collector(config, journal, client, clock=lambda: now)
        assert collector.collect()["failed_chunks"] == 1
        assert journal.db.execute("SELECT count(*) FROM checkpoints").fetchone()[0] == 0
        client.rows = [{"date": "2026-09-01", "open": 1, "high": 2, "low": 1, "close": 2}]
        config.collections["daily"].calendar = Calendar()
        assert collector.collect()["status"] == "partial"
        assert journal.db.execute("SELECT count(*) FROM checkpoints").fetchone()[0] == 0


def test_calendar_closed_does_not_call_provider(config, client, now):
    config.collections["daily"].calendar = Calendar(
        holidays={date(2026, 9, d) for d in (1, 2, 3)},
    )
    with Journal(config.storage) as journal:
        report = Collector(config, journal, client, clock=lambda: now).collect()
        assert report["status"] == "complete" and not client.calls
        acceptance = Collector(config, journal, client, clock=lambda: now).acceptance(
            as_of=now.date(),
            required_sources=["yahoo"],
        )
        assert not acceptance["passed"]


def test_session_budget_resume_and_duplicate_refresh(config, client, now):
    config.collections["daily"].chunk_days = 1
    config.max_chunks_per_session = 1
    with Journal(config.storage) as journal:
        collector = Collector(config, journal, client, clock=lambda: now)
        assert collector.collect()["deferred"]
        assert collector.collect()["resumed_chunks"] == 1
        assert collector.collect()["status"] == "complete"
        assert collector.collect()["resumed_chunks"] == 3
        config.max_chunks_per_session = 10
        report = collector.collect(refresh=True)
        assert report["reconciliation"]["duplicates"] == 3
        assert report["reconciliation"]["outside"] == 6


def test_crash_recovery_and_reset_preserve_data(config, client, now):
    with Journal(config.storage) as journal:
        collector = Collector(config, journal, client, clock=lambda: now)
        collector.collect()
        abandoned = journal.start(config.fingerprint())
        collector.reset_if_due()
        collector.clock = lambda: now + timedelta(days=7)
        collector.reset_if_due()
        assert client.resets == 1
        assert journal.get_meta("generation") == "1"
        journal.recover()
        report = json.loads(
            journal.db.execute(
                "SELECT report FROM sessions WHERE id=?",
                (abandoned,),
            ).fetchone()[0]
        )
        assert report["status"] == "interrupted"
        assert journal.db.execute("SELECT count(*) FROM observations").fetchone()[0] == 3
        assert journal.db.execute("SELECT count(*) FROM checkpoints").fetchone()[0] == 1


def test_scheduler_restart_does_not_repeat_completed_session(config, client, now):
    with Journal(config.storage) as journal:
        collector = Collector(config, journal, client, clock=lambda: now)
        collector.run(max_seconds=0.3)
        assert len(client.calls) == 1 and client.closed
        collector = Collector(config, journal, client, clock=lambda: now)
        collector.run(max_seconds=0.2)
        assert len(client.calls) == 1
        assert journal.db.execute("SELECT count(*) FROM sessions").fetchone()[0] == 1


def test_cancellation_writes_interrupted_report(config, client, now):
    stop = threading.Event()
    stop.set()
    with Journal(config.storage) as journal:
        report = Collector(config, journal, client, stop=stop, clock=lambda: now).collect()
        assert report["status"] == "interrupted" and not client.calls
        assert (journal.root / "reports" / f"{report['session_id']}.json").exists()


def test_acceptance_requires_new_calls_from_each_source(config, client, now):
    with Journal(config.storage) as journal:
        collector = Collector(config, journal, client, clock=lambda: now)
        collector.collect()
        accepted = collector.acceptance(as_of=now.date(), required_sources=["yahoo"])
        assert accepted["passed"] and accepted["checks"][0]["validated_rows"] == 3
        client.failures = {"yahoo"}
        failed = collector.acceptance(as_of=now.date(), required_sources=["yahoo", "ibkr"])
        assert not failed["passed"]
        assert not next(c for c in failed["checks"] if c["source"] == "yahoo")["passed"]
        assert next(c for c in failed["checks"] if c["source"] == "ibkr")["passed"]


def test_retry_then_continue(config, client, now):
    config.sources["yahoo"].retries = 2
    client.failures = {"yahoo"}
    with Journal(config.storage) as journal:
        report = Collector(config, journal, client, clock=lambda: now).collect()
        assert report["status"] == "complete"
        assert [call[0] for call in client.calls] == ["yahoo", "yahoo", "yahoo", "ibkr"]

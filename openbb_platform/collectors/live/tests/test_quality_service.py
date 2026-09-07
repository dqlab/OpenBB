import json
from datetime import timedelta

import pytest
from conftest import quote

from dq_live_market_data_collector.providers import ProviderFailure, Response
from dq_live_market_data_collector.service import Collector


def observations(journal):
    return [json.loads(row[0]) for row in journal.db.execute("SELECT payload FROM observations")]


def test_fallback_and_instrument_failure_isolation(rig):
    _, clock, provider, journal, service = rig

    def handler(source_id, source, instrument):
        if instrument.symbol == "AAPL" and source_id == "primary":
            raise ProviderFailure("timeout")
        return Response([quote(instrument.symbol, clock())])

    provider.handler = handler
    service.step()
    assert provider.calls == [("primary", "AAPL"), ("secondary", "AAPL"), ("primary", "SPY")]
    assert [row["source"] for row in observations(journal)] == ["secondary", "primary"]
    clock.advance(60)
    report = service.step()[0]
    assert report["totals"]["fallback_polls"] == 1
    assert report["totals"]["expected_polls"] == 12
    assert report["totals"]["missed_polls"] == 10
    assert report["health"] == "degraded"


def test_quality_reconciliation_and_no_field_mixing(rig):
    _, clock, provider, journal, service = rig

    def handler(source_id, source, instrument):
        if source_id == "primary":
            return Response([quote(instrument.symbol, clock(), last_price=None, bid=90)])
        return Response([quote(instrument.symbol, clock(), bid=99)])

    provider.handler = handler
    service.step()
    session_id = journal.running_sessions()[0]["id"]
    report = journal.report(session_id, clock() + timedelta(seconds=1))
    assert report["totals"]["input_rows"] == 4
    assert report["totals"]["accepted"] == 2
    assert report["totals"]["rejected"] == 2
    assert all(row["values"]["bid"] == 99 for row in observations(journal))
    assert len(list(journal.root.glob("bronze/*/*.json"))) == 4
    attempts = json.loads(journal.db.execute("SELECT attempts FROM polls LIMIT 1").fetchone()[0])
    assert attempts[0]["quarantine"][0]["reasons"] == ["missing_last_price"]


@pytest.mark.parametrize(
    "bad_fields,reason",
    [
        ({"last_price": "nan"}, "invalid_last_price"),
        ({"last_price": True}, "invalid_last_price"),
        ({"bid": 103}, "crossed_quote"),
        ({"symbol": "MSFT"}, "symbol_mismatch"),
        ({"currency": "EUR"}, "currency_mismatch"),
        ({"last_timestamp": "2026-09-07T13:30:00"}, "invalid_source_timestamp"),
        ({"delayed": True}, "delayed_request_not_accepted"),
    ],
)
def test_invalid_rows_quarantine_then_fallback(rig, bad_fields, reason):
    _, clock, provider, journal, service = rig

    def handler(source_id, source, instrument):
        row = quote(instrument.symbol, clock())
        if source_id == "primary":
            row.update(bad_fields)
        return Response([row])

    provider.handler = handler
    service.step()
    assert all(row["source"] == "secondary" for row in observations(journal))
    attempt = json.loads(journal.db.execute("SELECT attempts FROM polls LIMIT 1").fetchone()[0])[0]
    assert reason in attempt["quarantine"][0]["reasons"]


def test_stale_and_unknown_freshness_fail_over(rig):
    config, clock, provider, journal, service = rig
    config.collections["quotes"].max_age_seconds = 5

    def handler(source_id, source, instrument):
        stamp = clock() - timedelta(minutes=5) if source_id == "primary" else clock()
        return Response([quote(instrument.symbol, stamp)])

    provider.handler = handler
    service.step()
    assert all(row["source"] == "secondary" for row in observations(journal))


def test_verified_feed_requirement_is_not_satisfied_by_request_flag(rig):
    config, clock, provider, journal, service = rig
    config.collections["quotes"].accepted_feed_types = ["live"]
    service.step()
    assert not observations(journal)
    assert journal.db.execute("SELECT COUNT(*) FROM polls WHERE status='failed'").fetchone()[0] == 2
    config.sources["secondary"].feed_type_field = "market_data_type"
    provider.handler = lambda sid, source, inst: Response(
        [quote(inst.symbol, clock(), market_data_type=1)]
    )
    clock.advance(10)
    service.step()
    assert all(row["source"] == "secondary" for row in observations(journal))
    assert all(row["actual_feed_type"] == "live" for row in observations(journal))


def test_repeated_events_idempotent_and_corrections_retain_asof_history(rig):
    _, clock, provider, journal, service = rig
    stamp = clock()
    price = 100
    provider.handler = lambda sid, source, inst: Response(
        [quote(inst.symbol, stamp, last_price=price)]
    )
    service.step()
    first_available = clock().isoformat()
    clock.advance(10)
    service.step()
    assert len(observations(journal)) == 2
    assert journal.db.execute("SELECT SUM(duplicates) FROM polls").fetchone()[0] == 2
    price = 100.5
    clock.advance(10)
    service.step()
    assert len(observations(journal)) == 4
    before = journal.db.execute(
        "SELECT payload FROM observations WHERE available_at<=?", (first_available,)
    ).fetchall()
    assert all(json.loads(row[0])["values"]["last_price"] == 100 for row in before)
    assert len(before) == 2
    assert len({row["observation_key"] for row in observations(journal)}) == 2


def test_provider_revision_reuse_is_rejected(rig):
    config, clock, provider, journal, service = rig
    config.sources["primary"].revision_field = "revision"
    config.collections["quotes"].secondary = []
    stamp = clock()
    price = 100
    provider.handler = lambda sid, source, inst: Response(
        [quote(inst.symbol, stamp, last_price=price, revision="v1")]
    )
    service.step()
    price = 101
    clock.advance(10)
    service.step()
    assert len(observations(journal)) == 2
    attempts = json.loads(
        journal.db.execute("SELECT attempts FROM polls ORDER BY rowid DESC LIMIT 1").fetchone()[0]
    )
    assert attempts[0]["quarantine"][0]["reasons"] == ["source_revision_conflict"]


def test_same_slot_never_refetched_after_restart(rig):
    config, clock, provider, journal, service = rig
    service.step()
    service.shutdown()
    replacement = Collector(config, journal, provider=provider, clock=clock)
    replacement.step()
    assert len(provider.calls) == 2
    assert len(observations(journal)) == 2
    assert journal.running_sessions()[0]["status"] == "running"


def test_slow_response_after_close_is_archived_and_rejected(rig):
    _, clock, provider, journal, service = rig

    def handler(sid, source, instrument):
        clock.advance(70)
        return Response([quote(instrument.symbol, clock())])

    provider.handler = handler
    service.step()
    assert len(provider.calls) == 1
    assert not observations(journal)
    assert len(list(journal.root.glob("bronze/*/*.json"))) == 1
    report = service.step()[0]
    assert report["totals"]["rejected"] == 1
    assert report["totals"]["missed_polls"] == 11


def test_retries_are_bounded_and_failures_do_not_stop_next_session(rig):
    config, clock, provider, journal, service = rig
    config.sources["primary"].retries = 1
    provider.handler = lambda *_: (_ for _ in ()).throw(ProviderFailure("no_entitlement"))
    service.step()
    assert len(provider.calls) == 6
    assert not observations(journal)
    clock.advance(86400)
    provider.handler = None
    service.step()
    assert len(observations(journal)) == 2
    assert len(journal.db.execute("SELECT id FROM sessions").fetchall()) == 2


def test_changed_actual_feed_type_is_not_hidden_by_duplicate_detection(rig):
    config, clock, provider, journal, service = rig
    source = config.sources["primary"]
    source.feed_type_field = "feed"
    source.feed_type_map = {"1": "live", "3": "delayed"}
    config.collections["quotes"].accepted_feed_types = ["live", "delayed"]
    stamp = clock()
    feed = 1
    provider.handler = lambda sid, source, inst: Response([quote(inst.symbol, stamp, feed=feed)])
    service.step()
    feed = 3
    clock.advance(10)
    service.step()
    rows = observations(journal)
    assert len(rows) == 4
    assert {row["actual_feed_type"] for row in rows} == {"live", "delayed"}


def test_malformed_feed_metadata_is_unknown_and_does_not_crash(rig):
    config, clock, provider, journal, service = rig
    config.sources["primary"].feed_type_field = "feed"
    provider.handler = lambda sid, source, inst: Response([quote(inst.symbol, clock(), feed={})])
    service.step()
    assert len(observations(journal)) == 2
    assert all(row["actual_feed_type"] == "unknown" for row in observations(journal))


def test_gateway_conflict_is_archived_and_reported_without_hiding_fallback(rig):
    _, clock, provider, journal, service = rig

    def handler(source_id, source, instrument):
        if source_id == "primary":
            return Response(
                [quote(instrument.symbol, clock(), last_price=None)],
                metadata={
                    "gateway_error_codes": [10197],
                    "gateway_error_scope": "provider_call_interval",
                },
            )
        return Response([quote(instrument.symbol, clock())])

    provider.handler = handler
    service.step()
    session_id = journal.running_sessions()[0]["id"]
    report = journal.report(session_id, clock())
    assert report["sources"]["primary"]["outcomes"] == {"competing_session": 2}
    assert report["sources"]["primary"]["gateway_error_codes"] == {"10197": 2}
    assert report["totals"]["fallback_polls"] == 2
    assert (
        report["totals"]["input_rows"]
        == report["totals"]["accepted"] + report["totals"]["rejected"]
    )


def test_acceptance_requires_new_success_for_every_source_instrument_pair(rig):
    from dq_live_market_data_collector.acceptance import assess, checkpoint

    config, clock, provider, journal, service = rig
    service.step()
    before = checkpoint(journal)
    assert not assess(journal, config, before, ["primary"], 1)["passed"]

    def handler(source_id, source, instrument):
        if source_id == "primary":
            raise ProviderFailure("timeout")
        return Response([quote(instrument.symbol, clock())])

    provider.handler = handler
    clock.advance(10)
    service.step()
    report = assess(journal, config, before, ["primary", "secondary"], 1)
    assert not report["passed"]
    assert all(check["passed"] == (check["source"] == "secondary") for check in report["checks"])
    assert assess(journal, config, before, ["secondary"], 1)["passed"]
    provider.handler = None
    clock.advance(10)
    service.step()
    assert assess(journal, config, before, ["primary", "secondary"], 1)["passed"]
    with journal.db:
        journal.db.execute("UPDATE observations SET exported=0 WHERE seq>?", (before[1],))
    assert not assess(journal, config, before, ["primary", "secondary"], 1)["passed"]


def test_changed_model_keeps_identical_snapshot_as_a_distinct_contract(rig):
    config, clock, provider, journal, service = rig
    stamp = clock()
    provider.handler = lambda sid, source, inst: Response([quote(inst.symbol, stamp)])
    service.step()
    initial = observations(journal)
    assert len(initial) == 2
    config.sources["primary"].model = "AnotherSnapshotModel"
    clock.advance(10)
    service.step()
    current = observations(journal)
    assert len(current) == 4
    assert len({row["mapping_hash"] for row in current}) == 4


@pytest.mark.parametrize("feed_code,feed_name", [(3, "delayed"), (4, "delayed_frozen")])
def test_delayed_quotes_with_missing_optional_sides_are_archived_labeled_and_exported(
    rig, feed_code, feed_name
):
    config, clock, provider, journal, service = rig
    source = config.sources["primary"]
    source.requested_feed_type = "delayed"
    source.feed_type_field = "market_data_type"
    source.feed_type_map = {"3": "delayed", "4": "delayed_frozen"}
    source.timestamp_semantics = "provider_receive"
    config.collections["quotes"].accepted_feed_types = ["live", "delayed", "delayed_frozen"]
    provider.handler = lambda sid, source, inst: Response(
        [
            quote(
                inst.symbol,
                clock(),
                bid=None,
                ask=None,
                delayed=True,
                market_data_type=feed_code,
                raw_quote={"bid": -1, "ask": -1},
            )
        ]
    )
    service.step()
    rows = observations(journal)
    assert len(rows) == 2
    assert all(row["source"] == "primary" for row in rows)
    assert all(
        row["actual_feed_type"] == feed_name and row["requested_feed_type"] == "delayed"
        for row in rows
    )
    assert all(row["values"]["bid"] is None and row["values"]["ask"] is None for row in rows)
    assert all(
        row["event_time"] is None and row["available_at"] == clock().isoformat() for row in rows
    )
    assert (
        journal.db.execute("SELECT COUNT(*) FROM observations WHERE exported=1").fetchone()[0] == 2
    )
    archives = list(journal.root.glob("bronze/*/*.json"))
    assert len(archives) == 2
    assert all(
        json.loads(path.read_text())["rows"][0]["raw_quote"] == {"bid": -1, "ask": -1}
        for path in archives
    )
    service.step()
    assert len(provider.calls) == 2 and len(observations(journal)) == 2

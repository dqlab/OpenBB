"""Deferred failures must survive restart without duplicating published attempts."""

import json
from datetime import UTC, datetime

import pytest
from conftest import Clock, FakeProvider, quote
from openbb_collector_core.delivery import DeliveryConfig, read_received_session

from dq_live_market_data_collector.config import CollectorConfig
from dq_live_market_data_collector.providers import Response
from dq_live_market_data_collector.service import Collector
from dq_live_market_data_collector.storage import Journal


@pytest.fixture
def retry_rig(raw_config, tmp_path):
    collection = raw_config["collections"]["quotes"]
    collection.update(
        secondary=[],
        frequency_seconds=10800,
        quote_retry={"max_attempts": 3, "delay_seconds": 60, "opening_delay_seconds": 1200},
    )
    collection["schedule"]["end"] = "16:00"
    config = CollectorConfig.model_validate(raw_config)
    config.delivery = DeliveryConfig(
        collector_id="retry-test",
        transport="local",
        destination_root=str(tmp_path / "master"),
        local_retention="forever",
        incremental=True,
    )
    clock = Clock(datetime(2026, 9, 15, 13, 30, tzinfo=UTC))
    provider = FakeProvider(clock)
    provider.handler = lambda sid, source, instrument: Response(
        [quote(instrument.symbol, clock(), last_price=None if instrument.symbol == "AAPL" else 100)]
    )
    with Journal(config.storage) as journal:
        service = Collector(config, journal, provider=provider, clock=clock)
        yield config, clock, provider, journal, service


def test_retry_does_not_block_other_symbols_and_survives_restart(retry_rig, tmp_path):
    config, clock, provider, journal, service = retry_rig
    service.step()
    assert provider.calls == [("primary", "AAPL"), ("primary", "SPY")]
    assert journal.db.execute("SELECT count(*) FROM polls").fetchone()[0] == 1
    state = journal.db.execute("SELECT * FROM quote_retries").fetchone()
    assert state["next_attempt_at"] == "2026-09-15T13:50:00+00:00"
    service.step()
    assert len(provider.calls) == 2
    service.shutdown()
    journal.close()

    clock.advance(1200)
    with Journal(config.storage) as recovered:
        provider = FakeProvider(clock)
        restarted = Collector(config, recovered, provider=provider, clock=clock)
        restarted.step()
        assert provider.calls == [("primary", "AAPL")]
        assert recovered.db.execute("SELECT count(*) FROM quote_retries").fetchone()[0] == 0
        assert recovered.db.execute("SELECT count(*) FROM observations").fetchone()[0] == 2
        row = recovered.db.execute("SELECT * FROM polls WHERE instrument_id='apple'").fetchone()
        assert (row["input_rows"], row["accepted"], row["rejected"]) == (2, 1, 1)
        assert [a["retry_round"] for a in json.loads(row["attempts"])] == [1, 2]
        restarted.step()
        assert len(provider.calls) == 1
        assert recovered.db.execute("SELECT count(*) FROM polls").fetchone()[0] == 2

    master = tmp_path / "master"
    manifests = list(master.glob("remote_collectors/*/batches/*/*/*.manifest.json"))
    assert len(manifests) == 2
    counts = []
    for manifest in manifests:
        read_received_session(master, manifest.relative_to(master).as_posix())
        counts.append(json.loads(manifest.read_text())["metadata"]["observation_count"])
    assert counts == [1, 1]
    assert len(list(config.storage.root.glob("bronze/*/*.json"))) == 3


def test_retry_budget_is_finite_and_commits_one_failed_poll(retry_rig):
    config, clock, provider, journal, service = retry_rig
    service.step()
    clock.advance(1200)
    service.step()
    clock.advance(60)
    service.step()
    assert journal.db.execute("SELECT count(*) FROM quote_retries").fetchone()[0] == 0
    row = journal.db.execute("SELECT * FROM polls WHERE instrument_id='apple'").fetchone()
    assert row["status"] == "failed"
    assert (row["input_rows"], row["accepted"], row["rejected"]) == (3, 0, 3)
    assert len(json.loads(row["attempts"])) == 3
    service.step()
    assert len(provider.calls) == 4  # Three AAPL requests, one independent SPY request.


def test_expired_retry_closes_without_a_provider_request(retry_rig):
    config, clock, provider, journal, service = retry_rig
    service.step()
    clock.advance(7 * 3600)
    service.step()
    assert len(provider.calls) == 2
    assert journal.db.execute("SELECT count(*) FROM quote_retries").fetchone()[0] == 0
    assert (
        journal.db.execute("SELECT status FROM polls WHERE instrument_id='apple'").fetchone()[0]
        == "failed"
    )
    session = journal.db.execute("SELECT status,report_pending FROM sessions").fetchone()
    assert tuple(session) == ("completed", 0)


def test_retry_stops_at_permanent_entitlement_or_contract_errors(retry_rig):
    config, clock, provider, journal, service = retry_rig
    provider.handler = lambda *args: Response([], metadata={"gateway_error_codes": [354]})
    service.step()
    assert journal.db.execute("SELECT count(*) FROM quote_retries").fetchone()[0] == 0
    assert journal.db.execute("SELECT count(*) FROM polls").fetchone()[0] == 2


def test_default_policy_preserves_existing_config_fingerprints(raw_config):
    from hashlib import sha256

    config = CollectorConfig.model_validate(raw_config)
    payload = config.model_dump(mode="json")
    payload.pop("storage")
    payload.pop("delivery")
    for collection in payload["collections"].values():
        collection.pop("quote_retry")
        collection["schedule"]["holidays"].sort()
        for name in ("valid_from", "valid_through"):
            collection["schedule"].pop(name)
    expected = sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    assert config.fingerprint() == expected

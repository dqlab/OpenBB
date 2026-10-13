"""Common acquisition time survives sequential validation, fallback and close."""

from conftest import Clock, FakeProvider, quote

from dq_live_market_data_collector.config import CollectorConfig
from dq_live_market_data_collector.providers import Response
from dq_live_market_data_collector.service import Collector
from dq_live_market_data_collector.storage import Journal


def run_batch(raw_config, *, empty_first=False, close=False):
    raw_config["sources"]["primary"]["batch_size"] = 20
    config = CollectorConfig.model_validate(raw_config)
    clock = Clock()
    provider = FakeProvider(clock)
    calls = []

    def batch(source_id, source, instruments):
        calls.append([i.symbol for i in instruments])
        clock.advance(61 if close else 2)
        return [
            Response(
                [] if empty_first and i == 0 else [quote(item.symbol, clock())],
                metadata={"batch_id": "shared"},
            )
            for i, item in enumerate(instruments)
        ]

    provider.fetch_batch = batch

    def single(source_id, source, instrument):
        clock.advance(3)
        return Response([quote(instrument.symbol, clock())])

    provider.handler = single
    with Journal(config.storage) as journal:
        service = Collector(config, journal, provider=provider, clock=clock)
        service.step()
        polls = [dict(row) for row in journal.db.execute("select * from polls")]
        observations = [dict(row) for row in journal.db.execute("select * from observations")]
        service.step()
    return calls, provider.calls, polls, observations


def test_one_batch_and_no_duplicate_poll(raw_config):
    calls, singles, polls, observations = run_batch(raw_config)
    assert calls == [["AAPL", "SPY"]]
    assert singles == [] and len(polls) == len(observations) == 2
    assert len({r["received_at"] for r in observations}) == 1
    assert all(r["available_at"] == r["received_at"] for r in observations)


def test_missing_contract_uses_existing_fallback(raw_config):
    calls, singles, polls, observations = run_batch(raw_config, empty_first=True)
    assert calls == [["AAPL", "SPY"]]
    assert singles == [("secondary", "AAPL")]
    assert len(observations) == 2
    by_id = {r["instrument_id"]: r for r in observations}
    assert by_id["spy"]["received_at"] < by_id["apple"]["received_at"]


def test_completed_envelope_after_close_is_archived_and_rejected(raw_config):
    calls, singles, polls, observations = run_batch(raw_config, close=True)
    assert calls == [["AAPL", "SPY"]] and singles == []
    assert len(polls) == 2 and observations == []

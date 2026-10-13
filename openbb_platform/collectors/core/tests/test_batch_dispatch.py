"""The batch boundary keeps per-item row limits and a total byte/time budget."""

import multiprocessing as mp
from dataclasses import dataclass

import pytest
from openbb_core.provider.abstract.annotated_result import AnnotatedResult
from test_dispatch import SampleData, SampleFetcher, Source
from test_dispatch import registered as _registered

from openbb_collector_core import ProviderFailure
from openbb_collector_core.process import ProcessClient
from openbb_collector_core.providers import ProviderDispatcher

registered = _registered


@dataclass
class BatchSource(Source):
    batch_size: int = 3


@pytest.fixture
def batch_hook(registered, monkeypatch):
    def hook(requests, credentials):
        return [
            AnnotatedResult(
                result=[SampleData(symbol=r["symbol"], last_price=123.5)],
                metadata={"item": r["symbol"]},
            )
            for r in requests
        ]

    monkeypatch.setattr(SampleFetcher, "collect_batch", staticmethod(hook), raising=False)
    monkeypatch.setattr(SampleFetcher, "collection_batch_limit", 3, raising=False)


def test_batch_worker_reuses_one_process_preserves_order_and_row_budget(batch_hook):
    client = ProcessClient()
    client._context = mp.get_context("fork")
    source = BatchSource(max_rows=1)
    try:
        results = client.fetch_batch("feed", source, [{"symbol": "A"}, {"symbol": "B"}])
        pid = client._workers["feed"].process.pid
        assert [r.rows[0]["symbol"] for r in results] == ["A", "B"]
        assert client.fetch("feed", source, {"symbol": "C"}).rows[0]["symbol"] == "C"
        assert client._workers["feed"].process.pid == pid
    finally:
        client.close()


def test_batch_credentials_filtered_and_not_returned(batch_hook, monkeypatch):
    monkeypatch.setenv("BATCH_TEST_KEY", "test-secret")

    def hook(requests, credentials):
        assert credentials == {"arbitrary_feed_api_key": "test-secret"}
        return [AnnotatedResult(result=[], metadata={}) for _ in requests]

    monkeypatch.setattr(SampleFetcher, "collect_batch", staticmethod(hook))
    dispatcher = ProviderDispatcher(
        BatchSource(credentials_env={"arbitrary_feed_api_key": "BATCH_TEST_KEY"}).model_dump()
    )
    assert dispatcher.fetch_batch([{"symbol": "A"}])[0].rows == []


def test_unsupported_batch_is_explicit(registered):
    with pytest.raises(ProviderFailure, match="unsupported_provider_batch"):
        ProviderDispatcher(BatchSource().model_dump()).fetch_batch([{"symbol": "A"}])


def test_total_byte_budget_applies_to_batch(batch_hook):
    client = ProcessClient()
    client._context = mp.get_context("fork")
    try:
        with pytest.raises(ProviderFailure, match="response_byte_limit"):
            client.fetch_batch(
                "feed", BatchSource(max_response_bytes=200), [{"symbol": "A"}, {"symbol": "B"}]
            )
        assert not client._workers
    finally:
        client.close()


def test_batch_timeout_stops_worker_and_recovers(batch_hook, monkeypatch):
    import time

    original = SampleFetcher.collect_batch

    def slow(requests, credentials):
        time.sleep(5)
        return original(requests, credentials)

    monkeypatch.setattr(SampleFetcher, "collect_batch", staticmethod(slow))
    client = ProcessClient()
    client._context = mp.get_context("fork")
    source = BatchSource(timeout_seconds=0.05)
    try:
        with pytest.raises(ProviderFailure, match="timeout"):
            client.fetch_batch("feed", source, [{"symbol": "A"}])
        assert not client._workers
        monkeypatch.setattr(SampleFetcher, "collect_batch", staticmethod(original))
        source.timeout_seconds = 5
        assert client.fetch_batch("feed", source, [{"symbol": "B"}])[0].rows[0]["symbol"] == "B"
    finally:
        client.close()

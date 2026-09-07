"""Dispatch and process contracts tested with arbitrary registered providers."""

import importlib.metadata
import json
import multiprocessing as mp
from dataclasses import asdict, dataclass, field
from datetime import date
from types import SimpleNamespace
from warnings import warn

import pytest
from openbb_core.provider.abstract.annotated_result import AnnotatedResult
from openbb_core.provider.abstract.data import Data
from openbb_core.provider.abstract.fetcher import Fetcher
from openbb_core.provider.abstract.provider import Provider
from openbb_core.provider.abstract.query_params import QueryParams
from openbb_core.provider.query_executor import QueryExecutor

from openbb_collector_core import ProviderFailure
from openbb_collector_core.process import ProcessClient, _worker
from openbb_collector_core.providers import (
    ProviderDispatcher,
    executor_for,
    request_limits,
    request_parameters,
)
from openbb_collector_core.registry import get_collector


class SampleParams(QueryParams):
    symbol: str
    venue: str = "TEST"
    start_date: date | None = None
    end_date: date | None = None
    interval: str = "1d"


class SampleData(Data):
    symbol: str
    last_price: float


class SampleFetcher(Fetcher[SampleParams, list[SampleData]]):
    require_credentials = False
    collection_max_days = {"1m": 2, "default": 20}
    collection_overlap_days = 2

    @staticmethod
    def transform_query(params):
        return SampleParams(**params)

    @staticmethod
    def extract_data(query, credentials, **kwargs):
        warn("private vendor message that must not be retained", stacklevel=2)
        return [{"symbol": query.symbol, "last_price": 123.5, "venue": query.venue}]

    @staticmethod
    def transform_data(query, data, **kwargs):
        return AnnotatedResult(
            result=[SampleData(**row) for row in data], metadata={"publication_time": "unknown"}
        )


@dataclass
class Source:
    provider: str = "arbitrary_feed"
    model: str = "CustomSnapshot"
    parameters: dict = field(default_factory=lambda: {"venue": "LOCAL"})
    parameter_map: dict = field(default_factory=lambda: {"symbol": "symbol"})
    credentials_env: dict = field(default_factory=dict)
    max_rows: int = 2
    max_response_bytes: int = 10000
    timeout_seconds: float = 5
    min_interval_seconds: float = 0.1

    def model_dump(self, **kwargs):
        return asdict(self)


@pytest.fixture
def registered(monkeypatch):
    providers = {
        name: Provider(
            name=name,
            description="Synthetic provider",
            credentials=["api_key"],
            fetcher_dict={"CustomSnapshot": SampleFetcher},
        )
        for name in ("arbitrary_feed", "another_feed")
    }

    def entries(*, group, name):
        if group == "openbb_provider_extension" and name in providers:
            return [SimpleNamespace(load=lambda: providers[name])]
        return []

    monkeypatch.setattr(importlib.metadata, "entry_points", entries)
    # Package provenance discovery also consults all metadata; isolate that
    # unrelated inventory while testing exact selected-provider registration.
    monkeypatch.setattr(importlib.metadata, "packages_distributions", lambda: {})
    executor_for.cache_clear()
    yield providers
    executor_for.cache_clear()


@pytest.mark.parametrize("provider", ["arbitrary_feed", "another_feed"])
def test_config_selects_registered_provider_and_model(registered, provider, monkeypatch):
    calls = []
    original = QueryExecutor.execute

    async def execute(self, **kwargs):
        calls.append(kwargs)
        return await original(self, **kwargs)

    monkeypatch.setattr(QueryExecutor, "execute", execute)
    source = Source(provider=provider)
    params = request_parameters(source, {"symbol": "BRK-B"})
    dispatcher = ProviderDispatcher(source.model_dump())
    response = dispatcher.fetch(params)
    assert response.rows == [{"symbol": "BRK-B", "last_price": 123.5, "venue": "LOCAL"}]
    assert response.warning_count == 1
    assert response.metadata["provider"] == provider
    assert response.metadata["model"] == "CustomSnapshot"
    assert response.metadata["publication_time"] == "unknown"
    assert "private vendor" not in json.dumps(response.metadata)
    assert calls[0]["provider_name"] == provider
    assert calls[0]["model_name"] == source.model
    assert calls[0]["params"] == {"symbol": "BRK-B", "venue": "LOCAL"}


def test_provider_model_and_parameter_failures_do_not_fall_back(registered):
    with pytest.raises(ProviderFailure, match="not_installed"):
        request_parameters(Source(provider="missing"), {"symbol": "A"})
    with pytest.raises(ProviderFailure, match="unsupported_provider_model"):
        request_parameters(Source(model="MissingModel"), {"symbol": "A"})
    with pytest.raises(ProviderFailure, match="unsupported_provider_parameters"):
        request_parameters(Source(parameters={"typo": 1}), {"symbol": "A"})
    with pytest.raises(ProviderFailure, match="invalid_parameter_mapping"):
        request_parameters(Source(parameter_map={"symbol": "missing"}), {"symbol": "A"})
    with pytest.raises(ProviderFailure, match="invalid_parameter_mapping"):
        request_parameters(Source(parameters={"symbol": "override"}), {"symbol": "A"})


def test_limits_come_from_selected_fetcher(registered):
    assert request_limits("arbitrary_feed", "CustomSnapshot", "1m") == (2, 2)
    assert request_limits("arbitrary_feed", "CustomSnapshot", "1d") == (20, 2)


def test_credentials_are_resolved_and_filtered_without_entering_request(registered, monkeypatch):
    source = Source(credentials_env={"arbitrary_feed_api_key": "COLLECTOR_TEST_TOKEN"})
    with pytest.raises(ProviderFailure, match="missing_credentials"):
        ProviderDispatcher(source.model_dump())
    monkeypatch.setenv("COLLECTOR_TEST_TOKEN", "local-test-secret")
    dispatcher = ProviderDispatcher(source.model_dump())
    response = dispatcher.fetch(request_parameters(source, {"symbol": "A"}))
    assert (
        dispatcher.credentials["arbitrary_feed_api_key"].get_secret_value() == "local-test-secret"
    )
    assert "local-test-secret" not in json.dumps(response.metadata)
    source.credentials_env = {"another_feed_api_key": "COLLECTOR_TEST_TOKEN"}
    with pytest.raises(ProviderFailure, match="unsupported_credentials"):
        ProviderDispatcher(source.model_dump())


def test_actual_executor_runs_in_reused_bounded_worker(registered):
    if "fork" not in mp.get_all_start_methods():
        pytest.skip("Synthetic entrypoint inheritance requires POSIX fork")
    client = ProcessClient()
    client._context = mp.get_context("fork")
    source = Source()
    try:
        first = client.fetch("configured", source, {"symbol": "A"})
        pid = client._workers["configured"].process.pid
        second = client.fetch("configured", source, {"symbol": "B"})
        assert first.rows[0]["symbol"] == "A" and second.rows[0]["symbol"] == "B"
        assert pid == client._workers["configured"].process.pid
        assert first.metadata["provider"] == source.provider
        source.provider = "another_feed"
        third = client.fetch("configured", source, {"symbol": "C"})
        assert third.metadata["provider"] == "another_feed"
        assert client._workers["configured"].process.pid != pid
    finally:
        client.close()
    assert not client._workers


def test_missing_credential_uses_the_same_bounded_byte_protocol(registered):
    source = Source(credentials_env={"arbitrary_feed_api_key": "ABSENT_COLLECTOR_KEY"})
    sent = []
    connection = SimpleNamespace(send_bytes=sent.append, close=lambda: None)
    _worker(connection, source.model_dump())
    assert [json.loads(payload) for payload in sent] == [{"error": "missing_credentials"}]


def test_provider_code_does_not_depend_on_collectors():
    from pathlib import Path

    root = Path(__file__).resolve().parents[3]
    # Only the engine source is constrained; configurations may name any vendor.
    for package in ("core", "historical", "live"):
        for path in (root / "collectors" / package / "src").rglob("*.py"):
            source = path.read_text()
            assert "openbb_ibkr" not in source, path
            assert '== "ibkr"' not in source, path
            assert "from openbb import obb" not in source, path


def test_installed_engine_entrypoints():
    for name in ("historical", "live"):
        engine = get_collector(name)
        assert engine.name == name
        assert callable(engine.factory) and callable(engine.load_config)

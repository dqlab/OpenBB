"""Offline SDK-boundary controls through the real registered model and executor."""

import asyncio

import pytest
import yfinance
from openbb_collector_core.providers import executor_for, fetcher_for
from openbb_yfinance.models.equity_profile import YFinanceEquityProfileFetcher

CANDIDATES = [
    {},
    None,
    {"unrecognizedIdentity": None},
    {"longName": "Missing symbol"},
    {"symbol": None, "longName": None, "native_extra": {"x": []}},
    {"symbol": "A", "dividendYield": 0.125},
]
IDS = [
    "empty-object",
    "null-object",
    "unknown-only",
    "missing-symbol-control",
    "null-and-extra",
    "renamed-native-key",
]


def execute(params=None):
    """Run the real registered model through the actual QueryExecutor."""
    executor_for.cache_clear()
    return asyncio.run(
        executor_for("yfinance").execute(
            provider_name="yfinance",
            model_name="EquityProfileSource",
            params=params or {"symbol": "A"},
            credentials={},
        )
    )


def ticker(monkeypatch, value, calls=None):
    """Install an offline SDK ticker whose get_info returns or raises ``value``."""

    class Ticker:
        def __init__(self, symbol):
            self.symbol = symbol

        def get_info(self):
            if calls is not None:
                calls.append(self.symbol)
            if isinstance(value, Exception):
                raise value
            return value

    monkeypatch.setattr(yfinance, "Ticker", Ticker)


@pytest.mark.parametrize("candidate", CANDIDATES, ids=IDS)
def test_source_captures_sdk_return_before_business_projection(monkeypatch, candidate):
    """The SDK/client return survives verbatim, including empty and unknown shapes."""
    calls = []
    ticker(monkeypatch, candidate, calls)
    result = execute()
    assert calls == ["A"]
    assert result[0].payload == [candidate]
    assert result[0].model_dump() == {
        "evidence_format": "yfinance-profile-extraction-v1",
        "payload": [candidate],
    }


@pytest.mark.parametrize(
    "candidate,key",
    [
        ({"symbol": "A", "dividendYield": 0.125}, "dividendYield"),
        ({"symbol": "A", "symbol_native": None}, "symbol_native"),
        ({"symbol": "A", "longName": None, "native_extra": {"x": []}}, "native_extra"),
        ({"symbol": "A", "longName": 123}, "longName"),
    ],
)
def test_source_never_renames_or_drops_returned_keys(monkeypatch, candidate, key):
    """No rename, whitelist projection or type coercion at the source boundary."""
    ticker(monkeypatch, candidate)
    payload = execute()[0].payload[0]
    assert payload == candidate
    assert key in payload
    assert "dividend_yield" not in payload


def test_source_does_not_call_the_typed_profile_helper(monkeypatch):
    """Capture must not pass through the projecting typed extraction."""

    async def projected(*args, **kwargs):
        raise AssertionError("typed profile extraction must not run acquisition")

    monkeypatch.setattr(
        YFinanceEquityProfileFetcher, "aextract_data", staticmethod(projected)
    )
    ticker(monkeypatch, {"symbol": "A", "longName": "Offline"})
    assert execute()[0].payload == [{"symbol": "A", "longName": "Offline"}]


def test_transport_failure_is_not_converted_into_empty_success(monkeypatch):
    """A raised client error stays an error; it never becomes captured evidence."""
    ticker(monkeypatch, RuntimeError("offline transport failure"))
    with pytest.raises(Exception) as caught:
        execute()
    assert not isinstance(caught.value, AssertionError)


@pytest.mark.parametrize("value", [object(), {1: "bad"}, (1, 2), float("nan")])
def test_source_rejects_opaque_protocol_values(value):
    """Fail protocol-invalid values without a generic string fallback."""
    fetcher = fetcher_for("yfinance", "EquityProfileSource")
    with pytest.raises(ValueError, match="non_json_source"):
        fetcher.transform_data(fetcher.transform_query({"symbol": "A"}), [value])


def test_source_rejects_non_object_success_without_inventing_fields():
    """A non-mapping client return is preserved as such, not repaired."""
    fetcher = fetcher_for("yfinance", "EquityProfileSource")
    result = fetcher.transform_data(fetcher.transform_query({"symbol": "A"}), ["text"])
    assert result[0].payload == ["text"]


def test_original_typed_profile_compatibility():
    """Keep the legacy standard model strict and unchanged."""
    f = YFinanceEquityProfileFetcher
    q = f.transform_query({"symbol": "A"})
    assert (
        f.transform_data(q, [{"symbol": "A", "longName": "Offline"}])[0].name
        == "Offline"
    )
    for row in [{"longName": "Offline"}, {"symbol": "A", "longName": 123}]:
        with pytest.raises(ValueError):
            f.transform_data(q, [row])
    assert fetcher_for("yfinance", "EquityInfo") is f


@pytest.mark.parametrize("symbol", ["", "A,B", 12, None, "A" * 257])
def test_invalid_symbol_is_rejected_before_sdk_call(monkeypatch, symbol):
    """Invalid request shapes must not reach the client."""
    calls = []
    ticker(monkeypatch, {}, calls)
    with pytest.raises(Exception):
        execute({"symbol": symbol})
    assert calls == []


def test_unexpected_parameter_is_rejected_before_sdk_call(monkeypatch):
    """Unknown parameters never reach the SDK."""
    calls = []
    ticker(monkeypatch, {}, calls)
    with pytest.raises(Exception):
        execute({"symbol": "A", "unexpected": True})
    assert calls == []


def test_nested_non_json_value_is_rejected():
    """Reject non-finite numbers inside nested containers."""
    fetcher = fetcher_for("yfinance", "EquityProfileSource")
    with pytest.raises(ValueError, match="non_json_source"):
        fetcher.transform_data(
            fetcher.transform_query({"symbol": "A"}), [{"nested": [float("inf")]}]
        )

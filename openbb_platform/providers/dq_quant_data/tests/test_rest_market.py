"""Market data uses REST credentials and preserves complete capture provenance."""

import builtins
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from openbb_core.app.model.abstract.error import OpenBBError

from openbb_dq_quant_data import client, market, market_router, options_chains


def payload():
    return {
        "data": [
            {
                "symbol": "SPX-C",
                "strike": "6000.25",
                "expiration": "2026-10-16",
                "option_type": "call",
                "bid": "10.123456789012345678",
                "ask": None,
                "collector_observed_at": "2026-10-12T16:00:00Z",
                "currency": "USD",
            }
        ],
        "meta": {
            "capture_id": "capture",
            "publication_id": "publication",
            "rows": 1,
            "capture_rows": 1,
            "response_complete": True,
            "source_time": None,
        },
    }


def response(body, status=200):
    return SimpleNamespace(status_code=status, json=lambda: body)


def test_standard_provider_and_router_share_rest_credentials_and_provenance(monkeypatch):
    credentials = {
        "dq_quant_data_api_url": "https://data.example:8016",
        "dq_quant_data_api_key": "test-key",
        "dq_quant_data_api_ca_file": "/test/ca.crt",
    }
    request = Mock(return_value=response(payload()))
    monkeypatch.setattr(client, "make_request", request)
    original = builtins.__import__

    def no_database_import(name, *args, **kwargs):
        if name.startswith("dq_quant_invest_data"):
            raise AssertionError("REST client must not load a local database")
        return original(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", no_database_import)
    monkeypatch.setenv("DQ_MARKET_REMOTE_CONFIG", "/obsolete/ssh-config")
    fetcher = options_chains.DQOptionsChainsFetcher
    query = fetcher.transform_query({"symbol": "SPX", "capture_id": "capture"})
    raw = fetcher.extract_data(query, credentials)
    standard = fetcher.transform_data(query, raw)
    cc = SimpleNamespace(
        user_settings=SimpleNamespace(credentials=SimpleNamespace(model_dump=lambda: credentials))
    )
    routed = market_router.chain(cc, symbol="SPX", capture_id="capture")
    assert standard.result.model_dump() == routed.results.model_dump()
    assert standard.metadata == routed.extra == payload()["meta"]
    assert raw["data"][0]["ask"] is None
    args, kwargs = request.call_args
    assert args == ("https://data.example:8016/api/v1/market-store/chain",)
    assert kwargs["headers"] == {"X-API-Key": "test-key"}
    assert kwargs["verify"] == "/test/ca.crt"
    assert kwargs["allow_redirects"] is False
    assert kwargs["params"]["capture_id"] == "capture"
    assert "collector_observed_at" in kwargs["params"]["fields"]
    assert "config_path" not in kwargs["params"]


@pytest.mark.parametrize("status", [401, 403, 302, 503])
def test_http_failure_never_reads_local_files(monkeypatch, status):
    monkeypatch.setattr(client, "make_request", Mock(return_value=response({}, status)))
    with pytest.raises(OpenBBError, match=f"HTTP {status}"):
        market._query("chain", None)


@pytest.mark.parametrize(
    "change,match",
    [
        ({"rows": 2}, "partial chain"),
        ({"capture_rows": 2}, "membership"),
        ({"response_complete": False}, "incomplete capture"),
        ({"next_cursor": "unexpected"}, "partial chain"),
    ],
)
def test_partial_chain_is_rejected(monkeypatch, change, match):
    body = payload()
    body["meta"].update(change)
    monkeypatch.setattr(client, "make_request", Mock(return_value=response(body)))
    with pytest.raises(OpenBBError, match=match):
        client.DQAPIClient(None).get_market_result("chain")


def test_expiry_filter_retains_capture_membership_metadata(monkeypatch):
    body = payload()
    body["meta"]["capture_rows"] = 20
    monkeypatch.setattr(client, "make_request", Mock(return_value=response(body)))
    assert (
        client.DQAPIClient(None).get_market_result("chain", {"expiration": "2026-10-16"})["meta"][
            "capture_rows"
        ]
        == 20
    )


def test_history_serialization_and_cursor_are_preserved(monkeypatch):
    body = {"data": [], "meta": {"next_cursor": "cursor", "publication_id": "pub"}}
    request = Mock(return_value=response(body))
    monkeypatch.setattr(client, "make_request", request)
    assert (
        client.DQAPIClient(None).get_market_result(
            "history", {"instruments": ["a", "b"], "as_of": datetime(2026, 10, 10, tzinfo=UTC)}
        )
        == body
    )
    assert request.call_count == 1
    assert request.call_args.kwargs["params"] == {
        "instruments": "a,b",
        "as_of": "2026-10-10T00:00:00+00:00",
    }

"""Offline contract and registration tests for the DQ integration."""

from datetime import date
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from openbb_core.app.model.abstract.error import OpenBBError
from openbb_core.app.router import Router
from pydantic import SecretStr, ValidationError

from openbb_dq_quant_data import (
    client,
    dq_quant_data_provider,
    market_router,
    models,
    options_chains,
    router,
)


def test_registrations():
    assert dq_quant_data_provider.name == "dq_quant_data"
    assert set(dq_quant_data_provider.fetcher_dict) == {
        "OptionsChains",
        "FuturesCurve",
        "EquityHistorical",
        "EtfHistorical",
        "EquitySearch",
        "EquityInfo",
        "CalendarDividend",
        "CalendarSplits",
        "CompanyFilings",
        "FredSeries",
    }
    assert isinstance(router.router, Router)
    assert isinstance(market_router.router, Router)
    assert options_chains.DQOptionsChainsFetcher.require_credentials is False


def response(data, meta=None, status=200):
    return SimpleNamespace(status_code=status, json=lambda: {"data": data, "meta": meta or {}})


def test_http_uses_openbb_settings_and_preserves_pagination(monkeypatch):
    request = Mock(
        side_effect=[
            response([{"value": 0}], {"snapshot_id": "s1", "next_cursor": "c1"}),
            response([{"value": None}], {"snapshot_id": "s1"}),
        ]
    )
    monkeypatch.setattr(client, "make_request", request)
    result = client.DQAPIClient(
        {
            "dq_quant_data_api_url": SecretStr("https://data.example"),
            "dq_quant_data_api_key": SecretStr("test-key"),
        }
    ).get_result("/api/v1/market/bars", {"start_date": date(2026, 1, 1), "end_date": None})
    assert result["data"] == [{"value": 0}, {"value": None}]
    assert len(result["meta"]["pages"]) == 2
    args, kwargs = request.call_args
    assert args == ("https://data.example/api/v1/market/bars",)
    assert kwargs["headers"] == {"X-API-Key": "test-key"}
    assert kwargs["params"] == {"start_date": "2026-01-01", "cursor": "c1"}
    assert kwargs["allow_redirects"] is False


@pytest.mark.parametrize(
    "pages,match",
    [
        ([response([], {"next_cursor": "x"}), response([], {"next_cursor": "x"})], "cursor"),
        (
            [
                response([], {"snapshot_id": "a", "next_cursor": "x"}),
                response([], {"snapshot_id": "b"}),
            ],
            "snapshot",
        ),
        ([response([], status=401)], "HTTP 401"),
        ([response([], status=302)], "HTTP 302"),
        ([response([1])], "invalid records"),
        ([response([{}] * 100001)], "100,000"),
        ([response([], {"next_cursor": str(i)}) for i in range(100)], "100 pages"),
    ],
)
def test_http_rejects_partial_or_invalid_results(monkeypatch, pages, match):
    monkeypatch.setattr(client, "make_request", Mock(side_effect=pages))
    with pytest.raises(OpenBBError, match=match):
        client.DQAPIClient(None).get("/api/v1/market/bars")


@pytest.mark.parametrize("url", ["file:///data", "https://user:key@host", "https://host?q=key"])
def test_api_url_validation(url):
    with pytest.raises(OpenBBError):
        client.DQAPIClient({"api_url": url})


def test_governance_router_uses_credentials_and_metadata(monkeypatch):
    request = Mock(return_value=response([{"dataset": "market.bars"}], {"snapshot_id": "s"}))
    monkeypatch.setattr(client, "make_request", request)
    cc = SimpleNamespace(
        user_settings=SimpleNamespace(
            credentials=SimpleNamespace(
                model_dump=lambda: {"dq_quant_data_api_url": "https://dq.example"}
            )
        )
    )
    result = router.quality(cc, dataset="market.bars")
    assert result.results == [{"dataset": "market.bars"}]
    assert result.extra["pages"][0]["snapshot_id"] == "s"
    assert request.call_args.args[0] == "https://dq.example/api/v1/quality/status"


def test_historical_values_preserve_null_and_zero():
    row = {
        "timestamp": "2026-01-02T15:30:00+00:00",
        "canonical_symbol": "SPY",
        "open": 100,
        "high": 102,
        "low": 99,
        "close": 101,
        "volume": None,
        "adjusted_close": 0,
        "adj_close": 50,
    }
    equity = models.DQEquityHistoricalFetcher.transform_data(
        models.DQEquityHistoricalFetcher.transform_query({"symbol": "SPY"}), [row]
    )[0]
    etf = models.DQEtfHistoricalFetcher.transform_data(
        models.DQEtfHistoricalFetcher.transform_query({"symbol": "SPY"}), [row]
    )[0]
    assert equity.adj_close == 0
    assert equity.volume is None and etf.volume is None
    assert equity.date.isoformat() == "2026-01-02T15:30:00+00:00"


def test_dividend_zero_is_not_replaced():
    row = {
        "symbol": "AAPL",
        "action_type": "dividend",
        "ex_date": "2026-01-01",
        "value": 0,
        "dividend": 12,
    }
    result = models.DQCalendarDividendFetcher.transform_data(
        models.DQCalendarDividendFetcher.transform_query({}), [row]
    )
    assert result[0].amount == 0


def test_fred_missing_series_id_falls_back_to_query():
    result = models.DQFredSeriesFetcher.transform_data(
        models.DQFredSeriesFetcher.transform_query({"symbol": "GDP"}),
        [{"observation_date": "2026-01-01", "value": None}],
    )
    assert result[0].model_dump()["GDP"] is None


def capture():
    return {
        "data": [
            {
                "symbol": "SPX-C",
                "expiration": "2026-09-18",
                "strike": "6000.25",
                "option_type": "call",
                "bid": "10.25",
                "ask": None,
                "collector_observed_at": "2026-09-10T12:00:00+00:00",
                "event_time": None,
                "instrument_uid": "one",
                "currency": "USD",
            },
            {
                "symbol": "SPX-P",
                "expiration": "2026-09-18",
                "strike": "6000.25",
                "option_type": "put",
                "bid": None,
                "ask": "11.25",
                "collector_observed_at": "2026-09-10T12:00:00+00:00",
                "event_time": None,
                "instrument_uid": "two",
                "currency": "USD",
            },
        ],
        "meta": {
            "capture_id": "cap",
            "publication_id": "pub",
            "layer": "gold",
            "quality": {"status": "passed"},
            "complete": True,
        },
    }


def test_chain_provider_matches_router_and_keeps_metadata(monkeypatch):
    query_mock = Mock(return_value=capture())
    monkeypatch.setattr(options_chains, "_query", query_mock)
    fetcher = options_chains.DQOptionsChainsFetcher
    query = fetcher.transform_query(
        {
            "symbol": "spx",
            "config_path": "/tmp/config.yaml",
            "capture_id": "cap",
            "publication_id": "pub",
        }
    )
    raw = fetcher.extract_data(query, None)
    result = fetcher.transform_data(query, raw)
    expected = market_router.project_chain(raw)
    assert result.result.model_dump() == expected.results.model_dump()
    assert result.metadata == expected.extra == raw["meta"]
    assert result.result.bid == [10.25, None]
    assert result.result.ask == [None, 11.25]
    assert result.result.contract_symbol == ["SPX-C", "SPX-P"]
    args, kwargs = query_mock.call_args
    assert args == ("chain", "/tmp/config.yaml")
    assert kwargs["symbol"] == "SPX"
    assert kwargs["publication_id"] == "pub" and kwargs["capture_id"] == "cap"
    assert kwargs["availability"] == "central" and kwargs["allow_incomplete"] is False


@pytest.mark.parametrize(
    "values", [{"max_rows": 0}, {"max_rows": 100001}, {"layer": "unknown"}, {"option_type": "C"}]
)
def test_chain_bounds_and_semantics(values):
    with pytest.raises(ValidationError):
        options_chains.DQOptionsChainsFetcher.transform_query({"symbol": "SPX", **values})


@pytest.mark.parametrize("volume", [None, 0, 1.5])
def test_etf_keeps_units_adjustment_and_source_metadata(volume):
    row = {
        "timestamp": "2026-01-02",
        "symbol": "SPY",
        "open": 100,
        "high": 102,
        "low": 99,
        "close": 101,
        "volume": volume,
        "currency": "USD",
        "adjustment_status": "unadjusted",
        "instrument_id": "spy-id",
        "snapshot_id": "snapshot",
        "volume_unit": "provider_defined",
    }
    result = models.DQEtfHistoricalFetcher.transform_data(
        models.DQEtfHistoricalFetcher.transform_query({"symbol": "SPY"}), [row]
    )[0]
    assert result.volume == volume
    assert result.model_dump()["adjustment_status"] == "unadjusted"
    assert result.model_dump()["currency"] == "USD"
    assert result.model_dump()["instrument_id"] == "spy-id"
    assert result.model_dump()["snapshot_id"] == "snapshot"
    assert result.model_dump()["volume_unit"] == "provider_defined"


def test_search_supports_company_name_and_symbol_mode(monkeypatch):
    records = Mock(
        return_value=[
            {"symbol": "AAPL", "name": "Apple Inc."},
            {"symbol": "MSFT", "name": "Microsoft"},
        ]
    )
    monkeypatch.setattr(models, "_records", records)
    query = models.DQEquitySearchFetcher.transform_query({"query": "apple"})
    raw = models.DQEquitySearchFetcher.extract_data(query, None)
    result = models.DQEquitySearchFetcher.transform_data(query, raw)
    assert [row.symbol for row in result] == ["AAPL"]
    assert records.call_args.args[1] == {}
    query = models.DQEquitySearchFetcher.transform_query({"query": "AAPL", "is_symbol": True})
    models.DQEquitySearchFetcher.extract_data(query, None)
    assert records.call_args.args[1] == {"query": "AAPL"}


def test_fred_result_limit():
    result = models.DQFredSeriesFetcher.transform_data(
        models.DQFredSeriesFetcher.transform_query({"symbol": "GDP", "limit": 1}),
        [
            {"observation_date": "2026-01-01", "value": 1},
            {"observation_date": "2026-02-01", "value": 2},
        ],
    )
    assert len(result) == 1


@pytest.mark.parametrize(
    "fetcher,params,row,field,expected",
    [
        (
            models.DQEquityInfoFetcher,
            {"symbol": "AAPL"},
            {"canonical_symbol": "AAPL", "name": "Apple", "currency": "USD"},
            "symbol",
            "AAPL",
        ),
        (
            models.DQCalendarSplitsFetcher,
            {},
            {
                "symbol": "AAPL",
                "action_type": "reverse_split",
                "effective_date": "2026-01-01",
                "value": 0.25,
            },
            "denominator",
            4,
        ),
        (
            models.DQCompanyFilingsFetcher,
            {"symbol": "AAPL"},
            {
                "canonical_symbol": "AAPL",
                "filing_date": "2026-01-01",
                "form": "10-K",
                "filing_url": "https://www.sec.gov/fixture",
                "accession": "id",
            },
            "report_type",
            "10-K",
        ),
    ],
)
def test_other_standard_fetchers(fetcher, params, row, field, expected):
    result = fetcher.transform_data(fetcher.transform_query(params), [row])
    assert getattr(result[0], field) == expected


def test_optional_local_backend_error(monkeypatch):
    import builtins

    from openbb_dq_quant_data import market

    original_import = builtins.__import__

    def blocked_import(name, *args, **kwargs):
        if name.startswith("dq_quant_invest_data"):
            raise ModuleNotFoundError("not installed", name="dq_quant_invest_data")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", blocked_import)
    with pytest.raises(OpenBBError, match="Install dq-quant-invest-data"):
        market._query("status", None)

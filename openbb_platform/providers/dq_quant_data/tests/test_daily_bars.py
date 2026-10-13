"""Daily stock/ETF/index mapping, adjustment, bounds and source metadata."""

from unittest.mock import Mock

import pytest
from openbb_core.app.model.abstract.error import OpenBBError
from pydantic import ValidationError

from openbb_dq_quant_data import client, daily_bars


@pytest.mark.parametrize(
    "fetcher,asset,symbol",
    [
        (daily_bars.DQEquityHistoricalFetcher, "stock", "AAPL"),
        (daily_bars.DQEtfHistoricalFetcher, "etf", "SPY"),
        (daily_bars.DQIndexHistoricalFetcher, "index", "SPX"),
    ],
)
def test_daily_rest_mapping_and_metadata(monkeypatch, fetcher, asset, symbol):
    payload = {
        "data": [
            {
                "symbol": symbol,
                "date": "2026-10-09",
                "open": "10",
                "high": "12",
                "low": "9",
                "close": "11",
                "volume": None,
                "adjusted_close": 0,
                "asset_class": asset,
                "adjustment": "splits_only",
                "known_at": "2026-10-10T00:00:00Z",
                "source_provider": "yahoo_finance",
                "snapshot_id": "source",
            }
        ],
        "meta": {"rows": 1, "response_complete": True, "source_snapshot_ids": ["source"]},
    }
    request = Mock(return_value=Mock(status_code=200, json=lambda: payload))
    monkeypatch.setattr(client, "make_request", request)
    query = fetcher.transform_query(
        {
            "symbol": symbol.lower(),
            "start_date": "2026-10-01",
            "as_of": "2026-10-12T00:00:00Z",
            "adjustment": "source",
        }
    )
    result = fetcher.transform_data(query, fetcher.extract_data(query, {"api_url": "https://nas"}))
    assert result.result[0].close == 11
    assert result.result[0].volume is None
    assert result.result[0].model_dump()["source_provider"] == "yahoo_finance"
    assert result.result[0].model_dump()["known_at"] == "2026-10-10T00:00:00Z"
    assert result.metadata == payload["meta"]
    assert request.call_args.args == ("https://nas/api/v1/market/daily-bars",)
    params = request.call_args.kwargs["params"]
    assert params["asset_class"] == asset and params["symbols"] == symbol
    assert params["start_date"] == "2026-10-01"
    assert "symbol" not in params and "interval" not in params


def test_invalid_daily_bar_fails_instead_of_dropping_row():
    fetcher = daily_bars.DQEquityHistoricalFetcher
    query = fetcher.transform_query({"symbol": "AAPL"})
    with pytest.raises(OpenBBError, match="silent row loss"):
        fetcher.transform_data(query, {"data": [{"date": "2026-10-09", "close": 12}], "meta": {}})


@pytest.mark.parametrize(
    "params", [{"interval": "1m"}, {"as_of": "2026-10-10T00:00:00"}, {"max_rows": 100001}]
)
def test_daily_query_rejects_unsupported_frequency_or_naive_cutoff(params):
    with pytest.raises(ValidationError):
        daily_bars.DQEquityHistoricalFetcher.transform_query({"symbol": "AAPL", **params})


def test_daily_api_cannot_silently_truncate(monkeypatch):
    monkeypatch.setattr(
        client,
        "make_request",
        Mock(
            return_value=Mock(
                status_code=200,
                json=lambda: {"data": [], "meta": {"rows": 1, "response_complete": True}},
            )
        ),
    )
    with pytest.raises(OpenBBError, match="partial"):
        client.DQAPIClient(None).get_market_result("daily-bars", {"symbols": "AAPL"})

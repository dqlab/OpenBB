"""Tests for Stooq fetchers."""

from datetime import date

import pytest
from openbb_stooq.models.currency_historical import StooqCurrencyHistoricalFetcher
from openbb_stooq.models.equity_historical import StooqEquityHistoricalFetcher
from openbb_stooq.models.index_historical import StooqIndexHistoricalFetcher

CSV_RESPONSE = """Date,Open,High,Low,Close,Volume
2024-01-02,100.0,102.0,99.5,101.5,123456
2024-01-03,101.0,103.0,100.0,102.5,234567
"""


@pytest.fixture()
def mock_stooq_request(monkeypatch):
    """Mock Stooq CSV downloads and return captured request parameters."""
    requests = []

    async def mock_request(url, **kwargs):
        requests.append((url, kwargs["params"]))
        return CSV_RESPONSE

    monkeypatch.setattr(
        "openbb_stooq.utils.helpers.amake_request",
        mock_request,
    )
    return requests


def test_stooq_equity_historical_fetcher(mock_stooq_request):
    """Test the Stooq equity historical fetcher."""
    params = {
        "symbol": "AAPL,MSFT.DE",
        "start_date": date(2024, 1, 1),
        "end_date": date(2024, 1, 5),
        "interval": "1d",
        "country": "us",
    }

    result = StooqEquityHistoricalFetcher().test(
        params,
        {"stooq_api_key": "MOCK_API_KEY"},
    )

    assert result is None
    assert [request[1]["s"] for request in mock_stooq_request] == [
        "aapl.us",
        "msft.de",
    ]


def test_stooq_index_historical_fetcher(mock_stooq_request):
    """Test the Stooq index historical fetcher."""
    params = {
        "symbol": "SPX",
        "start_date": date(2024, 1, 1),
        "end_date": date(2024, 1, 5),
        "interval": "1W",
    }

    result = StooqIndexHistoricalFetcher().test(
        params,
        {"stooq_api_key": "MOCK_API_KEY"},
    )

    assert result is None
    assert mock_stooq_request[0][1]["s"] == "^spx"
    assert mock_stooq_request[0][1]["i"] == "w"


def test_stooq_currency_historical_fetcher(mock_stooq_request):
    """Test the Stooq currency historical fetcher."""
    params = {
        "symbol": "EUR/USD",
        "start_date": date(2024, 1, 1),
        "end_date": date(2024, 1, 5),
        "interval": "1M",
    }

    result = StooqCurrencyHistoricalFetcher().test(
        params,
        {"stooq_api_key": "MOCK_API_KEY"},
    )

    assert result is None
    assert mock_stooq_request[0][1]["s"] == "eurusd"
    assert mock_stooq_request[0][1]["i"] == "m"

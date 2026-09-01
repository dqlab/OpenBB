"""Tests for Stooq helper functions."""

import pytest
from openbb_core.provider.utils.errors import EmptyDataError, UnauthorizedError
from openbb_stooq.utils.helpers import (
    normalize_currency_symbol,
    normalize_equity_symbol,
    normalize_index_symbol,
    parse_csv_response,
)


def test_symbol_normalization():
    """Test Stooq symbol normalization rules."""
    assert normalize_equity_symbol("AAPL") == "aapl.us"
    assert normalize_equity_symbol("BMW.DE") == "bmw.de"
    assert normalize_equity_symbol("PKN.PL") == "pkn"
    assert normalize_equity_symbol("PKN", "pl") == "pkn"
    assert normalize_index_symbol("SPX") == "^spx"
    assert normalize_index_symbol("^DJI") == "^dji"
    assert normalize_currency_symbol("EUR/USD") == "eurusd"


def test_parse_csv_response():
    """Test parsing a valid Stooq CSV response."""
    result = parse_csv_response(
        "Date,Open,High,Low,Close,Volume\n2024-01-02,1,2,0.5,1.5,100\n",
        "aapl.us",
    )

    assert result == [
        {
            "date": "2024-01-02",
            "open": "1",
            "high": "2",
            "low": "0.5",
            "close": "1.5",
            "volume": "100",
            "symbol": "AAPL.US",
        }
    ]


@pytest.mark.parametrize(
    "response",
    [
        "Get your apikey: https://stooq.com/q/d/?s=aapl.us&get_apikey",
        "<!DOCTYPE html><html><body>verification</body></html>",
    ],
)
def test_parse_csv_response_rejects_auth_pages(response):
    """Test that credential and browser-verification pages are explicit errors."""
    with pytest.raises(UnauthorizedError):
        parse_csv_response(response, "aapl.us")


def test_parse_csv_response_rejects_empty_data():
    """Test that a header-only CSV is reported as empty data."""
    with pytest.raises(EmptyDataError):
        parse_csv_response("Date,Open,High,Low,Close,Volume\n", "missing.us")

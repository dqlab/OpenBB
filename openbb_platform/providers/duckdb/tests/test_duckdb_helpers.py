"""Tests for DuckDB helper functions."""

from datetime import date

import duckdb
import pytest
from openbb_core.app.model.abstract.error import OpenBBError
from openbb_core.provider.utils.errors import EmptyDataError
from openbb_duckdb.utils.helpers import query_historical_data, quote_identifier


@pytest.fixture()
def database_path(tmp_path):
    """Create a DuckDB database with a qualified historical-price view."""
    path = tmp_path / "prices.duckdb"
    with duckdb.connect(str(path)) as connection:
        connection.execute("CREATE SCHEMA market_data")
        connection.execute(
            """
            CREATE TABLE market_data."equity prices" (
                Symbol VARCHAR,
                Date DATE,
                Open DOUBLE,
                High DOUBLE,
                Low DOUBLE,
                Close DOUBLE,
                Volume BIGINT,
                "Adjusted Close" DOUBLE
            )
            """
        )
        connection.executemany(
            'INSERT INTO market_data."equity prices" VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
            [
                ("aapl", date(2024, 1, 2), 100, 102, 99, 101, 1000, 100.5),
                ("MSFT", date(2024, 1, 2), 200, 203, 199, 202, 2000, 201.5),
                ("AAPL", date(2024, 1, 3), 101, 104, 100, 103, 1100, 102.5),
            ],
        )
    return str(path)


def test_quote_identifier():
    """Qualified and unusual identifiers are quoted safely."""
    assert quote_identifier("market_data.equity prices") == ('"market_data"."equity prices"')
    assert quote_identifier('prices"; DROP TABLE prices; --') == ('"prices""; DROP TABLE prices; --"')


def test_query_historical_data_filters_and_normalizes(database_path):
    """Queries apply symbol/date filters and normalize returned column names."""
    result = query_historical_data(
        database_path=database_path,
        table="market_data.equity prices",
        symbol="AAPL, msft",
        start_date=date(2024, 1, 3),
        end_date=date(2024, 1, 3),
        credentials=None,
        required_columns={"open", "high", "low", "close"},
    )

    assert len(result) == 1
    assert result[0]["symbol"] == "AAPL"
    assert result[0]["date"] == date(2024, 1, 3)
    assert result[0]["adjusted_close"] == 102.5
    assert result[0]["vwap"] is None


def test_query_uses_configured_database_path(database_path):
    """The provider credential supplies the path when the query omits it."""
    result = query_historical_data(
        database_path=None,
        table="market_data.equity prices",
        symbol="MSFT",
        start_date=None,
        end_date=None,
        credentials={"duckdb_database_path": database_path},
        required_columns={"open", "high", "low", "close"},
    )

    assert [record["symbol"] for record in result] == ["MSFT"]


def test_query_requires_database_path():
    """A missing database configuration produces an actionable error."""
    with pytest.raises(OpenBBError, match="database path is required"):
        query_historical_data(
            database_path=None,
            table="equity_historical",
            symbol="AAPL",
            start_date=None,
            end_date=None,
            credentials=None,
            required_columns={"close"},
        )


def test_query_reports_missing_columns(tmp_path):
    """Relations that do not meet the standard model contract are rejected."""
    path = tmp_path / "invalid.duckdb"
    with duckdb.connect(str(path)) as connection:
        connection.execute("CREATE TABLE prices(symbol VARCHAR, date DATE)")

    with pytest.raises(OpenBBError, match="missing required columns: close"):
        query_historical_data(
            database_path=str(path),
            table="prices",
            symbol="AAPL",
            start_date=None,
            end_date=None,
            credentials=None,
            required_columns={"close"},
        )


def test_query_reports_empty_results(database_path):
    """Empty result sets use the standard provider error."""
    with pytest.raises(EmptyDataError, match="No DuckDB data found"):
        query_historical_data(
            database_path=database_path,
            table="market_data.equity prices",
            symbol="NVDA",
            start_date=None,
            end_date=None,
            credentials=None,
            required_columns={"open", "high", "low", "close"},
        )

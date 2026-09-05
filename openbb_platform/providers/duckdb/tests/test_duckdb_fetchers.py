"""Tests for DuckDB historical-price fetchers."""

from datetime import date

import duckdb
import pytest
from openbb_duckdb import duckdb_provider
from openbb_duckdb.models.crypto_historical import DuckDBCryptoHistoricalFetcher
from openbb_duckdb.models.currency_historical import DuckDBCurrencyHistoricalFetcher
from openbb_duckdb.models.equity_historical import DuckDBEquityHistoricalFetcher
from openbb_duckdb.models.etf_historical import DuckDBEtfHistoricalFetcher
from openbb_duckdb.models.index_historical import DuckDBIndexHistoricalFetcher


@pytest.fixture()
def database_path(tmp_path):
    """Create each default historical-price table."""
    path = tmp_path / "market.duckdb"
    table_names = (
        "crypto_historical",
        "currency_historical",
        "equity_historical",
        "etf_historical",
        "index_historical",
    )
    with duckdb.connect(str(path)) as connection:
        for table in table_names:
            connection.execute(
                f"""
                CREATE TABLE {table} (
                    symbol VARCHAR,
                    date DATE,
                    open DOUBLE,
                    high DOUBLE,
                    low DOUBLE,
                    close DOUBLE,
                    volume BIGINT,
                    vwap DOUBLE
                )
                """
            )
            connection.execute(
                f"INSERT INTO {table} VALUES (?, ?, ?, ?, ?, ?, ?, ?)",  # noqa: S608
                ["TEST", date(2024, 1, 2), 10, 12, 9, 11, 100, 10.5],
            )
    return str(path)


@pytest.mark.parametrize(
    "fetcher",
    [
        DuckDBCryptoHistoricalFetcher,
        DuckDBCurrencyHistoricalFetcher,
        DuckDBEquityHistoricalFetcher,
        DuckDBEtfHistoricalFetcher,
        DuckDBIndexHistoricalFetcher,
    ],
)
def test_duckdb_historical_fetchers(fetcher, database_path):
    """Every registered historical fetcher completes the OpenBB TET pipeline."""
    result = fetcher().test(
        {
            "symbol": "TEST",
            "database_path": database_path,
            "start_date": date(2024, 1, 1),
            "end_date": date(2024, 1, 3),
        }
    )

    assert result is None


def test_duckdb_provider_registration():
    """The provider exposes all supported standard models."""
    assert duckdb_provider.name == "duckdb"
    assert duckdb_provider.credentials == ["duckdb_database_path"]
    assert set(duckdb_provider.fetcher_dict) == {
        "CryptoHistorical",
        "CurrencyHistorical",
        "EquityHistorical",
        "EtfHistorical",
        "IndexHistorical",
    }

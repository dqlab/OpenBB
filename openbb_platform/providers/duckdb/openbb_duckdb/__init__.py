"""OpenBB DuckDB provider module."""

from openbb_core.provider.abstract.provider import Provider
from openbb_duckdb.models.crypto_historical import DuckDBCryptoHistoricalFetcher
from openbb_duckdb.models.currency_historical import DuckDBCurrencyHistoricalFetcher
from openbb_duckdb.models.equity_historical import DuckDBEquityHistoricalFetcher
from openbb_duckdb.models.etf_historical import DuckDBEtfHistoricalFetcher
from openbb_duckdb.models.futures_curve import DuckDBFuturesCurveFetcher
from openbb_duckdb.models.futures_historical import DuckDBFuturesHistoricalFetcher
from openbb_duckdb.models.index_historical import DuckDBIndexHistoricalFetcher
from openbb_duckdb.models.options_chains import DuckDBOptionsChainsFetcher

duckdb_provider = Provider(
    name="duckdb",
    website="https://duckdb.org",
    description=(
        "DuckDB provides read-only access to historical market data stored in a user-owned local DuckDB database."
    ),
    credentials=["database_path"],
    fetcher_dict={
        "CryptoHistorical": DuckDBCryptoHistoricalFetcher,
        "CurrencyHistorical": DuckDBCurrencyHistoricalFetcher,
        "EquityHistorical": DuckDBEquityHistoricalFetcher,
        "EtfHistorical": DuckDBEtfHistoricalFetcher,
        "IndexHistorical": DuckDBIndexHistoricalFetcher,
        "OptionsChains": DuckDBOptionsChainsFetcher,
        "FuturesHistorical": DuckDBFuturesHistoricalFetcher,
        "FuturesCurve": DuckDBFuturesCurveFetcher,
    },
    repr_name="DuckDB",
    instructions=(
        "Set duckdb_database_path in OpenBB user settings, or pass database_path "
        "with each query. The database is opened in read-only mode."
    ),
)

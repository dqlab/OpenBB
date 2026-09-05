"""DuckDB Currency Historical Price Model."""

from typing import Any

from openbb_core.provider.abstract.fetcher import Fetcher
from openbb_core.provider.standard_models.currency_historical import (
    CurrencyHistoricalData,
    CurrencyHistoricalQueryParams,
)
from openbb_core.provider.utils.descriptions import DATA_DESCRIPTIONS
from openbb_duckdb.utils.helpers import query_historical_data
from pydantic import Field


class DuckDBCurrencyHistoricalQueryParams(CurrencyHistoricalQueryParams):
    """DuckDB Currency Historical Price Query."""

    __json_schema_extra__ = {
        "symbol": {"multiple_items_allowed": True},
    }

    database_path: str | None = Field(
        default=None,
        description=("Path to the local DuckDB database. Overrides the configured duckdb_database_path credential."),
    )
    table: str = Field(
        default="currency_historical",
        min_length=1,
        description="Qualified DuckDB table or view containing currency prices.",
    )


class DuckDBCurrencyHistoricalData(CurrencyHistoricalData):
    """DuckDB Currency Historical Price Data."""

    symbol: str | None = Field(
        default=None,
        description=DATA_DESCRIPTIONS.get("symbol", ""),
    )


class DuckDBCurrencyHistoricalFetcher(
    Fetcher[
        DuckDBCurrencyHistoricalQueryParams,
        list[DuckDBCurrencyHistoricalData],
    ]
):
    """Fetch historical currency prices from DuckDB."""

    require_credentials = False

    @staticmethod
    def transform_query(
        params: dict[str, Any],
    ) -> DuckDBCurrencyHistoricalQueryParams:
        """Transform the query parameters."""
        return DuckDBCurrencyHistoricalQueryParams(**params)

    @staticmethod
    def extract_data(
        query: DuckDBCurrencyHistoricalQueryParams,
        credentials: dict[str, str] | None,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        """Return historical currency prices from DuckDB."""
        return query_historical_data(
            database_path=query.database_path,
            table=query.table,
            symbol=query.symbol,
            start_date=query.start_date,
            end_date=query.end_date,
            credentials=credentials,
            required_columns={"close"},
        )

    @staticmethod
    def transform_data(
        query: DuckDBCurrencyHistoricalQueryParams,
        data: list[dict[str, Any]],
        **kwargs: Any,
    ) -> list[DuckDBCurrencyHistoricalData]:
        """Transform the data to the standard format."""
        return [DuckDBCurrencyHistoricalData.model_validate(item) for item in data]

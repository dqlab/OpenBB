"""DuckDB Index Historical Price Model."""

from typing import Any

from openbb_core.provider.abstract.fetcher import Fetcher
from openbb_core.provider.standard_models.index_historical import (
    IndexHistoricalData,
    IndexHistoricalQueryParams,
)
from openbb_duckdb.utils.helpers import query_historical_data
from pydantic import Field


class DuckDBIndexHistoricalQueryParams(IndexHistoricalQueryParams):
    """DuckDB Index Historical Price Query."""

    __json_schema_extra__ = {
        "symbol": {"multiple_items_allowed": True},
    }

    database_path: str | None = Field(
        default=None,
        description=("Path to the local DuckDB database. Overrides the configured duckdb_database_path credential."),
    )
    table: str = Field(
        default="index_historical",
        min_length=1,
        description="Qualified DuckDB table or view containing index prices.",
    )


class DuckDBIndexHistoricalData(IndexHistoricalData):
    """DuckDB Index Historical Price Data."""


class DuckDBIndexHistoricalFetcher(
    Fetcher[
        DuckDBIndexHistoricalQueryParams,
        list[DuckDBIndexHistoricalData],
    ]
):
    """Fetch historical index prices from DuckDB."""

    require_credentials = False

    @staticmethod
    def transform_query(params: dict[str, Any]) -> DuckDBIndexHistoricalQueryParams:
        """Transform the query parameters."""
        return DuckDBIndexHistoricalQueryParams(**params)

    @staticmethod
    def extract_data(
        query: DuckDBIndexHistoricalQueryParams,
        credentials: dict[str, str] | None,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        """Return historical index prices from DuckDB."""
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
        query: DuckDBIndexHistoricalQueryParams,
        data: list[dict[str, Any]],
        **kwargs: Any,
    ) -> list[DuckDBIndexHistoricalData]:
        """Transform the data to the standard format."""
        return [DuckDBIndexHistoricalData.model_validate(item) for item in data]

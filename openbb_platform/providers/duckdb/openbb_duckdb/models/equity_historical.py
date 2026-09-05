"""DuckDB Equity Historical Price Model."""

from typing import Any

from openbb_core.provider.abstract.fetcher import Fetcher
from openbb_core.provider.standard_models.equity_historical import (
    EquityHistoricalData,
    EquityHistoricalQueryParams,
)
from openbb_core.provider.utils.descriptions import DATA_DESCRIPTIONS
from openbb_duckdb.utils.helpers import query_historical_data
from pydantic import Field


class DuckDBEquityHistoricalQueryParams(EquityHistoricalQueryParams):
    """DuckDB Equity Historical Price Query."""

    __json_schema_extra__ = {
        "symbol": {"multiple_items_allowed": True},
    }

    database_path: str | None = Field(
        default=None,
        description=("Path to the local DuckDB database. Overrides the configured duckdb_database_path credential."),
    )
    table: str = Field(
        default="equity_historical",
        min_length=1,
        description="Qualified DuckDB table or view containing equity prices.",
    )


class DuckDBEquityHistoricalData(EquityHistoricalData):
    """DuckDB Equity Historical Price Data."""

    symbol: str | None = Field(
        default=None,
        description=DATA_DESCRIPTIONS.get("symbol", ""),
    )


class DuckDBEquityHistoricalFetcher(
    Fetcher[
        DuckDBEquityHistoricalQueryParams,
        list[DuckDBEquityHistoricalData],
    ]
):
    """Fetch historical equity prices from DuckDB."""

    require_credentials = False

    @staticmethod
    def transform_query(
        params: dict[str, Any],
    ) -> DuckDBEquityHistoricalQueryParams:
        """Transform the query parameters."""
        return DuckDBEquityHistoricalQueryParams(**params)

    @staticmethod
    def extract_data(
        query: DuckDBEquityHistoricalQueryParams,
        credentials: dict[str, str] | None,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        """Return historical equity prices from DuckDB."""
        return query_historical_data(
            database_path=query.database_path,
            table=query.table,
            symbol=query.symbol,
            start_date=query.start_date,
            end_date=query.end_date,
            credentials=credentials,
            required_columns={"open", "high", "low", "close"},
        )

    @staticmethod
    def transform_data(
        query: DuckDBEquityHistoricalQueryParams,
        data: list[dict[str, Any]],
        **kwargs: Any,
    ) -> list[DuckDBEquityHistoricalData]:
        """Transform the data to the standard format."""
        return [DuckDBEquityHistoricalData.model_validate(item) for item in data]

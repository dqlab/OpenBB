"""DuckDB ETF Historical Price Model."""

from typing import Any

from openbb_core.provider.abstract.fetcher import Fetcher
from openbb_core.provider.standard_models.etf_historical import (
    EtfHistoricalData,
    EtfHistoricalQueryParams,
)
from openbb_core.provider.utils.descriptions import DATA_DESCRIPTIONS
from openbb_duckdb.utils.helpers import query_historical_data
from pydantic import Field


class DuckDBEtfHistoricalQueryParams(EtfHistoricalQueryParams):
    """DuckDB ETF Historical Price Query."""

    __json_schema_extra__ = {
        "symbol": {"multiple_items_allowed": True},
    }

    database_path: str | None = Field(
        default=None,
        description=("Path to the local DuckDB database. Overrides the configured duckdb_database_path credential."),
    )
    table: str = Field(
        default="etf_historical",
        min_length=1,
        description="Qualified DuckDB table or view containing ETF prices.",
    )


class DuckDBEtfHistoricalData(EtfHistoricalData):
    """DuckDB ETF Historical Price Data."""

    symbol: str | None = Field(
        default=None,
        description=DATA_DESCRIPTIONS.get("symbol", ""),
    )


class DuckDBEtfHistoricalFetcher(
    Fetcher[
        DuckDBEtfHistoricalQueryParams,
        list[DuckDBEtfHistoricalData],
    ]
):
    """Fetch historical ETF prices from DuckDB."""

    require_credentials = False

    @staticmethod
    def transform_query(params: dict[str, Any]) -> DuckDBEtfHistoricalQueryParams:
        """Transform the query parameters."""
        return DuckDBEtfHistoricalQueryParams(**params)

    @staticmethod
    def extract_data(
        query: DuckDBEtfHistoricalQueryParams,
        credentials: dict[str, str] | None,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        """Return historical ETF prices from DuckDB."""
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
        query: DuckDBEtfHistoricalQueryParams,
        data: list[dict[str, Any]],
        **kwargs: Any,
    ) -> list[DuckDBEtfHistoricalData]:
        """Transform the data to the standard format."""
        return [DuckDBEtfHistoricalData.model_validate(item) for item in data]

"""DuckDB futures historical prices."""

from datetime import date
from typing import Any

from openbb_core.provider.abstract.fetcher import Fetcher
from openbb_core.provider.standard_models.futures_historical import (
    FuturesHistoricalData,
    FuturesHistoricalQueryParams,
)
from openbb_duckdb.utils.derivatives import DuckDBQueryParams, read_derivatives
from pydantic import Field, model_validator


class DuckDBFuturesHistoricalQueryParams(FuturesHistoricalQueryParams, DuckDBQueryParams):
    """Read stored contracts without rolling or adjusting their prices."""

    __json_schema_extra__ = {"symbol": {"multiple_items_allowed": True}}
    table: str = Field(default="futures_historical", min_length=1, description="DuckDB table or qualified view.")
    expiration: str | None = Field(
        default=None, pattern=r"^\d{4}-(0[1-9]|1[0-2])$", description="Expiration month YYYY-MM."
    )

    @model_validator(mode="after")
    def validate_range(self):
        """Reject reversed ranges and invalid expiration years."""
        if self.start_date and self.end_date and self.start_date > self.end_date:
            raise ValueError("start_date must not exceed end_date.")
        if self.expiration:
            date.fromisoformat(self.expiration + "-01")
        return self


class DuckDBFuturesHistoricalData(FuturesHistoricalData):
    """Stored futures prices, with source metadata retained."""

    symbol: str = Field(description="Stored futures symbol or root.")
    contract_symbol: str = Field(description="Unique exchange contract identifier.")
    expiration: date = Field(description="Actual contract expiration date.")


class DuckDBFuturesHistoricalFetcher(Fetcher[DuckDBFuturesHistoricalQueryParams, list[DuckDBFuturesHistoricalData]]):
    """Fetch bounded futures history."""

    require_credentials = False

    @staticmethod
    def transform_query(params: dict[str, Any]) -> DuckDBFuturesHistoricalQueryParams:
        """Validate parameters."""
        return DuckDBFuturesHistoricalQueryParams(**params)

    @staticmethod
    def extract_data(
        query: DuckDBFuturesHistoricalQueryParams, credentials: dict[str, str] | None, **kwargs: Any
    ) -> list[dict]:
        """Read explicit contracts and date bounds."""
        filters = []
        if query.start_date:
            filters.append(("date", ">=", query.start_date))
        if query.end_date:
            filters.append(("date", "<=", query.end_date))
        if query.expiration:
            from calendar import monthrange  # pylint: disable=import-outside-toplevel

            first = date.fromisoformat(query.expiration + "-01")
            last = first.replace(day=monthrange(first.year, first.month)[1])
            filters.extend([("expiration", ">=", first), ("expiration", "<=", last)])
        return read_derivatives(
            query,
            credentials,
            required={"close", "contract_symbol", "expiration"},
            filters=filters,
            order=("date", "symbol", "expiration", "contract_symbol"),
        )

    @staticmethod
    def transform_data(
        query: DuckDBFuturesHistoricalQueryParams, data: list[dict], **kwargs: Any
    ) -> list[DuckDBFuturesHistoricalData]:
        """Preserve prices, missing optional fields, and extra metadata."""
        return [DuckDBFuturesHistoricalData.model_validate(row) for row in data]

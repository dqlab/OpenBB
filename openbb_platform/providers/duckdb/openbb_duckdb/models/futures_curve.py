"""DuckDB futures curve snapshots."""

from datetime import date
from typing import Any

from openbb_core.provider.abstract.fetcher import Fetcher
from openbb_core.provider.standard_models.futures_curve import FuturesCurveData, FuturesCurveQueryParams
from openbb_duckdb.utils.derivatives import DuckDBQueryParams, read_derivatives
from pydantic import Field


class DuckDBFuturesCurveQueryParams(FuturesCurveQueryParams, DuckDBQueryParams):
    """Use exact observation dates, or the latest stored date."""

    table: str = Field(default="futures_curve", min_length=1, description="DuckDB table or qualified view.")


class DuckDBFuturesCurveData(FuturesCurveData):
    """Source curve prices and contract identifiers."""

    symbol: str = Field(description="Futures root.")
    contract_symbol: str = Field(description="Unique exchange contract identifier.")
    price: float = Field(description="Stored contract price in source quote units.")


class DuckDBFuturesCurveFetcher(Fetcher[DuckDBFuturesCurveQueryParams, list[DuckDBFuturesCurveData]]):
    """Read a futures curve without interpolating or constructing contracts."""

    require_credentials = False

    @staticmethod
    def transform_query(params: dict[str, Any]) -> DuckDBFuturesCurveQueryParams:
        """Validate parameters."""
        return DuckDBFuturesCurveQueryParams(**params)

    @staticmethod
    def extract_data(
        query: DuckDBFuturesCurveQueryParams, credentials: dict[str, str] | None, **kwargs: Any
    ) -> list[dict]:
        """Select exact dates, or the latest stored snapshot for this root."""
        dates = [date.fromisoformat(item) for item in str(query.date).split(",")] if query.date else None
        return read_derivatives(
            query,
            credentials,
            required={"expiration", "contract_symbol", "price"},
            snapshot=True,
            snapshot_dates=dates,
            order=("date", "expiration", "contract_symbol"),
        )

    @staticmethod
    def transform_data(
        query: DuckDBFuturesCurveQueryParams, data: list[dict], **kwargs: Any
    ) -> list[DuckDBFuturesCurveData]:
        """Keep the source expiration representation."""
        return [DuckDBFuturesCurveData.model_validate({**row, "expiration": str(row["expiration"])}) for row in data]

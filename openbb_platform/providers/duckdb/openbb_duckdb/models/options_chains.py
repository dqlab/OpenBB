"""DuckDB option chain snapshots."""

from datetime import date as dateType
from typing import Any, Literal

from openbb_core.provider.abstract.fetcher import Fetcher
from openbb_core.provider.standard_models.options_chains import OptionsChainsData, OptionsChainsQueryParams
from openbb_duckdb.utils.derivatives import DuckDBQueryParams, read_derivatives
from pydantic import Field


class DuckDBOptionsChainsQueryParams(OptionsChainsQueryParams, DuckDBQueryParams):
    """Read one underlying's daily chain snapshot."""

    table: str = Field(default="options_chains", min_length=1, description="DuckDB table or qualified view.")
    date: dateType | None = Field(default=None, description="Exact EOD date; defaults to the latest stored EOD snapshot.")
    expiration: dateType | None = Field(default=None, description="Filter an exact expiration date.")
    option_type: Literal["call", "put"] | None = Field(default=None, description="Filter call or put contracts.")


class DuckDBOptionsChainsData(OptionsChainsData):
    """Standard column-oriented option chains with source units preserved."""

    contract_symbol: list[str | None] = Field(
        default_factory=list, description="Stored contract symbols; null when unavailable."
    )


class DuckDBOptionsChainsFetcher(Fetcher[DuckDBOptionsChainsQueryParams, DuckDBOptionsChainsData]):
    """Read row-oriented snapshots and convert to the standard chain object."""

    require_credentials = False

    @staticmethod
    def transform_query(params: dict[str, Any]) -> DuckDBOptionsChainsQueryParams:
        """Validate parameters."""
        return DuckDBOptionsChainsQueryParams(**params)

    @staticmethod
    def extract_data(
        query: DuckDBOptionsChainsQueryParams, credentials: dict[str, str] | None, **kwargs: Any
    ) -> list[dict]:
        """Read one snapshot, optionally narrowed by expiration or side."""
        filters = []
        if query.expiration:
            filters.append(("expiration", "=", query.expiration))
        if query.option_type:
            filters.append(("option_type", "=", query.option_type))
        return read_derivatives(
            query,
            credentials,
            required={"expiration", "strike", "option_type"},
            symbol_column="underlying_symbol",
            symbol_fallback="underlying",
            date_column="eod_date",
            snapshot=True,
            snapshot_dates=[query.date] if query.date else None,
            filters=filters,
            order=("expiration", "strike", "option_type"),
        )

    @staticmethod
    def transform_data(query: DuckDBOptionsChainsQueryParams, data: list[dict], **kwargs: Any) -> DuckDBOptionsChainsData:
        """Align optional columns across contracts without imputing missing quotes."""
        for row in data:
            side = str(row["option_type"]).strip().lower()
            row["option_type"] = {"c": "call", "p": "put"}.get(side, side)
            if row["option_type"] not in {"call", "put"}:
                raise ValueError("Stored option_type must be C/P or call/put (case-insensitive).")
        for row in data:
            if row.get("dte") is None:
                row["dte"] = (
                    dateType.fromisoformat(str(row["expiration"])[:10])
                    - dateType.fromisoformat(str(row["eod_date"])[:10])
                ).days
        columns = set(DuckDBOptionsChainsData.model_fields) | {key for row in data for key in row}
        return DuckDBOptionsChainsData.model_validate({col: [row.get(col) for row in data] for col in columns})

"""BaoStock index historical prices."""

from typing import Any, Literal

from openbb_core.provider.abstract.fetcher import Fetcher
from openbb_core.provider.standard_models.index_historical import IndexHistoricalData, IndexHistoricalQueryParams
from pydantic import Field, field_validator, model_validator

from openbb_baostock.utils.helpers import fetch_history, normalize_symbols, transform_history, with_default_dates


class BaostockIndexHistoricalQueryParams(IndexHistoricalQueryParams):
    """BaoStock indices support daily, weekly, and monthly unadjusted bars."""

    __json_schema_extra__ = {
        "symbol": {"multiple_items_allowed": True},
        "interval": {"choices": ["1d", "1W", "1M"]},
    }
    interval: Literal["1d", "1W", "1M"] = Field(default="1d", description="Index bar frequency.")
    adjustment: Literal["unadjusted"] = Field(default="unadjusted", description="Indices use unadjusted values.")

    @field_validator("symbol", mode="before")
    @classmethod
    def to_upper(cls, value: str) -> str:
        """Normalize exchange-qualified index symbols."""
        return normalize_symbols(value)

    @model_validator(mode="after")
    def validate_dates(self) -> "BaostockIndexHistoricalQueryParams":
        """Reject reversed date windows before opening a session."""
        if self.start_date and self.end_date and self.start_date > self.end_date:
            raise ValueError("start_date must be on or before end_date.")
        return self


class BaostockIndexHistoricalData(IndexHistoricalData):
    """BaoStock index OHLC values in index points."""

    __alias_dict__ = {"symbol": "code", "prev_close": "preclose", "change_percent": "pctChg"}

    amount: float | None = Field(default=None, description="Aggregate trading value in CNY, as reported by BaoStock.")
    adjustment: Literal["unadjusted"] = Field(default="unadjusted", description="Indices use unadjusted values.")
    prev_close: float | None = Field(default=None, description="Previous close in index points.")
    change_percent: float | None = Field(default=None, description="Index change expressed as a fraction (0.01 = 1%).")


class BaostockIndexHistoricalFetcher(Fetcher[BaostockIndexHistoricalQueryParams, list[BaostockIndexHistoricalData]]):
    """Fetch BaoStock index history without credentials."""

    require_credentials = False

    @staticmethod
    def transform_query(params: dict[str, Any]) -> BaostockIndexHistoricalQueryParams:
        """Validate index symbols, frequency, and dates."""
        return BaostockIndexHistoricalQueryParams(**with_default_dates(params))

    @staticmethod
    async def aextract_data(
        query: BaostockIndexHistoricalQueryParams, credentials: dict[str, str] | None, **kwargs: Any
    ) -> list[dict[str, Any]]:
        """Extract index bars using only index-compatible fields."""
        return await fetch_history(query, is_index=True)

    @staticmethod
    def transform_data(
        query: BaostockIndexHistoricalQueryParams, data: list[dict[str, Any]], **kwargs: Any
    ) -> list[BaostockIndexHistoricalData]:
        """Normalize the response into the index schema."""
        return [BaostockIndexHistoricalData.model_validate(row) for row in transform_history(query, data)]

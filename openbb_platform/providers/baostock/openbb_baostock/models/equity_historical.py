"""BaoStock equity historical prices."""

from typing import Any, Literal

from openbb_core.provider.abstract.fetcher import Fetcher
from openbb_core.provider.standard_models.equity_historical import EquityHistoricalData, EquityHistoricalQueryParams
from pydantic import Field, field_validator, model_validator

from openbb_baostock.utils.helpers import (
    INTERVALS,
    fetch_history,
    normalize_symbols,
    transform_history,
    with_default_dates,
)


class BaostockEquityHistoricalQueryParams(EquityHistoricalQueryParams):
    """BaoStock history query with inclusive dates and explicit adjustment mode."""

    __json_schema_extra__ = {
        "symbol": {"multiple_items_allowed": True},
        "interval": {"choices": list(INTERVALS)},
    }

    interval: Literal["1d", "1W", "1M", "5m", "15m", "30m", "60m"] = Field(
        default="1d", description="Bar frequency. Intraday timestamps use Asia/Shanghai."
    )
    adjustment: Literal["unadjusted", "forward", "backward"] = Field(
        default="unadjusted",
        description="Price adjustment: unadjusted (3), forward/前复权 (2), or backward/后复权 (1).",
    )

    @field_validator("symbol", mode="before")
    @classmethod
    def to_upper(cls, value: str) -> str:
        """Normalize exchange-qualified symbols."""
        return normalize_symbols(value)

    @model_validator(mode="after")
    def validate_dates(self) -> "BaostockEquityHistoricalQueryParams":
        """Reject reversed date windows before opening a session."""
        if self.start_date and self.end_date and self.start_date > self.end_date:
            raise ValueError("start_date must be on or before end_date.")
        return self


class BaostockEquityHistoricalData(EquityHistoricalData):
    """BaoStock bars; blank prices remain null, including suspended sessions."""

    __alias_dict__ = {
        "symbol": "code",
        "prev_close": "preclose",
        "turnover_rate": "turn",
        "trade_status": "tradestatus",
        "change_percent": "pctChg",
        "pe_ttm": "peTTM",
        "pb_mrq": "pbMRQ",
        "ps_ttm": "psTTM",
        "pcf_ttm": "pcfNcfTTM",
        "is_st": "isST",
    }

    symbol: str = Field(description="Exchange-qualified BaoStock symbol, such as SH.600000.")
    open: float | None = Field(default=None, description="Opening price in the security's trading currency.")
    high: float | None = Field(default=None, description="Highest price in the security's trading currency.")
    low: float | None = Field(default=None, description="Lowest price in the security's trading currency.")
    close: float | None = Field(default=None, description="Closing price in the security's trading currency.")
    volume: int | None = Field(default=None, description="Trading volume in shares, as reported by BaoStock.")
    amount: float | None = Field(
        default=None, description="Trading value in the security's trading currency (CNY for A shares)."
    )
    adjustment: Literal["unadjusted", "forward", "backward"] = Field(description="Adjustment applied to these prices.")
    prev_close: float | None = Field(default=None, description="Previous closing price.")
    turnover_rate: float | None = Field(default=None, description="Turnover expressed as a fraction (0.01 = 1%).")
    trade_status: Literal[0, 1] | None = Field(default=None, description="0 = suspended; 1 = normal trading.")
    change_percent: float | None = Field(default=None, description="Price change expressed as a fraction (0.01 = 1%).")
    pe_ttm: float | None = Field(default=None, description="Trailing twelve month price-to-earnings ratio.")
    pb_mrq: float | None = Field(default=None, description="Price-to-book ratio for the most recent quarter.")
    ps_ttm: float | None = Field(default=None, description="Trailing twelve month price-to-sales ratio.")
    pcf_ttm: float | None = Field(default=None, description="Trailing twelve month price-to-cash-flow ratio.")
    is_st: bool | None = Field(default=None, description="Whether the security has special treatment (ST) status.")

    @field_validator("trade_status", mode="before")
    @classmethod
    def parse_trade_status(cls, value: Any) -> int | None:
        """Parse the SDK's string flag without converting missing values to zero."""
        return int(value) if value is not None else None


class BaostockEquityHistoricalFetcher(Fetcher[BaostockEquityHistoricalQueryParams, list[BaostockEquityHistoricalData]]):
    """Fetch BaoStock equity bars without credentials."""

    require_credentials = False

    @staticmethod
    def transform_query(params: dict[str, Any]) -> BaostockEquityHistoricalQueryParams:
        """Validate parameters and fill a bounded default date window."""
        return BaostockEquityHistoricalQueryParams(**with_default_dates(params))

    @staticmethod
    async def aextract_data(
        query: BaostockEquityHistoricalQueryParams, credentials: dict[str, str] | None, **kwargs: Any
    ) -> list[dict[str, Any]]:
        """Extract all requested symbols through the SDK."""
        return await fetch_history(query)

    @staticmethod
    def transform_data(
        query: BaostockEquityHistoricalQueryParams, data: list[dict[str, Any]], **kwargs: Any
    ) -> list[BaostockEquityHistoricalData]:
        """Normalize the response into the OpenBB schema."""
        return [BaostockEquityHistoricalData.model_validate(row) for row in transform_history(query, data)]

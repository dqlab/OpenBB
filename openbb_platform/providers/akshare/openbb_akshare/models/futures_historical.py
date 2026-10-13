"""AKShare China futures historical prices."""

from typing import Any

from openbb_core.provider.abstract.fetcher import Fetcher
from openbb_core.provider.standard_models.futures_historical import (
    FuturesHistoricalData,
    FuturesHistoricalQueryParams,
)
from openbb_core.provider.utils.errors import EmptyDataError
from pydantic import Field, field_validator, model_validator

from openbb_akshare.utils.helpers import clip_dates, normalize_symbol, query_frame


class AkshareFuturesHistoricalQueryParams(FuturesHistoricalQueryParams):
    """AKShare China futures daily bars for one continuous or dated contract."""

    symbol: str = Field(
        description="China futures root such as IF0 or RB0, or a dated contract such as IF2610.",
    )

    @field_validator("symbol", mode="after", check_fields=False)
    @classmethod
    def validate_symbol(cls, value: str) -> str:
        """Validate the exchange/Sina symbol before a request is made."""
        return normalize_symbol(value)

    @model_validator(mode="after")
    def validate_dates(self) -> "AkshareFuturesHistoricalQueryParams":
        """Reject reversed date windows before a request is made."""
        if self.start_date and self.end_date and self.start_date > self.end_date:
            raise ValueError("start_date must be on or before end_date.")
        return self


class AkshareFuturesHistoricalData(FuturesHistoricalData):
    """AKShare futures daily bars with source open-interest and settlement fields."""

    symbol: str = Field(description="Requested futures symbol.")
    open_interest: float | None = Field(
        default=None, description="Open interest in contracts, as reported by the source."
    )
    settle: float | None = Field(
        default=None, description="Settlement price as reported by the source; zero when the source omits it."
    )


class AkshareFuturesHistoricalFetcher(
    Fetcher[AkshareFuturesHistoricalQueryParams, list[AkshareFuturesHistoricalData]]
):
    """Fetch China futures history without credentials."""

    require_credentials = False

    @staticmethod
    def transform_query(params: dict[str, Any]) -> AkshareFuturesHistoricalQueryParams:
        """Validate symbol and date window."""
        return AkshareFuturesHistoricalQueryParams(**params)

    @staticmethod
    async def aextract_data(
        query: AkshareFuturesHistoricalQueryParams, credentials: dict[str, str] | None, **kwargs: Any
    ) -> list[dict[str, Any]]:
        """Extract daily bars for the requested contract."""
        return await query_frame("futures_zh_daily_sina", {"symbol": query.symbol})

    @staticmethod
    def transform_data(
        query: AkshareFuturesHistoricalQueryParams, data: list[dict[str, Any]], **kwargs: Any
    ) -> list[AkshareFuturesHistoricalData]:
        """Normalize the source frame, retaining the requested symbol and units."""
        rows = clip_dates(data, query.start_date, query.end_date)
        if not rows:
            raise EmptyDataError(f"No AKShare futures history for {query.symbol} in the requested date range.")
        records = []
        for row in rows:
            records.append(
                {
                    "date": row.get("date"),
                    "symbol": query.symbol,
                    "open": row.get("open"),
                    "high": row.get("high"),
                    "low": row.get("low"),
                    "close": row.get("close"),
                    "volume": row.get("volume"),
                    "open_interest": row.get("hold"),
                    "settle": row.get("settle"),
                }
            )
        return [AkshareFuturesHistoricalData.model_validate(row) for row in sorted(records, key=lambda r: r["date"])]

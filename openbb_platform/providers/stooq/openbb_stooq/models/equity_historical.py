"""Stooq Equity Historical Price Model."""

from typing import Any, Literal

from openbb_core.provider.abstract.fetcher import Fetcher
from openbb_core.provider.standard_models.equity_historical import (
    EquityHistoricalData,
    EquityHistoricalQueryParams,
)
from openbb_core.provider.utils.descriptions import (
    DATA_DESCRIPTIONS,
    QUERY_DESCRIPTIONS,
)
from openbb_stooq.utils.helpers import (
    INTERVALS,
    fetch_historical_data,
    normalize_equity_symbol,
    with_default_dates,
)
from pydantic import Field


class StooqEquityHistoricalQueryParams(EquityHistoricalQueryParams):
    """Stooq Equity Historical Price Query.

    Source: https://stooq.com/q/d/l/
    """

    __json_schema_extra__ = {
        "symbol": {"multiple_items_allowed": True},
        "interval": {"choices": list(INTERVALS)},
        "country": {"choices": ["us", "pl", "de", "uk", "jp", "hk", "hu"]},
    }

    interval: Literal["1d", "1W", "1M", "1Q", "1Y"] = Field(
        default="1d",
        description=QUERY_DESCRIPTIONS.get("interval", ""),
    )
    country: Literal["us", "pl", "de", "uk", "jp", "hk", "hu"] = Field(
        default="us",
        description=(
            "Default Stooq market suffix for symbols that do not already include one. Polish listings use no suffix."
        ),
    )


class StooqEquityHistoricalData(EquityHistoricalData):
    """Stooq Equity Historical Price Data."""

    symbol: str | None = Field(
        default=None,
        description=DATA_DESCRIPTIONS.get("symbol", ""),
    )


class StooqEquityHistoricalFetcher(
    Fetcher[
        StooqEquityHistoricalQueryParams,
        list[StooqEquityHistoricalData],
    ]
):
    """Fetch historical equity and ETF prices from Stooq."""

    @staticmethod
    def transform_query(params: dict[str, Any]) -> StooqEquityHistoricalQueryParams:
        """Transform the query parameters."""
        return StooqEquityHistoricalQueryParams(**with_default_dates(params))

    @staticmethod
    async def aextract_data(
        query: StooqEquityHistoricalQueryParams,
        credentials: dict[str, str] | None,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        """Return historical prices from Stooq."""
        symbols = [normalize_equity_symbol(symbol, query.country) for symbol in query.symbol.split(",")]
        return await fetch_historical_data(
            symbols=symbols,
            start_date=query.start_date,
            end_date=query.end_date,
            interval=query.interval,
            credentials=credentials,
            preferences=kwargs.get("preferences"),
        )

    @staticmethod
    def transform_data(
        query: StooqEquityHistoricalQueryParams,
        data: list[dict[str, Any]],
        **kwargs: Any,
    ) -> list[StooqEquityHistoricalData]:
        """Transform the data to the standard format."""
        return [StooqEquityHistoricalData.model_validate(item) for item in data]

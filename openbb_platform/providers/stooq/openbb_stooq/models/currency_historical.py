"""Stooq Currency Historical Price Model."""

from typing import Any, Literal

from openbb_core.provider.abstract.fetcher import Fetcher
from openbb_core.provider.standard_models.currency_historical import (
    CurrencyHistoricalData,
    CurrencyHistoricalQueryParams,
)
from openbb_core.provider.utils.descriptions import (
    DATA_DESCRIPTIONS,
    QUERY_DESCRIPTIONS,
)
from openbb_stooq.utils.helpers import (
    INTERVALS,
    fetch_historical_data,
    normalize_currency_symbol,
    with_default_dates,
)
from pydantic import Field


class StooqCurrencyHistoricalQueryParams(CurrencyHistoricalQueryParams):
    """Stooq Currency Historical Price Query.

    Source: https://stooq.com/q/d/l/
    """

    __json_schema_extra__ = {
        "symbol": {"multiple_items_allowed": True},
        "interval": {"choices": list(INTERVALS)},
    }

    interval: Literal["1d", "1W", "1M", "1Q", "1Y"] = Field(
        default="1d",
        description=QUERY_DESCRIPTIONS.get("interval", ""),
    )


class StooqCurrencyHistoricalData(CurrencyHistoricalData):
    """Stooq Currency Historical Price Data."""

    symbol: str | None = Field(
        default=None,
        description=DATA_DESCRIPTIONS.get("symbol", ""),
    )


class StooqCurrencyHistoricalFetcher(
    Fetcher[
        StooqCurrencyHistoricalQueryParams,
        list[StooqCurrencyHistoricalData],
    ]
):
    """Fetch historical currency prices from Stooq."""

    @staticmethod
    def transform_query(params: dict[str, Any]) -> StooqCurrencyHistoricalQueryParams:
        """Transform the query parameters."""
        return StooqCurrencyHistoricalQueryParams(**with_default_dates(params))

    @staticmethod
    async def aextract_data(
        query: StooqCurrencyHistoricalQueryParams,
        credentials: dict[str, str] | None,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        """Return historical currency prices from Stooq."""
        symbols = [normalize_currency_symbol(symbol) for symbol in query.symbol.split(",")]
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
        query: StooqCurrencyHistoricalQueryParams,
        data: list[dict[str, Any]],
        **kwargs: Any,
    ) -> list[StooqCurrencyHistoricalData]:
        """Transform the data to the standard format."""
        return [StooqCurrencyHistoricalData.model_validate(item) for item in data]

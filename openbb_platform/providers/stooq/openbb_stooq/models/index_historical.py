"""Stooq Index Historical Price Model."""

from typing import Any, Literal

from openbb_core.provider.abstract.fetcher import Fetcher
from openbb_core.provider.standard_models.index_historical import (
    IndexHistoricalData,
    IndexHistoricalQueryParams,
)
from openbb_core.provider.utils.descriptions import QUERY_DESCRIPTIONS
from openbb_stooq.utils.helpers import (
    INTERVALS,
    fetch_historical_data,
    normalize_index_symbol,
    with_default_dates,
)
from pydantic import Field


class StooqIndexHistoricalQueryParams(IndexHistoricalQueryParams):
    """Stooq Index Historical Price Query.

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


class StooqIndexHistoricalData(IndexHistoricalData):
    """Stooq Index Historical Price Data."""


class StooqIndexHistoricalFetcher(
    Fetcher[
        StooqIndexHistoricalQueryParams,
        list[StooqIndexHistoricalData],
    ]
):
    """Fetch historical index prices from Stooq."""

    @staticmethod
    def transform_query(params: dict[str, Any]) -> StooqIndexHistoricalQueryParams:
        """Transform the query parameters."""
        return StooqIndexHistoricalQueryParams(**with_default_dates(params))

    @staticmethod
    async def aextract_data(
        query: StooqIndexHistoricalQueryParams,
        credentials: dict[str, str] | None,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        """Return historical index prices from Stooq."""
        symbols = [normalize_index_symbol(symbol) for symbol in query.symbol.split(",")]
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
        query: StooqIndexHistoricalQueryParams,
        data: list[dict[str, Any]],
        **kwargs: Any,
    ) -> list[StooqIndexHistoricalData]:
        """Transform the data to the standard format."""
        return [StooqIndexHistoricalData.model_validate(item) for item in data]

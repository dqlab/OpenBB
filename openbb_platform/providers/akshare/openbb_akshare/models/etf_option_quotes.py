"""Bounded Sina option quote batches."""

import asyncio
from typing import Any

from openbb_core.provider.abstract.data import Data
from openbb_core.provider.abstract.fetcher import Fetcher
from openbb_core.provider.abstract.query_params import QueryParams
from pydantic import ConfigDict, Field, field_validator

from openbb_akshare.utils.etf_option_quotes import fetch_quotes, validate_symbols


class AkshareEtfOptionQuotesQueryParams(QueryParams):
    """At most fifty exact option contract identifiers in one source request."""

    model_config = ConfigDict(extra="forbid")
    symbols: str
    __json_schema_extra__ = {"symbols": {"multiple_items_allowed": True}}

    @field_validator("symbols")
    @classmethod
    def exact_symbols(cls, value: str) -> str:
        """Validate before spawning source I/O."""
        validate_symbols(value)
        return value


class AkshareEtfOptionQuotesData(Data):
    """A quote or explicit per-contract diagnostic, preserving raw source fields."""

    model_config = ConfigDict(extra="allow")
    symbol: str
    quote_error: str | None = None
    source_time: str | None = None
    bid: float | None = None
    ask: float | None = None
    bid_size: float | None = None
    ask_size: float | None = None
    last: float | None = None
    open_interest: float | None = None
    volume: float | None = None
    turnover: float | None = None
    underlying: str | None = None
    strike: float | None = None
    contract_name: str | None = None
    source_status: str | None = None
    raw_fields: dict[str, Any] = Field(default_factory=dict)


class AkshareEtfOptionQuotesFetcher(Fetcher[AkshareEtfOptionQuotesQueryParams, list[AkshareEtfOptionQuotesData]]):
    """Batch transport over the same Sina source as AKShare's per-contract quote."""

    require_credentials = False

    @staticmethod
    def transform_query(params: dict[str, Any]) -> AkshareEtfOptionQuotesQueryParams:
        """Validate the exact requested symbol set."""
        return AkshareEtfOptionQuotesQueryParams(**params)

    @staticmethod
    async def aextract_data(
        query: AkshareEtfOptionQuotesQueryParams,
        credentials: dict[str, str] | None,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        """Fetch off the event loop; the collector worker enforces hard deadlines."""
        return await asyncio.to_thread(fetch_quotes, query.symbols)

    @staticmethod
    def transform_data(
        query: AkshareEtfOptionQuotesQueryParams, data: list[dict[str, Any]], **kwargs: Any
    ) -> list[AkshareEtfOptionQuotesData]:
        """Retain explicit error rows instead of silently dropping contracts."""
        return [AkshareEtfOptionQuotesData.model_validate(row) for row in data]

# OpenBB and collector callback signatures retain protocol parameters.
# pylint: disable=unused-argument

"""Source-native CFFEX index option expiry discovery and bid/ask quote models."""

import asyncio
from typing import Any, Literal

from openbb_core.provider.abstract.data import Data
from openbb_core.provider.abstract.fetcher import Fetcher
from openbb_core.provider.abstract.query_params import QueryParams
from pydantic import ConfigDict, Field, field_validator

from openbb_akshare.utils.cffex_option_quotes import (
    fetch_expiries,
    fetch_quotes,
    validate_expiry_symbol,
)


class AkshareCffexOptionExpiriesQueryParams(QueryParams):
    """An explicit family; no default/main-contract assumption."""

    model_config = ConfigDict(extra="forbid", strict=True)
    product: Literal["IO", "HO", "MO"]


class AkshareCffexOptionQuotesQueryParams(QueryParams):
    """One exact source maturity, preserving lowercase family and YYMM."""

    model_config = ConfigDict(extra="forbid", strict=True)
    expiry_symbol: str = Field(
        description="Exact native maturity such as io2610, ho2610 or mo2610."
    )

    @field_validator("expiry_symbol")
    @classmethod
    def exact_expiry_symbol(cls, value: str) -> str:
        """Validate before scheduling source I/O, never normalize the source token."""
        return validate_expiry_symbol(value)


class AkshareCffexOptionExpiriesData(Data):
    """A displayed maturity with evidence, not an exchange-universe guarantee."""

    model_config = ConfigDict(extra="allow", alias_generator=None, allow_inf_nan=False)
    product: Literal["IO", "HO", "MO"]
    expiry_symbol: str
    universe_scope: Literal["sina_displayed_expiries"]
    source_sha256: str
    fetched_at: str


class AkshareCffexOptionQuotesData(Data):
    """One native contract with nullable unknown terms and explicit quote diagnostics."""

    model_config = ConfigDict(extra="allow", alias_generator=None, allow_inf_nan=False)
    symbol: str
    product: Literal["IO", "HO", "MO"]
    expiry_symbol: str
    exchange: Literal["CFFEX"]
    underlying_symbol: str
    underlying_source_symbol: str
    option_type: Literal["call", "put"]
    strike: float = Field(gt=0)
    expiration: str
    contract_unit: None = None
    source_time: None = None
    bid: float | None = Field(default=None, ge=0)
    ask: float | None = Field(default=None, ge=0)
    bid_size: float | None = Field(default=None, ge=0)
    ask_size: float | None = Field(default=None, ge=0)
    last: float | None = Field(default=None, ge=0)
    open_interest: float | None = Field(default=None, ge=0)
    source_info: dict[str, Any]
    raw_fields: dict[str, Any]
    quote_diagnostics: list[str]
    source_sha256: str
    fetched_at: str


class AkshareCffexOptionExpiriesFetcher(
    Fetcher[AkshareCffexOptionExpiriesQueryParams, list[AkshareCffexOptionExpiriesData]]
):
    """Native model AkshareCffexOptionExpiries, without credentials."""

    require_credentials = False

    @staticmethod
    def transform_query(
        params: dict[str, Any],
    ) -> AkshareCffexOptionExpiriesQueryParams:
        """Require an explicit supported family."""
        return AkshareCffexOptionExpiriesQueryParams(**params)

    @staticmethod
    async def aextract_data(
        query: AkshareCffexOptionExpiriesQueryParams,
        credentials: dict[str, str] | None,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        """Run bounded blocking HTTP off-loop; collector processes own hard timeouts."""
        return await asyncio.to_thread(fetch_expiries, query.product)

    @staticmethod
    def transform_data(
        query: AkshareCffexOptionExpiriesQueryParams,
        data: list[dict[str, Any]],
        **kwargs: Any,
    ) -> list[AkshareCffexOptionExpiriesData]:
        """Validate result rows while retaining the first row's raw evidence only."""
        return [AkshareCffexOptionExpiriesData.model_validate(row) for row in data]


class AkshareCffexOptionQuotesFetcher(
    Fetcher[AkshareCffexOptionQuotesQueryParams, list[AkshareCffexOptionQuotesData]]
):
    """Native model AkshareCffexOptionQuotes, preserving source contract IDs."""

    require_credentials = False

    @staticmethod
    def transform_query(params: dict[str, Any]) -> AkshareCffexOptionQuotesQueryParams:
        """Validate the literal maturity before any request."""
        return AkshareCffexOptionQuotesQueryParams(**params)

    @staticmethod
    async def aextract_data(
        query: AkshareCffexOptionQuotesQueryParams,
        credentials: dict[str, str] | None,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        """Issue one source request off-loop; do not fan out per-contract calls."""
        return await asyncio.to_thread(fetch_quotes, query.expiry_symbol)

    @staticmethod
    def transform_data(
        query: AkshareCffexOptionQuotesQueryParams,
        data: list[dict[str, Any]],
        **kwargs: Any,
    ) -> list[AkshareCffexOptionQuotesData]:
        """Keep unknown unit/event time null and raw evidence available."""
        return [AkshareCffexOptionQuotesData.model_validate(row) for row in data]

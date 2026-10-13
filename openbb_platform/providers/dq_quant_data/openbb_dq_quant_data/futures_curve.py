"""Stored futures term structures with source identity and availability metadata."""

from datetime import UTC, datetime
from datetime import date as dateType
from typing import Any, Literal

from openbb_core.app.model.abstract.error import OpenBBError
from openbb_core.provider.abstract.annotated_result import AnnotatedResult
from openbb_core.provider.abstract.fetcher import Fetcher
from openbb_core.provider.standard_models.futures_curve import (
    FuturesCurveData,
    FuturesCurveQueryParams,
)
from pydantic import Field, field_validator, model_validator

from openbb_dq_quant_data.client import DQAPIClient


def _utc(value: datetime | str) -> datetime:
    parsed = (
        datetime.fromisoformat(value.replace("Z", "+00:00")) if isinstance(value, str) else value
    )
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("as_of and source observation times require a UTC offset.")
    return parsed.astimezone(UTC)


def _now() -> datetime:
    return datetime.now(UTC)


class DQFuturesCurveQueryParams(FuturesCurveQueryParams):
    """Read one root's stored quotes; never request a live gateway snapshot."""

    date: dateType | None = Field(
        default=None, description="One exact UTC observation date; not a settlement-price request."
    )
    dataset: str = Field(
        default="sofr_futures_quotes", description="Configured futures quote dataset."
    )
    config_path: str | None = Field(
        default=None,
        description="Explicit local diagnostic override; otherwise use configured DQ REST API.",
    )
    as_of: datetime | str | None = Field(
        default=None, description="Knowledge cutoff with UTC offset; defaults to now."
    )
    publication_id: str | None = Field(
        default=None, description="Pin an immutable quote publication."
    )
    availability: Literal["central", "collector"] = Field(
        default="central", description="Actual publication or retrospective collector availability."
    )
    layer: Literal["gold", "silver"] = Field(
        default="gold", description="Gold eligible rows or explicit Silver diagnostics."
    )
    price_type: Literal["midpoint", "bid", "ask", "last"] = Field(
        default="midpoint",
        description="Selected source price; midpoint requires valid two-sided quotes. No fallback.",
    )
    max_age_seconds: int | None = Field(
        default=None,
        ge=1,
        le=86400,
        description="Quote window: defaults to 900 seconds for latest, 86400 for a dated query.",
    )
    max_skew_seconds: int = Field(
        default=300,
        ge=0,
        le=86400,
        description="Maximum observation spread for pricing eligibility.",
    )
    max_contracts: int = Field(
        default=1000, ge=1, le=1000, description="Maximum reference contracts; overflow fails."
    )
    allow_incomplete: bool = Field(
        default=False,
        description="Keep missing/invalid prices as null instead of failing an incomplete curve.",
    )

    @field_validator("date", mode="before")
    @classmethod
    def single_date(cls, value):
        if value is None:
            return None
        if isinstance(value, datetime):
            raise ValueError("date must be one YYYY-MM-DD value; use as_of for a timestamp.")
        if isinstance(value, dateType):
            return value
        if not isinstance(value, str) or len(value) != 10:
            raise ValueError("date must be one YYYY-MM-DD value.")
        return dateType.fromisoformat(value)

    @field_validator("as_of")
    @classmethod
    def aware_cutoff(cls, value):
        return _utc(value) if value is not None else None

    @model_validator(mode="after")
    def valid_bounds(self):
        age = self.max_age_seconds or (86400 if self.date else 900)
        if self.max_skew_seconds > age:
            raise ValueError("max_skew_seconds must not exceed the effective max_age_seconds.")
        return self


class DQFuturesCurveData(FuturesCurveData):
    """One qualified contract, including null prices in explicitly incomplete curves."""

    expiration: str = Field(
        description="Provider-qualified last-trade/expiration date, preserved as ISO date."
    )
    price: float | None = Field(
        default=None,
        description="Selected futures price in source quote units; not a yield or settlement mark.",
    )
    symbol: str = Field(description="Requested futures root.")
    contract_symbol: str = Field(description="Qualified local contract symbol.")
    contract_month: str | None = Field(
        default=None, description="Source contract/reference month, distinct from expiration."
    )
    instrument_uid: str = Field(description="Stable provider-qualified contract identity.")
    currency: str | None = Field(
        default=None, description="Source/reference currency without imputation."
    )
    bid: float | None = Field(default=None, description="Stored bid, without substitution.")
    ask: float | None = Field(default=None, description="Stored ask, without substitution.")
    last_trade_price: float | None = Field(
        default=None, description="Stored last price; event time may be unknown."
    )
    exchange: str | None = Field(default=None, description="Qualified listing exchange.")
    trading_class: str | None = Field(default=None, description="Qualified futures trading class.")
    multiplier: str | None = Field(
        default=None, description="Source contract multiplier, not applied to price."
    )
    bid_size: float | None = Field(default=None, description="Stored bid size.")
    ask_size: float | None = Field(default=None, description="Stored ask size.")
    volume: float | None = Field(
        default=None, description="Stored volume in its stated source unit."
    )
    open_interest: float | None = Field(
        default=None, description="Stored open interest when available."
    )
    size_unit: str | None = Field(
        default=None, description="Source size unit; unresolved values remain explicit."
    )
    volume_unit: str | None = Field(
        default=None, description="Source volume unit; unresolved values remain explicit."
    )
    price_type: str = Field(description="Selected price basis.")
    collector_observed_at: datetime | None = Field(
        default=None, description="Collector observation time in UTC."
    )
    event_time: datetime | None = Field(
        default=None, description="Exchange event time, null if unknown."
    )
    age_seconds: float | None = Field(
        default=None, description="Age relative to query time or dated window end."
    )
    actual_feed_type: str | None = Field(
        default=None, description="Source-reported live/delayed/frozen status."
    )
    gold_eligible: bool = Field(
        default=False, description="Whether the underlying source observation qualifies for Gold."
    )
    quality_flags: list[str] = Field(
        default_factory=list, description="Source quality flags and projection blockers."
    )
    observation_uid: str | None = Field(
        default=None, description="Pinned source observation identity."
    )
    reference_version_id: str | None = Field(
        default=None, description="Version of the qualified contract terms."
    )


class DQFuturesCurveFetcher(Fetcher[DQFuturesCurveQueryParams, list[DQFuturesCurveData]]):
    """Fetch one term structure from the platform's bounded futures REST API."""

    require_credentials = False

    @staticmethod
    def transform_query(params: dict[str, Any]) -> DQFuturesCurveQueryParams:
        return DQFuturesCurveQueryParams(**params)

    @staticmethod
    def extract_data(
        query: DQFuturesCurveQueryParams, credentials: dict | None, **kwargs: Any
    ) -> dict:
        params = query.model_dump(exclude={"config_path"}, exclude_none=True)
        if query.config_path is not None:
            # Optional server-side diagnostics share the same implementation.
            try:
                from dq_quant_invest_data.data.market.config import load_market_config
                from dq_quant_invest_data.data.market.futures_curve import read_curve
                from dq_quant_invest_data.data.market.reader import MarketReader
            except ImportError:
                raise OpenBBError(
                    "Local diagnostics require dq-quant-invest-data with futures support."
                ) from None
            return read_curve(MarketReader(load_market_config(query.config_path)), **params)
        return DQAPIClient(credentials).get_market_result("futures-curve", params)

    @staticmethod
    def transform_data(
        query: DQFuturesCurveQueryParams, data: dict, **kwargs: Any
    ) -> AnnotatedResult[list[DQFuturesCurveData]]:
        return AnnotatedResult(
            result=[DQFuturesCurveData.model_validate(row) for row in data["data"]],
            metadata=data["meta"],
        )

"""Stored futures term structures with source identity and availability metadata."""

import hashlib
import json
from datetime import UTC, datetime, timedelta
from datetime import date as dateType
from decimal import Decimal, InvalidOperation
from typing import Any, Literal

from openbb_core.app.model.abstract.error import OpenBBError
from openbb_core.provider.abstract.annotated_result import AnnotatedResult
from openbb_core.provider.abstract.fetcher import Fetcher
from openbb_core.provider.standard_models.futures_curve import (
    FuturesCurveData,
    FuturesCurveQueryParams,
)
from pydantic import Field, field_validator, model_validator

from openbb_dq_quant_data.market import _query


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
        default=None, description="Market-store YAML; otherwise DQ_MARKET_CONFIG."
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


def _window(query: DQFuturesCurveQueryParams) -> tuple[datetime, datetime, datetime, int]:
    cutoff = _utc(query.as_of) if query.as_of else _now()
    age = query.max_age_seconds or (86400 if query.date else 900)
    end = cutoff + timedelta(microseconds=1)
    if query.date:
        day_start = datetime.combine(query.date, datetime.min.time(), UTC)
        end = min(end, day_start + timedelta(days=1))
        start = max(day_start, end - timedelta(seconds=age))
        if end <= start:
            raise OpenBBError("The observation date is after the as_of cutoff.")
    else:
        start = cutoff - timedelta(seconds=age)
    return cutoff, start, end, age


def _number(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        parsed = Decimal(str(value))
        return parsed if parsed.is_finite() else None
    except (InvalidOperation, ValueError):
        return None


def _select_latest(records: list[dict]) -> dict[str, dict]:
    selected: dict[str, dict] = {}
    for row in records:
        uid = row["instrument_uid"]
        previous = selected.get(uid)
        at = _utc(row["collector_observed_at"])
        if previous:
            previous_at = _utc(previous["collector_observed_at"])
            if at < previous_at:
                continue
            if at == previous_at and row["content_hash"] != previous["content_hash"]:
                raise OpenBBError("Futures quote ordering is ambiguous at one observation time.")
        selected[uid] = row
    return selected


class DQFuturesCurveFetcher(Fetcher[DQFuturesCurveQueryParams, list[DQFuturesCurveData]]):
    """Project a bounded central-store futures basket onto FuturesCurve."""

    require_credentials = False

    @staticmethod
    def transform_query(params: dict[str, Any]) -> DQFuturesCurveQueryParams:
        return DQFuturesCurveQueryParams(**params)

    @staticmethod
    def extract_data(
        query: DQFuturesCurveQueryParams, credentials: dict | None, **kwargs: Any
    ) -> dict:
        cutoff, start, end, age = _window(query)
        status = _query("status", query.config_path, dataset=query.dataset)["data"]
        if len(status) != 1 or status[0]["kind"] != "quotes":
            raise OpenBBError("Futures curves require a configured quote dataset.")
        references = _query(
            "instruments",
            query.config_path,
            query=query.symbol,
            as_of=cutoff,
            limit=query.max_contracts + 1,
        )
        if references["meta"]["has_more"]:
            raise OpenBBError("Futures reference listing is truncated; narrow the root.")
        contracts = []
        for row in references["data"]:
            if row["asset_class"] != "future" or row["provider"] != status[0]["provider"]:
                continue
            terms = json.loads(row.get("terms_json") or "{}")
            if not terms.get("expiration") or not terms.get("local_symbol"):
                raise OpenBBError(
                    "Qualified futures reference terms lack expiration or local symbol."
                )
            expiry = dateType.fromisoformat(terms["expiration"])
            if expiry < (end - timedelta(microseconds=1)).date():
                continue
            contracts.append({**row, "terms": terms})
        if not contracts:
            raise OpenBBError("No unexpired qualified futures contracts match the root and source.")
        if len(contracts) > query.max_contracts:
            raise OpenBBError("Futures contract limit exceeded; narrow the root.")
        uids = [row["instrument_uid"] for row in contracts]
        if len(uids) != len(set(uids)):
            raise OpenBBError("Futures reference identities are duplicated.")
        common = dict(
            dataset=query.dataset,
            instruments=uids,
            as_of=cutoff,
            availability=query.availability,
            layer=query.layer,
            publication_id=query.publication_id,
        )
        if query.date:
            result = _query(
                "history",
                query.config_path,
                **common,
                start=start,
                end=end,
                revisions="all",
                limit=100000,
            )
        else:
            result = _query(
                "basket",
                query.config_path,
                **common,
                max_age_seconds=age,
                max_skew_seconds=query.max_skew_seconds,
            )
        if result["meta"].get("next_cursor"):
            raise OpenBBError("Futures quote candidate limit exceeded; narrow the age window.")
        return {
            "contracts": contracts,
            "quotes": result,
            "reference_meta": references["meta"],
            "as_of": cutoff,
            "start": start,
            "end": end,
            "age": age,
        }

    @staticmethod
    def transform_data(
        query: DQFuturesCurveQueryParams, data: dict, **kwargs: Any
    ) -> AnnotatedResult[list[DQFuturesCurveData]]:
        selected = _select_latest(data["quotes"]["data"])
        reference_time = data["end"] - timedelta(microseconds=1)
        output, missing, invalid, blocked, times = [], [], [], [], []
        for contract in data["contracts"]:
            uid, terms = contract["instrument_uid"], contract["terms"]
            row = selected.get(uid)
            flags = json.loads(row.get("quality_flags_json") or "[]") if row else ["missing_quote"]
            bid, ask, last = [
                _number(row.get(key)) if row else None for key in ("bid", "ask", "last_trade_price")
            ]
            price = {"bid": bid, "ask": ask, "last": last}.get(query.price_type)
            if query.price_type == "midpoint":
                price = (
                    (bid + ask) / 2
                    if bid is not None and ask is not None and 0 <= bid <= ask
                    else None
                )
            if price is not None and price < 0:
                price = None
            if row is None:
                missing.append(uid)
            elif price is None:
                invalid.append(uid)
                flags = [*flags, "selected_price_unavailable"]
            if row and not row["gold_eligible"]:
                blocked.append(uid)
            observed = _utc(row["collector_observed_at"]) if row else None
            if observed:
                times.append(observed)
            source_currency = row.get("currency") if row else None
            if source_currency and terms.get("currency") and source_currency != terms["currency"]:
                raise OpenBBError("Futures quote currency conflicts with qualified contract terms.")
            output.append(
                DQFuturesCurveData(
                    date=observed.date() if observed else None,
                    expiration=terms["expiration"],
                    price=price,
                    symbol=query.symbol,
                    contract_symbol=terms["local_symbol"],
                    contract_month=terms.get("contract_month"),
                    instrument_uid=uid,
                    currency=source_currency or terms.get("currency"),
                    bid=bid,
                    ask=ask,
                    last_trade_price=last,
                    exchange=terms.get("exchange"),
                    trading_class=terms.get("trading_class"),
                    multiplier=str(terms["multiplier"])
                    if terms.get("multiplier") is not None
                    else None,
                    bid_size=_number(row.get("bid_size")) if row else None,
                    ask_size=_number(row.get("ask_size")) if row else None,
                    volume=_number(row.get("volume")) if row else None,
                    open_interest=_number(row.get("open_interest")) if row else None,
                    size_unit=row.get("size_unit") if row else None,
                    volume_unit=row.get("volume_unit") if row else None,
                    price_type=query.price_type,
                    collector_observed_at=observed,
                    event_time=row.get("event_time") if row else None,
                    age_seconds=(reference_time - observed).total_seconds() if observed else None,
                    actual_feed_type=row.get("actual_feed_type") if row else None,
                    gold_eligible=bool(row and row["gold_eligible"]),
                    quality_flags=flags,
                    observation_uid=row.get("observation_uid") if row else None,
                    reference_version_id=contract.get("version_id"),
                )
            )
        if (missing or invalid) and not query.allow_incomplete:
            raise OpenBBError(
                f"Incomplete futures curve: {len(missing)} missing "
                f"and {len(invalid)} invalid selected prices. "
                "Check layer/age bounds, or set allow_incomplete=True. Silver is diagnostic data."
            )
        output.sort(key=lambda row: (row.expiration, row.contract_symbol, row.instrument_uid))
        skew = (max(times) - min(times)).total_seconds() if times else None
        meta = {
            **data["quotes"]["meta"],
            "as_of": data["as_of"],
            "observation_start": data["start"],
            "observation_end_exclusive": data["end"],
            "quote_age_reference": reference_time,
            "price_type": query.price_type,
            "requested_contracts": len(output),
            "missing_instruments": sorted(missing),
            "invalid_price_instruments": sorted(invalid),
            "quality_blocked_instruments": sorted(blocked),
            "observed_time_skew_seconds": skew,
            "complete": not missing and not invalid,
            "eligible": not missing
            and not invalid
            and not blocked
            and skew is not None
            and skew <= query.max_skew_seconds,
            "max_age_seconds": data["age"],
            "max_skew_seconds": query.max_skew_seconds,
            "simultaneous_exchange_snapshot": False,
            "timing_basis": "collector_observation",
            "reference": data["reference_meta"],
            "reference_versions": {row.instrument_uid: row.reference_version_id for row in output},
        }
        meta["curve_snapshot_id"] = hashlib.sha256(
            json.dumps(
                [meta, [row.observation_uid for row in output]], default=str, sort_keys=True
            ).encode()
        ).hexdigest()
        return AnnotatedResult(result=output, metadata=meta)

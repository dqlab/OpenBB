"""Strict deployment configuration and explicit historical data contracts."""

from __future__ import annotations

import hashlib
import json
import re
from datetime import date, time
from pathlib import Path
from typing import Annotated, Literal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import yaml
from openbb_collector_core.delivery import DeliveryConfig
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

Name = Annotated[str, Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,79}$")]
FieldName = Annotated[str, Field(pattern=r"^[A-Za-z][A-Za-z0-9_]{0,79}$")]
Frequency = Literal["1m", "5m", "15m", "30m", "1h", "1d"]
BAR_SECONDS = {"1m": 60, "5m": 300, "15m": 900, "30m": 1800, "1h": 3600, "1d": 86400}
AssetType = Literal[
    "stock",
    "etf",
    "index",
    "future",
    "option",
    "future_option",
    "forex",
    "bond",
    "crypto",
    "cfd",
    "commodity",
]


class Model(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)


def valid_timezone(value: str) -> str:
    try:
        ZoneInfo(value)
    except (ZoneInfoNotFoundError, ValueError) as exc:
        raise ValueError("Use an installed IANA timezone") from exc
    return value


class Instrument(Model):
    symbol: str = Field(min_length=1, max_length=100)
    asset_type: AssetType
    currency: str = Field(pattern=r"^[A-Z]{3}$")
    timezone: str = "America/New_York"
    exchange: str = Field(default="SMART", min_length=1, max_length=80)
    primary_exchange: str | None = Field(default=None, max_length=80)
    con_id: int | None = Field(default=None, gt=0)
    source_symbols: dict[Name, str] = Field(default_factory=dict)

    _tz = field_validator("timezone")(valid_timezone)

    @field_validator("symbol")
    @classmethod
    def symbol_valid(cls, value: str) -> str:
        if "," in value or any(c.isspace() for c in value):
            raise ValueError("Use one explicit symbol per instrument")
        return value

    @field_validator("source_symbols")
    @classmethod
    def mappings_valid(cls, values: dict[str, str]) -> dict[str, str]:
        for value in values.values():
            if not value or len(value) > 100:
                raise ValueError("Invalid source symbol")
            cls.symbol_valid(value)
        return values


class Source(Model):
    provider: Name
    model: FieldName
    fixed_frequency: Frequency | None = None
    parameter_map: dict[FieldName, FieldName] = Field(
        default_factory=lambda: {
            "symbol": "symbol",
            "start_date": "start_date",
            "end_date": "end_date",
            "interval": "frequency",
        }
    )
    parameters: dict[FieldName, str | int | float | bool] = Field(default_factory=dict)
    credentials_env: dict[FieldName, str] = Field(default_factory=dict)
    field_map: dict[FieldName, FieldName] = Field(default_factory=dict)
    timestamp_field: FieldName = "date"
    timestamp_timezone: str | None = None
    adjustment: Literal["unadjusted", "splits_only", "splits_and_dividends", "provider_defined"] = (
        "provider_defined"
    )
    volume_unit: Literal["shares", "contracts", "lots", "provider_defined"] = "provider_defined"
    entitlement_reference: str | None = Field(default=None, max_length=200)
    timeout_seconds: float = Field(default=60, ge=0.1, le=300)
    min_interval_seconds: float = Field(default=2, ge=0.1, le=3600)
    retries: int = Field(default=1, ge=0, le=5)
    retry_delay_seconds: float = Field(default=2, ge=0, le=300)
    max_rows: int = Field(default=10000, ge=1, le=100000)
    max_response_bytes: int = Field(default=8000000, ge=1024, le=50000000)
    max_chunk_days: int = Field(default=7, ge=1, le=365)

    @field_validator("timestamp_timezone")
    @classmethod
    def tz_valid(cls, value: str | None) -> str | None:
        return valid_timezone(value) if value else value

    @model_validator(mode="after")
    def source_valid(self) -> Source:
        if "symbol" not in self.parameter_map.values():
            raise ValueError("Map the configured source symbol explicitly")
        if set(self.parameters) & set(self.parameter_map):
            raise ValueError("A parameter cannot have both a fixed and mapped value")
        for key, value in self.parameters.items():
            if re.search(r"key|token|password|secret|credential", key, re.I):
                raise ValueError("Use credentials_env references instead of inline credentials")
            if isinstance(value, str) and len(value) > 2048:
                raise ValueError("Provider parameter too long")
        if {"symbol", "provider", "model", "read_only"} & self.parameters.keys():
            raise ValueError("Reserved provider parameters")
        if self.adjustment != "provider_defined":
            if self.parameter_map.get("adjustment") != "adjustment":
                raise ValueError("Map the declared adjustment into the provider query")
        for value in self.credentials_env.values():
            if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
                raise ValueError("Credentials must reference environment variable names")
        return self


class Hours(Model):
    start: time = time(9, 30)
    end: time = time(16)

    @field_validator("start", "end")
    @classmethod
    def wall_time(cls, value: time) -> time:
        if value.tzinfo is not None:
            raise ValueError("Use local wall times without UTC offsets")
        return value


class Calendar(Hours):
    weekdays: list[int] = Field(default_factory=lambda: [0, 1, 2, 3, 4], min_length=1)
    holidays: set[date] = Field(default_factory=set)
    overrides: dict[date, Hours | None] = Field(default_factory=dict)

    @field_validator("weekdays")
    @classmethod
    def weekdays_valid(cls, values: list[int]) -> list[int]:
        if len(set(values)) != len(values) or any(v not in range(7) for v in values):
            raise ValueError("Use unique weekday numbers 0 (Monday) to 6 (Sunday)")
        return values


class Collection(Model):
    instrument_ids: list[Name] = Field(min_length=1, max_length=1000)
    primary: Name
    secondary: list[Name] = Field(default_factory=list, max_length=5)
    start_date: date
    end_date: date | None = None
    frequency: Frequency = "1d"
    fields: list[FieldName] = Field(
        default_factory=lambda: ["open", "high", "low", "close", "volume"]
    )
    required_fields: list[FieldName] = Field(
        default_factory=lambda: ["open", "high", "low", "close"]
    )
    what_to_show: Literal["TRADES", "MIDPOINT", "BID", "ASK", "ADJUSTED_LAST"] = "TRADES"
    use_rth: bool = True
    chunk_days: int = Field(default=7, ge=1, le=365)
    refresh_days: int = Field(default=3, ge=0, le=365)
    calendar: Calendar | None = None

    @model_validator(mode="after")
    def collection_valid(self) -> Collection:
        for values in (self.instrument_ids, self.fields, self.required_fields):
            if not values or len(set(values)) != len(values):
                raise ValueError("Instrument and field lists must be nonempty and unique")
        if not set(self.required_fields) <= set(self.fields):
            raise ValueError("Required fields must be selected in fields")
        if len(set([self.primary, *self.secondary])) != len([self.primary, *self.secondary]):
            raise ValueError("Primary and secondary sources must be distinct")
        if self.end_date and self.end_date <= self.start_date:
            raise ValueError("end_date is exclusive and must follow start_date")
        if self.start_date < date(1900, 1, 1):
            raise ValueError("Historical start must be 1900 or later")
        if self.calendar and not self.use_rth:
            raise ValueError("A coverage calendar describes RTH; extended hours need no calendar")
        if self.frequency == "1d" and self.calendar and self.calendar.end <= self.calendar.start:
            raise ValueError("Overnight daily bar date conventions need provider-specific coverage")
        return self


class Storage(Model):
    root: Path = Path("data/historical_market_data")
    format: Literal["jsonl", "csv", "parquet", "sqlite", "duckdb"] = "jsonl"
    export_batch_size: int = Field(default=500, ge=1, le=10000)


class Schedule(Model):
    timezone: str = "America/New_York"
    at: time = time(6)
    weekdays: list[int] = Field(default_factory=lambda: list(range(7)), min_length=1)
    holidays: set[date] = Field(default_factory=set)
    catch_up_days: int = Field(default=7, ge=1, le=366)
    retry_seconds: float = Field(default=300, ge=1, le=86400)
    heartbeat_seconds: float = Field(default=1, ge=0.1, le=60)
    reset: Literal["never", "weekly", "monthly"] = "weekly"
    include_current_session: bool = False

    _tz = field_validator("timezone")(valid_timezone)
    _days = field_validator("weekdays")(Calendar.weekdays_valid.__func__)
    _at = field_validator("at")(Hours.wall_time.__func__)


class CollectorConfig(Model):
    version: Literal[2] = 2
    delivery: DeliveryConfig | None = None
    instruments: dict[Name, Instrument] = Field(min_length=1, max_length=1000)
    sources: dict[Name, Source] = Field(min_length=1, max_length=32)
    collections: dict[Name, Collection] = Field(min_length=1, max_length=100)
    storage: Storage = Field(default_factory=Storage)
    schedule: Schedule = Field(default_factory=Schedule)
    max_chunks_per_session: int = Field(default=100, ge=1, le=10000)

    @model_validator(mode="after")
    def references_valid(self) -> CollectorConfig:
        for instrument in self.instruments.values():
            if set(instrument.source_symbols) - self.sources.keys():
                raise ValueError("Unknown source symbol mapping")
        for collection in self.collections.values():
            if set(collection.instrument_ids) - self.instruments.keys():
                raise ValueError("Unknown collection instrument")
            for source_id in [collection.primary, *collection.secondary]:
                if source_id not in self.sources:
                    raise ValueError("Unknown collection source")
                source = self.sources[source_id]
                required_context = {"symbol", "start_date", "end_date"}
                if not required_context <= set(source.parameter_map.values()):
                    raise ValueError("Historical queries must map symbol and dates")
                if "frequency" not in source.parameter_map.values():
                    if source.fixed_frequency != collection.frequency:
                        raise ValueError("Map frequency or declare the source's fixed_frequency")
                elif source.fixed_frequency is not None:
                    raise ValueError("Use either mapped or fixed frequency, not both")
                if collection.what_to_show != "TRADES":
                    if "what_to_show" not in source.parameter_map.values():
                        raise ValueError("Map the requested price type into the provider query")
                if not collection.use_rth:
                    if not {"use_rth", "extended_hours"} & set(source.parameter_map.values()):
                        raise ValueError("Map the requested extended-hours policy")
        return self

    def fingerprint(self) -> str:
        payload = self.model_dump(mode="json")
        for key in ("storage", "delivery", "schedule", "max_chunks_per_session"):
            payload.pop(key)
        for collection in payload["collections"].values():
            if collection["calendar"]:
                collection["calendar"]["holidays"].sort()
        return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()


def load_config(path: str | Path) -> CollectorConfig:
    path = Path(path).resolve()
    if path.stat().st_size > 2_000_000:
        raise ValueError("Configuration exceeds 2 MB")
    config = CollectorConfig.model_validate(yaml.safe_load(path.read_text(encoding="utf-8")))
    if not config.storage.root.is_absolute():
        config.storage.root = (path.parent / config.storage.root).resolve()
    if config.delivery:
        config.delivery.resolve_paths(path.parent)
    return config

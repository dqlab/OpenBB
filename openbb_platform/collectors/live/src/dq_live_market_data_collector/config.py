"""Validated configuration; paths are relative to the YAML file, never a NAS default."""

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
AssetType = Literal[
    "stock",
    "etf",
    "index",
    "future",
    "option",
    "future_option",
    "forex",
    "bond",
    "fund",
    "crypto",
    "cfd",
    "commodity",
]
FeedType = Literal["live", "delayed", "frozen", "delayed_frozen", "unknown"]


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)


def valid_timezone(value: str) -> str:
    try:
        ZoneInfo(value)
    except (ZoneInfoNotFoundError, ValueError) as exc:
        raise ValueError("Use an installed IANA timezone") from exc
    return value


class Source(StrictModel):
    provider: Name
    model: FieldName
    parameter_map: dict[FieldName, FieldName] = Field(default_factory=lambda: {"symbol": "symbol"})
    requested_feed_type: Literal[
        "live", "delayed", "frozen", "delayed_frozen", "provider_default"
    ] = "provider_default"
    parameters: dict[FieldName, str | int | float | bool | None] = Field(
        default_factory=dict, max_length=30
    )
    credentials_env: dict[FieldName, str] = Field(default_factory=dict)
    timeout_seconds: float = Field(default=20, ge=0.1, le=120)
    min_interval_seconds: float = Field(default=1, ge=0.1, le=3600)
    snapshot_wait_seconds: float = Field(default=4, ge=0.7, le=30)
    retries: int = Field(default=1, ge=0, le=5)
    max_rows: int = Field(default=10, ge=1, le=1000)
    max_response_bytes: int = Field(default=1_000_000, ge=1024, le=10_000_000)
    field_map: dict[FieldName, FieldName] = Field(default_factory=dict)
    timestamp_field: FieldName | None = None
    timestamp_timezone: str | None = None
    timestamp_unit: Literal["seconds", "milliseconds"] = "seconds"
    timestamp_semantics: Literal["unknown", "provider_event", "last_trade", "provider_receive"] = (
        "unknown"
    )
    revision_field: FieldName | None = None
    feed_type_field: FieldName | None = None
    feed_type_map: dict[str, FeedType] = Field(default_factory=dict)
    entitlement_reference: str | None = Field(default=None, max_length=200)
    adjustment: Literal["unadjusted", "provider_defined", "unknown"] = "unknown"
    size_unit: Literal["shares", "contracts", "lots", "provider_defined", "unknown"] = "unknown"

    @field_validator("timestamp_timezone")
    @classmethod
    def timezone_exists(cls, value: str | None) -> str | None:
        return valid_timezone(value) if value else value

    @model_validator(mode="after")
    def check_provider(self) -> Source:
        if "symbol" not in self.parameter_map.values():
            raise ValueError("Map the configured source symbol explicitly")
        if set(self.parameters) & set(self.parameter_map):
            raise ValueError("A parameter cannot have both a fixed and mapped value")
        if {"symbol", "provider", "model", "read_only"} & self.parameters.keys():
            raise ValueError("Use explicit mappings instead of reserved parameters")
        if self.requested_feed_type != "provider_default":
            if "requested_feed_type" not in self.parameter_map.values():
                raise ValueError("Map the requested feed type into the provider query")
        for value in self.parameters.values():
            if isinstance(value, str) and len(value) > 2048:
                raise ValueError("Provider parameter exceeds 2048 characters")
        for name in self.parameters:
            if re.search(r"key|token|password|secret|credential", name, re.I):
                raise ValueError("Use credentials_env references instead of inline credentials")
        for env in self.credentials_env.values():
            if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", env):
                raise ValueError("credentials_env values must be environment variable names")
        return self


class Instrument(StrictModel):
    symbol: str = Field(min_length=1, max_length=100)
    asset_type: AssetType
    currency: str = Field(pattern=r"^[A-Z]{3}$")
    exchange: str = Field(default="SMART", min_length=1, max_length=80)
    primary_exchange: str | None = None
    con_id: int | None = Field(default=None, gt=0)
    source_symbols: dict[Name, str] = Field(default_factory=dict)

    @field_validator("symbol")
    @classmethod
    def one_symbol(cls, value: str) -> str:
        if value != value.strip() or "," in value or any(c.isspace() for c in value):
            raise ValueError("Configure one explicit symbol per instrument")
        return value

    @field_validator("source_symbols")
    @classmethod
    def one_source_symbol(cls, values: dict[str, str]) -> dict[str, str]:
        for value in values.values():
            cls.one_symbol(value)
            if not value:
                raise ValueError("Source symbol cannot be empty")
        return values


class SessionHours(StrictModel):
    start: time
    end: time

    @field_validator("start", "end")
    @classmethod
    def wall_time(cls, value: time) -> time:
        if value.tzinfo is not None:
            raise ValueError("Session times are local wall times without offsets")
        return value


class Schedule(SessionHours):
    timezone: str = "America/New_York"
    weekdays: list[int] = Field(default_factory=lambda: [0, 1, 2, 3, 4], min_length=1)
    holidays: set[date] = Field(default_factory=set)
    overrides: dict[date, SessionHours | None] = Field(default_factory=dict)

    _timezone = field_validator("timezone")(valid_timezone)

    @field_validator("weekdays")
    @classmethod
    def weekdays_valid(cls, value: list[int]) -> list[int]:
        if len(set(value)) != len(value) or any(day not in range(7) for day in value):
            raise ValueError("weekdays must be unique integers 0 (Monday) to 6 (Sunday)")
        return value


class Collection(StrictModel):
    instrument_ids: list[Name] = Field(min_length=1, max_length=1000)
    primary: Name
    secondary: list[Name] = Field(default_factory=list, max_length=5)
    frequency_seconds: float = Field(default=60, ge=0.25, le=86400)
    fields: list[FieldName] = Field(default_factory=lambda: ["last_price", "bid", "ask"])
    required_fields: list[FieldName] = Field(default_factory=lambda: ["last_price"])
    max_age_seconds: float | None = Field(default=None, gt=0, le=604800)
    max_future_seconds: float = Field(default=5, ge=0, le=300)
    accepted_feed_types: list[FeedType] = Field(default_factory=lambda: ["live", "unknown"])
    reject_crossed_quotes: bool = True
    schedule: Schedule

    @model_validator(mode="after")
    def unique_fields(self) -> Collection:
        for values in (self.instrument_ids, self.fields, self.required_fields):
            if not values or len(values) != len(set(values)):
                raise ValueError("Instrument and field lists must be nonempty and unique")
        if not set(self.required_fields) <= set(self.fields):
            raise ValueError("required_fields must be included in fields")
        sources = [self.primary, *self.secondary]
        if len(sources) != len(set(sources)):
            raise ValueError("Primary and secondary sources must be distinct")
        if not self.accepted_feed_types:
            raise ValueError("accepted_feed_types cannot be empty")
        return self


class StorageConfig(StrictModel):
    root: Path = Path("data/live_market_data")
    format: Literal["jsonl", "csv", "parquet", "sqlite", "duckdb"] = "jsonl"
    export_batch_size: int = Field(default=200, ge=1, le=10000)


class ResetConfig(StrictModel):
    period: Literal["never", "weekly", "monthly"] = "weekly"
    timezone: str = "UTC"

    _timezone = field_validator("timezone")(valid_timezone)


class CollectorConfig(StrictModel):
    version: Literal[2] = 2
    delivery: DeliveryConfig | None = None
    sources: dict[Name, Source] = Field(min_length=1, max_length=32)
    instruments: dict[Name, Instrument] = Field(min_length=1, max_length=1000)
    collections: dict[Name, Collection] = Field(min_length=1, max_length=100)
    storage: StorageConfig = Field(default_factory=StorageConfig)
    reset: ResetConfig = Field(default_factory=ResetConfig)
    heartbeat_seconds: float = Field(default=1, ge=0.1, le=60)

    @model_validator(mode="after")
    def references(self) -> CollectorConfig:
        for instrument in self.instruments.values():
            if set(instrument.source_symbols) - self.sources.keys():
                raise ValueError("Unknown source in instrument source_symbols")
        for collection in self.collections.values():
            source_ids = [collection.primary, *collection.secondary]
            if set(source_ids) - self.sources.keys():
                raise ValueError("Collection references an unknown source")
            if set(collection.instrument_ids) - self.instruments.keys():
                raise ValueError("Collection references an unknown instrument")
            for source_id in source_ids:
                source = self.sources[source_id]
                if source.requested_feed_type in {"delayed", "delayed_frozen"} and not (
                    {"delayed", "delayed_frozen"} & set(collection.accepted_feed_types)
                ):
                    raise ValueError("A delayed request requires explicit delayed acceptance")
        return self

    def fingerprint(self) -> str:
        payload = self.model_dump(mode="json")
        # Storage is a deployment location, not part of the observation contract.
        payload.pop("storage")
        payload.pop("delivery")
        for collection in payload["collections"].values():
            collection["schedule"]["holidays"].sort()
        return hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()


def load_config(path: str | Path) -> CollectorConfig:
    path = Path(path).resolve()
    if path.stat().st_size > 2_000_000:
        raise ValueError("Configuration exceeds 2 MB")
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    config = CollectorConfig.model_validate(raw)
    if not config.storage.root.is_absolute():
        config.storage.root = (path.parent / config.storage.root).resolve()
    if config.delivery:
        config.delivery.resolve_paths(path.parent)
    return config

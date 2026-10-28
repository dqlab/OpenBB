"""Strict local-only configuration and an explicitly covered exchange calendar."""

from __future__ import annotations

import json
import re
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Literal
from zoneinfo import ZoneInfo

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from .identity import UNIT_FIELDS
from .identity import fingerprint as capture_fingerprint

SHANGHAI = ZoneInfo("Asia/Shanghai")

CALENDAR_SOURCE = (
    "akshare.tool_trade_date_hist_sina (Sina/SSE reference calendar; "
    "not an exchange tick/event calendar)"
)

# ``UNIT_FIELDS`` (re-exported above) is the only set of unit fields a payload may
# declare. An empty declaration means the payload's units are unknown, and the
# committed path refuses it as ``units_undetermined`` rather than inventing a label.



class ConfigurationError(ValueError):
    """Explicit safe diagnostics, without provider text or configuration values."""

    def __init__(self, code: str, message: str | None = None):
        self.code = code
        super().__init__(message or code)


def iso_date(value: object) -> date:
    """Accept dates and exact ISO dates, never timestamps or basic ISO spellings."""
    if type(value) is date:
        return value
    if not isinstance(value, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        raise ValueError("Expected an exact YYYY-MM-DD date")
    return date.fromisoformat(value)


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False, validate_default=True)


class SourceLimits(StrictModel):
    provider: Literal["akshare"] = "akshare"
    credentials_env: dict[str, str] = Field(default_factory=dict)
    timeout_seconds: float = Field(default=60, ge=0.1, le=300, strict=True)
    min_interval_seconds: float = Field(default=2, ge=0.1, le=3600, strict=True)
    max_rows: int = Field(default=10000, ge=1, le=100000, strict=True)
    max_response_bytes: int = Field(default=8000000, ge=1024, le=50000000, strict=True)
    retries: int = Field(default=2, ge=0, le=2, strict=True)
    retry_delay_seconds: float = Field(default=2, ge=0, le=60, strict=True)

    @field_validator("credentials_env")
    @classmethod
    def no_credentials(cls, value: dict[str, str]) -> dict[str, str]:
        if value:
            raise ValueError("CFFEX public sources require credentials_env: {}")
        return value


class Source(SourceLimits):
    model: Literal["AkshareFuturesHistDailyCffex", "AkshareCffexDaily"] = (
        "AkshareFuturesHistDailyCffex"
    )


class CalendarSource(SourceLimits):
    """Internal public calendar acquisition; never accepted as the daily source."""

    model: Literal["AkshareToolTradeDateHistSina"] = "AkshareToolTradeDateHistSina"


class Config(StrictModel):
    version: Literal[1] = 1
    root: Path = Path("data/cffex")
    calendar_file: Path
    # Private immutable capture store (``raw-acquisition-v1``). Independent of
    # the journal: acquisition may write it while the journal is closed.
    evidence_root: Path | None = None
    source: Source = Field(default_factory=Source)
    source_id: str = Field(default="cffex", pattern=r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,79}$")
    start_date: date = date(2010, 4, 16)
    update_start_date: date | None = None
    not_before: time = time(18, 30)
    # Explicit, fingerprinted unit declaration for the daily payload. The source
    # layout states no unit column, so a deployment that may commit rows must say
    # once, in configuration, what the numbers are in. Empty = undetermined.
    units: dict[str, str] = Field(default_factory=dict)

    @field_validator("units")
    @classmethod
    def declared_units(cls, value: dict[str, str]) -> dict[str, str]:
        if set(value) - set(UNIT_FIELDS):
            raise ValueError(f"units must be a subset of {UNIT_FIELDS}")
        if any(type(item) is not str or not item or len(item) > 64 for item in value.values()):
            raise ValueError("units must be non-empty strings")
        return dict(value)

    @field_validator("start_date", "update_start_date", mode="before")
    @classmethod
    def dates(cls, value: object) -> date | None:
        return None if value is None else iso_date(value)

    @field_validator("not_before", mode="before")
    @classmethod
    def cutoff(cls, value: object) -> time:
        if isinstance(value, str):
            if not re.fullmatch(r"\d{2}:\d{2}", value):
                raise ValueError("not_before must be HH:MM")
            value = time.fromisoformat(value)
        if not isinstance(value, time) or value.tzinfo or value.second or value.microsecond:
            raise ValueError("not_before must be a local HH:MM wall time")
        return value

    @model_validator(mode="after")
    def since_launch(self) -> Config:
        if self.start_date < date(2010, 4, 16):
            raise ValueError("start_date precedes CFFEX futures launch")
        if self.update_start_date and self.update_start_date < self.start_date:
            raise ValueError("update_start_date must not precede start_date")
        return self

    @property
    def evidence(self) -> Path:
        """Capture root; defaults beside the journal, never inside ``raw/``."""
        return Path(self.evidence_root) if self.evidence_root else Path(self.root) / "evidence"

    def fingerprint(self) -> str:
        """Stable identity of the transport authority that shapes a capture.

        Delegates to :func:`cffex.identity.fingerprint`, which the acquisition
        task also calls, so an acquire plan cannot name a generation the
        collector would derive differently. Clocks, calendars and cursors are
        deliberately excluded: only the provider, its limits, the source
        identity and the declared units decide whether two captures of the same
        day are comparable.
        """
        return capture_fingerprint(
            self.source_id, self.source.model_dump(mode="json"), self.units
        )


class TradingCalendar(StrictModel):
    timezone: Literal["Asia/Shanghai"]
    coverage_start: date
    coverage_end: date
    trading_dates: list[date]
    source: str = Field(min_length=1, max_length=2000)
    captured_at: datetime | None = None

    @field_validator("captured_at")
    @classmethod
    def captured_utc(cls, value: datetime | None) -> datetime | None:
        if value and (value.tzinfo is None or value.utcoffset() != timedelta(0)):
            raise ValueError("captured_at must be UTC")
        return value

    @field_validator("coverage_start", "coverage_end", mode="before")
    @classmethod
    def dates(cls, value: object) -> date:
        return iso_date(value)

    @field_validator("trading_dates", mode="before")
    @classmethod
    def list_dates(cls, value: object) -> list[date]:
        if not isinstance(value, list):
            raise ValueError("trading_dates must be a list")
        return [iso_date(day) for day in value]

    @model_validator(mode="after")
    def covered(self) -> TradingCalendar:
        if self.coverage_end < self.coverage_start:
            raise ValueError("Invalid calendar coverage")
        if len(set(self.trading_dates)) != len(self.trading_dates):
            raise ValueError("Duplicate calendar trading dates")
        if any(not self.coverage_start <= day <= self.coverage_end for day in self.trading_dates):
            raise ValueError("Trading date outside explicit calendar coverage")
        self.trading_dates.sort()
        return self

    def days(self, start: date, end: date) -> list[date]:
        """Inclusive bounds; omissions within asserted coverage are closed days."""
        if end < start:
            raise ConfigurationError("invalid_date_range", "end_date precedes start_date")
        if start < self.coverage_start or end > self.coverage_end:
            raise ConfigurationError(
                "calendar_coverage_exceeded", "Requested range exceeds explicit calendar coverage"
            )
        return [day for day in self.trading_dates if start <= day <= end]


class UniqueKeyLoader(yaml.SafeLoader):
    """Duplicate configuration keys are errors rather than last-value-wins changes."""

    def construct_mapping(self, node: yaml.MappingNode, deep: bool = False) -> dict:
        self.flatten_mapping(node)
        result = {}
        for key_node, value_node in node.value:
            key = self.construct_object(key_node, deep=deep)
            if not isinstance(key, str) or key in result:
                raise ValueError("Configuration mapping keys must be unique strings")
            result[key] = self.construct_object(value_node, deep=deep)
        return result


def load_config(path: str | Path) -> Config:
    path = Path(path).resolve()
    if path.stat().st_size > 2_000_000:
        raise ValueError("Configuration exceeds 2 MB")
    config = Config.model_validate(
        yaml.load(path.read_text(encoding="utf-8"), Loader=UniqueKeyLoader)
    )
    config.root = (path.parent / config.root).resolve()
    config.calendar_file = (path.parent / config.calendar_file).resolve()
    if config.evidence_root is not None:
        config.evidence_root = (path.parent / config.evidence_root).resolve()
    return config


def load_calendar(path: Path) -> TradingCalendar:
    if path.stat().st_size > 5_000_000:
        raise ValueError("Calendar exceeds 5 MB")
    return TradingCalendar.model_validate(json.loads(path.read_text(encoding="utf-8")))


def eligible_day(config: Config, now: datetime, as_of: date | None = None) -> date:
    """Use Shanghai date; a date-only as-of never authorizes future/unpublished data."""
    if now.tzinfo is None:
        raise ConfigurationError("invalid_clock", "Clock must be timezone-aware")
    local = now.astimezone(SHANGHAI)
    cutoff_day = local.date() - timedelta(days=local.time() < config.not_before)
    if as_of and as_of > cutoff_day:
        raise ConfigurationError(
            "publication_cutoff_exceeded", "as_of exceeds the last publication-eligible day"
        )
    return as_of or cutoff_day

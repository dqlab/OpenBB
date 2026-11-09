# Keep acquisition-only imports free of provider/parsing/journal initialization.
# pylint: disable=import-outside-toplevel
# Evidence schemas require exact built-in types, including rejection of bool-as-int.
# pylint: disable=unidiomatic-typecheck

"""Strict public-source configuration and fail-closed reference calendar.

Units are never inferred here.  ``units`` is a **deployment declaration** that is
part of the acquisition fingerprint, so the same store cannot silently accept a
different scale, and an empty declaration is a typed refusal on the committed
path (``units_undetermined``) rather than a fabricated label such as
``provider_defined``.
"""

from __future__ import annotations

import json
import re
from datetime import date, datetime, time
from pathlib import Path
from typing import Literal, cast
from zoneinfo import ZoneInfo

import yaml
from openbb_collector_core.delivery import DeliveryConfig
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

SHANGHAI = ZoneInfo("Asia/Shanghai")

# The unit labels a deployment may declare for this lane.  ``contract_unit`` is
# deliberately absent: a multiplier is a per-contract source field, not a lane
# declaration, and an unstated one stays ``None``/``unknown_contract_unit``.
UNIT_FIELDS = ("price_unit", "size_unit", "volume_unit", "turnover_unit")
FORBIDDEN_UNIT_LABELS = {"provider_defined", "unknown", "guess"}


class ConfigurationError(ValueError):
    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


def iso_date(value: object) -> date:
    if type(value) is date:
        return value
    if not isinstance(value, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        raise ConfigurationError("invalid_iso_date")
    return date.fromisoformat(value)


class StrictModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid", allow_inf_nan=False, validate_default=True
    )


class SourceLimits(StrictModel):
    """Duck-typed ProcessClient source fields, not native OpenBB catalog aliases."""

    provider: Literal["akshare"] = "akshare"
    credentials_env: dict[str, str] = Field(default_factory=dict)
    timeout_seconds: float = Field(default=45.0, ge=0.1, le=300, strict=True)
    min_interval_seconds: float = Field(default=2.0, ge=0.1, le=3600, strict=True)
    max_rows: int = Field(default=10000, ge=50, le=100000, strict=True)
    max_response_bytes: int = Field(default=8000000, ge=1024, le=50000000, strict=True)
    retries: int = Field(default=2, ge=0, le=2, strict=True)
    retry_delay_seconds: float = Field(default=2.0, ge=0, le=60, strict=True)

    @field_validator("credentials_env")
    @classmethod
    def public_only(cls, value: dict) -> dict:
        if value:
            raise ValueError("public_sources_require_empty_credentials")
        return value


class Source(SourceLimits):
    model: Literal[
        "AkshareEtfOptionContracts",
        "AkshareEtfOptionQuotes",
        "AkshareToolTradeDateHistSina",
    ]


class ExchangeConfig(StrictModel):
    source: Literal["official", "sina"]
    underlyings: list[str] | None = None

    @field_validator("underlyings")
    @classmethod
    def codes(cls, value: list[str] | None) -> list[str] | None:
        if value is not None:
            if (
                not value
                or len(set(value)) != len(value)
                or any(not re.fullmatch(r"[0-9]{6}", code) for code in value)
            ):
                raise ValueError("invalid_underlyings")
            return sorted(value)
        return value


class Config(StrictModel):
    # Pydantic replaces Field factories with the annotated runtime values.
    # pylint: disable=no-member
    version: Literal[1] = 1
    root: Path = Path("state/etf_options")
    delivery: DeliveryConfig | None = None
    calendar_file: Path
    # Where the immutable transport evidence lives.  Defaults to ``root/evidence``
    # and is resolved relative to the configuration file, exactly like ``root``.
    evidence: Path | None = None
    exchanges: dict[Literal["SSE", "SZSE"], ExchangeConfig] = Field(
        default_factory=lambda: cast(
            dict[Literal["SSE", "SZSE"], ExchangeConfig],
            {
                "SSE": ExchangeConfig(source="sina"),
                "SZSE": ExchangeConfig(source="official"),
            },
        )
    )
    limits: SourceLimits = Field(default_factory=SourceLimits)
    block_size: int = Field(default=50, ge=1, le=50, strict=True)
    max_requests: int = Field(default=250, ge=1, le=5000, strict=True)
    max_run_seconds: float = Field(default=1800.0, ge=1, le=21600, strict=True)
    not_before: time = time(15, 10)
    quote_not_before: time = time(14, 57)
    source_id: str = Field(
        default="etf_options", pattern=r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,79}$"
    )
    # Deployment unit declaration.  Empty means "nobody stated the units", which
    # the committed path refuses instead of labelling them.
    units: dict[str, str] = Field(default_factory=dict)

    @field_validator("root", "calendar_file", "evidence", mode="before")
    @classmethod
    def local_paths(cls, value: object) -> object:
        if value is None:
            return value
        if (
            not isinstance(value, (str, Path))
            or not str(value).strip()
            or ":" in str(value)
        ):
            raise ValueError("local_filesystem_path_required")
        return value

    @field_validator("not_before", "quote_not_before", mode="before")
    @classmethod
    def wall_time(cls, value: object) -> time:
        if isinstance(value, str) and re.fullmatch(r"[0-9]{2}:[0-9]{2}", value):
            value = time.fromisoformat(value)
        if (
            not isinstance(value, time)
            or value.tzinfo
            or value.second
            or value.microsecond
        ):
            raise ValueError("local_HH_MM_required")
        return value

    @field_validator("units")
    @classmethod
    def declared_units(cls, value: dict) -> dict:
        unknown = set(value) - set(UNIT_FIELDS)
        if unknown:
            raise ValueError(f"units must be a subset of {UNIT_FIELDS}")
        for label in value.values():
            if not isinstance(label, str) or not label.strip():
                raise ValueError("unit labels must be non-empty strings")
            if label.strip().lower() in FORBIDDEN_UNIT_LABELS:
                raise ValueError(
                    f"unit label {label!r} is not a source or deployment fact"
                )
        return dict(value)

    @model_validator(mode="after")
    def safe(self) -> Config:
        if not self.exchanges:
            raise ValueError("empty_exchanges")
        if self.not_before < time(15, 10) or not time(
            14, 57
        ) <= self.quote_not_before <= time(15, 10):
            raise ValueError("unsafe_close_threshold")
        if self.calendar_file.resolve() == self.root.resolve():
            raise ValueError("calendar_path_is_root")
        if self.evidence is not None and self.evidence.resolve() == self.root.resolve():
            raise ValueError("evidence_path_is_root")
        return self

    def source(self, model: str) -> Source:
        return Source.model_validate({**self.limits.model_dump(), "model": model})

    def identity(self) -> dict:
        return self.model_dump(
            mode="json", exclude={"root", "calendar_file", "delivery", "evidence"}
        )

    @property
    def evidence_root(self) -> Path:
        return self.evidence if self.evidence is not None else (self.root / "evidence")

    def fingerprint(self) -> dict:
        """Acquisition identity: configuration plus the declared units.

        Units are part of it on purpose -- two deployments that disagree about
        the scale of the same numbers must not share a capture generation.
        """
        return {
            "source_id": self.source_id,
            "limits": self.limits.model_dump(mode="json"),
            "exchanges": {
                exchange: settings.model_dump(mode="json")
                for exchange, settings in sorted(self.exchanges.items())
            },
            "block_size": self.block_size,
            "units": dict(self.units),
        }

    def unit_authority(self) -> dict[str, str] | None:
        """The declared units, or ``None`` when nothing was declared.

        ``None`` is not a default: it is the absence the committed path refuses.
        """
        return dict(self.units) if self.units else None


class TradingCalendar(StrictModel):
    timezone: Literal["Asia/Shanghai"] = "Asia/Shanghai"
    coverage_start: date
    coverage_end: date
    trading_dates: list[date]
    source: str = Field(min_length=1)
    captured_at: datetime | None = None

    @field_validator("coverage_start", "coverage_end", mode="before")
    @classmethod
    def dates(cls, value: object) -> date:
        return iso_date(value)

    @field_validator("trading_dates", mode="before")
    @classmethod
    def session_dates(cls, value: object) -> list[date]:
        if not isinstance(value, list):
            raise ValueError("invalid_calendar_dates")
        return [iso_date(item) for item in value]

    @model_validator(mode="after")
    def covered(self) -> TradingCalendar:
        if not self.trading_dates or self.coverage_start > self.coverage_end:
            raise ValueError("empty_or_invalid_calendar")
        if len(set(self.trading_dates)) != len(self.trading_dates) or any(
            day.weekday() >= 5 or not self.coverage_start <= day <= self.coverage_end
            for day in self.trading_dates
        ):
            raise ValueError("invalid_calendar_dates")
        self.trading_dates.sort()
        return self

    def is_session(self, day: date) -> bool:
        if not self.coverage_start <= day <= self.coverage_end:
            raise ConfigurationError("calendar_coverage_exceeded")
        return day in self.trading_dates


class UniqueKeyLoader(yaml.SafeLoader):  # pylint: disable=too-many-ancestors
    def construct_mapping(self, node: yaml.MappingNode, deep: bool = False) -> dict:
        self.flatten_mapping(node)
        result = {}
        for key_node, value_node in node.value:
            key = self.construct_object(key_node, deep=deep)
            if not isinstance(key, str) or key in result:
                raise ValueError("duplicate_or_invalid_yaml_key")
            result[key] = self.construct_object(value_node, deep=deep)
        return result


def load_config(path: str | Path) -> Config:
    path = Path(path).resolve()
    if path.stat().st_size > 2_000_000:
        raise ValueError("config_size_limit")
    cfg = Config.model_validate(yaml.load(path.read_text(), Loader=UniqueKeyLoader))
    cfg.root = (path.parent / cfg.root).resolve()
    cfg.calendar_file = (path.parent / cfg.calendar_file).resolve()
    if cfg.evidence is not None:
        cfg.evidence = (path.parent / cfg.evidence).resolve()
    if cfg.delivery is not None:
        cfg.delivery.resolve_paths(path.parent)
    from .storage import protected_target  # pylint: disable=cyclic-import

    if protected_target(cfg.root, cfg.calendar_file):
        raise ConfigurationError("protected_calendar_path")
    return cfg


def load_calendar(path: Path) -> TradingCalendar:
    if path.stat().st_size > 5_000_000:
        raise ValueError("calendar_size_limit")
    return TradingCalendar.model_validate(json.loads(path.read_text()))

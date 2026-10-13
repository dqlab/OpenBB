"""AKShare request validation, dataframe normalization, and async execution."""

import asyncio
import math
import re
from collections.abc import Callable
from datetime import (
    date as dateType,
    datetime,
)
from typing import Any

import pandas as pd
from openbb_core.app.model.abstract.error import OpenBBError

from openbb_akshare.utils.catalog import resolve_method

_SYMBOL = re.compile(r"^[A-Za-z][A-Za-z0-9]{0,15}$")
_INDEX_SYMBOL = re.compile(r"^(sh|sz|bj)[0-9]{6}$")
_PERIODS = {"1", "5", "15", "30", "60"}


def normalize_symbol(value: str) -> str:
    """Validate and upper-case a China futures root or dated contract."""
    symbol = str(value).strip().upper()
    if not _SYMBOL.fullmatch(symbol):
        raise ValueError("Use an AKShare China futures symbol such as IF0, IF2610, or RB0.")
    return symbol


def normalize_index_symbol(value: str) -> str:
    """Validate and lower-case a Sina index symbol.

    Sina index symbols are lower-case and exchange-qualified (``sh000300``),
    so they must NOT go through the futures normalizer, which upper-cases.
    """
    symbol = str(value).strip().lower()
    if not _INDEX_SYMBOL.fullmatch(symbol):
        raise ValueError("Use a Sina index symbol such as sh000300, sz399001, or sh000001.")
    return symbol


def normalize_period(value: str) -> str:
    """Restrict the intraday frequency to the values the source supports."""
    period = str(value).strip()
    if period not in _PERIODS:
        raise ValueError(f"period must be one of {sorted(_PERIODS, key=int)}.")
    return period


def normalize_vars_list(value: str | list[str] | None) -> list[str] | None:
    """Split a comma-separated product list into validated, upper-cased codes.

    OpenBB joins list-valued parameters into a comma-separated string before
    they reach the query model, so both forms are accepted here.
    """
    if value is None:
        return None
    items = value.split(",") if isinstance(value, str) else list(value)
    codes = [str(item).strip().upper() for item in items if str(item).strip()]
    if not codes:
        return None
    for code in codes:
        if not re.fullmatch(r"[A-Z]{1,4}", code):
            raise ValueError(f"Invalid product code: {code!r}.")
    return codes


def source_date(value: dateType | datetime | str | None) -> str | None:
    """Serialize a query date to the ``YYYYMMDD`` form the source requires."""
    if value is None:
        return None
    if isinstance(value, datetime):
        value = value.date()
    if isinstance(value, str):
        value = dateType.fromisoformat(value)
    return value.strftime("%Y%m%d")


def _json_safe(value: Any) -> Any:
    """Convert pandas/NumPy scalars to JSON-safe Python values without guessing units."""
    if value is None or value is pd.NaT:
        return None
    if isinstance(value, float):
        return None if math.isnan(value) or math.isinf(value) else value
    if isinstance(value, (datetime, dateType)):
        return value.isoformat()
    if hasattr(value, "item") and not isinstance(value, (str, bytes, bool)):
        try:
            value = value.item()
        except (ValueError, AttributeError):
            return value
        if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
            return None
    return value


def frame_to_records(frame: Any) -> list[dict[str, Any]]:
    """Normalize a source dataframe, or a per-contract mapping of dataframes, into records."""
    if frame is None:
        return []
    if isinstance(frame, dict):
        parts = [part for part in frame.values() if isinstance(part, pd.DataFrame) and not part.empty]
        if not parts:
            return []
        frame = pd.concat(parts, ignore_index=True)
    if not isinstance(frame, pd.DataFrame):
        raise OpenBBError("AKShare returned a non-tabular response.")
    if frame.empty:
        return []
    return [
        {str(key): _json_safe(value) for key, value in row.items()}
        for row in frame.to_dict(orient="records")
    ]


def clip_dates(
    records: list[dict[str, Any]],
    start: dateType | None,
    end: dateType | None,
) -> list[dict[str, Any]]:
    """Apply a requested inclusive date window to normalized ``date`` records."""
    if start is None and end is None:
        return records
    kept = []
    for row in records:
        raw = row.get("date")
        if raw is None:
            continue
        day = dateType.fromisoformat(str(raw)[:10])
        if start is not None and day < start:
            continue
        if end is not None and day > end:
            continue
        kept.append(row)
    return kept


def _call(method: str, params: dict[str, Any]) -> list[dict[str, Any]]:
    """Run one blocking AKShare call and normalize its response."""
    function: Callable[..., Any] = resolve_method(method)
    arguments = {key: value for key, value in params.items() if value is not None}
    try:
        frame = function(**arguments)
    except TypeError as exc:
        raise OpenBBError(f"AKShare {method} rejected the query parameters: {exc}") from exc
    except Exception as exc:  # Network, parsing, and source-side failures.
        raise OpenBBError(f"AKShare {method} failed: {exc}") from exc
    return frame_to_records(frame)


async def query_frame(method: str, params: dict[str, Any]) -> list[dict[str, Any]]:
    """Run a blocking AKShare call off the event loop."""
    return await asyncio.to_thread(_call, method, params)

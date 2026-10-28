"""Business judgement for CFFEX: downstream of the raw capture, never inside it.

Every function here reads already-captured rows and decides. Nothing in this
module may call a provider, open a journal, or invent a field the source did not
send.

**Where the trading date comes from.** Two payload kinds reach this module:

* a *literal source body* (``raw-bytes``), parsed by :mod:`.parse`. The exchange
  layout has no date column, so the only date available is the requested one. The
  caller must declare ``date_source="request_parameter"`` and the committed row
  records exactly that; nothing here pretends the row carried it. A row that does
  carry its own date (a layout that adds a date column, or a tampered capture) must
  still agree with the request, which is what keeps ``row_date_mismatch``
  reachable rather than dead;
* a *provider-transformed* payload whose rows carry ``date`` themselves
  (``date_source="row"``). The pre-existing suite uses this contract.
"""

from __future__ import annotations

import dataclasses
import re
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from .config import CALENDAR_SOURCE, UNIT_FIELDS, TradingCalendar, iso_date

CONTRACT = re.compile(r"(IF|IH|IC|IM|TS|TF|T|TL)(\d{2})(0[1-9]|1[0-2])")
MISSING = (None, "", "-", "--")
UNIT_KEYS = UNIT_FIELDS
UNIT_AUTHORITIES = ("source", "configuration", "absent")
DATE_SOURCES = ("row", "request_parameter")


class DataError(ValueError):
    """Stable diagnostic code safe for journals and CLI summaries."""


def source_date(value: object) -> date:
    try:
        if isinstance(value, str) and re.fullmatch(r"\d{8}", value):
            value = f"{value[:4]}-{value[4:6]}-{value[6:]}"
        return iso_date(value)
    except (ValueError, TypeError) as exc:
        raise DataError("invalid_row_date") from exc


def field(row: dict[str, Any], *names: str) -> Any:
    present = [row[name] for name in names if name in row]
    if len(present) > 1 and any(value != present[0] for value in present[1:]):
        raise DataError("conflicting_field_aliases")
    return present[0] if present else None


def row_date(row: dict[str, Any]) -> date:
    """The date the *source* states for this row; never a requested target."""
    if not isinstance(row, dict):
        raise DataError("invalid_row")
    return source_date(field(row, "date", "trading_date"))


@dataclass(frozen=True)
class UnitAuthority:
    """The units a payload's values are in, and who said so.

    ``source``          every row must state its own unit field;
    ``configuration``   a row field wins, an explicitly declared configuration
                        value is used where the row states none;
    ``absent``          nobody stated a unit -- the payload is refused with
                        ``units_undetermined`` instead of being labelled
                        ``provider_defined``.

    Nothing in this module ever substitutes a guessed unit.
    """

    values: dict[str, str] = dataclasses.field(default_factory=dict)
    authority: str = "absent"

    def __post_init__(self) -> None:
        if self.authority not in UNIT_AUTHORITIES:
            raise ValueError("invalid_unit_authority")
        if type(self.values) is not dict or set(self.values) - set(UNIT_KEYS):
            raise ValueError("invalid_unit_authority")
        if self.authority == "source" and self.values:
            raise ValueError("invalid_unit_authority")
        if self.authority == "configuration" and set(self.values) != set(UNIT_KEYS):
            raise ValueError("invalid_unit_authority")
        if any(type(value) is not str or not value for value in self.values.values()):
            raise ValueError("invalid_unit_authority")


def declared_units(values: dict[str, str] | None) -> UnitAuthority:
    """The unit authority a configuration declares; empty means ``absent``."""
    return UnitAuthority(dict(values or {}), "configuration" if values else "absent")


def _observed_date(row: Any, requested: date, date_source: str) -> date:
    """The row's own date when it states one, else the request it came from."""
    if not isinstance(row, dict):
        raise DataError("invalid_row")
    stated = field(row, "date", "trading_date")
    if stated is None or stated in MISSING:
        if date_source != "request_parameter":
            raise DataError("invalid_row_date")
        return requested
    observed = source_date(stated)
    if observed != requested:
        # Reachable for both payload kinds: a row that names a date must agree
        # with the date that was requested, however the date got there.
        raise DataError("row_date_mismatch")
    return observed


def _source_units(row: dict[str, Any], units: UnitAuthority | None) -> tuple[dict[str, str], str]:
    """Verbatim units plus their provenance; never a guess, never a default."""
    if units is None:
        values = {key: row.get(key, "provider_defined") for key in UNIT_KEYS}
        if any(type(value) is not str or not value for value in values.values()):
            raise DataError("invalid_source_units")
        return values, "legacy_provider_defined"
    if units.authority == "absent":
        raise DataError("units_undetermined")
    values: dict[str, str] = {}
    from_source = True
    for key in UNIT_KEYS:
        value = row.get(key)
        if value is None or value in MISSING:
            value = units.values.get(key)
            from_source = False
        if type(value) is not str or not value:
            raise DataError("units_undetermined")
        values[key] = value
    return values, "source" if from_source else units.authority


def source_dates(rows: Any) -> list[str | None]:
    """Every date the payload itself declares, in order, `None` when absent.

    Reporting only: the verdict is enforced by :func:`normalize`. This exists so
    an attempt detail can show requested-vs-actual without rewriting the payload.
    """
    if not isinstance(rows, list):
        return []
    result: list[str | None] = []
    for row in rows:
        if not isinstance(row, dict):
            result.append(None)
            continue
        try:
            result.append(row_date(row).isoformat())
        except DataError:
            result.append(None)
    return result


def number(value: Any, name: str, *, integer: bool = False, positive: bool = False) -> Any:
    if value in MISSING:
        return None
    if isinstance(value, bool) or not isinstance(value, (str, int, float, Decimal)):
        raise DataError(f"invalid_{name}")
    try:
        parsed = Decimal(str(value))
    except InvalidOperation as exc:
        raise DataError(f"invalid_{name}") from exc
    if not parsed.is_finite() or parsed < 0 or (positive and parsed <= 0):
        raise DataError(f"invalid_{name}")
    if integer:
        if parsed != parsed.to_integral_value() or parsed > 9223372036854775807:
            raise DataError(f"invalid_{name}")
        return int(parsed)
    result = float(parsed)
    # Decimal can be finite while its conversion to SQLite REAL overflows.
    if not (result < float("inf")) or (result == 0 and parsed != 0):
        raise DataError(f"invalid_{name}")
    return result


def normalize(
    rows: list[dict[str, Any]],
    requested: date,
    *,
    date_source: str = "row",
    units: UnitAuthority | None = None,
) -> list[dict[str, Any]]:
    """Accept one requested day's payload, or reject the whole day.

    ``requested`` is the date the collector asked for. ``date_source`` says where
    the payload's date came from: ``row`` (the row states it) or
    ``request_parameter`` (the layout states none, so the request is the only
    date). Every emitted ``trading_date`` is recorded with that provenance.

    ``units`` is the unit authority for the payload. ``None`` is the pre-existing
    direct-call contract (row fields, ``provider_defined`` when absent) and is
    never used by the committed path; the committed path always supplies an
    authority, and ``absent`` is refused as ``units_undetermined``.
    """
    if date_source not in DATE_SOURCES:
        raise ValueError("invalid_date_source")
    if units is not None and not isinstance(units, UnitAuthority):
        raise ValueError("invalid_unit_authority")
    if not isinstance(rows, list) or not rows:
        raise DataError("empty_trading_day")
    result = []
    seen = set()
    for row in rows:
        if not isinstance(row, dict):
            raise DataError("invalid_row")
        observed = _observed_date(row, requested, date_source)
        symbol = field(row, "symbol", "contract_symbol")
        match = CONTRACT.fullmatch(symbol) if isinstance(symbol, str) else None
        if match is None:
            raise DataError("invalid_futures_contract")
        if symbol in seen:
            raise DataError("duplicate_contract")
        seen.add(symbol)
        product = match.group(1)
        variety = row.get("variety")
        if variety is not None and variety != product:
            raise DataError("variety_mismatch")
        values = {
            key: number(field(row, key), key)
            for key in ("open", "high", "low", "close", "turnover")
        }
        for key in ("settle", "pre_settle"):
            values[key] = number(field(row, key), key, positive=True)
        for key in ("volume", "open_interest"):
            values[key] = number(field(row, key), key, integer=True)
        if values["volume"] is None:
            raise DataError("missing_volume")
        flags = []
        ohlc = [values[key] for key in ("open", "high", "low", "close")]
        missing = any(value is None for value in ohlc)
        sentinel = any(value == 0 for value in ohlc)
        if missing or sentinel:
            if values["volume"] != 0:
                raise DataError("missing_or_zero_ohlc_with_volume")
            if missing:
                flags.append("missing_ohlc_zero_volume")
            if sentinel:
                flags.append("zero_ohlc_sentinel_zero_volume")
        else:
            if not (
                values["low"]
                <= min(values["open"], values["close"])
                <= max(values["open"], values["close"])
                <= values["high"]
            ):
                raise DataError("ohlc_order")
        for key in ("settle", "pre_settle", "open_interest", "turnover"):
            if values[key] is None:
                flags.append(f"missing_{key}")
        if variety is None:
            flags.append("missing_source_variety")
        # Units are recorded verbatim; no factor is applied. A payload whose
        # units nobody stated is refused, not labelled and not guessed.
        source_units, unit_authority = _source_units(row, units)
        result.append(
            {
                "trading_date": observed.isoformat(),
                "date_source": date_source,
                "contract_symbol": symbol,
                "product": product,
                "source_variety": variety,
                **values,
                "quality_flags": sorted(flags),
                "source_units": source_units,
                "unit_authority": unit_authority,
            }
        )
    return sorted(result, key=lambda row: row["contract_symbol"])


def contract_set_shrink(previous: set[str], rows: list[dict[str, Any]]) -> None:
    """Same-date contract listings may grow, never silently disappear.

    Cross-date comparisons are invalid (listings and expiries move), so the
    caller passes the symbols already stored for this exact trading date.
    """
    observed = {row["contract_symbol"] for row in rows}
    if not previous.issubset(observed):
        raise DataError("contract_set_shrink")


def accept_calendar(
    rows: Any,
    previous: TradingCalendar | None,
    max_rows: int,
    *,
    captured_at: datetime | None = None,
) -> TradingCalendar:
    """Validate a captured reference calendar before it may replace the file.

    A truncated response must not revoke coverage that was already asserted, so
    coverage/session regressions are rejections, not silent rewrites.
    """
    if not isinstance(rows, list) or not rows:
        raise DataError("empty_calendar")
    if len(rows) > max_rows:
        raise DataError("calendar_row_limit")
    dates = []
    for row in rows:
        if not isinstance(row, dict):
            raise DataError("invalid_calendar_dates")
        try:
            dates.append(iso_date(row.get("trade_date")))
        except (ValueError, TypeError) as exc:
            raise DataError("invalid_calendar_dates") from exc
    if len(set(dates)) != len(dates) or any(day.weekday() >= 5 for day in dates):
        raise DataError("invalid_calendar_dates")
    dates.sort()
    calendar = TradingCalendar(
        timezone="Asia/Shanghai",
        coverage_start=dates[0],
        coverage_end=dates[-1],
        trading_dates=dates,
        source=CALENDAR_SOURCE,
        captured_at=captured_at or datetime.now(UTC),
    )
    if previous is not None:
        if (
            calendar.coverage_start > previous.coverage_start
            or calendar.coverage_end < previous.coverage_end
        ):
            raise DataError("calendar_coverage_regression")
        overlap_start = max(calendar.coverage_start, previous.coverage_start)
        overlap_end = min(calendar.coverage_end, previous.coverage_end)
        new_sessions = set(dates)
        if any(
            overlap_start <= day <= overlap_end and day not in new_sessions
            for day in previous.trading_dates
        ):
            raise DataError("calendar_sessions_removed")
    return calendar

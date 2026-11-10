"""Identity/freshness gates; diagnostic quotes are not necessarily pricing eligible.

Raw values remain in immutable acquisition envelopes. Normalization never supplies
missing prices, assumes an adjusted strike, or guesses an option multiplier.
Post-close acquisition retains same-day source-displayed quotes, not necessarily
synchronized closing quotes. Early/unknown event times are diagnostic only.

Everything below the ``DataError`` boundary is a **business** verdict.  Since the
raw-only split moved every verdict out of acquisition, this module also owns the
predicates that used to be inlined in ``runtime``/``storage``/``delivery``:
calendar coverage acceptance, snapshot-shrink protection, unit authority and the
instrument identity construction.  They are the same rules, in one place, with
the same codes -- the pre-existing tests still call ``contracts``/``quotes``
directly and are unchanged.
"""

from __future__ import annotations

import math
import re
from datetime import date, datetime, time
from typing import Any

from .config import SHANGHAI, iso_date


class DataError(ValueError):
    pass


def number(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise DataError("invalid_numeric")
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise DataError("invalid_numeric") from exc
    if not math.isfinite(result):
        raise DataError("nonfinite_numeric")
    return result


def code(value: Any, length: int) -> str:
    if not isinstance(value, str) or not re.fullmatch(rf"[0-9]{{{length}}}", value):
        raise DataError("invalid_identity")
    return value


def contracts(
    rows: list[dict],
    exchange: str,
    source: str,
    day: date,
    underlyings: list[str] | None = None,
) -> list[dict]:
    if not rows:
        raise DataError("empty_universe")
    result, seen = [], set()
    for row in rows:
        symbol = code(row.get("symbol"), 8)
        if symbol in seen:
            raise DataError("duplicate_contract")
        seen.add(symbol)
        underlying = code(row.get("underlying"), 6)
        if row.get("exchange") != exchange:
            raise DataError("exchange_mismatch")
        if underlyings is not None and underlying not in underlyings:
            raise DataError("underlying_scope_mismatch")
        if row.get("metadata_source") != source:
            raise DataError("metadata_source_mismatch")
        if row.get("option_type") not in {"call", "put"}:
            raise DataError("invalid_option_type")
        strike = number(row.get("strike"))
        if strike is None or strike <= 0:
            raise DataError("invalid_strike")
        try:
            expiration = iso_date(row.get("expiration"))
            source_date = (
                iso_date(row["source_date"]) if row.get("source_date") else None
            )
        except ValueError as exc:
            raise DataError("invalid_contract_date") from exc
        if expiration < day:
            raise DataError("expired_contract")
        if source_date is not None and source_date != day:
            raise DataError("metadata_date_mismatch")
        if source == "official" and source_date is None:
            raise DataError("missing_metadata_date")
        unit = number(row.get("contract_unit"))
        if unit is not None and unit <= 0:
            raise DataError("invalid_contract_unit")
        flags = []
        if unit is None:
            flags.append("unknown_contract_unit")
        if source_date is None:
            # Sina current-universe discovery has no source metadata date. This is
            # explicit weaker provenance, NOT an invented date. Quote dates gate it.
            flags.append("metadata_source_date_unavailable")
        result.append(
            {
                **row,
                "strike": strike,
                "contract_unit": unit,
                "expiration": expiration.isoformat(),
                "quality_flags": flags,
            }
        )
    if underlyings and {row["underlying"] for row in result} != set(underlyings):
        raise DataError("missing_requested_underlying")
    return sorted(result, key=lambda row: row["symbol"])


def source_time(value: Any) -> datetime:
    if not isinstance(value, str) or not re.match(
        r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}", value
    ):
        raise DataError("missing_or_invalid_quote_time")
    try:
        stamp = datetime.fromisoformat(value)
    except ValueError as exc:
        raise DataError("missing_or_invalid_quote_time") from exc
    return (
        stamp.replace(tzinfo=SHANGHAI) if stamp.tzinfo is None else stamp
    ).astimezone(SHANGHAI)


def quotes(  # pylint: disable=too-many-positional-arguments
    rows: list[dict],
    metadata: list[dict],
    day: date,
    captured: datetime,
    now: datetime,
    threshold: time = time(14, 57),
) -> list[dict]:
    expected = {row["symbol"]: row for row in metadata}
    if not expected:
        raise DataError("empty_universe")
    symbols = [row.get("symbol") for row in rows]
    if len(symbols) != len(set(symbols)) or set(symbols) != set(expected):
        raise DataError("quote_universe_mismatch")
    if captured.tzinfo is None or now.tzinfo is None:
        raise DataError("invalid_capture_clock")
    result = []
    numeric_fields = [
        "bid",
        "ask",
        "bid_size",
        "ask_size",
        "last",
        "volume",
        "turnover",
        "open_interest",
    ] + [
        f"{key}_{level}"
        for level in range(1, 6)
        for key in ("bid", "ask", "bid_size", "ask_size")
    ]
    for row in rows:
        meta = expected[row["symbol"]]
        if row.get("quote_error"):
            raise DataError("quote_error")
        stamp = source_time(row.get("source_time"))
        if stamp.date() != day:
            raise DataError("quote_date_mismatch")
        if stamp > min(captured, now):
            raise DataError("quote_future_time")
        if row.get("underlying") != meta["underlying"]:
            raise DataError("underlying_mismatch")
        if row.get("expiration") is not None:
            try:
                expiry = iso_date(row["expiration"]).isoformat()
            except ValueError as exc:
                raise DataError("expiration_mismatch") from exc
            if expiry != meta["expiration"]:
                raise DataError("expiration_mismatch")
        if (
            row.get("option_type") is not None
            and row["option_type"] != meta["option_type"]
        ):
            raise DataError("option_type_mismatch")
        strike = number(row.get("strike"))
        if strike is None or not math.isclose(
            strike, meta["strike"], rel_tol=1e-10, abs_tol=1e-10
        ):
            raise DataError("strike_mismatch")
        normalized = {
            key: number(row.get(key))
            for key in numeric_fields
            if key in row or key in numeric_fields[:8]
        }
        flags = list(meta["quality_flags"])
        # Midnight is a source sentinel for unknown event time, not a fabricated
        # closing timestamp. Other early prints remain part of the daily chain.
        known_quote_time = stamp.time() != time(0)
        fresh_quote = known_quote_time and stamp.time() >= threshold
        if not known_quote_time:
            flags.append("unknown_quote_time")
        elif not fresh_quote:
            flags.extend(["preclose_quote", "stale_quote"])
        for key in ("bid", "ask", "last", "bid_size", "ask_size"):
            value = normalized[key]
            if value is None:
                flags.append(f"missing_{key}")
            elif value <= 0:
                flags.append(f"{'zero' if value == 0 else 'negative'}_{key}")
        for key in ("volume", "turnover", "open_interest"):
            value = normalized[key]
            if value is not None and value < 0:
                raise DataError("negative_activity")
        bid, ask = normalized["bid"], normalized["ask"]
        bid_size, ask_size = normalized["bid_size"], normalized["ask_size"]
        last = normalized["last"]
        if bid is not None and ask is not None and bid > ask:
            flags.append("crossed_market")
        # Bid/ask pricing requires a fresh, positive two-sided quote with sizes
        # and an observed contract unit. Missing/zero last can mean no trade and
        # does not invalidate that market; a negative last remains diagnostic.
        eligible = (
            fresh_quote
            and bid is not None
            and ask is not None
            and 0 < bid <= ask
            and bid_size is not None
            and bid_size > 0
            and ask_size is not None
            and ask_size > 0
            and meta["contract_unit"] is not None
            and (last is None or last >= 0)
        )
        result.append(
            {
                **row,
                **normalized,
                "source_time": stamp.isoformat(),
                "quality_flags": flags,
                "pricing_eligible": eligible,
            }
        )
    return sorted(result, key=lambda row: row["symbol"])


# ---------------------------------------------------------------------------
# Moved predicates: judgement that used to sit inside acquisition
# ---------------------------------------------------------------------------

#: The unit labels this lane may commit with.  ``provider_defined`` is never one
#: of them: it is the label CFFEX's first round invented, and it is refused here.
UNIT_AUTHORITIES = ("source", "configuration", "absent")


def unit_authority(source_declares: bool, declared: dict[str, str] | None) -> str:
    """Which authority the committed numbers would carry.

    ``source`` wins when the payload itself states the units.  Otherwise the
    deployment's declaration is used.  Otherwise the authority is ``absent`` and
    :func:`require_units` refuses -- there is no default and no invented label.
    """
    if source_declares:
        return "source"
    if declared:
        return "configuration"
    return "absent"


def require_units(authority: str) -> None:
    if authority not in UNIT_AUTHORITIES or authority == "absent":
        raise DataError("units_undetermined")


def unit_labels(
    authority: str, declared: dict[str, str] | None
) -> dict[str, str | None]:
    """The labels a record carries, or ``None`` when nobody stated them."""
    if authority == "configuration" and declared:
        return {
            key: declared.get(key)
            for key in ("price_unit", "size_unit", "volume_unit", "turnover_unit")
        }
    return {
        "price_unit": None,
        "size_unit": None,
        "volume_unit": None,
        "turnover_unit": None,
    }


def occ_identity(symbol: Any, exchange: Any) -> dict[str, str]:
    """Instrument identity from an eight-digit contract code, nothing more.

    This used to be built inline by the delivery sealer.  It is a business fact
    (which instrument a row is about), so it lives here; the exchange prefix is
    the only composed part, and no strike/expiry/side is encoded into it.
    """
    if exchange not in {"SSE", "SZSE"}:
        raise DataError("invalid_exchange")
    contract = code(symbol, 8)
    return {
        "symbol": contract,
        "contract_symbol": contract,
        "instrument_id": f"{exchange}:{contract}",
    }


def snapshot_shrink(
    previous: set[str], current: set[str], approved: bool = False
) -> None:
    """A same-day snapshot may not silently lose a contract it previously had.

    Moved from ``storage.Journal.validate_universe``; the code is unchanged, and
    ``approved`` is the audited manual override the CLI already exposes.
    """
    if not approved and not previous.issubset(current):
        raise DataError("universe_shrink_requires_manual_approval")


def coverage(rows: Any) -> list[date]:
    """Calendar acceptance: sorted, unique, weekday trading days, non-empty."""

    if not isinstance(rows, list) or not rows:
        raise DataError("empty_calendar")
    try:
        days = sorted(iso_date(row.get("trade_date")) for row in rows)
    except (ValueError, TypeError, AttributeError) as exc:
        raise DataError("invalid_calendar") from exc
    if any(day.weekday() >= 5 for day in days) or len(set(days)) != len(days):
        raise DataError("invalid_calendar")
    return days


def coverage_accepts(previous: Any, start: date, end: date, days: list[date]) -> None:
    """Refuse a calendar that regresses or drops a session it previously had."""
    if previous is None:
        return
    if start > previous.coverage_start or end < previous.coverage_end:
        raise DataError("calendar_coverage_regression")
    if not set(previous.trading_dates).issubset(set(days)):
        raise DataError("calendar_sessions_removed")


def expiry_coverage(rows: list[dict], sessions: set[str] | None) -> None:
    """Every accepted contract's expiry must be inside the calendar's coverage.

    ``None`` means the deployment did not supply a session set; the predicate is
    then a no-op rather than an invented acceptance.
    """
    if sessions is None:
        return
    for row in rows:
        if row["expiration"] not in sessions:
            raise DataError("expiry_not_a_session")

"""Normalized candidate history and explicit reconciliation; no canonical promotion."""

from __future__ import annotations

import hashlib
import json
import math
from datetime import UTC, date, datetime, time
from typing import Any
from zoneinfo import ZoneInfo

from .config import Collection, Instrument, Source
from .planner import Chunk, expected_times


def json_text(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def digest(value: Any) -> str:
    return hashlib.sha256(json_text(value).encode()).hexdigest()


def event_time(value: Any, instrument: Instrument, source: Source, frequency: str) -> str:
    if value is None:
        raise ValueError("missing_timestamp")
    text = str(value)
    if len(text) == 8 and text.isdigit():
        text = datetime.strptime(text, "%Y%m%d").date().isoformat()
    if frequency == "1d":
        if len(text) == 10:
            return date.fromisoformat(text).isoformat()
        stamp = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if stamp.tzinfo:
            stamp = stamp.astimezone(ZoneInfo(instrument.timezone))
        if stamp.time() != time():
            raise ValueError("daily_timestamp_not_session_date")
        return stamp.date().isoformat()
    stamp = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if stamp.tzinfo is None:
        tz = ZoneInfo(source.timestamp_timezone or instrument.timezone)
        stamp = stamp.replace(tzinfo=tz)
        back = stamp.astimezone(UTC).astimezone(tz)
        if back.replace(tzinfo=None) != stamp.replace(tzinfo=None):
            raise ValueError("nonexistent_timestamp")
        if stamp.replace(fold=0).utcoffset() != stamp.replace(fold=1).utcoffset():
            raise ValueError("ambiguous_timestamp")
    return stamp.astimezone(UTC).isoformat()


def normalize(
    rows: list[dict[str, Any]],
    source_id: str,
    source: Source,
    instrument: Instrument,
    collection: Collection,
    chunk: Chunk,
    observed_at: str,
    raw_hash: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], int, dict[str, Any]]:
    expected = expected_times(collection, instrument, chunk)
    accepted, rejected = [], []
    outside = 0
    seen: dict[str, str] = {}
    symbol = instrument.source_symbols.get(source_id, instrument.symbol)
    adjustment = source.adjustment
    for row_index, row in enumerate(rows):
        try:
            if row.get("symbol") and str(row["symbol"]) != symbol:
                raise ValueError("symbol_mismatch")
            if row.get("asset_type") and row["asset_type"] != instrument.asset_type:
                raise ValueError("contract_identity_mismatch")
            if row.get("currency") and row["currency"] != instrument.currency:
                raise ValueError("currency_mismatch")
            if instrument.con_id and row.get("con_id") not in (None, instrument.con_id):
                raise ValueError("contract_identity_mismatch")
            stamp = event_time(
                row.get(source.timestamp_field), instrument, source, collection.frequency
            )
            day = (
                date.fromisoformat(stamp)
                if collection.frequency == "1d"
                else (
                    datetime.fromisoformat(stamp).astimezone(ZoneInfo(instrument.timezone)).date()
                )
            )
            if (
                (stamp not in expected)
                if expected is not None
                else not (chunk.start <= day < chunk.end)
            ):
                outside += 1
                continue
            values: dict[str, float | None] = {}
            for name in collection.fields:
                value = row.get(source.field_map.get(name, name))
                if value is None:
                    if name in collection.required_fields:
                        raise ValueError("missing_required_field")
                    values[name] = None
                    continue
                if isinstance(value, bool):
                    raise ValueError("invalid_number")
                try:
                    number = float(value)
                except (TypeError, ValueError) as exc:
                    raise ValueError("invalid_number") from exc
                if not math.isfinite(number):
                    raise ValueError("nonfinite_number")
                if name in {"volume", "bar_count", "open_interest"} and number < 0:
                    if name not in collection.required_fields:
                        values[name] = None
                        continue
                    raise ValueError("negative_size")
                if (
                    name in {"open", "high", "low", "close", "wap", "vwap"}
                    and number < 0
                    and instrument.asset_type not in {"future", "future_option"}
                ):
                    raise ValueError("negative_price")
                values[name] = number
            low, high = values.get("low"), values.get("high")
            if low is not None and high is not None and low > high:
                raise ValueError("invalid_ohlc")
            for price in (values.get("open"), values.get("close")):
                if price is not None and (
                    (low is not None and price < low) or (high is not None and price > high)
                ):
                    raise ValueError("invalid_ohlc")
            prices = [values.get(k) for k in ("open", "high", "low", "close")]
            if all(v is not None for v in prices):
                opening, high, low, close = prices
                if not low <= min(opening, close) <= max(opening, close) <= high:
                    raise ValueError("invalid_ohlc")
            value_hash = digest(values)
            if stamp in seen and seen[stamp] != value_hash:
                raise ValueError("conflicting_response_duplicate")
            seen[stamp] = value_hash
            contract = {
                "instrument_id": chunk.instrument_id,
                "instrument": instrument.model_dump(mode="json"),
                "source": source_id,
                "provider": source.provider,
                "model": source.model,
                "parameters": source.parameters,
                "parameter_map": source.parameter_map,
                "source_symbol": symbol,
                "frequency": collection.frequency,
                "adjustment": adjustment,
                "volume_unit": source.volume_unit,
                "what_to_show": collection.what_to_show,
                "use_rth": collection.use_rth,
                "field_map": source.field_map,
                "fields": collection.fields,
                "timestamp_field": source.timestamp_field,
                "timestamp_timezone": source.timestamp_timezone,
            }
            record = {
                "observation_key": digest({"contract": contract, "event_time": stamp}),
                "values_hash": value_hash,
                "event_time": stamp,
                "observed_at": observed_at,
                "available_at": observed_at,
                "availability_basis": "collector_first_observed",
                "historical_published_at": None,
                "instrument_id": chunk.instrument_id,
                "symbol": instrument.symbol,
                "asset_type": instrument.asset_type,
                "currency": instrument.currency,
                "timezone": instrument.timezone,
                "source": source_id,
                "provider": source.provider,
                "source_symbol": symbol,
                "con_id": instrument.con_id,
                "frequency": collection.frequency,
                "adjustment": adjustment,
                "volume_unit": source.volume_unit,
                "what_to_show": collection.what_to_show,
                "use_rth": collection.use_rth,
                "values": values,
                "entitlement_reference": source.entitlement_reference,
                "raw_hash": raw_hash,
                "raw_row": row_index,
                "normalization_version": 1,
            }
            accepted.append(record)
        except (ValueError, TypeError, OverflowError) as exc:
            safe = (
                str(exc)
                if str(exc)
                in {
                    "missing_timestamp",
                    "daily_timestamp_not_session_date",
                    "nonexistent_timestamp",
                    "ambiguous_timestamp",
                    "symbol_mismatch",
                    "currency_mismatch",
                    "contract_identity_mismatch",
                    "missing_required_field",
                    "invalid_number",
                    "nonfinite_number",
                    "negative_size",
                    "negative_price",
                    "invalid_ohlc",
                    "conflicting_response_duplicate",
                }
                else "invalid_row"
            )
            rejected.append({"raw_row": row_index, "reason": safe, "raw_hash": raw_hash})
    actual = {record["event_time"] for record in accepted}
    missing = sorted(expected - actual) if expected is not None else []
    coverage = {
        "status": "unknown" if expected is None else ("gaps" if missing else "complete"),
        "expected": len(expected) if expected is not None else None,
        "missing": len(missing),
        "missing_sample": missing[:20],
    }
    return accepted, rejected, outside, coverage

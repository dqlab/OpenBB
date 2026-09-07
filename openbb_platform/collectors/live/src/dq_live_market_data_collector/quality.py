"""Snapshot contract v1, source identity checks, and explicit timestamp semantics."""

from __future__ import annotations

import hashlib
import json
import math
from datetime import UTC, datetime
from typing import Any
from zoneinfo import ZoneInfo

from .config import Collection, Instrument, Source

NUMERIC_FIELDS = {
    "bid",
    "ask",
    "last_price",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "bid_size",
    "ask_size",
    "last_size",
    "open_interest",
}
SIZE_FIELDS = {"volume", "bid_size", "ask_size", "last_size", "open_interest"}


def json_text(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def digest(value: Any) -> str:
    return hashlib.sha256(json_text(value).encode()).hexdigest()


def parse_timestamp(value: Any, source: Source) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        raise ValueError("Boolean timestamp")
    if isinstance(value, (int, float)):
        seconds = value / 1000 if source.timestamp_unit == "milliseconds" else value
        return datetime.fromtimestamp(seconds, tz=UTC)
    timestamp = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if timestamp.tzinfo is None:
        if not source.timestamp_timezone:
            raise ValueError("Naive source time without configured timezone")
        tz = ZoneInfo(source.timestamp_timezone)
        timestamp = timestamp.replace(tzinfo=tz)
        # Ambiguous or nonexistent local source times cannot identify an event.
        if timestamp.replace(fold=0).utcoffset() != timestamp.replace(
            fold=1
        ).utcoffset() or timestamp.astimezone(UTC).astimezone(tz).replace(
            tzinfo=None
        ) != timestamp.replace(tzinfo=None):
            raise ValueError("Ambiguous or nonexistent source time")
    return timestamp.astimezone(UTC)


def normalize(
    row: dict[str, Any],
    *,
    source_id: str,
    source: Source,
    instrument_id: str,
    instrument: Instrument,
    collection_id: str,
    collection: Collection,
    session_id: str,
    poll_id: str,
    received_at: datetime,
    raw_hash: str,
) -> tuple[dict[str, Any] | None, list[str]]:
    reasons: list[str] = []
    symbol = instrument.source_symbols.get(source_id, instrument.symbol)
    if str(row.get("symbol", "")).upper() != symbol.upper():
        reasons.append("symbol_mismatch")
    if row.get("currency") and row["currency"] != instrument.currency:
        reasons.append("currency_mismatch")
    if row.get("asset_type") and row["asset_type"] != instrument.asset_type:
        reasons.append("security_type_mismatch")
    if instrument.con_id is not None:
        con_id = row.get("con_id")
        if isinstance(con_id, bool) or not isinstance(con_id, int) or con_id <= 0:
            reasons.append("unqualified_contract")
        elif con_id != instrument.con_id:
            reasons.append("contract_mismatch")

    fields: dict[str, Any] = {}
    for name in collection.fields:
        raw_name = source.field_map.get(name, name)
        value = row.get(raw_name)
        if value is not None and name in NUMERIC_FIELDS:
            try:
                if isinstance(value, bool):
                    raise ValueError("Boolean market value")
                value = float(value)
                if not math.isfinite(value):
                    raise ValueError("Nonfinite market value")
                if name in SIZE_FIELDS and value < 0:
                    raise ValueError("Negative size")
                if (
                    name not in SIZE_FIELDS
                    and value < 0
                    and instrument.asset_type
                    not in {
                        "future",
                        "future_option",
                        "commodity",
                    }
                ):
                    raise ValueError("Negative price")
            except (TypeError, ValueError, OverflowError):
                reasons.append(f"invalid_{name}")
                value = None
        if name in collection.required_fields and (value is None or value == ""):
            reasons.append(f"missing_{name}")
        fields[name] = value
    bid, ask = fields.get("bid"), fields.get("ask")
    if (
        collection.reject_crossed_quotes
        and isinstance(bid, (int, float))
        and isinstance(ask, (int, float))
        and bid > ask
    ):
        reasons.append("crossed_quote")
    field = source.timestamp_field or "last_timestamp"
    timestamp = None
    try:
        timestamp = parse_timestamp(row.get(field), source)
    except (ValueError, TypeError, OverflowError, OSError):
        reasons.append("invalid_source_timestamp")
    if timestamp:
        age = (received_at - timestamp).total_seconds()
        if age < -collection.max_future_seconds:
            reasons.append("future_timestamp")
        if collection.max_age_seconds is not None and age > collection.max_age_seconds:
            reasons.append("stale_observation")
    elif collection.max_age_seconds is not None:
        reasons.append("freshness_unknown")
    feed = row.get(source.feed_type_field) if source.feed_type_field else None
    if isinstance(feed, (int, str)) and not isinstance(feed, bool):
        feed = source.feed_type_map.get(str(feed), feed)
    if not isinstance(feed, str) or feed not in {"live", "frozen", "delayed", "delayed_frozen"}:
        feed = "unknown"
    if feed not in collection.accepted_feed_types:
        reasons.append("feed_type_not_accepted")
    if row.get("delayed") is True and not (
        {"delayed", "delayed_frozen"} & set(collection.accepted_feed_types)
    ):
        reasons.append("delayed_request_not_accepted")
    if reasons:
        return None, sorted(set(reasons))
    source_time = timestamp.isoformat() if timestamp else None
    # Source timestamps identify observations only when their semantics are declared.
    identity_time = (
        source_time if source.timestamp_semantics != "unknown" and source_time else poll_id
    )
    mapping_hash = digest(
        {
            "instrument": instrument.model_dump(mode="json"),
            "route": source.model,
            "provider": source.provider,
            "model": source.model,
            "parameters": source.parameters,
            "parameter_map": source.parameter_map,
            "timestamp_semantics": source.timestamp_semantics,
            "timestamp_field": source.timestamp_field,
            "timestamp_timezone": source.timestamp_timezone,
            "timestamp_unit": source.timestamp_unit,
            "feed_type_field": source.feed_type_field,
            "feed_type_map": source.feed_type_map,
            "requested_feed_type": source.requested_feed_type,
            "revision_field": source.revision_field,
            "fields": collection.fields,
            "field_map": source.field_map,
            "adjustment": source.adjustment,
            "size_unit": source.size_unit,
        }
    )
    key = digest([mapping_hash, collection_id, instrument_id, source_id, identity_time])
    content_hash = digest(
        {
            "values": fields,
            "actual_feed_type": feed,
            "con_id": row.get("con_id"),
            "currency": instrument.currency,
        }
    )
    revision_value = row.get(source.revision_field) if source.revision_field else None
    revision = str(revision_value) if revision_value is not None else content_hash
    record_id = digest([key, revision, content_hash])
    received = received_at.astimezone(UTC).isoformat()
    event_time = (
        source_time if source.timestamp_semantics in {"provider_event", "last_trade"} else None
    )
    return {
        "contract_version": 1,
        "mapping_hash": mapping_hash,
        "content_hash": content_hash,
        "record_id": record_id,
        "observation_key": key,
        "session_id": session_id,
        "poll_id": poll_id,
        "collection_id": collection_id,
        "instrument_id": instrument_id,
        "symbol": instrument.symbol,
        "source_symbol": symbol,
        "asset_type": instrument.asset_type,
        "currency": instrument.currency,
        "currency_basis": "provider" if row.get("currency") else "configured",
        "exchange": row.get("exchange") or instrument.exchange,
        "con_id": row.get("con_id"),
        "source": source_id,
        "provider": source.provider,
        "route": source.model,
        "source_timestamp": source_time,
        "timestamp_semantics": source.timestamp_semantics,
        "event_time": event_time,
        "received_at": received,
        "available_at": received,
        "availability_basis": "collector_receive",
        "source_available_at": None,
        "source_revision": revision,
        "revision_basis": "provider" if revision_value is not None else "content_hash",
        "adjustment": source.adjustment,
        "size_unit": source.size_unit,
        "entitlement_reference": source.entitlement_reference,
        "actual_feed_type": feed,
        "requested_feed_type": source.requested_feed_type,
        "raw_hash": raw_hash,
        "values": fields,
    }, []

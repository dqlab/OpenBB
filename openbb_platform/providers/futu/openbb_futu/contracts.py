"""Lossless business-frame capture and conservative subscription planning.

These functions do not connect to a service or assert account entitlements.
"""

from __future__ import annotations

import json
import re
import time
from datetime import datetime, timezone
from typing import Any

KINDS = {
    "QUOTE": "quote_event",
    "ORDER_BOOK": "book_event",
    "TICKER": "trade_tick",
    "KLINE": "native_bar_1m",
}
ASSETS = ("equity", "etf", "option", "future")
CHANNELS = ("quote", "order_book", "ticker", "kline")


def business_frame(
    raw: str,
    *,
    run_id: str,
    connection_id: str,
    local_seq: int,
    plan_id: str | None = None,
    capability_id: str | None = None,
    active_secrets: tuple[str, ...] = (),
) -> dict[str, Any]:
    """Wrap an application message verbatim; reject control/auth messages.

    Typed business-shaped messages are preserved; ambiguous or sensitive frames fail closed.
    """
    if not isinstance(raw, str) or local_seq < 1:
        raise ValueError("expected text and positive local sequence")

    def unique_pairs(pairs):
        """Handle unique pairs."""
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate JSON key")
            result[key] = value
        return result

    try:
        obj = json.loads(raw, object_pairs_hook=unique_pairs)
    except (ValueError, TypeError) as exc:
        raise ValueError("unclassifiable frame: cannot safely archive") from exc
    if (
        not isinstance(obj, dict)
        or not isinstance(obj.get("type"), str)
        or not obj.get("type")
        or any(key in obj for key in ("id", "action", "session_id", "code"))
        or not isinstance(obj.get("data"), dict)
    ):
        raise ValueError("control or unclassifiable frame is not market data")

    # Never archive credentials even if they arrive in an unexpected typed frame.
    def sensitive(value: Any) -> bool:
        """Handle sensitive."""
        if isinstance(value, dict):
            return any(
                k.lower()
                in {
                    "authorization",
                    "access_token",
                    "refresh_token",
                    "credential_id",
                    "signature",
                    "private_key",
                }
                or sensitive(v)
                for k, v in value.items()
            )
        return isinstance(value, list) and any(sensitive(v) for v in value)

    def secret_value(value: Any) -> bool:
        if isinstance(value, dict):
            return any(secret_value(k) or secret_value(v) for k, v in value.items())
        if isinstance(value, list):
            return any(secret_value(v) for v in value)
        return isinstance(value, str) and (
            bool(re.search(r"(?i)\bbearer\s+\S+", value))
            or any(secret and secret in value for secret in active_secrets)
        )

    if sensitive(obj) or secret_value(obj):
        raise ValueError("frame contains credential material")
    typ = obj["type"]
    kind = KINDS.get(typ)
    if typ == "KLINE":
        data = obj.get("data")
        if (
            not isinstance(data, dict)
            or data.get("period") != "1m"
            or data.get("adjust") != "none"
        ):
            kind = None
    return {
        "schema_version": "futu_raw_v1",
        "provider": "futu",
        "api_family": "futunn_http_ws",
        "transport": "websocket",
        "collector_run_id": run_id,
        "connection_id": connection_id,
        "local_seq": local_seq,
        "received_at_utc": datetime.now(timezone.utc).isoformat(),
        "received_monotonic_ns": time.monotonic_ns(),
        "message_type": typ,
        "data_kind": kind,
        "provider_symbol": obj.get("symbol"),
        "subscription_plan_id": plan_id,
        "capability_snapshot_id": capability_id,
        "raw_payload": raw,
        "payload_encoding": "utf-8 text",
        "parse_status": "recognized" if kind else "unknown_type_or_variant",
    }


def plan(
    entries: list[dict[str, Any]],
    *,
    budget: int | None,
    pilot_limit: int = 0,
    discovery_complete: bool = False,
) -> dict[str, Any]:
    """Plan independent symbol/channel dimensions; unknown capacity uses a bounded pilot.

    Entries contain asset, symbol, channel, priority and capability. Only explicitly
    supported or untested dimensions are candidates; unknown is never entitlement.
    """
    if budget is not None and (isinstance(budget, bool) or budget < 0):
        raise ValueError("budget must be nonnegative or unknown")
    if pilot_limit < 0:
        raise ValueError("pilot_limit must be nonnegative")
    seen = set()
    for e in entries:
        if (
            e.get("asset") not in ASSETS
            or e.get("channel") not in CHANNELS
            or not isinstance(e.get("symbol"), str)
            or not e["symbol"]
        ):
            raise ValueError("invalid asset, channel, or symbol")
        key = (e["symbol"], e["channel"])
        if key in seen:
            raise ValueError("duplicate symbol/channel")
        seen.add(key)
    ordered = sorted(
        entries,
        key=lambda e: (e.get("priority", 100), e["asset"], e["symbol"], e["channel"]),
    )
    limit = budget if budget is not None else pilot_limit
    selected: list = []
    omitted: list = []
    for e in ordered:
        status = e.get("capability", "not_tested")
        if status not in {
            "supported",
            "not_tested",
            "not_entitled",
            "unsupported",
            "quota_exceeded",
            "no_data_observed",
            "docs_conflict",
        }:
            raise ValueError("invalid capability")
        if status not in {"supported", "not_tested"}:
            omitted.append({**e, "not_selected_reason": status})
        elif len(selected) >= limit:
            omitted.append(
                {
                    **e,
                    "not_selected_reason": (
                        "budget_shortfall"
                        if budget is not None
                        else "unknown_quota_pilot_limit"
                    ),
                }
            )
        else:
            selected.append(e)
    counts = {
        asset: {
            "discovered_dimensions": sum(e["asset"] == asset for e in entries),
            "selected_dimensions": sum(e["asset"] == asset for e in selected),
            "subscribed_dimensions": 0,
            "persisted_symbols": 0,
        }
        for asset in ASSETS
    }
    matrix = {
        asset: {
            channel: {
                "target_dimensions": None,
                "target_status": "unknown",
                "discovered_dimensions": sum(
                    e["asset"] == asset and e["channel"] == channel for e in entries
                ),
                "selected_dimensions": sum(
                    e["asset"] == asset and e["channel"] == channel for e in selected
                ),
                "subscription_status": "not_observed",
                "persistence_status": "not_observed",
            }
            for channel in CHANNELS
        }
        for asset in ASSETS
    }
    return {
        "discovery_complete": bool(discovery_complete),
        "budget_available": budget,
        "budget_required": sum(
            e.get("capability", "not_tested") in {"supported", "not_tested"}
            for e in entries
        ),
        "capacity_unit": "symbol/channel dimensions; account accounting unverified",
        "selected": selected,
        "not_selected": omitted,
        "by_asset": counts,
        "by_asset_channel": matrix,
        "coverage_partial": True,
        "live_verified": False,
    }

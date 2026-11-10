"""Seal complete ETF captures into the existing live-v1 delivery outbox.

SQLite is recovery/control-plane state only. The central platform receives immutable
normalized rows, full-underlying constructed raw snapshots and original acquisition
bytes. A delivery receipt is NOT a canonical publication or a gold promotion.
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, time
from pathlib import Path
from typing import Any

from openbb_collector_core.delivery import (
    DeliveryError,
    DeliveryStore,
    atomic_local,
    file_hash,
    json_bytes,
    local_path,
    read_delivery_status,
)

from .config import iso_date
from .quality import code, occ_identity, source_time, unit_authority
from .storage import digest, semantic_metadata, semantic_quote

RAW_LIMIT = 8_000_000


def _check(stop):
    if stop is not None and stop.is_set():
        raise InterruptedError("cancelled")


def _immutable(root: Path, relative: str, payload: bytes, limit: int) -> dict:
    if len(payload) > limit:
        raise DeliveryError("etf_artifact_byte_limit")
    path = local_path(root, relative)
    sha = hashlib.sha256(payload).hexdigest()
    if path.exists():
        if file_hash(path, limit) != (sha, len(payload)):
            raise DeliveryError("immutable_local_artifact_changed")
    else:
        atomic_local(path, payload)
        path.chmod(0o444)
    return {"path": relative, "sha256": sha, "role": "export", "remove": False}


def _evidence(config, journal, capture):
    """Preserve actual provider envelopes, including resumed checkpoint lineage.

    Originals have source objects rather than the live-v1 source string. Export
    role avoids inventing accepted raw-request counts for these audit artifacts.
    """
    artifacts: list[dict[str, Any]] = []
    locators: dict[str, list[dict[str, Any]]] = {}
    source_limit = min(config.limits.max_response_bytes, RAW_LIMIT)
    attempts = journal.db.execute(
        "SELECT * FROM attempts WHERE raw_path IS NOT NULL AND "
        "(run_id=? OR capture_id=? OR capture_id IN "
        "(SELECT id FROM captures WHERE trading_date=? AND exchange=? "
        "AND config_hash=? AND universe_hash=? "
        "AND rowid<=(SELECT rowid FROM captures WHERE id=?))) ORDER BY id",
        (
            capture["run_id"],
            capture["id"],
            capture["trading_date"],
            capture["exchange"],
            capture["config_hash"],
            capture["universe_hash"],
            capture["id"],
        ),
    )
    for attempt in attempts:
        original = local_path(journal.root, attempt["raw_path"])
        sha, _ = file_hash(original, source_limit)
        payload = original.read_bytes()
        envelope = json.loads(payload)
        relative = f"records/source_evidence/{sha}.json"
        artifact = _immutable(journal.root, relative, payload, source_limit)
        artifacts.append(artifact)
        for index, row in enumerate(envelope.get("rows", [])):
            if isinstance(row, dict) and row.get("symbol"):
                locators.setdefault(row["symbol"], []).append(
                    {
                        "attempt_id": attempt["id"],
                        "model": attempt["model"],
                        "original_path": attempt["raw_path"],
                        "export_path": relative,
                        "sha256": sha,
                        "raw_row": index,
                        "status": attempt["status"],
                        "observed_at": attempt["observed_at"],
                    }
                )
    return artifacts, locators


def _artifacts(
    config, journal, store, capture, sid, revision, stop
):  # pylint: disable=too-many-locals,too-many-positional-arguments
    metadata = json.loads(capture["contracts_json"])
    quotes = json.loads(capture["quotes_json"])
    symbols = [row["symbol"] for row in metadata]
    quoted = [row["symbol"] for row in quotes]
    if (
        not symbols  # pylint: disable=too-many-boolean-expressions
        or len(symbols) != len(set(symbols))
        or len(quoted) != len(set(quoted))
        or set(symbols) != set(quoted)
        or len(quotes) != capture["row_count"]
        or len(quotes) > config.limits.max_rows
    ):
        raise DeliveryError("etf_capture_membership_mismatch")
    exchange = capture["exchange"]
    if exchange not in {"SSE", "SZSE"}:
        raise DeliveryError("etf_exchange_invalid")
    observed = datetime.fromisoformat(capture["observed_at"])
    if observed.tzinfo is None:
        raise DeliveryError("etf_capture_clock_invalid")
    available = observed.astimezone(UTC).isoformat()
    # Units are a fact about the committed numbers, not a formatting choice: they
    # come from the deployment's declaration or they are absent.  ``provider_defined``
    # is never produced here.
    declared = config.unit_authority()
    authority = capture["unit_source"] or unit_authority(False, declared)
    acquisition_report = json.loads(capture["summary_json"])
    source_id = acquisition_report.get("config", {}).get("source_id", "etf_options")
    artifacts, locators = _evidence(config, journal, capture)
    master = {row["symbol"]: row for row in metadata}
    groups: dict[str, list[dict[str, Any]]] = {}
    for quote in sorted(quotes, key=lambda row: row["symbol"]):
        symbol = code(quote["symbol"], 8)
        meta = master[symbol]
        underlying = code(meta["underlying"], 6)
        if meta["exchange"] != exchange or quote["underlying"] != underlying:
            raise DeliveryError("etf_capture_identity_mismatch")
        identity = occ_identity(symbol, exchange)
        groups.setdefault(underlying, []).append(
            {
                **identity,
                "metadata": meta,
                "quote": quote,
                "source_evidence": locators.get(symbol, []),
            }
        )
    records, attempts = [], []
    collection = f"{exchange.lower()}_etf_chain"
    for underlying, members in sorted(groups.items()):
        _check(stop)
        poll = digest([sid, exchange, underlying])
        raw = {
            "source": "akshare",
            "source_id": source_id,
            "session_id": sid,
            "poll_id": poll,
            "observed_at": available,
            "chunk": {"collection_id": collection, "underlying_symbol": underlying},
            "exchange": exchange,
            "trading_date": capture["trading_date"],
            "envelope_kind": "collector_constructed_underlying_snapshot",
            "original_network_payload": False,
            "evidence_locator": capture["evidence_locator"],
            "unit_source": authority,
            "response_complete": True,
            "membership_hash": digest([row["symbol"] for row in members]),
            "rows": members,
        }
        payload = json_bytes(raw)
        raw_hash = hashlib.sha256(payload).hexdigest()
        artifact = _immutable(
            journal.root,
            f"bronze/{raw_hash[:2]}/{raw_hash}.json",
            payload,
            min(config.limits.max_response_bytes, RAW_LIMIT),
        )
        artifacts.append({**artifact, "role": "raw"})
        for index, member in enumerate(members):
            quote, meta, symbol = member["quote"], member["metadata"], member["symbol"]
            stamp = source_time(quote["source_time"])
            known = stamp.time() != time(0)
            content = digest(
                {
                    "metadata": semantic_metadata([meta])[0],
                    "quote": semantic_quote(quote),
                }
            )
            key = digest([sid, collection, exchange, symbol])
            flags = list(
                dict.fromkeys(
                    meta.get("quality_flags", []) + quote.get("quality_flags", [])
                )
            )
            values = {
                **quote,
                "contract_symbol": symbol,
                "underlying_symbol": underlying,
                "expiration": meta["expiration"],
                "option_type": meta["option_type"],
                "strike": meta["strike"],
                "contract_unit": meta.get("contract_unit"),
                "trading_date": capture["trading_date"],
                "pricing_eligible": quote.get("pricing_eligible") is True,
                "quality_flags": flags,
                "last_price": quote.get("last"),
                "source_original_status": quote.get("source_status"),
                "contract_metadata": meta,
                "source_evidence": member["source_evidence"],
                "quote_quality": (
                    "stale" if "stale_quote" in flags else "source_displayed"
                ),
            }
            records.append(
                {
                    "record_id": digest([key, content]),
                    "observation_key": key,
                    "session_id": sid,
                    "poll_id": poll,
                    "collection_id": collection,
                    "provider": "akshare",
                    "source": "akshare",
                    "source_id": source_id,
                    "source_symbol": symbol,
                    "symbol": symbol,
                    "instrument_id": member["instrument_id"],
                    "exchange": exchange,
                    "asset_type": "option",
                    "currency": "CNY",
                    "currency_basis": "exchange_contract",
                    "underlying_symbol": underlying,
                    "raw_hash": raw_hash,
                    "raw_row": index,
                    "content_hash": content,
                    "source_revision": content,
                    "revision_basis": "source_content_sha256",
                    # No provider sequence exists. This sentinel is not a guessed revision.
                    "revision": 0,
                    "event_time": stamp.astimezone(UTC).isoformat() if known else None,
                    "source_timestamp": quote["source_time"],
                    "timezone": "Asia/Shanghai",
                    "timestamp_semantics": (
                        "source_displayed_quote_time"
                        if known
                        else "source_midnight_unknown"
                    ),
                    "available_at": available,
                    "availability_basis": "collector_capture_completed",
                    "size_unit": "contracts",
                    "volume_unit": "contracts",
                    "quality_flags": flags,
                    "pricing_eligible": quote.get("pricing_eligible") is True,
                    "values": values,
                }
            )
        attempts.append(
            {
                "id": poll,
                "session_id": sid,
                "collection_id": collection,
                "attempted_at": available,
                "duplicates": 0,
                "attempts": [
                    {
                        "source": "akshare",
                        "status": "accepted",
                        "raw_hash": raw_hash,
                        "input_rows": len(members),
                        "rejected": 0,
                        "accounting_basis": "collector_constructed_underlying_snapshot",
                    }
                ],
            }
        )
    report = {
        "capture_id": capture["id"],
        "trading_date": capture["trading_date"],
        "exchange": exchange,
        "response_complete": True,
        "row_count": len(records),
        "pricing_eligible_count": capture["pricing_eligible_count"],
        "run_id": capture["run_id"],
        "acquisition_status": capture["run_status"],
        "acquisition_report": acquisition_report,
        "unit_source": authority,
        "evidence_locator": capture["evidence_locator"],
        "collector_observed_at": available,
        "central_publication_status": "not_requested",
    }
    for role, data in (
        ("observations", records),
        ("attempts", attempts),
        ("report", [report]),
    ):
        artifacts.append(
            {**store.snapshot("live", sid, revision, role, data), "remove": False}
        )
    return artifacts


def delivery_status(config, limit=20):
    return {
        "command": "delivery-status",
        "status": "complete",
        "central_publication_status": "not_requested",
        **read_delivery_status(config.root, config.delivery, limit),
    }


def deliver(
    config,
    journal,
    *,
    day=None,
    force=False,
    send=True,
    limit=100,
    pending_only=False,
    stop=None,
):
    """Run under Journal's exclusive writer lock; never load providers/calendars.

    Check the durable staged identity before materializing any artifact. All files
    are atomically sealed before core's transactional stage claim. A crash before
    claim leaves reusable immutable files; a crash after claim only retries send.
    """
    if not 1 <= limit <= 100:
        raise ValueError("Delivery attempt limit must be 1..100")
    if day is not None:
        day = iso_date(day).isoformat()
    base = {"command": "deliver", "central_publication_status": "not_requested"}
    if config.delivery is None:
        return {
            **base,
            "status": "skipped",
            "enabled": False,
            "counts": {},
            "jobs": [],
            "events": [],
        }
    store = DeliveryStore(journal.root, config.delivery)
    events, staged, remaining = [], 0, 0
    try:
        _check(stop)
        if not pending_only:
            for capture in journal.db.execute(
                "SELECT c.*,r.status AS run_status,r.summary_json,v.content_sha256 "
                "FROM captures c JOIN runs r ON r.id=c.run_id "
                "JOIN revisions v ON v.id=c.revision_id WHERE c.status='complete' "
                "AND c.response_complete=1 AND r.status<>'running' "
                "AND (? IS NULL OR c.trading_date=?) ORDER BY c.rowid",
                (day, day),
            ):
                _check(stop)
                sid, revision = f"etf-{capture['id']}", capture["content_sha256"]
                if store.staged("live", sid, revision):
                    continue
                if staged >= limit:
                    remaining += 1
                    continue
                try:
                    artifacts = _artifacts(
                        config, journal, store, capture, sid, revision, stop
                    )
                    _check(stop)
                    store.stage(
                        "live",
                        sid,
                        artifacts,
                        {
                            "revision": revision,
                            "observation_schema": "live_v1",
                            "capture_id": capture["id"],
                            "session_status": "complete",
                            "acquisition_status": capture["run_status"],
                            "run_id": capture["run_id"],
                            "exchange": capture["exchange"],
                            "trading_date": capture["trading_date"],
                            "response_complete": True,
                            "snapshot_semantics": (
                                "complete_underlying_membership_not_synchronous_quotes"
                            ),
                        },
                    )
                except InterruptedError:
                    raise
                except Exception as exc:
                    events.append(
                        {
                            "event": "session_sealing_failed",
                            "session_id": sid,
                            "status": "failed",
                            "error": (
                                str(exc)
                                if isinstance(exc, DeliveryError)
                                else "etf_session_sealing_failed"
                            ),
                        }
                    )
                staged += 1
        _check(stop)
        if send:
            events.extend(store.send_pending(force=force, limit=limit))
        _check(stop)
        counts = dict(
            store.db.execute(
                "SELECT status,count(*) FROM jobs WHERE target=? GROUP BY status",
                (store.target,),
            )
        )
        pending = sum(n for state, n in counts.items() if state != "delivered")
        failed = any(e.get("status") == "failed" or e.get("error") for e in events)
        result_status = (
            "failed" if failed else "pending" if pending or remaining else "complete"
        )
        return {
            **base,
            **store.status(),
            "enabled": True,
            "counts": counts,
            "status": result_status,
            "events": events,
            "unstaged_captures": remaining,
            "batch_limit_reached": remaining > 0,
        }
    finally:
        store.close()

"""Acceptance of newly collected samples, separated by source and instrument."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from .config import CollectorConfig
from .quality import digest
from .storage import Journal, atomic_json


def required_pairs(config: CollectorConfig, sources: list[str]) -> list[tuple[str, str, str]]:
    pairs = [
        (collection_id, instrument_id, source)
        for collection_id, collection in config.collections.items()
        for instrument_id in collection.instrument_ids
        for source in dict.fromkeys(sources)
        if source in [collection.primary, *collection.secondary]
    ]
    if set(sources) - {pair[2] for pair in pairs}:
        raise ValueError("Required sources must be routed by at least one collection")
    return pairs


def checkpoint(journal: Journal) -> tuple[int, int]:
    return (
        journal.db.execute("SELECT COALESCE(MAX(rowid),0) FROM polls").fetchone()[0],
        journal.db.execute("SELECT COALESCE(MAX(seq),0) FROM observations").fetchone()[0],
    )


def assess(
    journal: Journal,
    config: CollectorConfig,
    before: tuple[int, int],
    sources: list[str],
    minimum: int,
) -> dict[str, Any]:
    """Old successes and fallback successes cannot satisfy another source's gate."""
    checks = []
    for collection_id, instrument_id, source in required_pairs(config, sources):
        count = journal.db.execute(
            """SELECT COUNT(*) FROM polls p JOIN sessions s ON s.id=p.session_id
            WHERE p.rowid>? AND s.config_hash=? AND s.collection_id=?
            AND p.instrument_id=? AND p.source=? AND p.status='success'""",
            (before[0], config.fingerprint(), collection_id, instrument_id, source),
        ).fetchone()[0]
        checks.append(
            {
                "collection": collection_id,
                "instrument": instrument_id,
                "source": source,
                "successful_polls": count,
                "minimum": minimum,
                "passed": count >= minimum,
            }
        )
    counts = dict(
        journal.db.execute(
            """SELECT COUNT(*) AS new_records,
        COALESCE(SUM(exported=0),0) AS pending_output_records,
        COALESCE(SUM(event_time IS NULL),0) AS event_time_unknown,
        COALESCE(SUM(json_extract(payload,'$.actual_feed_type')='unknown'),0)
        AS actual_feed_type_unknown FROM observations WHERE seq>?""",
            (before[1],),
        ).fetchone()
    )
    outcomes: dict[str, Any] = {}
    for poll in journal.db.execute("SELECT attempts FROM polls WHERE rowid>?", (before[0],)):
        for attempt in json.loads(poll[0]):
            item = outcomes.setdefault(
                attempt["source"], {"outcomes": {}, "gateway_error_codes": {}}
            )
            status = attempt["status"]
            item["outcomes"][status] = item["outcomes"].get(status, 0) + 1
            for code in attempt.get("gateway_error_codes", []):
                key = str(code)
                item["gateway_error_codes"][key] = item["gateway_error_codes"].get(key, 0) + 1
    report = {
        "event": "live_acceptance",
        "report_version": 1,
        "reported_at": datetime.now(UTC).isoformat(),
        "config_hash": config.fingerprint(),
        "passed": bool(checks)
        and all(item["passed"] for item in checks)
        and not counts["pending_output_records"],
        "checks": checks,
        "sources": outcomes,
        **counts,
        "basis": "new_provider_polls_meeting_configured_contract",
        "gateway_error_scope": "provider_call_interval",
    }
    atomic_json(journal.root / "acceptance" / f"{digest(report)}.json", report, immutable=True)
    return report

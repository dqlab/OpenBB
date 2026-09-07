"""Seal historical sessions without copying the mutable acquisition database."""

import json

from openbb_collector_core.delivery import (
    DeliveryError,
    deliver_sessions,
    file_hash,
    local_path,
)


def _sessions(journal, store):
    for session in journal.db.execute(
        "SELECT id,config_hash,status,report FROM sessions "
        "WHERE status<>'running' AND report IS NOT NULL AND report_pending=0 ORDER BY rowid"
    ):
        sid = session["id"]
        if journal.pending_count(sid):
            continue
        report_path = f"reports/{sid}.json"
        revision, _ = file_hash(local_path(journal.root, report_path), store.config.max_bytes)
        if store.staged("historical", sid, revision):
            continue

        def artifacts(sid=sid, revision=revision, report_path=report_path):
            yield {"path": report_path, "role": "report"}
            for row in journal.db.execute(
                "SELECT raw_hash FROM raw_artifacts WHERE session_id=?", (sid,)
            ):
                raw_hash = row[0]
                yield {
                    "path": f"bronze/{raw_hash[:2]}/{raw_hash}.json",
                    "role": "raw",
                    "remove": True,
                }
            observations = (
                json.loads(row[0])
                for row in journal.db.execute(
                    "SELECT payload FROM observations WHERE session_id=? ORDER BY seq", (sid,)
                )
            )
            yield store.snapshot("historical", sid, revision, "observations", observations)
            attempts = (
                dict(row)
                for row in journal.db.execute(
                    "SELECT * FROM attempts WHERE session_id=? ORDER BY seq", (sid,)
                )
            )
            yield store.snapshot("historical", sid, revision, "attempts", attempts)
            legacy_quarantine = False
            for row in journal.db.execute(
                "SELECT detail,rejected FROM attempts WHERE session_id=? ORDER BY seq", (sid,)
            ):
                detail = json.loads(row[0])
                if raw_hash := detail.get("raw_hash"):
                    yield {
                        "path": f"bronze/{raw_hash[:2]}/{raw_hash}.json",
                        "role": "raw",
                        "remove": True,
                    }
                if path := detail.get("quarantine_path"):
                    yield {"path": path, "role": "quarantine", "remove": True}
                elif row[1]:
                    legacy_quarantine = True
            if legacy_quarantine:
                # Older journals did not record quarantine paths. Preserve their evidence too.
                for index, path in enumerate((journal.root / "quarantine").glob("*.json")):
                    if index >= store.config.max_files:
                        raise DeliveryError("legacy_quarantine_scan_limit")
                    safe = local_path(journal.root, path.relative_to(journal.root).as_posix())
                    file_hash(safe, min(store.config.max_bytes, 16_000_000))
                    with safe.open("rb") as stream:
                        payload = json.loads(stream.read(16_000_001))
                    if payload.get("session_id") == sid:
                        yield {
                            "path": path.relative_to(journal.root).as_posix(),
                            "role": "quarantine",
                            "remove": True,
                        }
            if journal.config.format in {"jsonl", "csv", "parquet"}:
                for row in journal.db.execute(
                    "SELECT DISTINCT export_batch FROM observations "
                    "WHERE session_id=? AND export_batch IS NOT NULL AND exported=1",
                    (sid,),
                ):
                    batch = row[0]
                    other = journal.db.execute(
                        "SELECT 1 FROM observations WHERE export_batch=? AND session_id<>? LIMIT 1",
                        (batch, sid),
                    ).fetchone()
                    if not other:
                        yield {
                            "path": f"records/{batch}.{journal.config.format}",
                            "role": "export",
                            "remove": True,
                        }

        yield (
            "historical",
            sid,
            revision,
            artifacts,
            {
                "config_hash": session["config_hash"],
                "session_status": session["status"],
                "pending_exports_at_delivery": 0,
                "observation_schema": "historical_v1",
            },
        )


def deliver(config, journal, *, force=False, send=True, limit=10):
    result = deliver_sessions(
        config,
        journal,
        lambda store: _sessions(journal, store),
        force=force,
        send=send,
        limit=limit,
    )
    result["pending_output_sessions"] = journal.db.execute(
        "SELECT COUNT(*) FROM sessions s WHERE status<>'running' AND EXISTS "
        "(SELECT 1 FROM observations o WHERE o.session_id=s.id AND o.exported=0)"
    ).fetchone()[0]
    return result

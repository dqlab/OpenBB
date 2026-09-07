"""Deliver immutable snapshots of completed or interrupted live sessions."""

import json

from openbb_collector_core.delivery import deliver_sessions, file_hash, local_path


def _sessions(journal, store):
    for session in journal.db.execute(
        "SELECT id,config_hash,status,report_path FROM sessions WHERE status<>'running' "
        "AND report_pending=0 AND report_path IS NOT NULL ORDER BY start"
    ):
        sid = session["id"]
        if journal.db.execute(
            "SELECT 1 FROM observations WHERE session_id=? AND exported=0 LIMIT 1", (sid,)
        ).fetchone():
            continue
        report_path = session["report_path"]
        revision, _ = file_hash(local_path(journal.root, report_path), store.config.max_bytes)
        if store.staged("live", sid, revision):
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
            yield store.snapshot("live", sid, revision, "observations", observations)
            attempts = (
                dict(row)
                for row in journal.db.execute(
                    "SELECT * FROM polls WHERE session_id=? ORDER BY rowid", (sid,)
                )
            )
            yield store.snapshot("live", sid, revision, "attempts", attempts)
            for row in journal.db.execute("SELECT attempts FROM polls WHERE session_id=?", (sid,)):
                for attempt in json.loads(row[0]):
                    if raw_hash := attempt.get("raw_hash"):
                        yield {
                            "path": f"bronze/{raw_hash[:2]}/{raw_hash}.json",
                            "role": "raw",
                            "remove": True,
                        }
            if journal.config.format in {"jsonl", "csv", "parquet"}:
                for row in journal.db.execute(
                    "SELECT id FROM export_batches WHERE session_id=? AND completed=1", (sid,)
                ):
                    yield {
                        "path": f"observations/{sid}/{row[0]}.{journal.config.format}",
                        "role": "export",
                        "remove": True,
                    }

        yield (
            "live",
            sid,
            revision,
            artifacts,
            {
                "config_hash": session["config_hash"],
                "session_status": session["status"],
                "pending_exports_at_delivery": 0,
                "observation_schema": "live_v1",
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

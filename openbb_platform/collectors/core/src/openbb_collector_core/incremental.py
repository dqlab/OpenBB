"""Complete-attempt batches and bounded collector status, retaining local evidence."""

from __future__ import annotations

import hashlib
import json
import os
import uuid
from datetime import UTC, datetime

from openbb_collector_core.delivery import (
    DeliveryError,
    LocalDestination,
    destination,
    json_bytes,
    local_path,
)


def incremental_sessions(config, journal, store, engine: str):
    if not config.delivery.incremental:
        return
    table = "polls" if engine == "live" else "attempts"
    if engine not in {"live", "historical"}:
        raise DeliveryError("unknown_incremental_engine")
    for session in journal.db.execute(
        "SELECT id,config_hash FROM sessions WHERE status='running' ORDER BY rowid"
    ).fetchall():
        sid = session["id"]
        saved = store.db.execute(
            "SELECT attempt_cursor,observation_cursor FROM incremental_cursors "
            "WHERE target=? AND engine=? AND session_id=?",
            (store.target, engine, sid),
        ).fetchone()
        attempt_cursor, observation_cursor = tuple(saved) if saved else (0, 0)
        pending = journal.db.execute(
            f"SELECT rowid AS batch_sequence,* FROM {table} WHERE session_id=? "
            "AND rowid>? ORDER BY rowid LIMIT ?",
            (sid, attempt_cursor, config.delivery.incremental_max_attempts),
        ).fetchall()
        selected, count = [], 0
        for row in pending:
            accepted = row["accepted"]
            if count + accepted > config.delivery.incremental_max_records:
                if not selected:
                    raise DeliveryError("single_attempt_exceeds_incremental_row_limit")
                break
            selected.append(dict(row))
            count += accepted
        if not selected:
            continue
        observations = journal.db.execute(
            "SELECT seq,payload,exported FROM observations WHERE session_id=? AND seq>? "
            "ORDER BY seq LIMIT ?",
            (sid, observation_cursor, count),
        ).fetchall()
        if len(observations) != count:
            raise DeliveryError("incremental_observation_count_mismatch")
        if any(not row["exported"] for row in observations):
            continue
        records = [json.loads(row["payload"]) for row in observations]
        upper = selected[-1]["batch_sequence"]
        last_observation = observations[-1]["seq"] if observations else observation_cursor
        revision = hashlib.sha256(
            json_bytes(["batch-v2", engine, sid, attempt_cursor, upper, last_observation])
        ).hexdigest()
        raw_hashes = {record["raw_hash"] for record in records}
        quarantine = set()
        for attempt in selected:
            attempt.pop("batch_sequence")
            if engine == "live":
                for detail in json.loads(attempt["attempts"]):
                    if detail.get("raw_hash"):
                        raw_hashes.add(detail["raw_hash"])
                attempt["collection_id"] = journal.db.execute(
                    "SELECT collection_id FROM sessions WHERE id=?", (sid,)
                ).fetchone()[0]
            else:
                detail = json.loads(attempt["detail"])
                if detail.get("raw_hash"):
                    raw_hashes.add(detail["raw_hash"])
                if detail.get("quarantine_path"):
                    quarantine.add(detail["quarantine_path"])

        # Copy bounded arrays into closure defaults before the generator advances sessions.
        def artifacts(
            records=records,
            selected=selected,
            raw_hashes=raw_hashes,
            quarantine=quarantine,
            revision=revision,
            sid=sid,
        ):
            yield store.snapshot(engine, sid, revision, "observations", records)
            yield store.snapshot(engine, sid, revision, "attempts", selected)
            for raw_hash in sorted(raw_hashes):
                yield {
                    "path": f"bronze/{raw_hash[:2]}/{raw_hash}.json",
                    "role": "raw",
                    "remove": False,
                }
            for name in sorted(quarantine):
                yield {"path": name, "role": "quarantine", "remove": False}

        yield (
            engine,
            sid,
            revision,
            artifacts,
            {
                "delivery_kind": "batch",
                "batch_complete": True,
                "batch_id": revision,
                "session_status": "running",
                "config_hash": session["config_hash"],
                "observation_schema": f"{engine}_v1",
                "observation_count": count,
                "attempt_count": len(selected),
                "pending_exports_at_delivery": 0,
                "incremental_cursor": {"attempt": upper, "observation": last_observation},
            },
        )


def heartbeat(
    config,
    journal,
    engine: str,
    now: datetime | None = None,
    session_states: dict[str, str] | None = None,
) -> None:
    if config.delivery is None or not config.delivery.incremental:
        return
    now = (now or datetime.now(UTC)).astimezone(UTC)
    last = journal.get_meta("central_heartbeat_sent_at")
    if last and (now - datetime.fromisoformat(last)).total_seconds() < 30:
        return
    collections = {}
    if engine == "live":
        for name in config.collections:
            value = journal.db.execute(
                "SELECT max(available_at) FROM observations WHERE collection_id=?", (name,)
            ).fetchone()[0]
            collections[name] = {
                "latest_observed_at": value,
                "session_state": (session_states or {}).get(name, "unknown"),
            }
    else:
        # One covering-index lookup per configured instrument; never rescan the
        # full retained journal or compute an unused live instrument summary.
        latest = {}
        for name, collection in config.collections.items():
            for instrument_id in collection.instrument_ids:
                if instrument_id not in latest:
                    latest[instrument_id] = journal.db.execute(
                        "SELECT max(available_at) FROM observations WHERE instrument_id=?",
                        (instrument_id,),
                    ).fetchone()[0]
            times = [latest[x] for x in collection.instrument_ids if latest.get(x)]
            collections[name] = {
                "latest_observed_at": max(times) if times else None,
                "session_state": "scheduled_historical",
            }
    content = json_bytes(
        {
            "schema_version": 1,
            "collector_id": config.delivery.collector_id,
            "engine": engine,
            "observed_at": now.isoformat(),
            "collections": collections,
            "local_files_retained": True,
        }
    )
    backend = destination(config.delivery, journal.root)
    name = f"remote_collectors/{config.delivery.collector_id}/status/heartbeat.json"
    temporary = name + f".{uuid.uuid4().hex}.tmp"
    try:
        with backend.open_write(temporary) as stream:
            stream.write(content)
            if isinstance(backend, LocalDestination):
                stream.flush()
                os.fsync(stream.fileno())
        with backend.open_read(temporary) as stream:
            if stream.read(65537) != content:
                raise DeliveryError("heartbeat_checksum_conflict")
        if isinstance(backend, LocalDestination):
            os.replace(local_path(backend.root, temporary), local_path(backend.root, name))
        else:
            # Atomic replacement is required for the mutable heartbeat; no remove/rename gap.
            backend.sftp.posix_rename(backend._path(temporary), backend._path(name))
        journal.set_meta("central_heartbeat_sent_at", now.isoformat())
    finally:
        backend.discard(temporary)
        backend.close()

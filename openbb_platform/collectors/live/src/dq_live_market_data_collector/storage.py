"""Single-writer SQLite journal, immutable evidence, and recoverable output batches."""

from __future__ import annotations

import csv
import importlib.util
import json
import os
import sqlite3
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .config import Collection, StorageConfig
from .quality import digest, json_text
from .schedule import Window, expected_slots


def atomic_json(path: Path, payload: Any, *, immutable: bool = False) -> None:
    atomic_bytes(path, (json_text(payload) + "\n").encode(), immutable=immutable)


def atomic_bytes(path: Path, payload: bytes, *, immutable: bool = False) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if immutable and path.exists():
        if path.read_bytes() != payload:
            raise ValueError("Immutable artifact content conflict")
        return
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("xb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        if os.name != "nt":
            descriptor = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
    finally:
        temporary.unlink(missing_ok=True)


class WriterLock:
    def __init__(self, root: Path) -> None:
        root.mkdir(parents=True, exist_ok=True)
        self.stream = (root / ".collector.lock").open("a+b")
        self.stream.seek(0)
        if not self.stream.read(1):
            self.stream.write(b"0")
            self.stream.flush()
        self.stream.seek(0)
        try:
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(self.stream.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(self.stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            self.stream.close()
            raise RuntimeError("Another collector owns this storage root") from exc

    def close(self) -> None:
        if not self.stream.closed:
            if os.name == "nt":
                import msvcrt

                self.stream.seek(0)
                msvcrt.locking(self.stream.fileno(), msvcrt.LK_UNLCK, 1)
            self.stream.close()


class Journal:
    """SQLite is authoritative; files and DuckDB are replayable materializations."""

    def __init__(self, config: StorageConfig) -> None:
        self.config = config
        self.root = config.root.resolve()
        dependency = {"parquet": "pyarrow", "duckdb": "duckdb"}.get(config.format)
        if dependency and importlib.util.find_spec(dependency) is None:
            raise ValueError(f"Install the '{config.format}' extra before starting collection")
        self.lock = WriterLock(self.root)
        self.db: sqlite3.Connection | None = None
        try:
            self.db = sqlite3.connect(self.root / "journal.sqlite3")
            self.db.row_factory = sqlite3.Row
            self.db.executescript("""
                PRAGMA journal_mode=WAL;
                PRAGMA synchronous=FULL;
                CREATE TABLE IF NOT EXISTS raw_artifacts (
                    raw_hash TEXT PRIMARY KEY, session_id TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS raw_artifacts_session ON raw_artifacts(session_id);
                PRAGMA foreign_keys=ON;
                CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS sessions (
                    id TEXT PRIMARY KEY, collection_id TEXT NOT NULL, config_hash TEXT NOT NULL,
                    start TEXT NOT NULL, end TEXT NOT NULL, spec TEXT NOT NULL,
                    generation INTEGER NOT NULL, status TEXT NOT NULL, reason TEXT,
                    report_pending INTEGER NOT NULL DEFAULT 0, report_path TEXT
                );
                CREATE TABLE IF NOT EXISTS polls (
                    id TEXT PRIMARY KEY, session_id TEXT NOT NULL REFERENCES sessions(id),
                    instrument_id TEXT NOT NULL, slot INTEGER NOT NULL, status TEXT NOT NULL,
                    source TEXT, fallback INTEGER NOT NULL, attempted_at TEXT NOT NULL,
                    input_rows INTEGER NOT NULL, accepted INTEGER NOT NULL,
                    duplicates INTEGER NOT NULL, rejected INTEGER NOT NULL, attempts TEXT NOT NULL,
                    UNIQUE(session_id, instrument_id, slot)
                );
                CREATE TABLE IF NOT EXISTS export_batches (
                    id TEXT PRIMARY KEY, session_id TEXT NOT NULL, record_ids TEXT NOT NULL,
                    completed INTEGER NOT NULL DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS observations (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT, record_id TEXT NOT NULL UNIQUE,
                    observation_key TEXT NOT NULL, revision TEXT NOT NULL,
                    values_hash TEXT NOT NULL, session_id TEXT NOT NULL REFERENCES sessions(id),
                    collection_id TEXT NOT NULL, instrument_id TEXT NOT NULL, source TEXT NOT NULL,
                    event_time TEXT, received_at TEXT NOT NULL, available_at TEXT NOT NULL,
                    payload TEXT NOT NULL, export_batch TEXT REFERENCES export_batches(id),
                    exported INTEGER NOT NULL DEFAULT 0
                );
                CREATE INDEX IF NOT EXISTS observations_revision
                    ON observations(observation_key, revision);
                CREATE INDEX IF NOT EXISTS observations_pending
                    ON observations(exported, export_batch, seq);
                CREATE INDEX IF NOT EXISTS observations_session ON observations(session_id);
                CREATE INDEX IF NOT EXISTS polls_session ON polls(session_id);
            """)
            previous_format = self.get_meta("format")
            if previous_format and previous_format != config.format:
                raise ValueError("Choose a new storage root when changing output format")
            self.set_meta("format", config.format)
            self.set_meta("schema_version", "1")
        except Exception:
            self.close()
            raise

    def __enter__(self) -> Journal:
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()

    def close(self) -> None:
        if self.db:
            self.db.close()
            self.db = None
        self.lock.close()

    def get_meta(self, key: str) -> str | None:
        row = self.db.execute("SELECT value FROM metadata WHERE key=?", (key,)).fetchone()
        return row[0] if row else None

    def set_meta(self, key: str, value: str) -> None:
        with self.db:
            self.db.execute(
                "INSERT INTO metadata VALUES (?,?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, value),
            )

    def ensure_session(
        self,
        collection_id: str,
        collection: Collection,
        config_hash: str,
        window: Window,
        generation: int,
    ) -> str:
        session_id = digest([config_hash, collection_id, window.start.isoformat()])
        with self.db:
            self.db.execute(
                """INSERT OR IGNORE INTO sessions
                (id, collection_id, config_hash, start, end, spec, generation, status)
                VALUES (?,?,?,?,?,?,?, 'running')""",
                (
                    session_id,
                    collection_id,
                    config_hash,
                    window.start.isoformat(),
                    window.end.isoformat(),
                    collection.model_dump_json(),
                    generation,
                ),
            )
            self.db.execute(
                "UPDATE sessions SET status='running', reason=NULL "
                "WHERE id=? AND status='interrupted'",
                (session_id,),
            )
        return session_id

    def running_sessions(self) -> list[dict[str, Any]]:
        return [
            dict(row) for row in self.db.execute("SELECT * FROM sessions WHERE status='running'")
        ]

    def finish_session(self, session_id: str, status: str, reason: str) -> None:
        with self.db:
            self.db.execute(
                "UPDATE sessions SET status=?, reason=?, report_pending=1 WHERE id=?",
                (status, reason, session_id),
            )

    def poll_exists(self, session_id: str, instrument_id: str, slot: int) -> bool:
        return (
            self.db.execute(
                "SELECT 1 FROM polls WHERE session_id=? AND instrument_id=? AND slot=?",
                (session_id, instrument_id, slot),
            ).fetchone()
            is not None
        )

    def archive(self, payload: dict[str, Any]) -> str:
        raw_hash = digest(payload)
        atomic_json(
            self.root / "bronze" / raw_hash[:2] / f"{raw_hash}.json",
            payload,
            immutable=True,
        )
        if session_id := payload.get("session_id"):
            with self.db:
                self.db.execute(
                    "INSERT OR IGNORE INTO raw_artifacts VALUES (?,?)", (raw_hash, session_id)
                )
        return raw_hash

    def revision_conflict(self, record: dict[str, Any]) -> bool:
        if record["revision_basis"] != "provider":
            return False
        row = self.db.execute(
            """SELECT 1 FROM observations WHERE observation_key=? AND revision=?
            AND values_hash<>? LIMIT 1""",
            (record["observation_key"], record["source_revision"], record["content_hash"]),
        ).fetchone()
        return row is not None

    def commit_poll(
        self,
        *,
        poll_id: str,
        session_id: str,
        instrument_id: str,
        slot: int,
        attempted_at: datetime,
        source_id: str | None,
        fallback: bool,
        attempts: list[dict[str, Any]],
        records: list[dict[str, Any]],
    ) -> None:
        input_rows = sum(item["input_rows"] for item in attempts)
        rejected = sum(item["rejected"] for item in attempts)
        accepted = duplicates = 0
        with self.db:
            if self.db.execute("SELECT 1 FROM polls WHERE id=?", (poll_id,)).fetchone():
                return
            for record in records:
                inserted = self.db.execute(
                    """INSERT OR IGNORE INTO observations
                    (record_id, observation_key, revision, values_hash, session_id,
                     collection_id, instrument_id, source, event_time, received_at,
                     available_at, payload, exported)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        record["record_id"],
                        record["observation_key"],
                        record["source_revision"],
                        record["content_hash"],
                        session_id,
                        record["collection_id"],
                        instrument_id,
                        record["source"],
                        record["event_time"],
                        record["received_at"],
                        record["available_at"],
                        json_text(record),
                        int(self.config.format == "sqlite"),
                    ),
                ).rowcount
                accepted += inserted
                duplicates += 1 - inserted
            if input_rows != accepted + duplicates + rejected:
                raise ValueError("Response reconciliation failed")
            status = "success" if accepted + duplicates else "failed"
            self.db.execute(
                "INSERT INTO polls VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    poll_id,
                    session_id,
                    instrument_id,
                    slot,
                    status,
                    source_id,
                    int(fallback),
                    attempted_at.isoformat(),
                    input_rows,
                    accepted,
                    duplicates,
                    rejected,
                    json_text(attempts),
                ),
            )

    def _next_batch(self) -> sqlite3.Row | None:
        pending = self.db.execute(
            "SELECT * FROM export_batches WHERE completed=0 ORDER BY rowid LIMIT 1"
        ).fetchone()
        if pending:
            return pending
        first = self.db.execute(
            """SELECT session_id FROM observations WHERE exported=0 AND export_batch IS NULL
            ORDER BY seq LIMIT 1"""
        ).fetchone()
        if not first:
            return None
        with self.db:
            ids = [
                row[0]
                for row in self.db.execute(
                    """SELECT record_id FROM observations WHERE exported=0 AND export_batch IS NULL
                    AND session_id=? ORDER BY seq LIMIT ?""",
                    (first[0], self.config.export_batch_size),
                )
            ]
            batch_id = digest(ids)
            self.db.execute(
                "INSERT INTO export_batches VALUES (?,?,?,0)",
                (batch_id, first[0], json_text(ids)),
            )
            self.db.executemany(
                "UPDATE observations SET export_batch=? WHERE record_id=?",
                [(batch_id, record_id) for record_id in ids],
            )
        return self.db.execute("SELECT * FROM export_batches WHERE id=?", (batch_id,)).fetchone()

    def export_pending(self, max_batches: int = 4) -> int:
        exported = 0
        for _ in range(max_batches):
            batch = self._next_batch()
            if batch is None:
                break
            rows = [
                json.loads(row[0])
                for row in self.db.execute(
                    "SELECT payload FROM observations WHERE export_batch=? ORDER BY seq",
                    (batch["id"],),
                )
            ]
            self._materialize(batch["id"], batch["session_id"], rows)
            with self.db:
                self.db.execute(
                    "UPDATE observations SET exported=1 WHERE export_batch=?", (batch["id"],)
                )
                self.db.execute("UPDATE export_batches SET completed=1 WHERE id=?", (batch["id"],))
                self.db.execute(
                    "UPDATE sessions SET report_pending=1 WHERE id=? AND status<>'running'",
                    (batch["session_id"],),
                )
            exported += len(rows)
        return exported

    def _materialize(self, batch_id: str, session_id: str, rows: list[dict[str, Any]]) -> None:
        output_format = self.config.format
        path = self.root / "observations" / session_id / f"{batch_id}.{output_format}"
        if output_format == "jsonl":
            atomic_bytes(
                path, ("".join(json_text(row) + "\n" for row in rows)).encode(), immutable=True
            )
        elif output_format == "csv":
            import io

            buffer = io.StringIO(newline="")
            flattened = [{**row, "values": json_text(row["values"])} for row in rows]
            writer = csv.DictWriter(buffer, fieldnames=list(flattened[0]))
            writer.writeheader()
            writer.writerows(flattened)
            atomic_bytes(path, buffer.getvalue().encode(), immutable=True)
        elif output_format == "parquet":
            import pyarrow as pa
            import pyarrow.parquet as pq

            flattened = [{**row, "values": json_text(row["values"])} for row in rows]
            # Explicit schema prevents all-null timestamp/conId batches changing field types.
            columns = [
                pa.field(key, pa.int64() if key in {"contract_version", "con_id"} else pa.string())
                for key in flattened[0]
            ]
            table = pa.Table.from_pylist(flattened, schema=pa.schema(columns))
            buffer = pa.BufferOutputStream()
            pq.write_table(table, buffer, compression="zstd")
            atomic_bytes(path, buffer.getvalue().to_pybytes(), immutable=True)
        elif output_format == "duckdb":
            import duckdb

            with duckdb.connect(str(self.root / "observations.duckdb")) as database:
                database.execute("""CREATE TABLE IF NOT EXISTS observations (
                    record_id VARCHAR PRIMARY KEY, session_id VARCHAR, instrument_id VARCHAR,
                    source VARCHAR, event_time TIMESTAMPTZ, available_at TIMESTAMPTZ, payload JSON
                )""")
                database.execute("BEGIN TRANSACTION")
                database.executemany(
                    "INSERT INTO observations VALUES (?,?,?,?,?,?,?) ON CONFLICT DO NOTHING",
                    [
                        (
                            row["record_id"],
                            row["session_id"],
                            row["instrument_id"],
                            row["source"],
                            row["event_time"],
                            row["available_at"],
                            json_text(row),
                        )
                        for row in rows
                    ],
                )
                database.execute("COMMIT")
        else:
            raise ValueError("SQLite records should not enter the export queue")

    def report(self, session_id: str, now: datetime) -> dict[str, Any]:
        session = dict(
            self.db.execute("SELECT * FROM sessions WHERE id=?", (session_id,)).fetchone()
        )
        spec = Collection.model_validate_json(session["spec"])
        start = datetime.fromisoformat(session["start"])
        end = datetime.fromisoformat(session["end"])
        coverage_end = end if session["status"] == "completed" else min(now, end)
        expected = expected_slots(start, coverage_end, spec.frequency_seconds)
        per_instrument: dict[str, Any] = {}
        totals = dict.fromkeys(
            (
                "expected_polls",
                "attempted_polls",
                "successful_polls",
                "failed_polls",
                "missed_polls",
                "fallback_polls",
                "input_rows",
                "accepted",
                "duplicates",
                "rejected",
            ),
            0,
        )
        for instrument_id in spec.instrument_ids:
            row = self.db.execute(
                """SELECT COUNT(*) AS attempted_polls,
                COALESCE(SUM(status='success'),0) AS successful_polls,
                COALESCE(SUM(status='failed'),0) AS failed_polls,
                COALESCE(SUM(fallback),0) AS fallback_polls,
                COALESCE(SUM(input_rows),0) AS input_rows,
                COALESCE(SUM(accepted),0) AS accepted,
                COALESCE(SUM(duplicates),0) AS duplicates,
                COALESCE(SUM(rejected),0) AS rejected
                FROM polls WHERE session_id=? AND instrument_id=?""",
                (session_id, instrument_id),
            ).fetchone()
            counts = dict(row)
            counts["expected_polls"] = expected
            counts["missed_polls"] = max(0, expected - counts["attempted_polls"])
            per_instrument[instrument_id] = counts
            for key, value in counts.items():
                totals[key] += value
        source_attempts: dict[str, Any] = {}
        for poll in self.db.execute("SELECT attempts FROM polls WHERE session_id=?", (session_id,)):
            for attempt in json.loads(poll[0]):
                counters = source_attempts.setdefault(
                    attempt["source"],
                    {"attempts": 0, "outcomes": {}, "warning_count": 0, "gateway_error_codes": {}},
                )
                counters["attempts"] += 1
                counters["warning_count"] += attempt["warning_count"]
                code = attempt["status"]
                counters["outcomes"][code] = counters["outcomes"].get(code, 0) + 1
                for error in attempt.get("gateway_error_codes", []):
                    key = str(error)
                    counts = counters["gateway_error_codes"]
                    counts[key] = counts.get(key, 0) + 1
        pending = self.db.execute(
            "SELECT COUNT(*) FROM observations WHERE session_id=? AND exported=0", (session_id,)
        ).fetchone()[0]
        limitations = dict(
            self.db.execute(
                """SELECT COUNT(*) AS records,
            COALESCE(SUM(event_time IS NULL),0) AS event_time_unknown,
            COALESCE(SUM(json_extract(payload,'$.actual_feed_type')='unknown'),0)
            AS actual_feed_type_unknown FROM observations WHERE session_id=?""",
                (session_id,),
            ).fetchone()
        )
        health = "ok"
        if any(
            totals[key] for key in ("failed_polls", "missed_polls", "fallback_polls", "rejected")
        ):
            health = "degraded"
        if not totals["successful_polls"]:
            health = "failed"
        if pending or session["status"] == "interrupted":
            health = "degraded" if totals["successful_polls"] else "failed"
        return {
            "report_version": 1,
            "session_id": session_id,
            "collection_id": session["collection_id"],
            "config_hash": session["config_hash"],
            "generation": session["generation"],
            "session_start": session["start"],
            "session_end": session["end"],
            "reported_at": now.astimezone(UTC).isoformat(),
            "session_status": session["status"],
            "reason": session["reason"],
            "health": health,
            "totals": totals,
            "instruments": per_instrument,
            "sources": source_attempts,
            "pending_output_records": pending,
            "semantics": limitations,
            "coverage_basis": "configured_session_calendar",
        }

    def write_reports(self, now: datetime) -> list[dict[str, Any]]:
        reports = []
        sessions = self.db.execute(
            "SELECT id FROM sessions WHERE report_pending=1 ORDER BY start LIMIT 100"
        ).fetchall()
        for session in sessions:
            report = self.report(session[0], now)
            relative = Path("reports") / session[0] / f"{digest(report)}.json"
            atomic_json(self.root / relative, report, immutable=True)
            atomic_json(self.root / "reports" / "latest.json", report)
            with self.db:
                self.db.execute(
                    "UPDATE sessions SET report_pending=0, report_path=? WHERE id=?",
                    (relative.as_posix(), session[0]),
                )
            reports.append(report)
        return reports


def read_status(root: Path, limit: int = 20) -> dict[str, Any]:
    if not 1 <= limit <= 100:
        raise ValueError("Status limit must be between 1 and 100")
    path = root.resolve() / "journal.sqlite3"
    if not path.exists():
        return {"sessions": [], "pending_output_records": 0}
    with sqlite3.connect(f"{path.as_uri()}?mode=ro", uri=True) as database:
        database.row_factory = sqlite3.Row
        sessions = [
            dict(row)
            for row in database.execute(
                """SELECT id, collection_id, start, end, generation, status, reason
                FROM sessions ORDER BY start DESC LIMIT ?""",
                (limit,),
            )
        ]
        pending = database.execute("SELECT COUNT(*) FROM observations WHERE exported=0").fetchone()[
            0
        ]
        metadata = dict(
            database.execute(
                "SELECT key,value FROM metadata "
                "WHERE key IN ('heartbeat','export_error','lifecycle')"
            ).fetchall()
        )
    return {
        "sessions": sessions,
        "pending_output_records": pending,
        "last_heartbeat": metadata.get("heartbeat"),
        "export_error": metadata.get("export_error") or None,
        "lifecycle": json.loads(metadata["lifecycle"]) if metadata.get("lifecycle") else None,
    }

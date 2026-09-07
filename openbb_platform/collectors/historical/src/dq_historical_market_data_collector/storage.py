"""Transactional checkpoints, immutable evidence and replayable file/database outputs."""

from __future__ import annotations

import contextlib
import csv
import importlib.util
import io
import json
import os
import sqlite3
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .config import Storage
from .quality import digest, json_text


def atomic_bytes(path: Path, payload: bytes, immutable: bool = False) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if immutable and path.exists():
        if path.read_bytes() != payload:
            raise ValueError("Immutable artifact conflict")
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


def atomic_json(path: Path, payload: Any, immutable: bool = False) -> None:
    atomic_bytes(path, (json_text(payload) + "\n").encode(), immutable)


class WriterLock:
    def __init__(self, root: Path) -> None:
        root.mkdir(parents=True, exist_ok=True)
        self.stream = (root / ".collector.lock").open("a+b")
        if self.stream.tell() == 0:
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
            raise RuntimeError("Another collector owns the storage root") from exc

    def close(self) -> None:
        if not self.stream.closed:
            if os.name == "nt":
                import msvcrt

                self.stream.seek(0)
                msvcrt.locking(self.stream.fileno(), msvcrt.LK_UNLCK, 1)
            self.stream.close()


class Journal:
    """SQLite is authoritative. Export acknowledgments are independently recoverable."""

    def __init__(self, config: Storage) -> None:
        self.config = config
        self.root = config.root.resolve()
        dependency = {"parquet": "pyarrow", "duckdb": "duckdb"}.get(config.format)
        if dependency and importlib.util.find_spec(dependency) is None:
            raise ValueError(f"Install the {config.format} extra")
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
                CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS sessions (
                    id TEXT PRIMARY KEY, started_at TEXT NOT NULL, ended_at TEXT,
                    config_hash TEXT NOT NULL, scheduled_day TEXT, status TEXT NOT NULL,
                    report TEXT, report_pending INTEGER NOT NULL DEFAULT 0
                );
                CREATE INDEX IF NOT EXISTS scheduled_sessions
                    ON sessions(config_hash, scheduled_day, status);
                CREATE TABLE IF NOT EXISTS checkpoints (
                    chunk_key TEXT PRIMARY KEY, session_id TEXT NOT NULL, source TEXT NOT NULL,
                    completed_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS observations (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT, record_id TEXT NOT NULL UNIQUE,
                    observation_key TEXT NOT NULL, revision INTEGER NOT NULL,
                    values_hash TEXT NOT NULL, available_at TEXT NOT NULL,
                    session_id TEXT NOT NULL, instrument_id TEXT NOT NULL,
                    source TEXT NOT NULL, event_time TEXT NOT NULL, payload TEXT NOT NULL,
                    export_batch TEXT, exported INTEGER NOT NULL DEFAULT 0,
                    UNIQUE(observation_key, revision)
                );
                CREATE INDEX IF NOT EXISTS observed_asof
                    ON observations(observation_key, available_at, seq);
                CREATE INDEX IF NOT EXISTS pending_output ON observations(exported, seq);
                CREATE TABLE IF NOT EXISTS attempts (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT, session_id TEXT NOT NULL,
                    chunk_key TEXT NOT NULL, collection_id TEXT NOT NULL,
                    instrument_id TEXT NOT NULL, source TEXT NOT NULL,
                    fallback INTEGER NOT NULL, success INTEGER NOT NULL,
                    input_rows INTEGER NOT NULL, accepted INTEGER NOT NULL,
                    duplicates INTEGER NOT NULL, rejected INTEGER NOT NULL,
                    outside INTEGER NOT NULL, detail TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS session_attempts ON attempts(session_id);
                CREATE TABLE IF NOT EXISTS batches (
                    id TEXT PRIMARY KEY, completed INTEGER NOT NULL DEFAULT 0
                );
            """)
            previous = self.get_meta("format")
            if previous and previous != config.format:
                raise ValueError("Use a new storage root to change output format")
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
        row = self.db.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return row[0] if row else None

    def set_meta(self, key: str, value: str) -> None:
        with self.db:
            self.db.execute(
                "INSERT INTO meta VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, value),
            )

    def start(self, config_hash: str, scheduled_day: str | None = None) -> str:
        session_id = uuid.uuid4().hex
        with self.db:
            self.db.execute(
                "INSERT INTO sessions(id,started_at,config_hash,scheduled_day,status) "
                "VALUES (?,?,?,?, 'running')",
                (session_id, datetime.now(UTC).isoformat(), config_hash, scheduled_day),
            )
        return session_id

    def completed(self, chunk_key: str) -> bool:
        return (
            self.db.execute(
                "SELECT 1 FROM checkpoints WHERE chunk_key=?",
                (chunk_key,),
            ).fetchone()
            is not None
        )

    def completed_for_day(self, chunk_key: str, day: str) -> bool:
        return (
            self.db.execute(
                "SELECT 1 FROM checkpoints c JOIN sessions s ON c.session_id=s.id "
                "WHERE c.chunk_key=? AND s.scheduled_day=?",
                (chunk_key, day),
            ).fetchone()
            is not None
        )

    def scheduled_complete(self, config_hash: str, day: str) -> bool:
        return (
            self.db.execute(
                "SELECT 1 FROM sessions WHERE config_hash=? AND scheduled_day=? "
                "AND status='complete' LIMIT 1",
                (config_hash, day),
            ).fetchone()
            is not None
        )

    def archive(self, payload: Any) -> str:
        raw_hash = digest(payload)
        atomic_json(self.root / "bronze" / raw_hash[:2] / f"{raw_hash}.json", payload, True)
        if isinstance(payload, dict) and (session_id := payload.get("session_id")):
            with self.db:
                self.db.execute(
                    "INSERT OR IGNORE INTO raw_artifacts VALUES (?,?)", (raw_hash, session_id)
                )
        return raw_hash

    def record_attempt(
        self,
        session_id: str,
        chunk_key: str,
        collection_id: str,
        instrument_id: str,
        source: str,
        fallback: bool,
        records: list[dict[str, Any]],
        rejected: list[dict[str, Any]],
        outside: int,
        detail: dict[str, Any],
        success: bool,
    ) -> dict[str, int]:
        accepted, duplicates = 0, 0
        if rejected:
            relative = f"quarantine/{uuid.uuid4().hex}.json"
            detail = {**detail, "quarantine_path": relative}
            atomic_json(
                self.root / relative,
                {"session_id": session_id, "source": source, "rows": rejected},
                True,
            )
        with self.db:
            for record in records:
                previous = self.db.execute(
                    "SELECT revision,values_hash,available_at FROM observations "
                    "WHERE observation_key=? ORDER BY revision DESC LIMIT 1",
                    (record["observation_key"],),
                ).fetchone()
                if previous and previous["values_hash"] == record["values_hash"]:
                    duplicates += 1
                    continue
                if previous and record["available_at"] < previous["available_at"]:
                    raise ValueError("Observation clock moved backwards")
                revision = previous["revision"] + 1 if previous else 1
                record = {
                    **record,
                    "revision": revision,
                    "session_id": session_id,
                    "record_id": digest(
                        [record["observation_key"], revision, record["values_hash"]]
                    ),
                }
                self.db.execute(
                    "INSERT INTO observations(record_id,observation_key,revision,values_hash,"
                    "available_at,session_id,instrument_id,source,event_time,payload) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?)",
                    (
                        record["record_id"],
                        record["observation_key"],
                        revision,
                        record["values_hash"],
                        record["available_at"],
                        session_id,
                        instrument_id,
                        source,
                        record["event_time"],
                        json_text(record),
                    ),
                )
                accepted += 1
            counts = {
                "input_rows": len(records) + len(rejected) + outside,
                "accepted": accepted,
                "duplicates": duplicates,
                "rejected": len(rejected),
                "outside": outside,
            }
            self.db.execute(
                "INSERT INTO attempts(session_id,chunk_key,collection_id,instrument_id,source,"
                "fallback,success,input_rows,accepted,duplicates,rejected,outside,detail) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    session_id,
                    chunk_key,
                    collection_id,
                    instrument_id,
                    source,
                    int(fallback),
                    int(success),
                    *counts.values(),
                    json_text(detail),
                ),
            )
            if success:
                self.db.execute(
                    "INSERT INTO checkpoints VALUES (?,?,?,?) ON CONFLICT(chunk_key) "
                    "DO UPDATE SET session_id=excluded.session_id,source=excluded.source,"
                    "completed_at=excluded.completed_at",
                    (chunk_key, session_id, source, datetime.now(UTC).isoformat()),
                )
        return counts

    def pending_count(self, session_id: str | None = None) -> int:
        query = "SELECT count(*) FROM observations WHERE exported=0"
        params = ()
        if session_id:
            query += " AND session_id=?"
            params = (session_id,)
        return self.db.execute(query, params).fetchone()[0]

    def export_pending(self) -> None:
        if self.config.format == "sqlite":
            with self.db:
                self.db.execute("UPDATE observations SET exported=1 WHERE exported=0")
            return
        while True:
            pending = self.db.execute(
                "SELECT id FROM batches WHERE completed=0 LIMIT 1",
            ).fetchone()
            if pending:
                batch_id = pending[0]
            else:
                seqs = [
                    r[0]
                    for r in self.db.execute(
                        "SELECT seq FROM observations WHERE exported=0 AND export_batch IS NULL "
                        "ORDER BY seq LIMIT ?",
                        (self.config.export_batch_size,),
                    )
                ]
                if not seqs:
                    return
                batch_id = uuid.uuid4().hex
                with self.db:
                    self.db.execute("INSERT INTO batches(id) VALUES (?)", (batch_id,))
                    self.db.executemany(
                        "UPDATE observations SET export_batch=? WHERE seq=?",
                        [(batch_id, seq) for seq in seqs],
                    )
            rows = [
                json.loads(r[0])
                for r in self.db.execute(
                    "SELECT payload FROM observations WHERE export_batch=? ORDER BY seq",
                    (batch_id,),
                )
            ]
            self._write_batch(batch_id, rows)
            with self.db:
                self.db.execute("UPDATE batches SET completed=1 WHERE id=?", (batch_id,))
                self.db.execute(
                    "UPDATE observations SET exported=1 WHERE export_batch=?",
                    (batch_id,),
                )

    def _write_batch(self, batch_id: str, rows: list[dict[str, Any]]) -> None:
        fmt = self.config.format
        path = self.root / "records" / f"{batch_id}.{fmt}"
        if fmt == "jsonl":
            atomic_bytes(path, "".join(json_text(row) + "\n" for row in rows).encode(), True)
            return
        if fmt in {"csv", "parquet"}:
            flat = []
            for row in rows:
                item = {k: v for k, v in row.items() if k != "values"}
                item.update({f"data_{k}": v for k, v in row["values"].items()})
                flat.append(item)
            fields = sorted({k for row in flat for k in row})
            if fmt == "csv":
                stream = io.StringIO(newline="")
                writer = csv.DictWriter(stream, fieldnames=fields)
                writer.writeheader()
                writer.writerows(flat)
                atomic_bytes(path, stream.getvalue().encode(), True)
            else:
                import pyarrow as pa
                import pyarrow.parquet as pq

                # Explicit nullable data schema prevents null-only batch type drift.
                arrays = {}
                for name in fields:
                    values = [row.get(name) for row in flat]
                    if name.startswith("data_"):
                        dtype = pa.float64()
                    elif name in {"revision", "con_id", "raw_row", "normalization_version"}:
                        dtype = pa.int64()
                    elif name == "use_rth":
                        dtype = pa.bool_()
                    else:
                        dtype = pa.string()
                    arrays[name] = pa.array(values, type=dtype)
                stream = pa.BufferOutputStream()
                pq.write_table(pa.table(arrays), stream)
                atomic_bytes(path, stream.getvalue().to_pybytes(), True)
            return
        import duckdb

        with duckdb.connect(str(self.root / "historical.duckdb")) as db:
            db.execute(
                "CREATE TABLE IF NOT EXISTS observations "
                "(record_id VARCHAR PRIMARY KEY, instrument_id VARCHAR, source VARCHAR,"
                "event_time VARCHAR, available_at VARCHAR, payload JSON)"
            )
            db.execute("BEGIN")
            try:
                db.executemany(
                    "INSERT INTO observations VALUES (?,?,?,?,?,?) ON CONFLICT DO NOTHING",
                    [
                        (
                            r["record_id"],
                            r["instrument_id"],
                            r["source"],
                            r["event_time"],
                            r["available_at"],
                            json_text(r),
                        )
                        for r in rows
                    ],
                )
                db.execute("COMMIT")
            except Exception:
                db.execute("ROLLBACK")
                raise

    def finish(self, session_id: str, status: str, summary: dict[str, Any]) -> dict[str, Any]:
        session = dict(
            self.db.execute(
                "SELECT * FROM sessions WHERE id=?",
                (session_id,),
            ).fetchone()
        )
        totals = dict(
            self.db.execute(
                "SELECT coalesce(sum(input_rows),0) AS input_rows,"
                "coalesce(sum(accepted),0) AS accepted,coalesce(sum(duplicates),0) AS duplicates,"
                "coalesce(sum(rejected),0) AS rejected,coalesce(sum(outside),0) AS outside "
                "FROM attempts WHERE session_id=?",
                (session_id,),
            ).fetchone()
        )
        sources = [
            dict(row)
            for row in self.db.execute(
                "SELECT source,count(*) AS attempts,sum(success) AS successful_requests,"
                "sum(fallback) AS fallback_attempts,sum(accepted) AS accepted,"
                "sum(duplicates) AS duplicates,sum(rejected) AS rejected "
                "FROM attempts WHERE session_id=? GROUP BY source",
                (session_id,),
            )
        ]
        instrument_results = [
            dict(row)
            for row in self.db.execute(
                "SELECT collection_id,instrument_id,source,count(*) AS attempts,"
                "sum(success) AS successful_requests,sum(accepted) AS accepted,"
                "sum(rejected) AS rejected FROM attempts WHERE session_id=? "
                "GROUP BY collection_id,instrument_id,source "
                "ORDER BY collection_id,instrument_id,source LIMIT 101",
                (session_id,),
            )
        ]
        coverage_counts = {"complete": 0, "gaps": 0, "unknown": 0}
        diagnostics: dict[str, int] = {}
        codes: set[int] = set()
        for row in self.db.execute("SELECT detail FROM attempts WHERE session_id=?", (session_id,)):
            detail = json.loads(row[0])
            coverage = detail.get("coverage")
            if isinstance(coverage, dict):
                state = coverage.get("status", "unknown")
                coverage_counts[state] = coverage_counts.get(state, 0) + 1
            elif coverage == "complete":
                coverage_counts["complete"] += 1
            code = detail.get("code", "ok")
            diagnostics[code] = diagnostics.get(code, 0) + 1
            codes.update(detail.get("gateway_error_codes", []))
        report = {
            "session_id": session_id,
            "status": status,
            "started_at": session["started_at"],
            "ended_at": datetime.now(UTC).isoformat(),
            "config_hash": session["config_hash"],
            "scheduled_day": session["scheduled_day"],
            **summary,
            "reconciliation": totals,
            "sources": sources,
            "instrument_results": instrument_results[:100],
            "instrument_results_truncated": len(instrument_results) > 100,
            "calendar_coverage_requests": coverage_counts,
            "diagnostics": diagnostics,
            "gateway_error_codes": sorted(codes),
            "pending_exports": self.pending_count(session_id),
            "historical_availability": "unknown; revisions available when first collected",
        }
        with self.db:
            self.db.execute(
                "UPDATE sessions SET ended_at=?,status=?,report=?,report_pending=1 WHERE id=?",
                (report["ended_at"], status, json_text(report), session_id),
            )
        self.write_reports()
        return report

    def write_reports(self) -> None:
        for row in self.db.execute("SELECT id,report FROM sessions WHERE report_pending=1"):
            atomic_json(
                self.root / "reports" / f"{row['id']}.json", json.loads(row["report"]), True
            )
            with self.db:
                self.db.execute("UPDATE sessions SET report_pending=0 WHERE id=?", (row["id"],))

    def recover(self) -> None:
        self.export_pending()
        for row in self.db.execute("SELECT id FROM sessions WHERE status='running'").fetchall():
            self.finish(row["id"], "interrupted", {"reason": "process_restart"})
        self.write_reports()


def read_status(root: Path, limit: int = 20) -> list[dict[str, Any]]:
    if not 1 <= limit <= 1000:
        raise ValueError("limit must be 1..1000")
    path = root / "journal.sqlite3"
    if not path.exists():
        return []
    with contextlib.closing(sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)) as db:
        return [
            json.loads(row[0])
            for row in db.execute(
                "SELECT report FROM sessions WHERE report IS NOT NULL ORDER BY rowid DESC LIMIT ?",
                (limit,),
            )
        ]


def read_records(
    root: Path,
    *,
    instrument_id: str,
    as_of: datetime,
    limit: int = 100,
    source: str | None = None,
) -> list[dict[str, Any]]:
    if not 1 <= limit <= 1000 or as_of.tzinfo is None:
        raise ValueError("Use limit 1..1000 and a timezone-aware as_of")
    path = root / "journal.sqlite3"
    if not path.exists():
        return []
    where = "instrument_id=? AND available_at<=?"
    args: list[Any] = [instrument_id, as_of.astimezone(UTC).isoformat()]
    if source:
        where += " AND source=?"
        args.append(source)
    query = (
        "SELECT payload FROM (SELECT payload,event_time,source,ROW_NUMBER() OVER "
        "(PARTITION BY observation_key ORDER BY revision DESC) AS rank "
        f"FROM observations WHERE {where}) WHERE rank=1 ORDER BY event_time,source LIMIT ?"
    )
    args.append(limit)
    with contextlib.closing(sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)) as db:
        return [json.loads(row[0]) for row in db.execute(query, args)]

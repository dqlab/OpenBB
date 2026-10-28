"""Single-writer durable journal, immutable acquisition envelopes, atomic day snapshots."""

from __future__ import annotations

import csv
import fcntl
import hashlib
import json
import os
import sqlite3
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .normalize import contract_set_shrink


class LockBusy(RuntimeError):
    pass


class JournalError(RuntimeError):
    """Typed refusal for private state that cannot be interpreted safely."""

    def __init__(self, code: str, detail: dict[str, Any] | None = None):
        self.code = code
        self.detail = detail or {}
        super().__init__(code)


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


def encoded(value: Any) -> str:
    return json.dumps(
        value, sort_keys=True, ensure_ascii=False, allow_nan=False, separators=(",", ":")
    )


def sync_directory(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


SCHEMA = """
CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS runs(
 id TEXT PRIMARY KEY, command TEXT NOT NULL, started_at TEXT NOT NULL,
 finished_at TEXT, status TEXT NOT NULL, summary_json TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS day_status(
 trading_date TEXT PRIMARY KEY, status TEXT NOT NULL, revision_id TEXT,
 provider TEXT, model TEXT, last_run_id TEXT, updated_at TEXT NOT NULL, error TEXT);
CREATE TABLE IF NOT EXISTS attempts(
 id TEXT PRIMARY KEY, run_id TEXT NOT NULL REFERENCES runs(id), trading_date TEXT NOT NULL,
 started_at TEXT NOT NULL, observed_at TEXT, finished_at TEXT, status TEXT NOT NULL,
 error TEXT, raw_path TEXT UNIQUE, warning_count INTEGER NOT NULL DEFAULT 0);
CREATE INDEX IF NOT EXISTS attempts_day ON attempts(trading_date, started_at);
CREATE TABLE IF NOT EXISTS revisions(
 id TEXT PRIMARY KEY, trading_date TEXT NOT NULL, attempt_id TEXT NOT NULL REFERENCES attempts(id),
 content_sha256 TEXT NOT NULL, observed_at TEXT NOT NULL,
 provider TEXT NOT NULL, model TEXT NOT NULL,
 metadata_json TEXT NOT NULL, rows_json TEXT NOT NULL, row_count INTEGER NOT NULL);
CREATE INDEX IF NOT EXISTS revisions_day ON revisions(trading_date, observed_at);
CREATE TABLE IF NOT EXISTS evidence(
 locator TEXT PRIMARY KEY, task_id TEXT NOT NULL, generation TEXT NOT NULL,
 trading_date TEXT NOT NULL, attempt INTEGER NOT NULL, plan_sha256 TEXT NOT NULL,
 entry_id TEXT NOT NULL, verdict TEXT NOT NULL, code TEXT, raw_path TEXT,
 detail_json TEXT NOT NULL, recorded_at TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS evidence_task ON evidence(task_id, generation);
CREATE TABLE IF NOT EXISTS futures_daily(
 trading_date TEXT NOT NULL, contract_symbol TEXT NOT NULL, product TEXT NOT NULL,
 source_variety TEXT, open REAL, high REAL, low REAL, close REAL, settle REAL, pre_settle REAL,
 volume INTEGER NOT NULL, open_interest INTEGER, turnover REAL,
 quality_flags_json TEXT NOT NULL, source_units_json TEXT NOT NULL,
 provider TEXT NOT NULL, model TEXT NOT NULL, observed_at TEXT NOT NULL,
 event_time_precision TEXT NOT NULL DEFAULT 'date', publication_time TEXT,
 metadata_json TEXT NOT NULL, revision_id TEXT NOT NULL REFERENCES revisions(id),
 PRIMARY KEY(trading_date, contract_symbol));
"""


class Journal:
    """The lock covers schema/recovery, raw writes, all requests, and cursor advancement."""

    def __init__(self, root: Path):
        self.root = Path(root)
        self._db: sqlite3.Connection | None = None
        self._lock = None

    @property
    def db(self) -> sqlite3.Connection:
        if self._db is None:
            raise RuntimeError("Journal is not open")
        return self._db

    def __enter__(self) -> Journal:
        self.root.mkdir(parents=True, exist_ok=True)
        self._lock = (self.root / "writer.lock").open("a+b")
        try:
            try:
                fcntl.flock(self._lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise LockBusy("writer_locked") from exc
            self._db = sqlite3.connect(self.root / "cffex.sqlite3", isolation_level=None, timeout=5)
            self.db.row_factory = sqlite3.Row
            # Refuse an unrecognised store before any pragma that writes the
            # file: an unknown schema must not be edited, probed or upgraded.
            version = self.db.execute("PRAGMA user_version").fetchone()[0]
            if version not in (0, 1):
                raise JournalError("journal_schema_mismatch", {"version": version})
            self.db.execute("PRAGMA foreign_keys=ON")
            self.db.execute("PRAGMA journal_mode=WAL")
            self.db.execute("PRAGMA synchronous=FULL")
            self.db.executescript(SCHEMA)
            self.db.execute("PRAGMA user_version=1")
            self.recover()
            return self
        except BaseException:
            self.__exit__(None, None, None)
            raise

    def __exit__(self, *_: object) -> None:
        if self._db is not None:
            self._db.close()
            self._db = None
        if self._lock is not None:
            fcntl.flock(self._lock, fcntl.LOCK_UN)
            self._lock.close()
            self._lock = None

    @contextmanager
    def transaction(self) -> Iterator[None]:
        self.db.execute("BEGIN IMMEDIATE")
        try:
            yield
            self.db.execute("COMMIT")
        except BaseException as exc:
            try:
                # SQLite RAISE(ROLLBACK) has already ended the transaction.
                if self.db.in_transaction:
                    self.db.execute("ROLLBACK")
            except BaseException as rollback_error:
                exc.add_note(f"CFFEX rollback failed: {type(rollback_error).__name__}")
            raise

    def recover(self) -> None:
        now = utc_now()
        with self.transaction():
            if self.db.execute(
                "SELECT 1 FROM attempts WHERE status='running' UNION ALL "
                "SELECT 1 FROM day_status WHERE status='running' LIMIT 1"
            ).fetchone():
                self.invalidate_update_cursor()
            self.db.execute(
                "UPDATE runs SET status='interrupted', finished_at=?, "
                "summary_json=json_set(summary_json, '$.status', 'interrupted') "
                "WHERE status='running'",
                (now,),
            )
            self.db.execute(
                "UPDATE attempts SET status='interrupted', finished_at=?, error='interrupted' "
                "WHERE status='running'",
                (now,),
            )
            self.db.execute(
                "UPDATE day_status SET status='failed', updated_at=?, error='interrupted' "
                "WHERE status='running'",
                (now,),
            )

    def get_meta(self, key: str) -> str | None:
        row = self.db.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return row[0] if row else None

    def set_meta(self, key: str, value: str) -> None:
        self.db.execute(
            "INSERT INTO meta VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )

    def invalidate_update_cursor(self) -> None:
        """A checkpoint cannot assert completeness while a date is being revalidated."""
        self.db.execute("DELETE FROM meta WHERE key='update_cursor'")

    def start_run(self, command: str, summary: dict[str, Any]) -> str:
        run_id = uuid.uuid4().hex
        self.db.execute(
            "INSERT INTO runs VALUES(?,?,?,NULL,'running',?)",
            (run_id, command, utc_now(), encoded(summary)),
        )
        return run_id

    def finish_run(self, run_id: str, summary: dict[str, Any]) -> None:
        self.db.execute(
            "UPDATE runs SET finished_at=?,status=?,summary_json=? WHERE id=?",
            (utc_now(), summary["status"], encoded(summary), run_id),
        )

    def abort_run(self, run_id: str, summary: dict[str, Any], error: str) -> None:
        """Terminalize only this run's unfinished work, preserving good revisions."""
        now = utc_now()
        with self.transaction():
            self.invalidate_update_cursor()
            self.db.execute(
                "UPDATE attempts SET status=?,finished_at=?,error=? "
                "WHERE run_id=? AND status='running'",
                (summary["status"], now, error, run_id),
            )
            self.db.execute(
                "UPDATE day_status SET status='failed',updated_at=?,error=? "
                "WHERE last_run_id=? AND status='running'",
                (now, error, run_id),
            )
            self.finish_run(run_id, summary)

    def complete(self, day: str, provider: str, model: str) -> bool:
        row = self.db.execute(
            "SELECT status,provider,model FROM day_status WHERE trading_date=?",
            (day,),
        ).fetchone()
        return bool(row and tuple(row) == ("complete", provider, model))

    def begin_attempt(self, run_id: str, day: str) -> str:
        attempt_id = uuid.uuid4().hex
        now = utc_now()
        with self.transaction():
            self.invalidate_update_cursor()
            self.db.execute(
                "INSERT INTO attempts(id,run_id,trading_date,started_at,status) "
                "VALUES(?,?,?,?,'running')",
                (attempt_id, run_id, day, now),
            )
            self.db.execute(
                "INSERT INTO day_status(trading_date,status,last_run_id,updated_at) "
                "VALUES(?,'running',?,?) ON CONFLICT(trading_date) DO UPDATE SET "
                "status='running',last_run_id=excluded.last_run_id,updated_at=excluded.updated_at,error=NULL",
                (day, run_id, now),
            )
        return attempt_id

    def archive(self, attempt_id: str, payload: dict[str, Any]) -> str:
        """Publish only completely fsynced files; never overwrite an acquisition."""
        directory = self.root / "raw" / payload["request"]["date"]
        directory.mkdir(parents=True, exist_ok=True)
        sync_directory(directory.parent)
        sync_directory(self.root)
        final = directory / f"{attempt_id}.json"
        temporary = directory / f".{attempt_id}.tmp"
        try:
            with temporary.open("xb") as stream:
                stream.write(encoded(payload).encode("utf-8"))
                stream.flush()
                os.fchmod(stream.fileno(), 0o444)
                os.fsync(stream.fileno())
            os.link(temporary, final)
            sync_directory(directory)
        finally:
            temporary.unlink(missing_ok=True)
        return str(final.relative_to(self.root))

    def attach_raw(self, attempt_id: str, raw_path: str, observed_at: str, warnings: int) -> None:
        self.db.execute(
            "UPDATE attempts SET raw_path=?,observed_at=?,warning_count=? WHERE id=?",
            (raw_path, observed_at, warnings, attempt_id),
        )

    # -- downstream verdicts on captured evidence ---------------------------

    def record_verdict(
        self,
        locator: str,
        *,
        task_id: str,
        generation: str,
        day: str,
        attempt: int,
        plan_sha256: str,
        entry_id: str,
        verdict: str,
        code: str | None,
        raw_path: str | None,
        detail: dict[str, Any] | None = None,
    ) -> None:
        """Record what downstream decided about one immutable capture.

        ``accepted`` is sticky: a later replay of the same capture cannot
        downgrade a day that was already committed from it. A ``rejected``
        verdict is what makes the next run capture a *new* generation instead
        of replaying bytes that were already judged unusable.
        """
        if verdict not in ("accepted", "rejected"):
            raise ValueError("invalid_verdict")
        with self.transaction():
            self.db.execute(
                "INSERT INTO evidence(locator,task_id,generation,trading_date,attempt,"
                "plan_sha256,entry_id,verdict,code,raw_path,detail_json,recorded_at) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?) "
                "ON CONFLICT(locator) DO UPDATE SET verdict=excluded.verdict,"
                "code=excluded.code, raw_path=COALESCE(excluded.raw_path,evidence.raw_path),"
                "detail_json=excluded.detail_json, recorded_at=excluded.recorded_at "
                "WHERE evidence.verdict!='accepted'",
                (
                    locator,
                    task_id,
                    generation,
                    day,
                    attempt,
                    plan_sha256,
                    entry_id,
                    verdict,
                    code,
                    raw_path,
                    encoded(detail or {}),
                    utc_now(),
                ),
            )

    def verdict(self, locator: str) -> str | None:
        row = self.db.execute("SELECT verdict FROM evidence WHERE locator=?", (locator,)).fetchone()
        return row[0] if row else None

    def evidence_rows(self) -> list[dict[str, Any]]:
        return [dict(row) for row in self.db.execute("SELECT * FROM evidence ORDER BY recorded_at")]

    # -- freshness identity for a refresh run -------------------------------

    def evidence_token(self, stamp: str) -> tuple[str, int]:
        """Read-only next freshness token; committed only after a preflight."""
        value = int(self.get_meta("evidence_sequence") or 0) + 1
        return f"{stamp}#{value}", value

    def commit_evidence_token(self, value: int) -> None:
        self.set_meta("evidence_sequence", str(value))

    def fail_attempt(self, attempt_id: str, day: str, error: str) -> None:
        now = utc_now()
        with self.transaction():
            self.invalidate_update_cursor()
            self.db.execute(
                "UPDATE attempts SET status='failed',finished_at=?,error=? WHERE id=?",
                (now, error, attempt_id),
            )
            self.db.execute(
                "UPDATE day_status SET status='failed',updated_at=?,error=? WHERE trading_date=?",
                (now, error, day),
            )

    def validate_contract_set(self, day: str, rows: list[dict[str, Any]]) -> None:
        """Compare only the same date; cross-date listings/expiries may change.

        The predicate itself lives downstream (``normalize.contract_set_shrink``);
        this method is the transactional defence-in-depth call site.
        """
        previous = {
            row[0]
            for row in self.db.execute(
                "SELECT contract_symbol FROM futures_daily WHERE trading_date=?", (day,)
            )
        }
        contract_set_shrink(previous, rows)

    def commit_day(
        self,
        attempt_id: str,
        day: str,
        rows: list[dict[str, Any]],
        metadata: dict[str, Any],
        observed_at: str,
        provider: str,
        model: str,
    ) -> bool:
        payload = encoded(rows)
        digest = hashlib.sha256(
            encoded({"provider": provider, "model": model, "rows": rows}).encode()
        ).hexdigest()
        metadata_json = encoded(metadata)
        with self.transaction():
            # Enforce defensively before any revision/quote/checkpoint mutation.
            self.validate_contract_set(day, rows)
            previous = self.db.execute(
                "SELECT r.id,r.content_sha256 FROM revisions r JOIN day_status d "
                "ON r.id=d.revision_id WHERE d.trading_date=?",
                (day,),
            ).fetchone()
            changed = previous is None or previous[1] != digest
            revision_id = uuid.uuid4().hex if changed else previous[0]
            if changed:
                self.db.execute(
                    "INSERT INTO revisions VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (
                        revision_id,
                        day,
                        attempt_id,
                        digest,
                        observed_at,
                        provider,
                        model,
                        metadata_json,
                        payload,
                        len(rows),
                    ),
                )
                for row in rows:
                    values = [
                        row[key]
                        for key in (
                            "trading_date",
                            "contract_symbol",
                            "product",
                            "source_variety",
                            "open",
                            "high",
                            "low",
                            "close",
                            "settle",
                            "pre_settle",
                            "volume",
                            "open_interest",
                            "turnover",
                        )
                    ]
                    values += [
                        encoded(row["quality_flags"]),
                        encoded(row["source_units"]),
                        provider,
                        model,
                        observed_at,
                        "date",
                        None,
                        metadata_json,
                        revision_id,
                    ]
                    columns = [
                        item[1] for item in self.db.execute("PRAGMA table_info(futures_daily)")
                    ]
                    assignments = ",".join(f"{key}=excluded.{key}" for key in columns[2:])
                    self.db.execute(
                        f"INSERT INTO futures_daily VALUES({','.join('?' for _ in values)}) "
                        f"ON CONFLICT(trading_date,contract_symbol) DO UPDATE SET {assignments}",
                        values,
                    )
            now = utc_now()
            self.db.execute(
                "UPDATE attempts SET status='complete',finished_at=? WHERE id=?", (now, attempt_id)
            )
            self.db.execute(
                "UPDATE day_status SET status='complete',revision_id=?,provider=?,model=?,"
                "updated_at=?,error=NULL WHERE trading_date=?",
                (revision_id, provider, model, now, day),
            )
        return changed


def read_connection(root: Path) -> sqlite3.Connection | None:
    path = Path(root) / "cffex.sqlite3"
    if not path.exists():
        return None
    db = sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)
    db.row_factory = sqlite3.Row
    return db


def status(root: Path, limit: int = 100) -> dict[str, Any]:
    db = read_connection(root)
    if db is None:
        return {"status": "not_initialized", "row_count": 0, "days": [], "runs": []}
    try:
        with db:
            db.execute("BEGIN")
            result = {
                "status": "ok",
                "row_count": db.execute("SELECT count(*) FROM futures_daily").fetchone()[0],
                "day_counts": dict(
                    db.execute("SELECT status,count(*) FROM day_status GROUP BY status")
                ),
                "update_cursor": None,
                "days": [
                    dict(row)
                    for row in db.execute(
                        "SELECT * FROM day_status ORDER BY trading_date DESC LIMIT ?", (limit,)
                    )
                ],
                "runs": [
                    dict(row)
                    for row in db.execute(
                        "SELECT * FROM runs ORDER BY started_at DESC LIMIT ?", (limit,)
                    )
                ],
            }
            cursor = db.execute("SELECT value FROM meta WHERE key='update_cursor'").fetchone()
            result["update_cursor"] = cursor[0] if cursor else None
            return result
    finally:
        db.close()


def protected_target(root: Path, target: Path, evidence_root: Path | None = None) -> bool:
    """Protect the WAL, lock inode, immutable envelopes and capture store."""
    root, target = Path(root).resolve(), Path(target).resolve()
    evidence = Path(evidence_root).resolve() if evidence_root else root / "evidence"
    return (
        target == root
        or target == root / "raw"
        or root / "raw" in target.parents
        or target == evidence
        or evidence in target.parents
        or (
            target.parent == root
            and (target.name == "writer.lock" or target.name.startswith("cffex.sqlite3"))
        )
    )


def export_csv(
    root: Path,
    output: Path,
    start: str | None = None,
    end: str | None = None,
    evidence_root: Path | None = None,
) -> int:
    """Read a consistent snapshot; atomically replace only a complete CSV export."""
    if start and end and end < start:
        raise ValueError("end_date precedes start_date")
    db = read_connection(root)
    if db is None:
        raise ValueError("Journal is not initialized")
    output = Path(output).resolve()
    if protected_target(root, output, evidence_root):
        # Avoid overwriting any lock, immutable raw envelope, or SQLite sidecar.
        db.close()
        raise ValueError("Export output targets protected journal storage")
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_name(f".{output.name}.{uuid.uuid4().hex}.tmp")
    count = 0
    try:
        db.execute("BEGIN")
        cursor = db.execute(
            "SELECT * FROM futures_daily WHERE (? IS NULL OR trading_date>=?) "
            "AND (? IS NULL OR trading_date<=?) ORDER BY trading_date,contract_symbol",
            (start, start, end, end),
        )
        with temporary.open("x", newline="", encoding="utf-8") as stream:
            writer = csv.writer(stream)
            writer.writerow([column[0] for column in cursor.description])
            for row in cursor:
                writer.writerow(tuple(row))
                count += 1
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, output)
        sync_directory(output.parent)
        return count
    finally:
        db.close()
        temporary.unlink(missing_ok=True)

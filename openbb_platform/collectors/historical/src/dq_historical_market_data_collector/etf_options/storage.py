# Evidence schemas require exact built-in types, including rejection of bool-as-int.
# pylint: disable=unidiomatic-typecheck

"""Local-filesystem SQLite WAL journal with a process-wide flock writer lease.

All raw attempts are immutable and fsynced. Current snapshots, revisions and run
completion commit together; failed refreshes never delete durable good data.

The journal is recovery/control-plane state.  It is **not** the evidence: literal
transport bytes live in the private ``raw-acquisition-v1`` store, and a capture
row carries the locator of that evidence plus the unit authority it was committed
under.  A capture whose units nobody stated cannot commit.
"""

from __future__ import annotations

import csv
import fcntl
import hashlib
import json
import os
import sqlite3
import uuid
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import BinaryIO

from .quality import DataError, snapshot_shrink


def encoded(value: object) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
    )


def digest(value: object) -> str:
    return hashlib.sha256(encoded(value).encode()).hexdigest()


def semantic_quote(row: dict) -> dict:
    """Project a normalized row without its known acquisition-only fields.

    These are explicit root paths, not a recursive timestamp-name filter:
    source_time, source_event_time and all raw source fields remain semantic.
    The projection is used only for hashing; stored audit rows remain intact.
    """
    return {
        key: value
        for key, value in row.items()
        if key not in {"fetched_at", "observed_at"}
    }


def semantic_metadata(metadata: list[dict]) -> list[dict]:
    """Contract terms/policy/discovery identity, independent of live observations.

    The Sina master embeds an ancillary full quote at source_row.quote. Its
    effective strike/expiry/adjustment are already normalized at the root; its
    prices, raw payload and event/acquisition times are NOT contract-universe
    terms. Exclude that exact provider path, retaining the discovery evidence
    and official raw adjustment fields. Actual captured quotes are hashed
    separately for revisions. Never modify metadata persisted for provenance.
    """
    result = []
    for row in metadata:
        projected = semantic_quote(row)
        if row.get("metadata_origin") == "sina_sse_discovery_and_quotes":
            source_row = row.get("source_row")
            if isinstance(source_row, dict):
                projected["source_row"] = {
                    key: value for key, value in source_row.items() if key != "quote"
                }
        result.append(projected)
    return sorted(result, key=lambda row: row["symbol"])


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


def sync_directory(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


class LockBusy(RuntimeError):
    pass


SCHEMA = """
CREATE TABLE IF NOT EXISTS runs(
 id TEXT PRIMARY KEY, started_at TEXT NOT NULL, finished_at TEXT, status TEXT NOT NULL,
 summary_json TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS captures(
 id TEXT PRIMARY KEY, run_id TEXT NOT NULL REFERENCES runs(id), trading_date TEXT NOT NULL,
 exchange TEXT NOT NULL, config_hash TEXT NOT NULL, universe_hash TEXT NOT NULL,
 observed_at TEXT NOT NULL, status TEXT NOT NULL, response_complete INTEGER NOT NULL DEFAULT 0,
 pricing_eligible_count INTEGER NOT NULL DEFAULT 0, row_count INTEGER NOT NULL DEFAULT 0,
 contracts_json TEXT NOT NULL, quotes_json TEXT, revision_id TEXT, error TEXT,
 unit_source TEXT NOT NULL DEFAULT 'absent', evidence_locator TEXT);
CREATE INDEX IF NOT EXISTS capture_identity
 ON captures(trading_date,exchange,config_hash,universe_hash);
CREATE TABLE IF NOT EXISTS attempts(
 id TEXT PRIMARY KEY, run_id TEXT NOT NULL REFERENCES runs(id),
 capture_id TEXT REFERENCES captures(id),
 started_at TEXT NOT NULL, observed_at TEXT, model TEXT NOT NULL, request_json TEXT NOT NULL,
 status TEXT NOT NULL, error TEXT, raw_path TEXT UNIQUE, warning_count INTEGER NOT NULL DEFAULT 0,
 stage TEXT, evidence_locator TEXT, evidence_sha256 TEXT, http_status INTEGER);
CREATE TABLE IF NOT EXISTS checkpoints(
 trading_date TEXT NOT NULL, exchange TEXT NOT NULL, config_hash TEXT NOT NULL,
 universe_hash TEXT NOT NULL, symbol TEXT NOT NULL, quote_json TEXT NOT NULL,
 observed_at TEXT NOT NULL, attempt_id TEXT NOT NULL REFERENCES attempts(id),
 PRIMARY KEY(trading_date,exchange,config_hash,universe_hash,symbol));
CREATE TABLE IF NOT EXISTS contract_metadata(
 capture_id TEXT NOT NULL REFERENCES captures(id), symbol TEXT NOT NULL,
 metadata_json TEXT NOT NULL,
 PRIMARY KEY(capture_id,symbol));
CREATE TABLE IF NOT EXISTS revisions(
 id TEXT PRIMARY KEY, trading_date TEXT NOT NULL, exchange TEXT NOT NULL,
 capture_id TEXT NOT NULL REFERENCES captures(id), content_sha256 TEXT NOT NULL,
 observed_at TEXT NOT NULL, contracts_json TEXT NOT NULL, quotes_json TEXT NOT NULL,
 UNIQUE(trading_date,exchange,content_sha256));
CREATE TABLE IF NOT EXISTS current_snapshots(
 trading_date TEXT NOT NULL, exchange TEXT NOT NULL, config_hash TEXT NOT NULL,
 universe_hash TEXT NOT NULL, revision_id TEXT NOT NULL REFERENCES revisions(id),
 capture_id TEXT NOT NULL REFERENCES captures(id), row_count INTEGER NOT NULL,
 pricing_eligible_count INTEGER NOT NULL, PRIMARY KEY(trading_date,exchange));
CREATE TABLE IF NOT EXISTS current_quotes(
 trading_date TEXT NOT NULL, exchange TEXT NOT NULL, symbol TEXT NOT NULL,
 revision_id TEXT NOT NULL REFERENCES revisions(id), contract_json TEXT NOT NULL,
 quote_json TEXT NOT NULL, pricing_eligible INTEGER NOT NULL,
 PRIMARY KEY(trading_date,exchange,symbol));
CREATE TABLE IF NOT EXISTS evidence_verdicts(
 locator TEXT PRIMARY KEY, verdict TEXT NOT NULL, code TEXT, recorded_at TEXT NOT NULL);
"""

#: Columns added after ``user_version`` 1.  Applied in place so an existing store
#: keeps every row; nothing is dropped and no default invents a unit label.
MIGRATIONS = {
    1: (
        "ALTER TABLE captures ADD COLUMN unit_source TEXT NOT NULL DEFAULT 'absent'",
        "ALTER TABLE captures ADD COLUMN evidence_locator TEXT",
        "ALTER TABLE attempts ADD COLUMN stage TEXT",
        "ALTER TABLE attempts ADD COLUMN evidence_locator TEXT",
        "ALTER TABLE attempts ADD COLUMN evidence_sha256 TEXT",
        "ALTER TABLE attempts ADD COLUMN http_status INTEGER",
    ),
}


class Journal:
    def __init__(self, root: Path):
        self.root = Path(root).resolve()
        self._db: sqlite3.Connection | None = None
        self._lock: BinaryIO | None = None

    @property
    def db(self) -> sqlite3.Connection:
        if self._db is None:
            raise RuntimeError("journal_not_open")
        return self._db

    def __enter__(self) -> Journal:
        self.root.mkdir(parents=True, exist_ok=True)
        self._lock = (self.root / "writer.lock").open("a+b")
        try:
            try:
                fcntl.flock(self._lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise LockBusy("writer_locked") from exc
            self._db = sqlite3.connect(
                self.root / "etf_options.sqlite3", isolation_level=None, timeout=5
            )
            self.db.row_factory = sqlite3.Row
            self.db.execute("PRAGMA foreign_keys=ON")
            if self.db.execute("PRAGMA journal_mode=WAL").fetchone()[0] != "wal":
                raise RuntimeError("local_wal_required")
            self.db.execute("PRAGMA synchronous=FULL")
            version = self.db.execute("PRAGMA user_version").fetchone()[0]
            if version not in (0, 1, 2):
                raise ValueError("unsupported_journal_schema")
            if version == 1:
                # Upgrade in place: additive columns only, every row preserved.
                with self.transaction():
                    for statement in MIGRATIONS[1]:
                        self.db.execute(statement)
            self.db.executescript(SCHEMA)
            self.db.execute("PRAGMA user_version=2")
            with self.transaction():
                self.db.execute(
                    "UPDATE runs SET status='interrupted',finished_at=?,"
                    "summary_json=json_set(summary_json,'$.status','interrupted') "
                    "WHERE status='running'",
                    (utc_now(),),
                )
                self.db.execute(
                    "UPDATE captures SET status='interrupted',error='interrupted' "
                    "WHERE status IN ('running','ready')"
                )
                self.db.execute(
                    "UPDATE attempts SET status='interrupted',error='interrupted' "
                    "WHERE status='running'"
                )
            return self
        except BaseException as exc:
            self.__exit__(type(exc), exc, exc.__traceback__)
            raise

    def __exit__(
        self, _type: object, original: BaseException | None, _traceback: object
    ) -> None:
        cleanup_errors = []
        try:
            if self._db is not None:
                self._db.close()
        except BaseException as cleanup:
            cleanup_errors.append(cleanup)
        finally:
            self._db = None
            if self._lock is not None:
                try:
                    fcntl.flock(self._lock, fcntl.LOCK_UN)
                except BaseException as cleanup:
                    cleanup_errors.append(cleanup)
                try:
                    self._lock.close()
                except BaseException as cleanup:
                    cleanup_errors.append(cleanup)
                self._lock = None
        if cleanup_errors:
            if original is not None:
                for cleanup_error in cleanup_errors:
                    original.add_note(
                        f"ETF journal cleanup failed: {type(cleanup_error).__name__}"
                    )
            else:
                raise cleanup_errors[0]

    @contextmanager
    def transaction(self):
        self.db.execute("BEGIN IMMEDIATE")
        try:
            yield
            self.db.execute("COMMIT")
        except BaseException as exc:
            try:
                if self.db.in_transaction:
                    self.db.execute("ROLLBACK")
            except BaseException as secondary:
                exc.add_note(f"ETF rollback failed: {type(secondary).__name__}")
            raise

    def start_run(self, summary: dict) -> str:
        run_id = uuid.uuid4().hex
        self.db.execute(
            "INSERT INTO runs VALUES(?,?,NULL,'running',?)",
            (run_id, utc_now(), encoded(summary)),
        )
        return run_id

    def finish_run(self, run_id: str, summary: dict) -> None:
        self.db.execute(
            "UPDATE runs SET finished_at=?,status=?,summary_json=? WHERE id=?",
            (utc_now(), summary["status"], encoded(summary), run_id),
        )

    def abort_run(self, run_id: str, summary: dict) -> None:
        with self.transaction():
            self.db.execute(
                "UPDATE captures SET status=?,error=? WHERE run_id=? "
                "AND status IN ('running','ready')",
                (summary["status"], summary["status"], run_id),
            )
            self.db.execute(
                "UPDATE attempts SET status=?,error=? WHERE run_id=? AND status='running'",
                (summary["status"], summary["status"], run_id),
            )
            self.finish_run(run_id, summary)

    def start_capture(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        self,
        run_id: str,
        day: str,
        exchange: str,
        config_hash: str,
        metadata: list[dict],
        observed_at: str,
        unit_source: str = "absent",
        evidence_locator: str | None = None,
    ) -> tuple[str, str]:
        capture_id, universe_hash = uuid.uuid4().hex, digest(
            semantic_metadata(metadata)
        )
        with self.transaction():
            self.db.execute(
                "INSERT INTO captures(id,run_id,trading_date,exchange,config_hash,"
                "universe_hash,observed_at,status,contracts_json,unit_source,evidence_locator) "
                "VALUES(?,?,?,?,?,?,?,'running',?,?,?)",
                (
                    capture_id,
                    run_id,
                    day,
                    exchange,
                    config_hash,
                    universe_hash,
                    observed_at,
                    encoded(metadata),
                    unit_source,
                    evidence_locator,
                ),
            )
            self.db.executemany(
                "INSERT INTO contract_metadata VALUES(?,?,?)",
                [(capture_id, row["symbol"], encoded(row)) for row in metadata],
            )
        return capture_id, universe_hash

    def validate_universe(
        self,
        day: str,
        exchange: str,
        metadata: list[dict],
        approve_change: bool = False,
    ) -> None:
        """Snapshot-shrink protection, delegated to its single owner.

        The predicate moved to :func:`..quality.snapshot_shrink` when judgement
        left acquisition; the code and the override semantics are unchanged.
        """
        old = {
            row[0]
            for row in self.db.execute(
                "SELECT symbol FROM current_quotes WHERE trading_date=? AND exchange=?",
                (day, exchange),
            )
        }
        snapshot_shrink(old, {row["symbol"] for row in metadata}, approve_change)

    def begin_attempt(  # pylint: disable=too-many-positional-arguments
        self,
        run_id: str,
        capture_id: str | None,
        model: str,
        request: dict,
        stage: str | None = None,
        evidence_locator: str | None = None,
        evidence_sha256: str | None = None,
    ) -> str:
        attempt = uuid.uuid4().hex
        self.db.execute(
            "INSERT INTO attempts(id,run_id,capture_id,started_at,model,request_json,status,"
            "stage,evidence_locator,evidence_sha256) VALUES(?,?,?,?,?,?,'running',?,?,?)",
            (
                attempt,
                run_id,
                capture_id,
                utc_now(),
                model,
                encoded(request),
                stage,
                evidence_locator,
                evidence_sha256,
            ),
        )
        return attempt

    def archive(self, attempt: str, envelope: dict, day: str | None = None) -> str:
        """Immutable, fsynced attempt document.

        ``day`` places it at ``raw/<requested-date>/<attempt>.json`` -- the legacy
        lane's raw archive location, one directory deeper so a delivery and a
        provider envelope can never overwrite each other.  Without ``day`` the
        location is the pre-existing ``raw/<attempt>.json``.
        """
        directory = self.root / "raw" / day if day else self.root / "raw"
        directory.mkdir(parents=True, exist_ok=True)
        sync_directory(self.root)
        if day:
            sync_directory(directory.parent)
        final, temporary = directory / f"{attempt}.json", directory / f".{attempt}.tmp"
        # Nonfinite offline values are preserved as JSON NaN/Infinity tokens in
        # raw only; the core worker canonicalizes production nonfinites to strings.
        payload = json.dumps(
            envelope, sort_keys=True, ensure_ascii=False, allow_nan=True
        )
        try:
            with temporary.open("xb") as stream:
                stream.write(payload.encode())
                stream.flush()
                os.fchmod(stream.fileno(), 0o444)
                os.fsync(stream.fileno())
            os.link(temporary, final)
            sync_directory(directory)
        finally:
            temporary.unlink(missing_ok=True)
        relative = str(final.relative_to(self.root))
        self.db.execute(
            "UPDATE attempts SET raw_path=?,observed_at=?,warning_count=? WHERE id=?",
            (
                relative,
                envelope["observed_at"],
                envelope.get("warning_count", 0),
                attempt,
            ),
        )
        # The transport status is a run observation, not part of the archived
        # document: only an envelope that actually carries one may write it, so
        # archiving a delivery never erases what ``attach_evidence`` recorded.
        if "http_status" in envelope:
            self.db.execute(
                "UPDATE attempts SET http_status=? WHERE id=?",
                (envelope["http_status"], attempt),
            )
        return relative

    def finish_attempt(self, attempt: str, error: str | None = None) -> None:
        self.db.execute(
            "UPDATE attempts SET status=?,error=? WHERE id=?",
            ("failed" if error else "complete", error, attempt),
        )

    # -- evidence binding ---------------------------------------------------

    def attach_evidence(
        self, attempt: str, locator: str | None, http_status: int | None
    ) -> None:
        self.db.execute(
            "UPDATE attempts SET evidence_locator=?,http_status=? WHERE id=?",
            (locator, http_status, attempt),
        )

    def evidence_of(self, attempt: str) -> str | None:
        row = self.db.execute(
            "SELECT evidence_locator FROM attempts WHERE id=?", (attempt,)
        ).fetchone()
        return row[0] if row else None

    def read_attempt_document(self, attempt: str) -> dict:
        row = self.db.execute(
            "SELECT raw_path FROM attempts WHERE id=?", (attempt,)
        ).fetchone()
        if row is None or not row[0]:
            raise DataError("missing_attempt_document")
        return json.loads((self.root / row[0]).read_text())

    def verdict(self, locator: str) -> str | None:
        """The downstream verdict for one capture locator, if any.

        This is what makes "a rejected payload is never replayed" and "a captured
        payload is never refetched" both true after a restart.
        """
        row = self.db.execute(
            "SELECT verdict FROM evidence_verdicts WHERE locator=?", (locator,)
        ).fetchone()
        return row[0] if row else None

    def record_verdict(
        self, locator: str, verdict: str, code: str | None = None
    ) -> None:
        if verdict not in {"accepted", "rejected"}:
            raise ValueError("invalid_evidence_verdict")
        self.db.execute(
            "INSERT OR REPLACE INTO evidence_verdicts VALUES(?,?,?,?)",
            (locator, verdict, code, utc_now()),
        )

    def checkpoint(  # pylint: disable=too-many-positional-arguments
        self,
        day: str,
        exchange: str,
        config_hash: str,
        universe_hash: str,
        rows: list[dict],
        observed_at: str,
        attempt: str,
    ) -> None:
        with self.transaction():
            self.db.executemany(
                "INSERT OR REPLACE INTO checkpoints VALUES(?,?,?,?,?,?,?,?)",
                [
                    (
                        day,
                        exchange,
                        config_hash,
                        universe_hash,
                        row["symbol"],
                        encoded(row),
                        observed_at,
                        attempt,
                    )
                    for row in rows
                ],
            )

    def resume(
        self, day: str, exchange: str, config_hash: str, universe_hash: str
    ) -> list[dict]:
        return [
            json.loads(row[0])
            for row in self.db.execute(
                "SELECT quote_json FROM checkpoints WHERE trading_date=? AND exchange=? "
                "AND config_hash=? AND universe_hash=? ORDER BY symbol",
                (day, exchange, config_hash, universe_hash),
            )
        ]

    def ready(self, capture_id: str, rows: list[dict], observed_at: str) -> None:
        self.db.execute(
            "UPDATE captures SET status='ready',quotes_json=?,row_count=?,"
            "pricing_eligible_count=?,observed_at=? WHERE id=?",
            (
                encoded(rows),
                len(rows),
                sum(row["pricing_eligible"] for row in rows),
                observed_at,
                capture_id,
            ),
        )

    def fail_capture(self, capture_id: str, error: str) -> None:
        self.db.execute(
            "UPDATE captures SET status='failed',error=? WHERE id=?",
            (error, capture_id),
        )

    def commit_capture(self, capture_id: str, approve_change: bool = False) -> bool:
        """Called only inside the run's cancellation-guarded transaction.

        The commit is the one place where a capture becomes an accepted snapshot,
        so it is also the one place where the unit authority must be real.  A
        capture whose numbers nobody stated the units of is refused typed
        (``units_undetermined``); it is never relabelled ``provider_defined``.
        """
        capture = self.db.execute(
            "SELECT * FROM captures WHERE id=?", (capture_id,)
        ).fetchone()
        if capture["unit_source"] not in {"source", "configuration"}:
            raise DataError("units_undetermined")
        metadata, rows = json.loads(capture["contracts_json"]), json.loads(
            capture["quotes_json"]
        )
        if not rows or {row["symbol"] for row in rows} != {
            row["symbol"] for row in metadata
        }:
            raise DataError("quote_universe_mismatch")
        day, exchange = capture["trading_date"], capture["exchange"]
        self.validate_universe(day, exchange, metadata, approve_change)
        content_hash = digest(
            {
                "contracts": semantic_metadata(metadata),
                "quotes": [
                    semantic_quote(row)
                    for row in sorted(rows, key=lambda row: row["symbol"])
                ],
            }
        )
        old = self.db.execute(
            "SELECT revision_id FROM current_snapshots WHERE trading_date=? AND exchange=?",
            (day, exchange),
        ).fetchone()
        revision = self.db.execute(
            "SELECT id FROM revisions WHERE trading_date=? AND exchange=? AND content_sha256=?",
            (day, exchange, content_hash),
        ).fetchone()
        revision_id = revision[0] if revision else uuid.uuid4().hex
        changed = old is None or old[0] != revision_id
        if revision is None:
            self.db.execute(
                "INSERT INTO revisions VALUES(?,?,?,?,?,?,?,?)",
                (
                    revision_id,
                    day,
                    exchange,
                    capture_id,
                    content_hash,
                    capture["observed_at"],
                    capture["contracts_json"],
                    capture["quotes_json"],
                ),
            )
        if changed:
            self.db.execute(
                "DELETE FROM current_quotes WHERE trading_date=? AND exchange=?",
                (day, exchange),
            )
            by_symbol = {row["symbol"]: row for row in metadata}
            self.db.executemany(
                "INSERT INTO current_quotes VALUES(?,?,?,?,?,?,?)",
                [
                    (
                        day,
                        exchange,
                        row["symbol"],
                        revision_id,
                        encoded(by_symbol[row["symbol"]]),
                        encoded(row),
                        int(row["pricing_eligible"]),
                    )
                    for row in rows
                ],
            )
        self.db.execute(
            "INSERT OR REPLACE INTO current_snapshots VALUES(?,?,?,?,?,?,?,?)",
            (
                day,
                exchange,
                capture["config_hash"],
                capture["universe_hash"],
                revision_id,
                capture_id,
                len(rows),
                capture["pricing_eligible_count"],
            ),
        )
        self.db.execute(
            "UPDATE captures SET status='complete',response_complete=1,revision_id=? WHERE id=?",
            (revision_id, capture_id),
        )
        return changed


def protected_target(root: Path, target: Path) -> bool:
    root, target = Path(root).resolve(), Path(target).resolve()
    return target == root or (
        root in target.parents
        and (
            target == root / "raw"
            or root / "raw" in target.parents
            or target == root / "evidence"
            or root / "evidence" in target.parents
            or target.name == "writer.lock"
            or target.name.startswith("etf_options.sqlite3")
        )
    )


def read_connection(root: Path):
    path = Path(root) / "etf_options.sqlite3"
    if not path.exists():
        return None
    db = sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)
    db.row_factory = sqlite3.Row
    return db


def status(root: Path, limit: int = 100) -> dict:
    if type(limit) is not int or not 1 <= limit <= 1000:
        raise ValueError("invalid_status_limit")
    db = read_connection(root)
    if db is None:
        return {
            "status": "not_initialized",
            "row_count": 0,
            "runs": [],
            "snapshots": [],
        }
    try:
        db.execute("BEGIN")
        return {
            "status": "ok",
            "row_count": db.execute("SELECT count(*) FROM current_quotes").fetchone()[
                0
            ],
            "runs": [
                dict(row)
                for row in db.execute(
                    "SELECT * FROM runs ORDER BY started_at DESC LIMIT ?", (limit,)
                )
            ],
            "snapshots": [
                dict(row)
                for row in db.execute(
                    "SELECT * FROM current_snapshots ORDER BY trading_date DESC,exchange LIMIT ?",
                    (limit,),
                )
            ],
            "capture_counts": dict(
                db.execute("SELECT status,count(*) FROM captures GROUP BY status")
            ),
        }
    finally:
        db.close()


def export_csv(root: Path, output: Path, day: str | None = None) -> int:
    output = Path(output).resolve()
    if protected_target(root, output):
        raise ValueError("protected_export_path")
    db = read_connection(root)
    if db is None:
        raise ValueError("journal_not_initialized")
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_name(f".{output.name}.{uuid.uuid4().hex}.tmp")
    try:
        db.execute("BEGIN")
        cursor = db.execute(
            "SELECT * FROM current_quotes WHERE (? IS NULL OR trading_date=?) "
            "ORDER BY trading_date,exchange,symbol",
            (day, day),
        )
        count = 0
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

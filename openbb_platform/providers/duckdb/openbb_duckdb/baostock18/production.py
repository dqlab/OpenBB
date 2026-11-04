"""Fail-closed production admission, with read-only evidence before every writer.

The preparatory CHECKPOINT is an operator window action, never part of preflight.
A changed catalog needs a fresh fingerprint, checkpoint certificate and backup,
including after partial imports and before visibility changes.
"""

# Identifiers below are constants or independently validated inventory identifiers.
# ruff: noqa: S608

import fcntl
import hashlib
import json
import os
import re
import shutil
import stat
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import duckdb
from openbb_duckdb.baostock18.contract import SCHEMA, TRANSFORM, AdmissionError
from openbb_duckdb.baostock18.manifest import encoded, sha256
from openbb_duckdb.baostock18.store import _check_owned, migrate, utcnow

CATALOG_NAME = "dq_quant_invest_data.duckdb"
A_SHARE_TABLES = (
    "attempt", "bar_revision", "bar_snapshot", "calendar_day", "factor_revision",
    "instrument_version", "job", "reference_snapshot",
)
FINGERPRINT_KEYS = {"size", "mtime_ns", "device", "inode", "header_sha256"}
READ_CONFIG = {"enable_external_access": False, "threads": 2, "memory_limit": "2GB"}


def load_json(path):
    """Load mandatory evidence without interpreting missing evidence as defaults."""
    try:
        return json.loads(Path(path).read_text())
    except (OSError, ValueError, TypeError) as exc:
        raise AdmissionError(f"Missing or invalid evidence: {path}") from exc


def write_json(path, value):
    """Create evidence exclusively; never overwrite earlier window receipts."""
    with Path(path).open("x") as stream:
        json.dump(value, stream, ensure_ascii=False, indent=2, default=str)
        stream.flush()
        os.fsync(stream.fileno())


def production_path(target):
    """Require one existing canonical, unaliased production-named DB file."""
    if target is None or not Path(target).is_absolute():
        raise AdmissionError("Explicit --production-target absolute path required")
    path = Path(target)
    if path.name != CATALOG_NAME:
        raise AdmissionError(f"Production DB name must be {CATALOG_NAME}")
    try:
        info = path.lstat()
        if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
                or path.resolve(strict=True) != path):
            raise AdmissionError("Production target must be a single DB file without aliases")
    except OSError as exc:
        raise AdmissionError("Production target must be an existing single DB file") from exc
    if path.with_name(path.name + ".access-windows").exists():
        raise AdmissionError("Access-window policy present; no production bypass")
    return path


def fingerprint(path):
    """Read exactly the first 4096 bytes through a non-symlink descriptor."""
    path = production_path(path)
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(fd, "rb") as stream:
        before = os.fstat(stream.fileno())
        header = stream.read(4096)
        after = os.fstat(stream.fileno())
    if before != after or after != path.stat():
        # atime can change on read, and is irrelevant to catalog identity.
        fields = ("st_size", "st_mtime_ns", "st_dev", "st_ino", "st_nlink")
        if any(getattr(before, key) != getattr(after, key) or
               getattr(after, key) != getattr(path.stat(), key) for key in fields):
            raise AdmissionError("Catalog changed while fingerprinting")
    return dict(size=after.st_size, mtime_ns=after.st_mtime_ns, device=after.st_dev,
                inode=after.st_ino, header_sha256=hashlib.sha256(header).hexdigest())


def timestamp(value):
    """Require an actual timezone-aware window time."""
    try:
        parsed = datetime.fromisoformat(value)
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ValueError("missing timezone")
        return parsed
    except (TypeError, ValueError) as exc:
        raise AdmissionError("Window/evidence time requires explicit timezone") from exc


@contextmanager
def market_lock(path):
    """Hold the existing platform mutex O_RDONLY, nonblocking, through close."""
    lock = path.with_suffix(".market.lock")
    try:
        fd = os.open(lock, os.O_RDONLY | os.O_NOFOLLOW)
    except OSError as exc:
        raise AdmissionError("Existing market.lock required") from exc
    with os.fdopen(fd, "rb") as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
            raise AdmissionError("market.lock must be a single regular file")
        try:
            fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise AdmissionError("market.lock busy; no other writer may hold it") from exc
        try:
            yield
            if (lock.stat().st_dev, lock.stat().st_ino) != (info.st_dev, info.st_ino):
                raise AdmissionError("market.lock identity changed")
        finally:
            fcntl.flock(stream.fileno(), fcntl.LOCK_UN)


def no_wal(path):
    """Do not replay, remove, truncate or checkpoint any WAL in preflight."""
    if os.path.lexists(path.with_name(path.name + ".wal")):
        raise AdmissionError("Catalog has WAL; CHECKPOINT must complete before admission")


def capture_fingerprint(target, window_started_at):
    """Acquire a fresh on-site fingerprint after the declared window began."""
    path = production_path(target)
    if timestamp(window_started_at) > utcnow():
        raise AdmissionError("Window has not started")
    with market_lock(path):
        no_wal(path)
        result = fingerprint(path)
    return {"format": "baostock18-production-fingerprint-v1", "target": str(path),
            "window_started_at": window_started_at, "observed_at": utcnow().isoformat(),
            "fingerprint": result}


def inventory(connection):
    """Return physical table inventory; views cannot substitute for baseline tables."""
    return sorted([s, t] for s, t in connection.execute(
        "SELECT schema_name,table_name FROM duckdb_tables() WHERE NOT internal"
    ).fetchall() if s != SCHEMA)


def check_basis(connection, owned):
    """Permit narrow staged bases; only unadjusted may have admission/events/pointers."""
    if not owned:
        return
    for table in ("basis_admission", "visibility_event", "dataset_pointer"):
        if connection.execute(
            f"SELECT count(*) FROM {SCHEMA}.{table} WHERE adjustment_basis IS NULL OR adjustment_basis<>?",
            ["unadjusted"],
        ).fetchone()[0]:
            raise AdmissionError("Catalog contains unauthorized basis")
    if connection.execute(
        f"SELECT count(*) FROM {SCHEMA}.bar_version WHERE adjustment_basis NOT IN (?,?) "
        "OR fixed_qfq_basis_date IS NOT NULL", ["unadjusted", "forward_dynamic_snapshot"],
    ).fetchone()[0]:
        raise AdmissionError("Catalog contains unauthorized basis/fixed forward adjustment")


@dataclass(frozen=True)
class ProductionAdmission:
    """Explicit caller approvals and fresh evidence; there are no production defaults."""

    target: str
    expected_catalog_fingerprint: str
    window_started_at: str
    baseline: str
    approved_manifest_hash: str
    approved_batch_size: int
    backup_receipt: str | None = None

    def check_target(self, target):
        """Check the raw caller path before any resolve could hide relative input."""
        if production_path(target) != production_path(self.target):
            raise AdmissionError("Production admission does not authorize this target")

    def approvals(self):
        """Validate explicit approval fields before opening even a read-only catalog."""
        path = production_path(self.target)
        if (not isinstance(self.approved_manifest_hash, str)
                or not re.fullmatch(r"[0-9a-f]{64}", self.approved_manifest_hash)
                or type(self.approved_batch_size) is not int
                or not 50_000 <= self.approved_batch_size <= 100_000):
            raise AdmissionError("Approved manifest hash and batch size (50000..100000) required")
        observed = load_json(self.expected_catalog_fingerprint)
        if (observed.get("format") != "baostock18-production-fingerprint-v1"
                or observed.get("target") != str(path)
                or observed.get("window_started_at") != self.window_started_at):
            raise AdmissionError("Fingerprint is not bound to this target/window")
        start, seen = timestamp(self.window_started_at), timestamp(observed.get("observed_at"))
        if not start <= seen <= utcnow():
            raise AdmissionError("Fingerprint must be obtained after window start")
        expected = observed.get("fingerprint", {})
        if set(expected) != FINGERPRINT_KEYS or expected != fingerprint(path):
            raise AdmissionError("Catalog fingerprint mismatch (size/mtime_ns/device/inode/header_sha256)")
        return path, observed

    def inspect(self, *, require_backup=True):
        """READ_ONLY preflight, called while market.lock is continuously held."""
        path, observed = self.approvals()
        no_wal(path)
        baseline = load_json(self.baseline)
        if (baseline.get("format") != "baostock18-production-baseline-v1"
                or set(baseline.get("a_share_counts", {})) != set(A_SHARE_TABLES)):
            raise AdmissionError("Approved a_share eight-table baseline required")
        if any(type(n) is not int or n < 0 for n in baseline["a_share_counts"].values()):
            raise AdmissionError("Invalid baseline row count")
        tables = baseline.get("tables")
        if not isinstance(tables, list) or not tables:
            raise AdmissionError("Approved catalog table inventory required")
        for pair in tables:
            if (not isinstance(pair, list) or len(pair) != 2
                    or any(not isinstance(x, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z_0-9]*", x) for x in pair)
                    or pair[0] == SCHEMA):
                raise AdmissionError("Invalid baseline inventory identifier")
        with duckdb.connect(str(path), read_only=True, config=READ_CONFIG) as connection:
            if inventory(connection) != sorted(tables):
                raise AdmissionError("Production baseline inventory mismatch")
            owned = _check_owned(connection, sorted(tuple(x) for x in tables))
            if owned:
                migrate(connection, owned, read_only=True)
            check_basis(connection, owned)
            counts = {t: connection.execute(f'SELECT count(*) FROM a_share."{t}"').fetchone()[0]
                      for t in A_SHARE_TABLES}
            if counts != baseline["a_share_counts"]:
                raise AdmissionError("Preflight a_share counts differ from approved baseline")
        if require_backup:
            self.check_backup(observed)
        if fingerprint(path) != observed["fingerprint"]:
            raise AdmissionError("Catalog changed during read-only preflight")
        return {"mode": "production", "preflight": "passed", "target": str(path),
                "catalog_fingerprint": observed["fingerprint"], "a_share_counts": counts,
                "manifest_hash": self.approved_manifest_hash, "batch_size": self.approved_batch_size,
                "checked_at": utcnow().isoformat()}

    def check_backup(self, observed):
        """Verify the backup receipt, manifest bytes and every backed-up payload."""
        receipt = load_json(self.backup_receipt)
        if (receipt.get("format") != "baostock18-production-backup-v1"
                or receipt.get("target") != self.target
                or receipt.get("catalog_fingerprint") != observed["fingerprint"]
                or receipt.get("window_started_at") != self.window_started_at
                or receipt.get("approved_manifest_hash") != self.approved_manifest_hash
                or receipt.get("approved_batch_size") != self.approved_batch_size
                or receipt.get("baseline_sha256") != sha256(Path(self.baseline))):
            raise AdmissionError("Backup receipt does not bind current catalog/approval/baseline")
        manifest_path = Path(receipt.get("manifest", ""))
        if (not manifest_path.is_absolute() or not manifest_path.is_file()
                or sha256(manifest_path) != receipt.get("manifest_sha256")):
            raise AdmissionError("Backup manifest hash mismatch")
        manifest = load_json(manifest_path)
        if manifest.get("format") != "baostock18-backup-files-v1" or not manifest.get("files"):
            raise AdmissionError("Invalid backup manifest")
        catalog = []
        for item in manifest["files"]:
            backup = Path(item["backup"])
            if (not backup.is_absolute() or not backup.is_file() or backup.is_symlink()
                    or backup.resolve().parent != manifest_path.resolve().parent
                    or backup.stat().st_size != item["size"] or sha256(backup) != item["sha256"]):
                raise AdmissionError("Backup payload hash/size/path mismatch")
            if item["source"] == self.target:
                catalog.append(item)
        if len(catalog) != 1 or catalog[0]["size"] != observed["fingerprint"]["size"]:
            raise AdmissionError("Backup manifest must contain exactly this catalog")
        # Compare full catalog bytes, not just the cheap header fingerprint.
        if catalog[0]["sha256"] != sha256(Path(self.target)):
            raise AdmissionError("Backup catalog hash differs from current catalog")
        checkpoint = receipt.get("checkpoint", {})
        if (checkpoint.get("format") != "baostock18-checkpoint-v1"
                or checkpoint.get("target") != self.target
                or checkpoint.get("catalog_fingerprint") != observed["fingerprint"]
                or checkpoint.get("window_started_at") != self.window_started_at
                or checkpoint.get("checkpoint_completed") is not True
                or not timestamp(self.window_started_at) <= timestamp(checkpoint.get("completed_at"))
                <= timestamp(observed["observed_at"])):
            raise AdmissionError("Backup requires completed CHECKPOINT bound to current catalog/window")


def create_backup(admission, destination, checkpoint_receipt, dependencies=()):
    """Copy a closed checkpointed catalog/dependencies; source stays READ_ONLY.

    dependencies must be the operator's frozen raw/reference/Parquet inventory.
    This receipt certifies only the listed files, not unlisted external sources.
    """
    path = production_path(admission.target)
    destination = Path(destination)
    if not destination.is_absolute() or destination.exists() or path.parent in destination.parents:
        raise AdmissionError("Backup requires a new absolute directory outside catalog directory")
    with market_lock(path):
        admission.inspect(require_backup=False)
        observed = load_json(admission.expected_catalog_fingerprint)
        checkpoint = load_json(checkpoint_receipt)
        # Validate checkpoint before creating any backup output.
        if (checkpoint.get("format") != "baostock18-checkpoint-v1"
                or checkpoint.get("target") != str(path)
                or checkpoint.get("catalog_fingerprint") != observed["fingerprint"]
                or checkpoint.get("window_started_at") != admission.window_started_at
                or checkpoint.get("checkpoint_completed") is not True
                or not timestamp(admission.window_started_at) <= timestamp(checkpoint.get("completed_at"))
                <= timestamp(observed["observed_at"])):
            raise AdmissionError("Completed CHECKPOINT receipt required before backup")
        sources = [path, *(Path(p).resolve(strict=True) for p in dependencies)]
        if len(set(sources)) != len(sources):
            raise AdmissionError("Duplicate backup source")
        destination.mkdir(parents=True, exist_ok=False)
        files = []
        for number, source in enumerate(sources):
            backup = destination / f"{number:08d}.backup"
            before = source.stat()
            digest = sha256(source)
            shutil.copyfile(source, backup)
            with backup.open("rb") as stream:
                os.fsync(stream.fileno())
            after = source.stat()
            if ((before.st_size, before.st_mtime_ns, before.st_dev, before.st_ino)
                    != (after.st_size, after.st_mtime_ns, after.st_dev, after.st_ino)
                    or sha256(backup) != digest):
                raise AdmissionError("Backup source changed or copy hash mismatch")
            files.append({"source": str(source), "backup": str(backup), "size": before.st_size, "sha256": digest})
        if fingerprint(path) != observed["fingerprint"]:
            raise AdmissionError("Catalog changed during backup")
        manifest_path = destination / "manifest.json"
        write_json(manifest_path, {"format": "baostock18-backup-files-v1", "files": files})
        receipt = {"format": "baostock18-production-backup-v1", "target": str(path),
                   "catalog_fingerprint": observed["fingerprint"], "checkpoint": checkpoint,
                   "window_started_at": admission.window_started_at,
                   "approved_manifest_hash": admission.approved_manifest_hash,
                   "approved_batch_size": admission.approved_batch_size,
                   "baseline_sha256": sha256(Path(admission.baseline)),
                   "manifest": str(manifest_path), "manifest_sha256": sha256(manifest_path),
                   "completed_at": utcnow().isoformat()}
        write_json(destination / "receipt.json", receipt)
        return receipt


def preflight(admission):
    """Run all preflight checks read-only, with no target or lock creation."""
    path = production_path(admission.target)
    with market_lock(path):
        return admission.inspect()


@contextmanager
def production_writer(admission, *, verify_ready=False):
    """Hold market.lock across read-only preflight, migration and writer close."""
    path = production_path(admission.target)
    with market_lock(path):
        result = admission.inspect()
        if verify_ready:
            with duckdb.connect(str(path), read_only=True, config=READ_CONFIG) as connection:
                reconcile_snapshot(connection, admission.approved_manifest_hash, admission.approved_batch_size)
        # Close the read-only connection, retain the same mutex, recheck identity.
        if fingerprint(path) != result["catalog_fingerprint"]:
            raise AdmissionError("Catalog changed before writer open")
        with duckdb.connect(str(path), config={
            **READ_CONFIG, "temp_directory": str(path.parent / (path.name + ".spill")),
        }) as connection:
            connection.execute("SET TimeZone='UTC'")
            if fingerprint(path) != result["catalog_fingerprint"]:
                raise AdmissionError("Catalog changed while opening writer")
            migrate(connection, _check_owned(connection, sorted(
                tuple(x) for x in load_json(admission.baseline)["tables"])))
            yield connection


def verify_production(admission):
    """Read-only reconciliation before a separately authorized pointer transaction."""
    path = production_path(admission.target)
    with market_lock(path):
        result = admission.inspect()
        with duckdb.connect(str(path), read_only=True, config=READ_CONFIG) as connection:
            result.update(reconcile_snapshot(connection, admission.approved_manifest_hash, admission.approved_batch_size))
        return result


def reconcile_snapshot(c, sid, batch_size):
    """Reconcile staged membership, lineage, quality and receipts before visibility."""
    snapshot = c.execute(
        f"SELECT state,expected_rows,manifest_json FROM {SCHEMA}.snapshot WHERE snapshot_id=?", [sid],
    ).fetchone()
    if not snapshot or snapshot[0] != "ready" or hashlib.sha256(
            encoded(json.loads(snapshot[2])).encode()).hexdigest() != sid:
        raise AdmissionError("Verify requires ready snapshot with approved manifest hash")
    literal = dict(c.execute(
        f"SELECT disposition,count(*) FROM {SCHEMA}.literal_version WHERE snapshot_id=? GROUP BY 1", [sid],
    ).fetchall())
    receipts = c.execute(
        f"SELECT coalesce(sum(rows),0),coalesce(sum(clean),0),coalesce(sum(conditional),0),"
        f"coalesce(sum(quarantine),0),coalesce(sum(lineage_rows),0),count(*) "
        f"FROM {SCHEMA}.batch_receipt WHERE snapshot_id=?", [sid],
    ).fetchone()
    lineage = c.execute(f"SELECT count(*) FROM {SCHEMA}.row_lineage WHERE snapshot_id=?", [sid]).fetchone()[0]
    clean = c.execute(
        f"SELECT count(*) FROM {SCHEMA}.bar_version b JOIN {SCHEMA}.literal_version l "
        "USING(snapshot_id,value_id) WHERE snapshot_id=? AND adjustment_basis='unadjusted' "
        "AND disposition='clean'", [sid],
    ).fetchone()[0]
    bad_bars = c.execute(
        f"SELECT count(*) FROM {SCHEMA}.bar_version b JOIN {SCHEMA}.literal_version l "
        "USING(snapshot_id,value_id) WHERE snapshot_id=? AND disposition='quarantine'", [sid],
    ).fetchone()[0]
    typed = c.execute(f"SELECT count(*) FROM {SCHEMA}.bar_version WHERE snapshot_id=?", [sid]).fetchone()[0]
    missing_lineage = c.execute(
        f"SELECT count(*) FROM {SCHEMA}.literal_version l WHERE snapshot_id=? AND NOT EXISTS "
        f"(SELECT 1 FROM {SCHEMA}.row_lineage r WHERE r.snapshot_id=l.snapshot_id AND r.value_id=l.value_id)", [sid],
    ).fetchone()[0]
    missing_quality = c.execute(
        f"SELECT count(*) FROM {SCHEMA}.literal_version l WHERE snapshot_id=? AND disposition<>'clean' "
        f"AND NOT EXISTS (SELECT 1 FROM {SCHEMA}.quality q WHERE q.snapshot_id=l.snapshot_id "
        "AND q.value_id=l.value_id AND q.disposition=l.disposition AND q.ruleset_version=?)", [sid, TRANSFORM],
    ).fetchone()[0]
    duplicate_keys = c.execute(
        f"SELECT count(*) FROM (SELECT value_id FROM {SCHEMA}.literal_version WHERE snapshot_id=? "
        "GROUP BY value_id HAVING count(*)<>1)", [sid],
    ).fetchone()[0]
    batches = c.execute(
        f"SELECT batch_number,rows,last_value_id FROM {SCHEMA}.batch_receipt "
        "WHERE snapshot_id=? ORDER BY batch_number", [sid],
    ).fetchall()
    previous = ""
    for number, (actual_number, rows, last) in enumerate(batches, 1):
        if (actual_number != number or not 1 <= rows <= batch_size or last <= previous
                or (number < len(batches) and rows != batch_size)):
            raise AdmissionError("Verify batch sequence/approved batch size mismatch")
        previous = last
    if (list(receipts[:5]) != [snapshot[1], literal.get("clean", 0), literal.get("conditional", 0),
                              literal.get("quarantine", 0), lineage]
            or sum(literal.values()) != snapshot[1] or bad_bars or missing_lineage or missing_quality
            or duplicate_keys or typed != snapshot[1] - literal.get("quarantine", 0)):
        raise AdmissionError("Verify PK/lineage/quality/batch reconciliation failed")
    return dict(verification="passed", snapshot_id=sid, rows=snapshot[1], clean_unadjusted=clean,
                conditional=literal.get("conditional", 0), quarantine=literal.get("quarantine", 0),
                lineage_rows=lineage, batches=receipts[5])

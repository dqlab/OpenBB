"""Checksummed migration runner restricted to a dedicated offline database."""

# SQL interpolation below uses only the constant schema, never user identifiers.
# ruff: noqa: S608

import hashlib
import json
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

import duckdb
from openbb_duckdb.baostock18.contract import SCHEMA, AdmissionError


def utcnow():
    """Actual local commit/visibility time, never the historical security date."""
    return datetime.now(timezone.utc)


def _rehearsal_baseline(path, receipt_path):
    """Bind an explicit completed clone receipt to this existing file identity.

    This opt-in is only for migration rehearsals. It does not admit a production
    catalog, initialize a missing clone, or relax the default offline boundary.
    """
    if receipt_path is None:
        return None
    receipt_path = Path(receipt_path).expanduser().resolve(strict=True)
    receipt = json.loads(receipt_path.read_text())
    if (receipt.get("format") != "baostock18-rehearsal-clone-v1"
            or receipt.get("purpose") != "migration rehearsal only; NOT a production backup"
            or receipt.get("production_admitted") is not False):
        raise AdmissionError("Invalid rehearsal clone receipt")
    if (not path.is_file() or str(path) != receipt.get("target_path")
            or path.parent != receipt_path.parent
            or path.name != "rehearsal.duckdb"
            or path == Path(receipt["source_path"]).resolve()
            or str(path) == receipt.get("source_physical_path")):
        raise AdmissionError("Rehearsal receipt does not authorize this target")
    stat = path.stat()
    if ([stat.st_dev, stat.st_ino] != receipt.get("target_identity")
            or [stat.st_dev, stat.st_ino] == receipt.get("source_identity")
            or stat.st_nlink != 1):
        raise AdmissionError("Rehearsal file identity changed or aliases the source")
    baseline = receipt.get("baseline_tables")
    if not baseline or any(row[0] == SCHEMA for row in baseline):
        raise AdmissionError("Invalid rehearsal baseline tables")
    return sorted(tuple(row) for row in baseline)


def _check_owned(connection, baseline=None):
    tables = connection.execute("SELECT schema_name,table_name FROM duckdb_tables() WHERE NOT internal").fetchall()
    if not tables and baseline is None:
        return False
    foreign = [(s, t) for s, t in tables if s != SCHEMA]
    if foreign and baseline is None:
        raise AdmissionError("Offline importer refuses a catalog containing other datasets")
    if baseline is not None and sorted(foreign) != baseline:
        raise AdmissionError("Rehearsal baseline table inventory changed")
    if not any(s == SCHEMA for s, _ in tables):
        return False
    try:
        marker = connection.execute(f"SELECT contract FROM {SCHEMA}.offline_store").fetchall()
    except duckdb.Error as exc:
        raise AdmissionError("Not a dedicated BaoStock18 offline store") from exc
    if marker != [("baostock18-offline-v1",)]:
        raise AdmissionError("Invalid offline store marker")
    return True


@contextmanager
def transaction(connection):
    """Rollback even KeyboardInterrupt/SystemExit, without advancing a receipt."""
    connection.execute("BEGIN")
    try:
        yield
        connection.execute("COMMIT")
    except BaseException:
        connection.execute("ROLLBACK")
        raise


@contextmanager
def offline_writer(path, *, rehearsal_receipt=None):
    """Inspect existing catalogs READ_ONLY before opening a writer; no production defaults."""
    path = Path(path).expanduser().resolve()
    if path.with_name(path.name + ".access-windows").exists():
        raise AdmissionError("Access-window policy present; no offline writer bypass")
    baseline = _rehearsal_baseline(path, rehearsal_receipt)
    if path.is_file():
        with duckdb.connect(str(path), read_only=True, config={"enable_external_access": False}) as connection:
            _check_owned(connection, baseline)
    path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(path), config={
        "threads": 2, "memory_limit": "2GB", "enable_external_access": False,
        "temp_directory": str(path.parent / (path.name + ".spill")),
    }) as connection:
        connection.execute("SET TimeZone='UTC'")
        owned = _check_owned(connection, baseline)
        sql = Path(__file__).with_name("migrations").joinpath("001_narrow_basis.sql").read_text()
        digest = hashlib.sha256(sql.encode()).hexdigest()
        if not owned:
            with transaction(connection):
                connection.execute(sql)
                connection.execute(f"INSERT INTO {SCHEMA}.schema_version VALUES (1,?,?)", [digest, utcnow()])
        elif connection.execute(f"SELECT version,sha256 FROM {SCHEMA}.schema_version").fetchall() != [(1, digest)]:
            raise AdmissionError("Migration checksum/version mismatch")
        yield connection

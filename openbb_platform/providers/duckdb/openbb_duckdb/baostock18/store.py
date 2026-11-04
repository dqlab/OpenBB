"""Checksummed migration runner restricted to a dedicated offline database."""

# SQL interpolation below uses only the constant schema, never user identifiers.
# ruff: noqa: S608

import hashlib
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

import duckdb
from openbb_duckdb.baostock18.contract import SCHEMA, AdmissionError


def utcnow():
    """Actual local commit/visibility time, never the historical security date."""
    return datetime.now(timezone.utc)


def _check_owned(connection):
    tables = connection.execute("SELECT schema_name,table_name FROM duckdb_tables() WHERE NOT internal").fetchall()
    if not tables:
        return False
    if any(s != SCHEMA for s, _ in tables):
        raise AdmissionError("Offline importer refuses a catalog containing other datasets")
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
def offline_writer(path):
    """Inspect existing catalogs READ_ONLY before opening a writer; no production defaults."""
    path = Path(path).expanduser().resolve()
    if path.is_file():
        with duckdb.connect(str(path), read_only=True, config={"enable_external_access": False}) as connection:
            _check_owned(connection)
    path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(path), config={
        "threads": 2, "memory_limit": "2GB", "enable_external_access": False,
        "temp_directory": str(path.parent / (path.name + ".spill")),
    }) as connection:
        connection.execute("SET TimeZone='UTC'")
        owned = _check_owned(connection)
        sql = Path(__file__).with_name("migrations").joinpath("001_narrow_basis.sql").read_text()
        digest = hashlib.sha256(sql.encode()).hexdigest()
        if not owned:
            with transaction(connection):
                connection.execute(sql)
                connection.execute(f"INSERT INTO {SCHEMA}.schema_version VALUES (1,?,?)", [digest, utcnow()])
        elif connection.execute(f"SELECT version,sha256 FROM {SCHEMA}.schema_version").fetchall() != [(1, digest)]:
            raise AdmissionError("Migration checksum/version mismatch")
        yield connection

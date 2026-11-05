"""Versioned content identity for one admitted snapshot, independent of DuckDB layout."""

# Identifiers come exclusively from the fixed migration and table allowlist.
# ruff: noqa: S608
import hashlib
from copy import deepcopy

from openbb_duckdb.baostock18.contract import SCHEMA, AdmissionError
from openbb_duckdb.baostock18.manifest import encoded
from openbb_duckdb.baostock18.production import check_basis, reconcile_snapshot
from openbb_duckdb.baostock18.store import migrate

CONTENT_FORMAT = "baostock18-content-identity-v1"
DATA_TABLES = ("snapshot", "literal_version", "row_lineage", "quality", "bar_version", "batch_receipt")
CONTROL_TABLES = ("basis_admission", "visibility_event", "dataset_pointer")
TABLES = ("schema_version", "offline_store", *DATA_TABLES, *CONTROL_TABLES)


def table_digest(connection, table):
    """SHA256 of sorted fixed-width row SHA256s, with all cells.

    JSON struct encoding preserves names, types, nulls and string boundaries.
    Each row includes its PK. External sorting and bounded Python batches avoid unbounded
    string_agg aggregates, and does not depend on physical/insertion order.
    """
    if table not in TABLES:
        raise AdmissionError("Unknown content identity table")
    columns = connection.execute(
        "SELECT column_name,data_type,is_nullable FROM information_schema.columns "
        "WHERE table_schema=? AND table_name=? ORDER BY ordinal_position", [SCHEMA, table],
    ).fetchall()
    selected = [name for name, _, _ in columns]
    if not selected:
        raise AdmissionError("Missing content identity columns")
    quoted = ['"' + name.replace('"', '""') + '"' for name in selected]
    projection = ",".join(f"{name} := {name}" for name in quoted)
    cursor = connection.execute(
        f"SELECT sha256(to_json(struct_pack({projection}))) AS h FROM {SCHEMA}.{table} ORDER BY h"
    )
    digest, count = hashlib.sha256(), 0
    while rows := cursor.fetchmany(8192):
        digest.update("".join(row[0] for row in rows).encode("ascii"))
        count += len(rows)
    value = {"columns": [list(c) for c in columns],
             "rows": count, "row_hashes_sha256": digest.hexdigest()}
    return dict(value, sha256=hashlib.sha256(encoded(value).encode()).hexdigest())


def content_identity(connection, snapshot_id, batch_size):
    """Reconcile and hash the complete admission namespace in one caller-held snapshot."""
    connection.execute("SET TimeZone='UTC'")
    tables = connection.execute(
        "SELECT table_name FROM duckdb_tables() WHERE schema_name=? ORDER BY 1", [SCHEMA],
    ).fetchall()
    if [t[0] for t in tables] != sorted(TABLES):
        raise AdmissionError("Admission content schema inventory mismatch")
    migrate(connection, True, read_only=True)
    check_basis(connection, True)
    if connection.execute(f"SELECT snapshot_id FROM {SCHEMA}.snapshot").fetchall() != [(snapshot_id,)]:
        raise AdmissionError("Admission content requires exactly the approved snapshot_id")
    measured = reconcile_snapshot(connection, snapshot_id, batch_size)
    definitions = connection.execute(
        "SELECT table_name,sql FROM duckdb_tables() WHERE schema_name=? ORDER BY 1", [SCHEMA],
    ).fetchall()
    views = connection.execute(
        "SELECT view_name,sql FROM duckdb_views() WHERE schema_name=? ORDER BY 1", [SCHEMA],
    ).fetchall()
    digests = {table: table_digest(connection, table) for table in TABLES}
    value = dict(format=CONTENT_FORMAT, snapshot_id=snapshot_id, batch_size=batch_size,
                 measured=measured, definitions=[list(d) for d in definitions], views=[list(v) for v in views],
                 tables=digests)
    return dict(value, sha256=hashlib.sha256(encoded(value).encode()).hexdigest())


def require_identity(actual, expected):
    """Never accept just counts, a caller-supplied hash, or a different pointer."""
    if (not isinstance(expected, dict) or expected.get("format") != CONTENT_FORMAT
            or actual != expected):
        raise AdmissionError("Admission content identity/counts/snapshot/pointer changed")


def require_unpublished(identity):
    """Publication starts only from the explicitly bound zero control state."""
    if any(identity["tables"][table]["rows"] for table in CONTROL_TABLES):
        raise AdmissionError("Deferred approval requires zero admission/events/pointers")


def after_publication(connection, before):
    """Update only the three control digests after the exclusive writer commits."""
    value = deepcopy(before)
    value.pop("sha256")
    for table in CONTROL_TABLES:
        value["tables"][table] = table_digest(connection, table)
    return dict(value, sha256=hashlib.sha256(encoded(value).encode()).hexdigest())

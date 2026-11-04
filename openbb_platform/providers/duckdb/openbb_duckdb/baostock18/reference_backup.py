"""Explicit reduced backups: catalog copy, exhaustive frozen reference verification.

The reference contract lists SELECT projections of persistent catalog references.
Every projection is executed again against the closed catalog, so a caller cannot
silently omit rows from a frozen inventory. Unknown path columns and external
views fail closed. Payloads remain in place; this is not a full disaster backup.
"""

# Only validated identifiers are interpolated into read-only SQL.
# ruff: noqa: S608

import hashlib
import json
import os
import re
import sqlite3
import stat
import time
from pathlib import Path

from openbb_duckdb.baostock18.backup import BLOCK_BYTES
from openbb_duckdb.baostock18.contract import AdmissionError

REDUCED_SCOPE = "catalog_with_reference_verification"
SCOPES = ("full", REDUCED_SCOPE)
IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z_0-9]*\Z")
HASH = re.compile(r"[0-9a-f]{64}\Z")
PATH_COLUMN = re.compile(r"(?:^|_)(?:path|key|uri|url|location)$")
PROVENANCE_COLUMNS = {"business_key", "source_key", "provider_key", "original_path"}
JSON_PATH_KEYS = {"path", "storage_key", "raw_key", "silver_key", "manifest_key", "payload_key", "evidence_key"}


def json_references(value):
    """Discover nested file references; hashes are byte hashes only, never IDs."""
    if isinstance(value, dict):
        for key, child in value.items():
            if child and isinstance(child, str) and (key in JSON_PATH_KEYS or key.endswith("_path")):
                prefix = key.removesuffix("_key").removesuffix("_path")
                digest = value.get(prefix + "_sha256")
                if key == "path":
                    digest = value.get("sha256")
                yield child, digest
            elif isinstance(child, (dict, list)):
                yield from json_references(child)
    elif isinstance(value, list):
        for child in value:
            yield from json_references(child)


def check_deadline(deadline):
    """One monotonic deadline covers enumeration, hashing, copying and restore."""
    if time.monotonic() >= deadline:
        raise AdmissionError("Backup and restore deadline exceeded; abort")


def stable_hash(path, deadline):
    """Hash a canonical regular file and detect identity/content-stat changes."""
    path = Path(path)
    check_deadline(deadline)
    try:
        before = path.lstat()
        if not path.is_absolute() or path.resolve(strict=True) != path or not stat.S_ISREG(before.st_mode):
            raise AdmissionError(f"Reference requires a canonical regular file: {path}")
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
        digest = hashlib.sha256()
        with os.fdopen(fd, "rb") as stream:
            opened = os.fstat(stream.fileno())
            for block in iter(lambda: stream.read(BLOCK_BYTES), b""):
                check_deadline(deadline)
                digest.update(block)
            closed = os.fstat(stream.fileno())
        after = path.stat()
    except OSError as exc:
        raise AdmissionError(f"Reference missing or unreadable: {path}") from exc
    for key in ("st_size", "st_mtime_ns", "st_dev", "st_ino"):
        if len({getattr(s, key) for s in (before, opened, closed, after)}) != 1:
            raise AdmissionError(f"Reference changed while hashing: {path}")
    return before.st_size, digest.hexdigest()


def copy_checked(source, target, deadline):
    """Copy through bounded buffers; independently hash the durable destination."""
    source = Path(source)
    check_deadline(deadline)
    before = source.lstat()
    if not source.is_absolute() or source.resolve(strict=True) != source or not stat.S_ISREG(before.st_mode):
        raise AdmissionError("Copy source must be a canonical regular file")
    digest = hashlib.sha256()
    fd = os.open(source, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(fd, "rb") as inp, Path(target).open("xb") as out:
        opened = os.fstat(inp.fileno())
        for block in iter(lambda: inp.read(BLOCK_BYTES), b""):
            check_deadline(deadline)
            digest.update(block)
            out.write(block)
        closed = os.fstat(inp.fileno())
        out.flush()
        os.fsync(out.fileno())
    after = source.stat()
    for key in ("st_size", "st_mtime_ns", "st_dev", "st_ino"):
        if len({getattr(s, key) for s in (before, opened, closed, after)}) != 1:
            raise AdmissionError("Copy source changed")
    size, digest = before.st_size, digest.hexdigest()
    if stable_hash(target, deadline) != (size, digest):
        raise AdmissionError("Backup/restore copy hash mismatch")
    return {"source": str(source), "backup": str(target), "size": size, "sha256": digest}


def _identifier(value):
    if not isinstance(value, str) or not IDENTIFIER.fullmatch(value):
        raise AdmissionError("Invalid reference contract identifier")
    return '"' + value + '"'


def enumerate_references(connection, contract, ledger, expected_fingerprint, deadline, *, freeze=False):
    """Execute reviewed column projections; compare full schema and view coverage.

    A rule has schema/table/path_column/hash_column/root. A hash_column may be
    null only when a separately frozen inventory supplies a byte SHA256. Excluded
    path-like columns require an explicit provenance-only explanation. Embedded
    JSON references are also traversed; their relative root must be explicit.
    """
    if (contract.get("format") != "baostock18-catalog-reference-contract-v1"
            or contract.get("catalog_fingerprint") != expected_fingerprint):
        raise AdmissionError("Reference enumeration contract must bind catalog fingerprint")
    columns = connection.execute(
        "SELECT c.table_schema,c.table_name,c.column_name FROM information_schema.columns c "
        "JOIN duckdb_tables() t ON t.schema_name=c.table_schema AND t.table_name=c.table_name "
        "WHERE NOT t.internal ORDER BY 1,2,3"
    ).fetchall()
    if contract.get("columns") != [list(row) for row in columns]:
        raise AdmissionError("Cannot enumerate references: catalog schema coverage changed")
    # Dynamic/glob/table-function dependencies require a dedicated resolver.
    for (sql,) in connection.execute("SELECT sql FROM duckdb_views() WHERE NOT internal").fetchall():
        if re.search(r"\b(?:read_\w+|\w+_scan)\s*\(|://", sql, re.I):
            raise AdmissionError("Cannot enumerate references: unsupported external view")
    covered = set()
    rule_counts = []

    def mark(value, digest, root):
        if not value or not isinstance(value, str) or "://" in value or any(x in value for x in "*?["):
            raise AdmissionError("Cannot enumerate nonliteral reference")
        source = Path(value) if Path(value).is_absolute() else root / value
        if source.resolve() != source or not source.is_relative_to(root):
            raise AdmissionError(f"Reference escapes its reviewed root: {source}")
        if digest is not None and not HASH.fullmatch(digest):
            raise AdmissionError("Reference has invalid expected SHA256")
        existing = ledger.execute("SELECT sha256 FROM expected WHERE path=?", [str(source)]).fetchone()
        if freeze and existing is None:
            size, actual_hash = stable_hash(source, deadline)
            if digest is not None and actual_hash != digest:
                raise AdmissionError(f"Reference catalog hash mismatch: {source}")
            ledger.execute("INSERT INTO expected VALUES (?,?,?,1)", [str(source), size, actual_hash])
            return
        if existing is None or (digest is not None and existing[0] != digest):
            raise AdmissionError(f"Reference inventory missing/hash mismatch: {source}")
        ledger.execute("UPDATE expected SET seen=1 WHERE path=?", [str(source)])

    for rule in contract.get("rules", []):
        names = [rule.get(k) for k in ("schema", "table", "path_column")]
        schema, table, column = map(_identifier, names)
        key = tuple(names)
        if key not in columns or key in covered:
            raise AdmissionError("Invalid/duplicate reference projection")
        covered.add(key)
        hash_column = rule.get("hash_column")
        digest_sql = _identifier(hash_column) if hash_column else "NULL"
        root = Path(rule["root"])
        if not root.is_absolute() or root.resolve(strict=True) != root:
            raise AdmissionError("Reference root must be canonical absolute directory")
        cursor = connection.execute(
            f"SELECT DISTINCT {column},{digest_sql} FROM {schema}.{table} WHERE {column} IS NOT NULL"
        )
        count = 0
        while rows := cursor.fetchmany(512):
            for value, digest in rows:
                check_deadline(deadline)
                mark(value, digest, root)
                count += 1
        rule_counts.append({**rule, "references": count})
    for exclusion in contract.get("provenance_columns", []):
        key = tuple(exclusion.get("column", []))
        if (key not in columns or key in covered or not exclusion.get("reason")
                or key[2] not in PROVENANCE_COLUMNS):
            raise AdmissionError("Invalid provenance-only column declaration")
        covered.add(key)
    required = {row for row in columns if PATH_COLUMN.search(row[2])}
    if not required <= covered:
        raise AdmissionError(f"Cannot enumerate references: uncovered path columns {sorted(required - covered)}")
    for schema, table, column in columns:
        if column.endswith("_json") or column in ("details", "payload"):
            s, t, col = map(_identifier, (schema, table, column))
            cursor = connection.execute(
                f"SELECT DISTINCT CAST({col} AS VARCHAR) FROM {s}.{t} WHERE regexp_matches(CAST({col} AS VARCHAR), ?)",
                [r'"(?:[^" ]*_)?(?:path|storage_key|raw_key|silver_key|manifest_key|payload_key)"\s*:'],
            )
            count = 0
            while rows := cursor.fetchmany(512):
                for (value,) in rows:
                    check_deadline(deadline)
                    references = list(json_references(json.loads(value)))
                    if references:
                        root = Path(contract.get("json_root", ""))
                        if not root.is_absolute() or root.resolve(strict=True) != root:
                            raise AdmissionError("Cannot enumerate JSON references without canonical json_root")
                        for filename, digest in references:
                            mark(filename, digest, root)
                            count += 1
            if count:
                rule_counts.append({"schema": schema, "table": table, "json_column": column, "references": count})
    if ledger.execute("SELECT count(*) FROM expected WHERE seen=0").fetchone()[0]:
        raise AdmissionError("Reference inventory contains unbound/unenumerated files")
    ledger.commit()
    return rule_counts


def freeze_reference_inventory(connection, contract, destination, fingerprint, deadline):
    """Freeze byte hashes from the same exhaustive projections used by backup.

    Existing catalog hashes take precedence and must match. Unpinned files receive
    a measured byte hash here, then are independently verified during backup.
    The inventory and scratch database must be new operator evidence paths.
    """
    destination = Path(destination)
    with sqlite3.connect(destination.with_suffix(".sqlite")) as ledger:
        ledger.execute("CREATE TABLE expected(path TEXT PRIMARY KEY, size INTEGER, sha256 TEXT, seen INTEGER)")
        projections = enumerate_references(connection, contract, ledger, fingerprint, deadline, freeze=True)
        with destination.open("x") as stream:
            for path, size, digest in ledger.execute("SELECT path,size,sha256 FROM expected ORDER BY path"):
                check_deadline(deadline)
                stream.write(json.dumps({"path": path, "size": size, "sha256": digest}) + "\n")
            stream.flush()
            os.fsync(stream.fileno())
        count, size = ledger.execute("SELECT count(*),coalesce(sum(size),0) FROM expected").fetchone()
    return {"count": count, "bytes": size, "projections": projections}


def verify_references(connection, contract, manifest, destination, fingerprint, deadline):
    """Verify every enumerated reference against a frozen SHA256 without copying."""
    started = time.monotonic()
    database = destination / "reference-index.sqlite"
    result = {"count": 0, "bytes": 0, "missing": 0, "mismatch": 0, "status": "running",
              "coverage": "all persistent file references in reviewed catalog contract",
              "contents_copied": False}
    try:
        with sqlite3.connect(database) as ledger:
            ledger.execute("CREATE TABLE expected(path TEXT PRIMARY KEY, size INTEGER, sha256 TEXT, seen INTEGER)")
            previous = ""
            with Path(manifest).open() as stream:
                for line in stream:
                    check_deadline(deadline)
                    item = json.loads(line)
                    path, size, digest = item["path"], item["size"], item["sha256"]
                    if (not isinstance(path, str) or path <= previous or not Path(path).is_absolute()
                            or type(size) is not int or size < 0 or not HASH.fullmatch(digest)):
                        raise AdmissionError("Sorted unique SHA256 reference inventory required")
                    ledger.execute("INSERT INTO expected VALUES (?,?,?,0)", [path, size, digest])
                    previous = path
            result["projections"] = enumerate_references(connection, contract, ledger, fingerprint, deadline)
            with (destination / "reference-verification.jsonl").open("x") as output:
                for path, size, digest in ledger.execute("SELECT path,size,sha256 FROM expected ORDER BY path"):
                    try:
                        actual = stable_hash(path, deadline)
                    except AdmissionError:
                        if not Path(path).exists():
                            result["missing"] += 1
                        raise
                    if actual != (size, digest):
                        result["mismatch"] += 1
                        raise AdmissionError(f"Reference hash/size mismatch: {path}")
                    result["count"] += 1
                    result["bytes"] += size
                    info = Path(path).stat()
                    output.write(json.dumps({"path": path, "size": size, "sha256": digest,
                                             "mtime_ns": info.st_mtime_ns, "device": info.st_dev,
                                             "inode": info.st_ino}) + "\n")
                output.flush()
                os.fsync(output.fileno())
            result["status"] = "passed"
    except AdmissionError as exc:
        result["status"] = "failed"
        result["error"] = str(exc)
        if "inventory missing/hash mismatch" in str(exc):
            result["mismatch"] += 1
        raise
    except (OSError, ValueError, KeyError, TypeError, sqlite3.Error) as exc:
        result["status"] = "failed"
        result["error"] = str(exc)
        raise AdmissionError("Cannot enumerate/verify catalog reference inventory") from exc
    finally:
        result["seconds"] = time.monotonic() - started
        with (destination / "reference-verification-summary.json").open("x") as stream:
            json.dump(result, stream, indent=2)
    return result

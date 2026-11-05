"""Content binding for a candidate database AND its local Parquet dependencies."""

import glob
import hashlib
import json
import re
from pathlib import Path

import duckdb
from openbb_duckdb.baostock18.contract import TRANSFORM, AdmissionError


def encoded(value) -> str:
    """Canonical JSON used for stable snapshot identities."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def sha256(path: Path) -> str:
    """Hash a file with bounded memory (Python 3.10 compatible)."""
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(4 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def source_connection(path: Path, spill: Path):
    """No HTTP filesystem, extension downloading, or writes to candidate paths."""
    connection = duckdb.connect(str(path), read_only=True, config={
        "threads": 2, "memory_limit": "2GB", "temp_directory": str(spill),
        "autoinstall_known_extensions": False, "autoload_known_extensions": False,
    })
    connection.execute("SET disabled_filesystems='HTTPFileSystem'")
    connection.execute("SET TimeZone='UTC'")
    return connection


def dependency_paths(connection) -> set[Path]:
    """Discover only literal local Parquet dependencies; refuse dynamic sources."""
    dependencies = set()
    for (sql,) in connection.execute("SELECT sql FROM duckdb_views() WHERE NOT internal").fetchall():
        tokens = re.findall(r"read_parquet\('((?:[^']|'')*)'\)", sql)
        if len(tokens) != len(re.findall(r"read_parquet\s*\(", sql, flags=re.IGNORECASE)):
            raise AdmissionError("Unsupported dynamic Parquet dependency")
        if "://" in sql or re.search(r"read_(?:csv|json|text|blob)\s*\(", sql, flags=re.IGNORECASE):
            raise AdmissionError("Candidate must contain only local Parquet source views")
        for token in tokens:
            pattern = token.replace("''", "'")
            if not Path(pattern).is_absolute():
                raise AdmissionError("Candidate dependencies must be absolute local paths")
            matches = glob.glob(pattern)
            if not matches:
                raise AdmissionError(f"Missing Parquet dependency: {pattern}")
            dependencies.update(Path(p).resolve(strict=True) for p in matches)
    return dependencies


def prepare_manifest(candidate, *, symbols=None, start_date="1990-12-19",
                     end_date="2026-10-15", evidence_files=()) -> dict:
    """Bind every local read_parquet dependency, source contract and exact scope.

    Evidence files can bind the pre-existing raw inventory/hash ledgers. Their
    inclusion does not assert that every raw payload has been re-audited.
    """
    path = Path(candidate).expanduser().resolve(strict=True)
    dependencies = {path, *(Path(p).resolve(strict=True) for p in evidence_files)}
    with source_connection(path, path.parent / ".manifest-unused-spill") as connection:
        contract = connection.execute("SELECT contract FROM candidate_contract").fetchall()
        if len(contract) != 1 or not contract[0][0]:
            raise AdmissionError("Expected one candidate contract identity")
        dependencies.update(dependency_paths(connection))
    files = [{"path": str(p), "bytes": p.stat().st_size, "sha256": sha256(p)}
             for p in sorted(dependencies)]
    return {
        "format": "baostock18-offline-manifest-v1", "transform_version": TRANSFORM,
        "candidate": str(path), "contract_id": contract[0][0], "files": files,
        "scope": {"symbols": sorted({s.lower() for s in symbols}) if symbols else [],
                  "start_date": start_date, "end_date": end_date},
        "availability_basis": "reconstructed captures; historical source availability unknown; not PIT certified",
    }


def verify_manifest(manifest: dict) -> dict:
    """Reject a changed dependency before any target writes; return stat guards."""
    if manifest.get("format") != "baostock18-offline-manifest-v1" or manifest.get("transform_version") != TRANSFORM:
        raise AdmissionError("Unsupported manifest/transform version")
    paths = set()
    signatures = {}
    for item in manifest["files"]:
        path = Path(item["path"])
        if not path.is_absolute() or "://" in str(path) or not path.is_file():
            raise AdmissionError(f"Missing local input: {path}")
        stat = path.stat()
        if stat.st_size != item["bytes"] or sha256(path) != item["sha256"]:
            raise AdmissionError(f"Input hash mismatch: {path}")
        paths.add(str(path.resolve()))
        signatures[str(path)] = (stat.st_size, stat.st_mtime_ns)
    if str(Path(manifest["candidate"]).resolve()) not in paths:
        raise AdmissionError("Manifest must bind candidate database itself")
    return signatures


def verify_unchanged(signatures: dict):
    """Detect changes during the read before making the snapshot ready."""
    for filename, previous in signatures.items():
        stat = Path(filename).stat()
        if (stat.st_size, stat.st_mtime_ns) != previous:
            raise AdmissionError(f"Input changed during import: {filename}")

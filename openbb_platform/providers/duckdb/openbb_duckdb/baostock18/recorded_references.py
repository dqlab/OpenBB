"""Explicit recorded-hash verification with deterministic stratified byte sampling."""

import hashlib
import heapq
import json
import math
import os
import sqlite3
import stat
import time
from pathlib import Path

from openbb_duckdb.baostock18.contract import AdmissionError
from openbb_duckdb.baostock18.reference_backup import (
    HASH,
    RECORDED_SCOPE,
    bounded_sql,
    check_deadline,
    enumerate_references,
    json_root_distribution,
    stable_hash,
)

SAMPLE_SEED = "baostock18-recorded-reference-v1"


def sample_count(count):
    """Check all small fixtures; otherwise at least 1000 or 0.5%, capped at 5000."""
    return min(count, 5000, max(1000, math.ceil(count / 200)))


class Sampler:
    """Keep bounded deterministic random priorities and one per root/layer stratum."""

    def __init__(self):
        self.candidates = []
        self.strata = {}
        self.count = 0

    def add(self, path, root):
        """Select reproducibly, independent of catalog row order and Python hashes."""
        priority = int.from_bytes(hashlib.sha256((SAMPLE_SEED + "\0" + str(path)).encode()).digest(), "big")
        item = (-priority, str(path))
        heapq.heappush(self.candidates, item)
        if len(self.candidates) > 5000:
            heapq.heappop(self.candidates)
        relative = path.relative_to(root)
        # Root, top directory, depth and extension cover storage roots and layers.
        key = (str(root), relative.parts[0] if len(relative.parts) > 1 else ".", len(relative.parts), path.suffix)
        best = self.strata.get(key)
        if best is None or item > best:
            self.strata[key] = item
        self.count += 1

    def selected(self):
        """Reserve stratum coverage before filling the remaining random sample."""
        target = sample_count(self.count)
        if len(self.strata) > target:
            raise AdmissionError("Sampling cap cannot cover all reference root/layer strata")
        selected = {p for _, p in self.strata.values()}
        for _, path in sorted(self.candidates, reverse=True):
            if len(selected) == target:
                break
            selected.add(path)
        return selected


def recorded_inventory(connection, contract, manifest, fingerprint, deadline, *, destination=None):
    """Freeze or verify all metadata/recorded hashes and recompute a bounded sample.

    A missing hash is always computed. Catalog/contract sizes, when supplied,
    must match the frozen record and the regular file's actual size. Hashes from
    records are compared to each other without reading unsampled payload bytes.
    No successful receipt is produced for a missing file or any mismatch.
    """
    started = time.monotonic()
    manifest = Path(manifest)
    freezing = destination is None
    database = manifest.with_suffix(".sqlite") if freezing else Path(destination) / "reference-index.sqlite"
    summary = (manifest.with_suffix(".summary.json") if freezing
               else Path(destination) / "reference-verification-summary.json")
    result = dict(
        mode=RECORDED_SCOPE, status="running", count=0, bytes=0,
        recorded_hash_comparisons=0, recomputed_files=0, recomputed_bytes=0,
        missing_hash_recomputed_files=0, sample_recomputed_files=0, sample_recomputed_bytes=0,
        unread_bytes=0, missing=0, size_mismatch=0, hash_mismatch=0, mismatch=0, changed=0,
        sample_seed=SAMPLE_SEED, sample_strata=0, contents_copied=False,
        coverage="all persistent file references in reviewed catalog contract",
    )
    sampler = Sampler()

    def fail(kind, message):
        result[kind] += 1
        if kind in ("size_mismatch", "hash_mismatch"):
            result["mismatch"] += 1
        raise AdmissionError(message)

    try:
        with sqlite3.connect(database) as ledger, bounded_sql(connection, deadline):
            ledger.execute("CREATE TABLE expected(path TEXT PRIMARY KEY, size INTEGER, sha256 TEXT, seen INTEGER)")
            ledger.execute("CREATE TABLE checked(path TEXT PRIMARY KEY, mtime_ns INTEGER, device INTEGER, inode INTEGER, "
                           "computed INTEGER, recorded INTEGER)")
            if not freezing:
                previous = ""
                with manifest.open() as stream:
                    for line in stream:
                        check_deadline(deadline)
                        item = json.loads(line)
                        path, size, digest = item["path"], item["size"], item.get("sha256")
                        if (not isinstance(path, str) or path <= previous or not Path(path).is_absolute()
                                or type(size) is not int or size < 0
                                or (digest is not None and (not isinstance(digest, str) or not HASH.fullmatch(digest)))):
                            raise AdmissionError("Sorted unique reference inventory with valid sizes/hashes required")
                        ledger.execute("INSERT INTO expected VALUES (?,?,?,0)", [path, size, digest])
                        previous = path

            def mark(source, digest, recorded_size, root):
                check_deadline(deadline)
                path = str(source)
                existing = ledger.execute("SELECT size,sha256,seen FROM expected WHERE path=?", [path]).fetchone()
                if existing is None and not freezing:
                    fail("missing", f"Reference inventory missing: {source}")
                if recorded_size is not None and (type(recorded_size) is not int or recorded_size < 0):
                    raise AdmissionError(f"Invalid reference recorded size: {source}")
                if existing is not None:
                    if digest is not None and existing[1] is not None and digest != existing[1]:
                        fail("hash_mismatch", f"Reference recorded hash mismatch: {source}")
                    if recorded_size is not None and recorded_size != existing[0]:
                        fail("size_mismatch", f"Reference recorded size mismatch: {source}")
                    if existing[2]:
                        # Still compare every repeated catalog hash and size above.
                        return
                try:
                    info = source.lstat()
                    if source.resolve(strict=True) != source or not stat.S_ISREG(info.st_mode):
                        raise AdmissionError(f"Reference requires a canonical regular file: {source}")
                except FileNotFoundError:
                    fail("missing", f"Reference missing: {source}")
                expected_size = existing[0] if existing else recorded_size
                if expected_size is not None and info.st_size != expected_size:
                    fail("size_mismatch", f"Reference size mismatch: {source}")
                expected_digest = (existing[1] if existing else None) or digest
                recorded = expected_digest is not None
                computed = expected_digest is None
                if computed:
                    actual_size, expected_digest = stable_hash(source, deadline)
                    if actual_size != info.st_size:
                        fail("size_mismatch", f"Reference changed size while hashing: {source}")
                    result["missing_hash_recomputed_files"] += 1
                    result["recomputed_files"] += 1
                    result["recomputed_bytes"] += info.st_size
                else:
                    result["recorded_hash_comparisons"] += 1
                if existing is None:
                    ledger.execute("INSERT INTO expected VALUES (?,?,?,1)", [path, info.st_size, expected_digest])
                else:
                    ledger.execute("UPDATE expected SET sha256=?,seen=1 WHERE path=?", [expected_digest, path])
                ledger.execute("INSERT INTO checked VALUES (?,?,?,?,?,?)",
                               [path, info.st_mtime_ns, info.st_dev, info.st_ino, int(computed), int(recorded)])
                sampler.add(source, root)
                result["count"] += 1
                result["bytes"] += info.st_size

            result["projections"] = enumerate_references(connection, contract, ledger, fingerprint, deadline, record=mark)
            result["json_roots"] = json_root_distribution(contract, result["projections"])
            result["directories"] = sum(p["references"] for p in result["projections"]
                                        if p.get("kind") == "capture_directory")
            selected = sampler.selected()
            result["sample_target"] = sample_count(result["count"])
            result["sample_strata"] = len(sampler.strata)
            result["sample_paths"] = sorted(selected)
            for path in sorted(selected):
                size, digest = ledger.execute("SELECT size,sha256 FROM expected WHERE path=?", [path]).fetchone()
                computed = ledger.execute("SELECT computed FROM checked WHERE path=?", [path]).fetchone()[0]
                if not computed:
                    actual_size, actual_digest = stable_hash(path, deadline)
                    if actual_size != size:
                        fail("size_mismatch", f"Sample reference size mismatch: {path}")
                    if actual_digest != digest:
                        fail("hash_mismatch", f"Sample reference hash mismatch: {path}")
                    ledger.execute("UPDATE checked SET computed=1 WHERE path=?", [path])
                    result["recomputed_files"] += 1
                    result["recomputed_bytes"] += size
                result["sample_recomputed_files"] += 1
                result["sample_recomputed_bytes"] += size
            output_path = manifest if freezing else Path(destination) / "reference-verification.jsonl"
            with output_path.open("x") as stream:
                for path, size, digest, mtime, device, inode in ledger.execute(
                    "SELECT e.path,e.size,e.sha256,c.mtime_ns,c.device,c.inode "
                    "FROM expected e JOIN checked c USING(path) ORDER BY e.path"
                ):
                    check_deadline(deadline)
                    stream.write(json.dumps(dict(path=path, size=size, sha256=digest, mtime_ns=mtime,
                                                 device=device, inode=inode)) + "\n")
                stream.flush()
                os.fsync(stream.fileno())
            result["unread_bytes"] = result["bytes"] - result["recomputed_bytes"]
            result["status"] = "passed"
    except (AdmissionError, OSError, ValueError, KeyError, TypeError, sqlite3.Error) as exc:
        result.update(status="failed", error=str(exc))
        # Resolvers/directory inventory can fail before reaching mark.
        if "missing" in str(exc).lower() and not result["missing"]:
            result["missing"] += 1
        if "inventory changed" in str(exc) and not result["hash_mismatch"]:
            result["hash_mismatch"] += 1
            result["mismatch"] += 1
        if isinstance(exc, AdmissionError):
            raise
        raise AdmissionError("Cannot enumerate/verify recorded catalog references") from exc
    finally:
        result["unread_bytes"] = result["bytes"] - result["recomputed_bytes"]
        result["seconds"] = time.monotonic() - started
        with summary.open("x") as stream:
            json.dump(result, stream, indent=2)
    return result

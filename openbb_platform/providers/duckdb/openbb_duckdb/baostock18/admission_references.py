"""Bounded verification of this admission's capture_inventory, never foreign references."""

# Only fixed admission identifiers appear in SQL.
# ruff: noqa: S608
import json
import os
import stat
import time
from pathlib import Path

from openbb_duckdb.baostock18.contract import SCHEMA, AdmissionError
from openbb_duckdb.baostock18.manifest import source_connection
from openbb_duckdb.baostock18.production import load_json
from openbb_duckdb.baostock18.recorded_references import SAMPLE_SEED, Sampler, sample_count
from openbb_duckdb.baostock18.reference_backup import (
    HASH,
    bounded_sql,
    check_deadline,
    directory_files,
    stable_hash,
)

COVERAGE = "this admission capture_inventory and every imported row_lineage reference"


def verify_admission_references(connection, config, output, deadline):  # noqa: PLR0912
    """Check all membership/types/sizes/recorded hashes, hash unpinned files + sample.

    A supplied inventory must be explicitly pinned by its recorded SHA256. Without
    one the candidate has no member hashes, so every file is actually hashed.
    All lineage locators (including capture_id) must resolve into this exact set.
    """
    from openbb_duckdb.baostock18.driver import precheck_input_references

    started = time.monotonic()
    output, root = Path(output), Path(config["admission_root"])
    if not root.is_absolute() or root.resolve(strict=True) != root or not root.is_dir():
        raise AdmissionError("Canonical admission root required")
    inventory = output / "capture-directories.jsonl"
    pinned = config.get("capture_inventory_receipt")
    if pinned:
        receipt = load_json(pinned)
        recorded = config.get("capture_inventory_sha256")
        if (receipt.get("status") != "passed" or not isinstance(recorded, str) or not HASH.fullmatch(recorded)
                or receipt.get("inventory_sha256") != recorded):
            raise AdmissionError("Explicit passed SHA256-pinned capture inventory required")
        source = Path(receipt["inventory"])
        if stable_hash(source, deadline)[1] != recorded:
            raise AdmissionError("Capture inventory recorded hash mismatch")
        # Keep a local immutable copy of small evidence, never copy raw references.
        with source.open("rb") as inp, inventory.open("xb") as out:
            while block := inp.read(1024 * 1024):
                check_deadline(deadline)
                out.write(block)
            out.flush()
            os.fsync(out.fileno())
        if stable_hash(inventory, deadline)[1] != recorded:
            raise AdmissionError("Capture inventory copy hash mismatch")
        missing_hash_precomputed = False
    else:
        pre = precheck_input_references(dict(config, max_verify_seconds=max(0, deadline-time.monotonic())),
                                       destination=inventory)
        if pre["status"] != "passed":
            raise AdmissionError("Deferred capture verification failed")
        missing_hash_precomputed = True
    expected_dirs, expected_files = {}, {}
    with inventory.open() as stream:
        for line in stream:
            check_deadline(deadline)
            item = json.loads(line)
            directory = Path(item["directory"])
            if str(directory) in expected_dirs:
                raise AdmissionError("Duplicate capture inventory directory")
            if (not directory.is_absolute() or not directory.is_relative_to(root)
                    or directory.resolve(strict=True) != directory):
                raise AdmissionError("Capture directory escapes admission root")
            expected_dirs[str(directory)] = item["files"]
            for member in item["files"]:
                path = Path(member["path"])
                size, digest = member["size"], member.get("sha256")
                if (not path.is_absolute() or not path.is_relative_to(directory)
                        or type(size) is not int or size < 0
                        or (digest is not None and (not isinstance(digest, str) or not HASH.fullmatch(digest)))):
                    raise AdmissionError("Invalid capture file size/path/recorded hash")
                if str(path) in expected_files and expected_files[str(path)] != member:
                    raise AdmissionError("Conflicting capture inventory file hashes")
                expected_files[str(path)] = member
    manifest = load_json(config["manifest"])
    candidate_paths = {}
    with source_connection(Path(manifest["candidate"]), output / "candidate-spill") as c, bounded_sql(c, deadline):
        for capture_id, relative in c.execute("SELECT capture_id,relative FROM capture_inventory").fetchall():
            if not isinstance(relative, str) or not relative or "://" in relative or any(v in relative for v in "*?["):
                raise AdmissionError("Invalid candidate capture locator")
            path = Path(relative) if Path(relative).is_absolute() else root / relative
            candidate_paths[(str(capture_id), relative)] = str(path)
    if set(candidate_paths.values()) != set(expected_dirs):
        raise AdmissionError("Candidate capture_inventory membership changed")
    total, unresolved = 0, 0
    for source_path, capture_id, count in connection.execute(
        f"SELECT source_path,capture_id,count(*) FROM {SCHEMA}.row_lineage "
        "WHERE snapshot_id=? GROUP BY 1,2", [config["approved_manifest_hash"]],
    ).fetchall():
        check_deadline(deadline)
        total += count
        if (capture_id, source_path) not in candidate_paths:
            unresolved += count
    if unresolved:
        raise AdmissionError(f"Unresolved admission lineage rows: {unresolved}/{total}")
    sampler = Sampler()
    for name, members in sorted(expected_dirs.items()):
        files = directory_files(Path(name), root, deadline)
        if [str(p) for p in files] != sorted(m["path"] for m in members):
            raise AdmissionError("Capture directory membership changed")
    computed, computed_bytes, missing_hash, recorded_comparisons = 0, 0, 0, 0
    measured = {}
    for name, member in sorted(expected_files.items()):
        check_deadline(deadline)
        path = Path(name)
        info = path.lstat()
        if path.resolve(strict=True) != path or not stat.S_ISREG(info.st_mode) or info.st_size != member["size"]:
            raise AdmissionError(f"Capture reference type/size mismatch: {path}")
        digest = member.get("sha256")
        if digest is None:
            size, digest = stable_hash(path, deadline)
            if size != member["size"]:
                raise AdmissionError("Capture reference changed while hashing")
            computed += 1
            missing_hash += 1
            computed_bytes += size
        else:
            recorded_comparisons += 1
        measured[name] = (info, digest, member.get("sha256") is None)
        sampler.add(path, root)
    selected = sampler.selected()
    for name in sorted(selected):
        info, digest, already = measured[name]
        if not already:
            if stable_hash(name, deadline) != (info.st_size, digest):
                raise AdmissionError(f"Capture sample recorded hash mismatch: {name}")
            computed += 1
            computed_bytes += info.st_size
    # Detect changes throughout the pass, also in unsampled files and directories.
    for name, (before, _, _) in measured.items():
        after = Path(name).lstat()
        if any(getattr(before, k) != getattr(after, k) for k in
               ("st_dev", "st_ino", "st_size", "st_mtime_ns", "st_ctime_ns", "st_mode")):
            raise AdmissionError(f"Capture reference changed during verification: {name}")
    for name, members in expected_dirs.items():
        if [str(p) for p in directory_files(Path(name), root, deadline)] != sorted(m["path"] for m in members):
            raise AdmissionError("Capture membership changed during verification")
    measured_path = output / "reference-verification.jsonl"
    with measured_path.open("x") as stream:
        for name, (info, digest, was_missing) in sorted(measured.items()):
            stream.write(json.dumps(dict(path=name, size=info.st_size, sha256=digest,
                                         recorded_sha256=expected_files[name].get("sha256"),
                                         recomputed=missing_hash_precomputed or was_missing or name in selected)) + "\n")
        stream.flush()
        os.fsync(stream.fileno())
    return dict(status="passed", coverage=COVERAGE, contents_copied=False,
                inventory=str(inventory), inventory_sha256=stable_hash(inventory, deadline)[1],
                directories=len(expected_dirs), count=len(expected_files),
                bytes=sum(m["size"] for m in expected_files.values()),
                lineage_rows=total, unresolved_lineage_rows=unresolved,
                recorded_hash_comparisons=recorded_comparisons,
                missing_hash_recomputed_files=len(expected_files) if missing_hash_precomputed else missing_hash,
                candidate_unrecorded_files_precomputed=len(expected_files) if missing_hash_precomputed else 0,
                recomputed_files=len(expected_files) if missing_hash_precomputed else computed,
                recomputed_bytes=sum(m["size"] for m in expected_files.values())
                if missing_hash_precomputed else computed_bytes,
                measured_inventory=str(measured_path), measured_inventory_sha256=stable_hash(measured_path, deadline)[1],
                sample_seed=SAMPLE_SEED,
                sample_target=sample_count(len(expected_files)), sample_recomputed_files=len(selected),
                sample_paths=sorted(selected), missing=0, mismatch=0, changed=0, seconds=time.monotonic()-started)

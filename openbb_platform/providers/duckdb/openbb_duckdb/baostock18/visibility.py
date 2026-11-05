"""Deferred publication: read-only verification followed by a short approval transaction."""

# All SQL identifiers are fixed admission names or the fixed legacy table list.
# ruff: noqa: S608
import fcntl
import os
import stat
import sys
import time
from contextlib import contextmanager
from pathlib import Path

import duckdb
from openbb_duckdb.baostock18.admission_references import COVERAGE, verify_admission_references
from openbb_duckdb.baostock18.content_identity import (
    content_identity,
    require_identity,
    require_unpublished,
)
from openbb_duckdb.baostock18.contract import SCHEMA, AdmissionError
from openbb_duckdb.baostock18.manifest import sha256
from openbb_duckdb.baostock18.production import (
    READ_CONFIG,
    ProductionAdmission,
    load_json,
    market_lock,
    no_wal,
    production_path,
    write_json,
)
from openbb_duckdb.baostock18.reference_backup import CATALOG_ONLY, HASH, bounded_sql
from openbb_duckdb.baostock18.store import transaction, utcnow

DEFERRED = "IMPORT_COMPLETE_VISIBILITY_DEFERRED"
VERIFIED_FORMAT = "baostock18-visibility-verification-v1"
APPROVED_FORMAT = "baostock18-visibility-approval-v1"


def expected_counts(clean, quarantine, batches):
    """Build an explicit admission count contract, including zero conditional rows."""
    if any(type(n) is not int or n < 0 for n in (clean, quarantine, batches)) or not clean or not batches:
        raise AdmissionError("Explicit positive clean/batch and nonnegative quarantine counts required")
    return dict(rows=clean + quarantine, clean_unadjusted=clean, quarantine=quarantine,
                conditional=0, batches=batches)


def check_counts(actual, expected):
    """Require measured reconciliation, never trust counts in a command alone."""
    if any(type(actual.get(k)) is not int or actual[k] != v for k, v in expected.items()):
        raise AdmissionError("Admission counts differ from expected clean/quarantine/batches")


def verify_deferred(config, deferred_receipt, output):
    """Verify only imported capture references and bind admission content READ_ONLY.

    Physical catalog fingerprints remain import/backup evidence. They are never
    used as content identity for deferred verification, approval or replay.
    """
    deferred = load_json(deferred_receipt)
    original = {k: v for k, v in config.items() if k not in {
        "capture_inventory_receipt", "capture_inventory_sha256", "visibility_verify_seconds"}}
    if (config.get("visibility") != "deferred" or deferred.get("status") != DEFERRED
            or config.get("backup_scope") != CATALOG_ONLY or deferred.get("backup_scope") != CATALOG_ONLY
            or deferred.get("config") != original):
        raise AdmissionError("Matching deferred driver receipt/config required")
    target = production_path(config["target"])
    output = Path(output)
    if not output.is_absolute():
        raise AdmissionError("New absolute verification directory required")
    output.mkdir(exist_ok=False)
    started = time.monotonic()
    cap = config.get("visibility_verify_seconds", 1200)
    if type(cap) not in (int, float) or not 0 < cap <= 1200:
        raise AdmissionError("Admission visibility verification cap must be <=1200 seconds")
    expected = expected_counts(config["expected_clean_rows"], config["expected_quarantine"], config["expected_batches"])
    result = dict(format=VERIFIED_FORMAT, status="FAIL", target=str(target),
                  snapshot_id=config["approved_manifest_hash"], expected_counts=expected,
                  approved_batch_size=config["approved_batch_size"],
                  baseline_sha256=sha256(Path(config["baseline"])),
                  deferred_receipt=str(deferred_receipt), deferred_receipt_sha256=sha256(Path(deferred_receipt)))
    try:
        if config["expected_rows"] != expected["rows"]:
            raise AdmissionError("Configured total differs from clean/quarantine counts")
        backup = Path(config["output"]) / "before-import-backup/receipt.json"
        proof = load_json(backup)
        if (proof.get("format") != "baostock18-production-backup-v1"
                or proof.get("scope") != CATALOG_ONLY or proof.get("target") != str(target)
                or proof.get("approved_manifest_hash") != config["approved_manifest_hash"]
                or proof.get("approved_batch_size") != config["approved_batch_size"]
                or proof.get("baseline_sha256") != result["baseline_sha256"]
                or proof.get("restore_check", {}).get("status") != "passed"
                or not isinstance(proof.get("catalog_sha256_before"), str)
                or not HASH.fullmatch(proof["catalog_sha256_before"])
                or proof.get("restore_check", {}).get("read_only") is not True
                or proof.get("restore_check", {}).get("admission_physical_tables") != 0
                or proof.get("restore_check") != deferred.get("restore_check")
                or proof.get("catalog_sha256_before") != proof.get("catalog_sha256_after")
                or proof.get("catalog_sha256_before") != proof.get("restore_check", {}).get("sha256")):
            raise AdmissionError("Completed catalog-only backup and restore receipt required")
        with publisher_lock(target), market_lock(target):
            no_wal(target)
            with (duckdb.connect(str(target), read_only=True, config=READ_CONFIG) as c,
                  bounded_sql(c, started + cap), transaction(c)):
                identity = content_identity(c, config["approved_manifest_hash"], config["approved_batch_size"])
                require_unpublished(identity)
                check_counts(identity["measured"], expected)
                imported = deferred.get("imported", {})
                if (identity["measured"]["lineage_rows"] != imported.get("lineage_rows")
                        or imported.get("snapshot_id") != config["approved_manifest_hash"]):
                    raise AdmissionError("Imported lineage/snapshot receipt mismatch")
                reference = verify_admission_references(c, config, output, started + cap)
            no_wal(target)
        # Preserve the original backup; this small receipt binds existing evidence.
        backup_path = output / "after-import-backup/receipt.json"
        backup_path.parent.mkdir()
        write_json(backup_path, dict(format="baostock18-deferred-backup-evidence-v1", scope=CATALOG_ONLY,
                                    source_receipt=str(backup), source_receipt_sha256=sha256(backup)))
        evidence = output / "after-import-fingerprint.json"
        write_json(evidence, dict(format="baostock18-admission-content-evidence-v1", target=str(target),
                                  content_identity=identity))
        result.update(status="PASS", content_identity=identity, verified=identity["measured"],
                      backup_receipt=str(backup_path), backup_receipt_sha256=sha256(backup_path),
                      reference_verification=reference)
        argv = [sys.executable, "-m", "openbb_duckdb.baostock18", "visibility-approve",
                "--production-target", str(target), "--expected-catalog-fingerprint", str(evidence),
                "--window-started-at", config["window_started_at"], "--baseline", config["baseline"],
                "--approved-manifest-hash", config["approved_manifest_hash"],
                "--approved-batch-size", str(config["approved_batch_size"]), "--visibility", "deferred",
                "--backup-receipt", str(backup_path), "--verification-receipt", str(output / "receipt.json"),
                "--expected-clean-rows", str(expected["clean_unadjusted"]),
                "--expected-quarantine", str(expected["quarantine"]), "--expected-batches", str(expected["batches"]),
                "--output", str(output / "approval.json")]
        write_json(output / "approve-command.json", {"argv": argv})
        result["next_command"] = argv
    except BaseException as exc:
        result.update(status="FAIL", error=repr(exc))
        raise
    finally:
        result.update(seconds=time.monotonic() - started, completed_at=utcnow().isoformat(), read_only=True)
        write_json(output / "receipt.json", result)
    return result


@contextmanager
def publisher_lock(path):
    """Match the driver's publisher -> market lock order; refuse another publisher."""
    lock = path.with_suffix(".publisher.lock")
    try:
        fd = os.open(lock, os.O_RDONLY | os.O_NOFOLLOW)
    except OSError as exc:
        raise AdmissionError("Existing publisher.lock required") from exc
    with os.fdopen(fd, "rb") as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
            raise AdmissionError("publisher.lock must be a single regular file")
        try:
            fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise AdmissionError("publisher.lock busy; no other writer may hold it") from exc
        try:
            yield
            if (lock.stat().st_dev, lock.stat().st_ino) != (info.st_dev, info.st_ino):
                raise AdmissionError("publisher.lock identity changed")
        finally:
            fcntl.flock(stream.fileno(), fcntl.LOCK_UN)


def _bound_file(path, digest):
    """Malformed or missing receipt paths are failed gates, never omitted checks."""
    if not isinstance(path, str) or not isinstance(digest, str) or not HASH.fullmatch(digest):
        return False
    try:
        return sha256(Path(path)) == digest
    except OSError as exc:
        raise AdmissionError(f"Missing bound evidence: {path}") from exc


def _verification(admission, receipt_path, expected):
    receipt = load_json(receipt_path)
    observed = load_json(admission.expected_catalog_fingerprint)
    backup = load_json(admission.backup_receipt)
    if (receipt.get("format") != VERIFIED_FORMAT or receipt.get("status") != "PASS"
            or receipt.get("target") != admission.target
            or receipt.get("snapshot_id") != admission.approved_manifest_hash
            or receipt.get("approved_batch_size") != admission.approved_batch_size
            or observed.get("format") != "baostock18-admission-content-evidence-v1"
            or observed.get("target") != admission.target
            or receipt.get("content_identity") != observed.get("content_identity")
            or receipt.get("baseline_sha256") != sha256(Path(admission.baseline))
            or receipt.get("expected_counts") != expected
            or receipt.get("backup_receipt") != admission.backup_receipt
            or receipt.get("backup_receipt_sha256") != sha256(Path(admission.backup_receipt))
            or backup.get("format") != "baostock18-deferred-backup-evidence-v1"
            or backup.get("scope") != CATALOG_ONLY
            or not _bound_file(backup.get("source_receipt"), backup.get("source_receipt_sha256"))
            or receipt.get("verified", {}).get("verification") != "passed"):
        raise AdmissionError("Reference verification receipt must be PASS and bind the same content/approval")
    reference = receipt.get("reference_verification", {})
    if (reference.get("status") != "passed" or reference.get("missing") != 0 or reference.get("mismatch") != 0
            or reference.get("changed") != 0 or reference.get("unresolved_lineage_rows") != 0
            or reference.get("lineage_rows") != receipt["verified"].get("lineage_rows")
            or reference.get("coverage") != COVERAGE or reference.get("contents_copied") is not False
            or not _bound_file(reference.get("inventory"), reference.get("inventory_sha256"))
            or not _bound_file(reference.get("measured_inventory"), reference.get("measured_inventory_sha256"))):
        raise AdmissionError("Reference verification receipt is not PASS")
    check_counts(receipt.get("verified", {}), expected)
    require_unpublished(receipt["content_identity"])
    return receipt


def approve_visibility(admission: ProductionAdmission, verification_receipt, *, expected_clean_rows,
                       expected_quarantine, expected_batches, output, fault_hook=None):
    """Hash admission under an exclusive native DB lock, then publish in a short transaction.

    Native cross-process locking plus rejecting already open in-process handles
    prevents another connection from changing admission between hashing and BEGIN.
    Replay checks the complete post-approval content, independent of file layout.
    """
    expected = expected_counts(expected_clean_rows, expected_quarantine, expected_batches)
    if admission.visibility != "deferred":
        raise AdmissionError("visibility-approve requires explicit visibility deferred")
    path = production_path(admission.target)
    output = Path(output)
    if not output.is_absolute():
        raise AdmissionError("Approval output must be an absolute receipt path")
    with publisher_lock(path), market_lock(path):
        no_wal(path)
        receipt = _verification(admission, verification_receipt, expected)
        binding = dict(target=str(path), snapshot_id=admission.approved_manifest_hash,
                       content_identity_before=receipt["content_identity"], expected_counts=expected,
                       verification_receipt=str(verification_receipt),
                       verification_receipt_sha256=sha256(Path(verification_receipt)),
                       fingerprint_receipt_sha256=sha256(Path(admission.expected_catalog_fingerprint)),
                       backup_receipt_sha256=sha256(Path(admission.backup_receipt)))
        previous = load_json(output) if output.exists() else None
        if previous is not None and (previous.get("format") != APPROVED_FORMAT or previous.get("status") != "PASS"
                         or any(previous.get(k) != v for k, v in binding.items())):
            raise AdmissionError("Approval replay receipt/content mismatch")
        # READ_ONLY first refuses all other native writers without opening our own writer.
        with duckdb.connect(str(path), read_only=True, config=READ_CONFIG) as c:
            if previous is None:
                # Only the short native writer below recomputes the full content.
                require_unpublished(receipt["content_identity"])
        if previous is not None:
            with (duckdb.connect(str(path), read_only=True, config=READ_CONFIG) as c,
                  bounded_sql(c, time.monotonic()+1200)):
                actual = content_identity(c, admission.approved_manifest_hash, admission.approved_batch_size)
                require_identity(actual, previous.get("content_identity_after"))
            no_wal(path)
            return dict(previous, changed=False, read_only=True)
        # An existing same-process read-write handle would defeat the exclusive
        # cross-process lock. Probe read-only above rejects such a handle too.
        with duckdb.connect(str(path), config=READ_CONFIG) as c, bounded_sql(c, time.monotonic()+1200):
            c.execute("SET TimeZone='UTC'")
            actual = content_identity(c, admission.approved_manifest_hash, admission.approved_batch_size)
            require_identity(actual, receipt["content_identity"])
            require_unpublished(actual)
            started = time.monotonic()
            with transaction(c):
                now = utcnow()
                c.execute(f"INSERT INTO {SCHEMA}.basis_admission VALUES (?, 'unadjusted', ?, ?)",
                          [admission.approved_manifest_hash, now, expected_clean_rows])
                c.execute(f"INSERT INTO {SCHEMA}.visibility_event VALUES (1, 'unadjusted', ?, ?)",
                          [admission.approved_manifest_hash, now])
                c.execute(f"INSERT INTO {SCHEMA}.dataset_pointer VALUES ('unadjusted', ?, 1)",
                          [admission.approved_manifest_hash])
                if fault_hook:
                    fault_hook(c)
                # Only three small control tables changed; hash them before COMMIT.
                from openbb_duckdb.baostock18.content_identity import after_publication
                after = after_publication(c, actual)
            transaction_seconds = time.monotonic() - started
        no_wal(path)
        result = dict(binding, format=APPROVED_FORMAT, status="PASS", changed=True,
                      event_id=1, known_at=now.isoformat(), measured=actual["measured"],
                      transaction_seconds=transaction_seconds, content_identity_after=after)
        write_json(output, result)
        return result

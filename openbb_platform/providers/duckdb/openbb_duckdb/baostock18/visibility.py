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
from openbb_duckdb.baostock18.contract import SCHEMA, AdmissionError
from openbb_duckdb.baostock18.manifest import sha256
from openbb_duckdb.baostock18.production import (
    A_SHARE_TABLES,
    READ_CONFIG,
    ProductionAdmission,
    check_basis,
    fingerprint,
    inventory,
    load_json,
    market_lock,
    no_wal,
    production_path,
    reconcile_snapshot,
    timestamp,
    write_json,
)
from openbb_duckdb.baostock18.reference_backup import REFERENCE_SCOPES
from openbb_duckdb.baostock18.store import _check_owned, migrate, transaction, utcnow

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
    """Resume only verification/backup gates, with no catalog writer or pointer change."""
    from openbb_duckdb.baostock18.driver import Driver

    deferred = load_json(deferred_receipt)
    if (config.get("visibility") != "deferred" or deferred.get("status") != DEFERRED
            or deferred.get("config") != config):
        raise AdmissionError("Matching deferred driver receipt/config required")
    target = production_path(config["target"])
    if fingerprint(target) != deferred.get("catalog_fingerprint"):
        raise AdmissionError("Deferred catalog fingerprint mismatch; obtain a reviewed fresh packet")
    expected = expected_counts(config["expected_clean_rows"], config["expected_quarantine"], config["expected_batches"])
    if config["expected_rows"] != expected["rows"]:
        raise AdmissionError("Configured total differs from clean/quarantine counts")
    driver = Driver(dict(config, output=str(output)))
    result = {"format": VERIFIED_FORMAT, "status": "FAIL", "target": str(target),
              "snapshot_id": config["approved_manifest_hash"], "expected_counts": expected,
              "approved_batch_size": config["approved_batch_size"],
              "catalog_fingerprint": deferred["catalog_fingerprint"],
              "baseline_sha256": sha256(Path(config["baseline"])),
              "deferred_receipt": str(deferred_receipt), "deferred_receipt_sha256": sha256(Path(deferred_receipt))}
    try:
        driver.capture_inventory = deferred["capture_reference_verification"]
        common = driver.prepare("after-import", staged=True,
                                checkpoint_receipt=Path(config["output"]) / "staged-checkpoint.json")
        driver.cli("production-preflight", "preflight", common=common)
        verified = driver.cli("production-verify", "verify", common=common)
        check_counts(verified, expected)
        if verified["catalog_fingerprint"] != result["catalog_fingerprint"]:
            raise AdmissionError("Verification catalog fingerprint mismatch")
        sample = driver.inspect("unpublished", published=False)
        driver.reader("reader-unpublished", sample, published=False)
        backup_path = driver.root / "after-import-backup" / "receipt.json"
        backup = load_json(backup_path)
        if fingerprint(target) != result["catalog_fingerprint"]:
            raise AdmissionError("Catalog changed during deferred verification")
        result.update(status="PASS", verified=verified, backup_receipt=str(backup_path),
                      backup_receipt_sha256=sha256(backup_path), reference_verification=backup["reference_verification"])
        argv = [sys.executable, "-m", "openbb_duckdb.baostock18", "visibility-approve", *common,
                "--verification-receipt", str(driver.root / "receipt.json"),
                "--expected-clean-rows", str(expected["clean_unadjusted"]),
                "--expected-quarantine", str(expected["quarantine"]),
                "--expected-batches", str(expected["batches"]),
                "--output", str(driver.root / "approval.json")]
        write_json(driver.root / "approve-command.json", {"argv": argv})
        result["next_command"] = argv
    except BaseException as exc:
        result.update(status="FAIL", error=repr(exc))
        raise
    finally:
        result.update(seconds=time.monotonic() - driver.started, commands=driver.commands,
                      completed_at=utcnow().isoformat(), read_only=True)
        write_json(driver.root / "receipt.json", result)
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


def _verification(admission, receipt_path, expected):
    receipt = load_json(receipt_path)
    observed = load_json(admission.expected_catalog_fingerprint)
    backup = load_json(admission.backup_receipt)
    if (receipt.get("format") != VERIFIED_FORMAT or receipt.get("status") != "PASS"
            or receipt.get("target") != admission.target
            or receipt.get("snapshot_id") != admission.approved_manifest_hash
            or receipt.get("approved_batch_size") != admission.approved_batch_size
            or receipt.get("catalog_fingerprint") != observed.get("fingerprint")
            or receipt.get("catalog_fingerprint") != backup.get("catalog_fingerprint")
            or receipt.get("baseline_sha256") != sha256(Path(admission.baseline))
            or receipt.get("expected_counts") != expected
            or receipt.get("backup_receipt") != admission.backup_receipt
            or receipt.get("backup_receipt_sha256") != sha256(Path(admission.backup_receipt))
            or backup.get("scope") not in REFERENCE_SCOPES
            or receipt.get("reference_verification") != backup.get("reference_verification")
            or receipt.get("verified", {}).get("verification") != "passed"):
        raise AdmissionError("Reference verification receipt must be PASS and bind the same fingerprint/approval")
    reference = receipt.get("reference_verification", {})
    if reference.get("status") != "passed" or reference.get("missing") != 0 or reference.get("mismatch") != 0:
        raise AdmissionError("Reference verification receipt is not PASS")
    check_counts(receipt.get("verified", {}), expected)
    return receipt, observed


def _inspect(connection, admission, expected):
    """Recheck the foreign baseline and admission even on an idempotent replay."""
    baseline = load_json(admission.baseline)
    if (baseline.get("format") != "baostock18-production-baseline-v1"
            or inventory(connection) != sorted(baseline["tables"])
            or {t: connection.execute(f'SELECT count(*) FROM a_share."{t}"').fetchone()[0]
                for t in A_SHARE_TABLES} != baseline["a_share_counts"]):
        raise AdmissionError("Approval baseline changed")
    migrate(connection, _check_owned(connection, sorted(tuple(x) for x in baseline["tables"])), read_only=True)
    check_basis(connection, True)
    measured = reconcile_snapshot(connection, admission.approved_manifest_hash, admission.approved_batch_size)
    check_counts(measured, expected)
    return measured


def approve_visibility(admission: ProductionAdmission, verification_receipt, *, expected_clean_rows,
                       expected_quarantine, expected_batches, output, fault_hook=None):
    """Recheck all gates before a short transaction; same receipt path replays read-only.

    The immutable approval receipt binds the post-commit fingerprint for retries.
    A changed catalog (even after approval) cannot reuse it. If commit succeeds but
    receipt persistence fails, fail closed and require operator reconciliation.
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
        receipt, observed = _verification(admission, verification_receipt, expected)
        binding = dict(target=str(path), snapshot_id=admission.approved_manifest_hash,
                       catalog_fingerprint_before=receipt["catalog_fingerprint"], expected_counts=expected,
                       verification_receipt=str(verification_receipt),
                       verification_receipt_sha256=sha256(Path(verification_receipt)),
                       fingerprint_receipt_sha256=sha256(Path(admission.expected_catalog_fingerprint)),
                       backup_receipt_sha256=sha256(Path(admission.backup_receipt)))
        if output.exists():
            previous = load_json(output)
            if (previous.get("format") != APPROVED_FORMAT or previous.get("status") != "PASS"
                    or any(previous.get(k) != v for k, v in binding.items())
                    or fingerprint(path) != previous.get("catalog_fingerprint_after")):
                raise AdmissionError("Approval replay receipt/fingerprint mismatch")
            with duckdb.connect(str(path), read_only=True, config=READ_CONFIG) as c:
                _inspect(c, admission, expected)
                event = c.execute(
                    f"SELECT p.snapshot_id,p.event_id,e.known_at FROM {SCHEMA}.dataset_pointer p "
                    f"JOIN {SCHEMA}.visibility_event e USING(event_id) WHERE p.adjustment_basis='unadjusted'"
                ).fetchone()
            if (event != (admission.approved_manifest_hash, previous["event_id"], timestamp(previous["known_at"]))
                    or fingerprint(path) != previous["catalog_fingerprint_after"]):
                raise AdmissionError("Approval replay pointer/fingerprint mismatch")
            return dict(previous, changed=False, read_only=True)
        # All expensive admission/reference/backup/reconciliation checks precede BEGIN.
        admission.inspect()
        with duckdb.connect(str(path), read_only=True, config=READ_CONFIG) as c:
            measured = _inspect(c, admission, expected)
            if any(c.execute(f"SELECT count(*) FROM {SCHEMA}.{t}").fetchone()[0]
                   for t in ("basis_admission", "visibility_event", "dataset_pointer")):
                raise AdmissionError("Deferred approval requires zero admission/events/pointers")
        if fingerprint(path) != observed["fingerprint"]:
            raise AdmissionError("Catalog changed before approval writer")
        with duckdb.connect(str(path), config=READ_CONFIG) as c:
            c.execute("SET TimeZone='UTC'")
            if fingerprint(path) != observed["fingerprint"]:
                raise AdmissionError("Catalog changed while opening approval writer")
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
            transaction_seconds = time.monotonic() - started
        no_wal(path)
        result = dict(binding, format=APPROVED_FORMAT, status="PASS", changed=True,
                      event_id=1, known_at=now.isoformat(), measured=measured,
                      transaction_seconds=transaction_seconds, catalog_fingerprint_after=fingerprint(path))
        write_json(output, result)
        return result

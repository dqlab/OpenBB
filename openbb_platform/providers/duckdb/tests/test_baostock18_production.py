"""Real local production-shaped catalogs; no production path/service access."""

# Only fixture/internal schema identifiers enter SQL.
# ruff: noqa: S608

import hashlib
import json
import os
import shutil
import subprocess
import sys
from dataclasses import replace
from datetime import date, datetime, timedelta
from pathlib import Path

import duckdb
import pytest
from openbb_duckdb.baostock18 import import_candidate, read_bars, set_visible
from openbb_duckdb.baostock18.__main__ import main
from openbb_duckdb.baostock18.contract import SCHEMA, AdmissionError
from openbb_duckdb.baostock18.manifest import encoded, sha256
from openbb_duckdb.baostock18.production import (
    A_SHARE_TABLES,
    CATALOG_NAME,
    ProductionAdmission,
    capture_fingerprint,
    create_backup,
    fingerprint,
    inventory,
    market_lock,
    preflight,
    verify_production,
    write_json,
)
from openbb_duckdb.baostock18.store import utcnow

from .test_baostock18_admission import make_candidate, query, read


def make_catalog(directory):
    """Copy a small seed with all eight legacy tables into the isolated target."""
    directory.mkdir()
    seed = directory.parent / (directory.name + "-seed.duckdb")
    target = directory / CATALOG_NAME
    with duckdb.connect(str(seed)) as c:
        c.execute("CREATE SCHEMA a_share")
        for table in A_SHARE_TABLES:
            c.execute(f"CREATE TABLE a_share.{table} AS SELECT 42 AS sentinel")
        c.execute("CHECKPOINT")
    shutil.copyfile(seed, target)
    target.with_suffix(".market.lock").touch()
    return target


def authorize(root, target, manifest, label, *, backup=True, baseline=None):
    """Perform an actual local CHECKPOINT, fresh observation and verified backup."""
    evidence = root / label
    evidence.mkdir()
    window = (utcnow() - timedelta(seconds=1)).isoformat()
    with market_lock(target), duckdb.connect(str(target)) as c:
        c.execute("CHECKPOINT")
    checkpoint = {"format": "baostock18-checkpoint-v1", "target": str(target),
                  "window_started_at": window, "checkpoint_completed": True,
                  "completed_at": utcnow().isoformat(), "catalog_fingerprint": fingerprint(target)}
    write_json(evidence / "checkpoint.json", checkpoint)
    write_json(evidence / "fingerprint.json", capture_fingerprint(target, window))
    baseline_path = Path(baseline) if baseline else root / "baseline.json"
    if not baseline_path.exists():
        with duckdb.connect(str(target), read_only=True) as c:
            tables = inventory(c)
        write_json(baseline_path, {"format": "baostock18-production-baseline-v1",
                                   "tables": tables, "a_share_counts": {t: 1 for t in A_SHARE_TABLES}})
    admission = ProductionAdmission(
        target=str(target), expected_catalog_fingerprint=str(evidence / "fingerprint.json"),
        window_started_at=window, baseline=str(baseline_path),
        approved_manifest_hash=hashlib.sha256(encoded(manifest).encode()).hexdigest(),
        approved_batch_size=50_000, backup_receipt=str(evidence / "backup" / "receipt.json"),
    )
    if backup:
        create_backup(admission, evidence / "backup", evidence / "checkpoint.json")
    return admission


@pytest.fixture
def prepared(tmp_path):
    """Prepare explicit approval evidence and a checkpointed tiny copied catalog."""
    manifest = make_candidate(tmp_path / "candidate.duckdb")
    target = make_catalog(tmp_path / "catalog")
    approval = authorize(tmp_path, target, manifest, "first")
    return manifest, target, approval


@pytest.mark.parametrize("field,value", [
    ("size", 1), ("mtime_ns", 1), ("device", -1), ("inode", -1), ("header_sha256", "0" * 64),
])
def test_each_fingerprint_field_must_match(prepared, field, value):
    """A mismatch in any field aborts with byte-for-byte unchanged target."""
    manifest, target, approval = prepared
    evidence = Path(approval.expected_catalog_fingerprint)
    record = json.loads(evidence.read_text())
    record["fingerprint"][field] = value
    evidence.write_text(json.dumps(record))
    before = fingerprint(target), sha256(target)
    with pytest.raises(AdmissionError, match="fingerprint mismatch"):
        import_candidate(manifest, target, production=approval)
    assert (fingerprint(target), sha256(target)) == before


@pytest.mark.parametrize("change,error", [
    ({"approved_manifest_hash": None}, "Approved"),
    ({"approved_batch_size": None}, "Approved"),
    ({"backup_receipt": None}, "evidence"),
    ({"target": "relative/dq_quant_invest_data.duckdb"}, "absolute"),
    ({"target": "/wrong-name.duckdb"}, "DB name"),
])
def test_missing_or_invalid_approval_aborts(prepared, change, error):
    """No default approval or path normalization may grant production access."""
    _, target, approval = prepared
    before = fingerprint(target), sha256(target)
    with pytest.raises(AdmissionError, match=error):
        preflight(replace(approval, **change))
    assert (fingerprint(target), sha256(target)) == before


def test_missing_flag_and_relative_raw_target(prepared, monkeypatch):
    """Default writer and CLI cannot select production implicitly."""
    manifest, target, approval = prepared
    before = sha256(target)
    with pytest.raises(AdmissionError, match="production-target"):
        import_candidate(manifest, target)
    monkeypatch.setattr(sys, "argv", ["baostock18", "production-import"])
    with pytest.raises(SystemExit) as exc:
        main()
    assert exc.value.code == 2
    monkeypatch.chdir(target.parent)
    with pytest.raises(AdmissionError, match="absolute"):
        import_candidate(manifest, target.name, production=approval)
    assert sha256(target) == before


@pytest.mark.parametrize("what,error", [
    ("receipt_missing", "evidence"), ("manifest_hash", "manifest hash"),
    ("payload_hash", "payload hash"), ("binding", "bind current"),
    ("checkpoint", "CHECKPOINT"), ("baseline", "counts differ"),
])
def test_bad_backup_or_baseline_aborts_without_write(prepared, what, error):
    """Missing, tampered or stale backup evidence never opens a writer."""
    manifest, target, approval = prepared
    receipt_path = Path(approval.backup_receipt)
    receipt = json.loads(receipt_path.read_text())
    if what == "receipt_missing":
        receipt_path.unlink()
    elif what == "manifest_hash":
        Path(receipt["manifest"]).write_text("{}")
    elif what == "payload_hash":
        payload = json.loads(Path(receipt["manifest"]).read_text())["files"][0]["backup"]
        with Path(payload).open("r+b") as stream:
            stream.write(b"broken")
    elif what == "baseline":
        baseline = json.loads(Path(approval.baseline).read_text())
        baseline["a_share_counts"]["attempt"] = 2
        Path(approval.baseline).write_text(json.dumps(baseline))
    else:
        if what == "binding":
            receipt["catalog_fingerprint"]["inode"] += 1
        else:
            receipt["checkpoint"]["checkpoint_completed"] = False
        receipt_path.write_text(json.dumps(receipt))
    before = fingerprint(target), sha256(target)
    with pytest.raises(AdmissionError, match=error):
        import_candidate(manifest, target, production=approval)
    assert (fingerprint(target), sha256(target)) == before


def test_preflight_is_read_only_and_wal_or_lock_abort(prepared):
    """A successful preflight and rejected WAL/busy/missing locks leave bytes intact."""
    _, target, approval = prepared
    before = fingerprint(target), sha256(target)
    assert preflight(approval)["preflight"] == "passed"
    assert (fingerprint(target), sha256(target)) == before
    wal = target.with_name(target.name + ".wal")
    wal.write_bytes(b"not checkpointed")
    with pytest.raises(AdmissionError, match="WAL"):
        preflight(approval)
    wal.unlink()  # Synthetic invalid WAL only; never a real database recovery action.
    with market_lock(target), pytest.raises(AdmissionError, match="busy"):
        preflight(approval)
    target.with_suffix(".market.lock").unlink()
    with pytest.raises(AdmissionError, match="market.lock required"):
        preflight(approval)
    assert (fingerprint(target), sha256(target)) == before


def test_native_uncooperative_writer_is_refused(prepared):
    """DuckDB's own lock also rejects an external writer ignoring market.lock."""
    _, target, approval = prepared
    code = "import duckdb,sys; c=duckdb.connect(sys.argv[1]); print('ready',flush=True); sys.stdin.readline()"
    with subprocess.Popen([sys.executable, "-c", code, str(target)], stdin=subprocess.PIPE,  # noqa: S603
                          stdout=subprocess.PIPE, text=True) as process:
        try:
            assert process.stdout.readline().strip() == "ready"
            with pytest.raises(duckdb.Error):
                preflight(approval)
        finally:
            process.communicate("done\n", timeout=15)


@pytest.mark.parametrize("kind", ["hardlink", "symlink", "directory", "policy", "missing"])
def test_target_must_be_single_database_file(prepared, tmp_path, kind):
    """Neither aliases nor access policy bypasses are admitted."""
    _, target, approval = prepared
    if kind == "hardlink":
        os.link(target, tmp_path / "alias")
    elif kind == "symlink":
        alias = tmp_path / CATALOG_NAME
        alias.symlink_to(target)
        approval = replace(approval, target=str(alias))
    elif kind == "directory":
        alias = tmp_path / CATALOG_NAME
        alias.mkdir()
        approval = replace(approval, target=str(alias))
    elif kind == "policy":
        target.with_name(target.name + ".access-windows").mkdir()
    else:
        target.unlink()
    with pytest.raises(AdmissionError):
        preflight(approval)


def test_old_window_changed_manifest_batch_and_mixed_rehearsal_aborted(prepared):
    """Fingerprint freshness and explicit approval remain mandatory on API calls."""
    manifest, target, approval = prepared
    record_path = Path(approval.expected_catalog_fingerprint)
    record = json.loads(record_path.read_text())
    record["observed_at"] = (utcnow() - timedelta(days=1)).isoformat()
    record_path.write_text(json.dumps(record))
    with pytest.raises(AdmissionError, match="after window"):
        preflight(approval)
    for kwargs in ({"batch_size": 100_000}, {"rehearsal_receipt": "clone.json"}):
        with pytest.raises(AdmissionError):
            import_candidate(manifest, target, production=approval, **kwargs)
    with pytest.raises(AdmissionError, match="Manifest hash"):
        import_candidate({**manifest, "contract_id": "changed"}, target, production=approval)


def test_production_interrupt_resume_quarantine_and_replay(tmp_path):
    """Keep the failed batch invisible, resume, quarantine 15838, replay idempotently."""
    manifest = make_candidate(tmp_path / "candidate.duckdb", rows=50_001, quarantined=15_838)
    target = make_catalog(tmp_path / "catalog")
    approval = authorize(tmp_path, target, manifest, "first")

    def interrupt(c, number):
        if number == 2:
            assert c.execute(f"SELECT count(*) FROM {SCHEMA}.literal_version").fetchone()[0] == 65_839
            raise KeyboardInterrupt("rollback this batch")

    with pytest.raises(KeyboardInterrupt):
        import_candidate(manifest, target, production=approval, fault_hook=interrupt)
    assert query(target, f"SELECT count(*) FROM {SCHEMA}.literal_version") == [(50_000,)]
    assert query(target, f"SELECT count(*) FROM {SCHEMA}.batch_receipt") == [(1,)]
    assert read(target, basis="unadjusted") == []
    with pytest.raises(AdmissionError, match="fingerprint mismatch"):
        import_candidate(manifest, target, production=approval)
    approval = authorize(tmp_path, target, manifest, "resume")
    receipt = import_candidate(manifest, target, production=approval)
    assert receipt["rows"] == 65_839 and receipt["quarantine"] == 15_838 and receipt["new_batches"] == 1
    assert read(target, basis="unadjusted") == []
    approval = authorize(tmp_path, target, manifest, "verify")
    assert verify_production(approval)["clean_unadjusted"] == 50_001
    assert import_candidate(manifest, target, production=approval)["new_batches"] == 0
    approval = authorize(tmp_path, target, manifest, "visible")
    event = set_visible(target, receipt["snapshot_id"], basis="unadjusted",
                        expected_clean_rows=50_001, production=approval)
    assert len(read(target, basis="unadjusted")) == 50_001
    assert query(target, f"SELECT count(*) FROM {SCHEMA}.consumable") == [(50_001,)]
    assert len(read(target, basis="unadjusted", as_of=utcnow() - timedelta(days=1))) == 0
    assert len(read(target, basis="unadjusted", as_of=datetime.fromisoformat(
        event["known_at"]))) == 50_001
    approval = authorize(tmp_path, target, manifest, "replay")
    assert import_candidate(manifest, target, production=approval)["new_batches"] == 0
    approval = authorize(tmp_path, target, manifest, "repeat-pointer")
    assert not set_visible(target, receipt["snapshot_id"], basis="unadjusted",
                           expected_clean_rows=50_001, production=approval)["changed"]
    for table in A_SHARE_TABLES:
        assert query(target, f"SELECT * FROM a_share.{table}") == [(42,)]
    for kwargs in ({"basis": "forward_dynamic_snapshot"},
                   {"basis": "unadjusted", "fixed_qfq_basis_date": date(2026, 10, 15)}):
        with pytest.raises(AdmissionError):
            read_bars(target, symbols=["SH.600000"], start_date=date(2000, 1, 1),
                      end_date=date(2000, 1, 2), **kwargs)


def test_pointer_failure_rolls_back_admission_event_and_pointer(prepared, tmp_path):
    """Failure after pointer mutation rolls back its complete short transaction."""
    manifest, target, approval = prepared
    receipt = import_candidate(manifest, target, production=approval)
    approval = authorize(tmp_path, target, manifest, "visible")

    def fail(c):
        assert c.execute(f"SELECT count(*) FROM {SCHEMA}.dataset_pointer").fetchone()[0] == 1
        raise RuntimeError("simulated visibility failure")

    with pytest.raises(RuntimeError, match="visibility failure"):
        set_visible(target, receipt["snapshot_id"], basis="unadjusted", expected_clean_rows=1,
                    production=approval, fault_hook=fail)
    for table in ("basis_admission", "visibility_event", "dataset_pointer"):
        assert query(target, f"SELECT count(*) FROM {SCHEMA}.{table}") == [(0,)]
    assert read(target, basis="unadjusted") == []


def test_unauthorized_basis_and_changed_migration_rejected(prepared, tmp_path):
    """Existing admission schema corruption is refused before a new writer."""
    manifest, target, approval = prepared
    import_candidate(manifest, target, production=approval)
    with duckdb.connect(str(target)) as c:
        c.execute(f"INSERT INTO {SCHEMA}.visibility_event VALUES (1,'forward',NULL,now())")
    approval = authorize(tmp_path, target, manifest, "basis", backup=False)
    before = sha256(target)
    with pytest.raises(AdmissionError, match="unauthorized basis"):
        preflight(approval)
    assert sha256(target) == before
    with duckdb.connect(str(target)) as c:
        c.execute(f"DELETE FROM {SCHEMA}.visibility_event")
        c.execute(f"UPDATE {SCHEMA}.schema_version SET sha256='changed'")
    approval = authorize(tmp_path, target, manifest, "checksum", backup=False)
    before = sha256(target)
    with pytest.raises(AdmissionError, match="checksum"):
        preflight(approval)
    assert sha256(target) == before


def test_missing_quality_prevents_production_visibility(tmp_path):
    """Read-only visibility preflight reconciles evidence before a pointer transaction."""
    manifest = make_candidate(tmp_path / "candidate.duckdb", quarantined=1)
    target = make_catalog(tmp_path / "catalog")
    approval = authorize(tmp_path, target, manifest, "first")
    receipt = import_candidate(manifest, target, production=approval)
    with duckdb.connect(str(target)) as c:
        c.execute(f"DELETE FROM {SCHEMA}.quality")
    approval = authorize(tmp_path, target, manifest, "visibility")
    before = fingerprint(target), sha256(target)
    with pytest.raises(AdmissionError, match="quality"):
        set_visible(target, receipt["snapshot_id"], basis="unadjusted", expected_clean_rows=1, production=approval)
    assert (fingerprint(target), sha256(target)) == before
    assert read(target, basis="unadjusted") == []

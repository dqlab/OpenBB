"""Reduced scope is opt-in and fails closed before any production writer."""

import inspect
import json
import sys
from dataclasses import replace
from pathlib import Path

import duckdb
import pytest
from openbb_duckdb.baostock18 import import_candidate, production, set_visible
from openbb_duckdb.baostock18.__main__ import main
from openbb_duckdb.baostock18.backup import backup_items
from openbb_duckdb.baostock18.contract import AdmissionError
from openbb_duckdb.baostock18.manifest import prepare_manifest, sha256
from openbb_duckdb.baostock18.production import create_backup, fingerprint, preflight, write_json
from openbb_duckdb.baostock18.reference_backup import REDUCED_SCOPE, bounded_sql, freeze_reference_inventory

from .test_baostock18_admission import make_candidate
from .test_baostock18_production import authorize, make_catalog


@pytest.fixture
def reduced(tmp_path):
    """Create frozen catalog projections and reference bytes in an isolated tree."""
    manifest = make_candidate(tmp_path / "candidate.duckdb")
    target = make_catalog(tmp_path / "catalog")
    reference = tmp_path / "data.parquet"
    reference.write_bytes(b"immutable synthetic reference payload")
    with duckdb.connect(str(target)) as connection:
        connection.execute("CREATE TABLE main.refs(path VARCHAR, sha256 VARCHAR)")
        connection.execute("INSERT INTO main.refs VALUES (?,?)", [str(reference), sha256(reference)])
    approval = authorize(tmp_path, target, manifest, "reduced", backup=False)
    with duckdb.connect(str(target), read_only=True) as connection:
        columns = connection.execute(
            "SELECT c.table_schema,c.table_name,c.column_name FROM information_schema.columns c "
            "JOIN duckdb_tables() t ON t.schema_name=c.table_schema AND t.table_name=c.table_name "
            "WHERE NOT t.internal ORDER BY 1,2,3"
        ).fetchall()
    contract = tmp_path / "reference-contract.json"
    write_json(contract, {
        "format": "baostock18-catalog-reference-contract-v1", "catalog_fingerprint": fingerprint(target),
        "columns": columns, "rules": [{"schema": "main", "table": "refs", "path_column": "path",
                                       "hash_column": "sha256", "root": str(tmp_path)}],
    })
    inventory = tmp_path / "references.jsonl"
    inventory.write_text(json.dumps({"path": str(reference), "size": reference.stat().st_size,
                                      "sha256": sha256(reference)}) + "\n")
    destination = tmp_path / "backup"
    kwargs = {"backup_scope": REDUCED_SCOPE, "reference_manifest": inventory,
              "reference_contract": contract, "max_seconds": 60}
    checkpoint = Path(approval.expected_catalog_fingerprint).with_name("checkpoint.json")
    return approval, destination, checkpoint, kwargs, reference


def test_full_is_default_and_reduced_requires_explicit_scope(reduced):
    """Neither a reference manifest nor a short time budget opts into less backup."""
    approval, destination, checkpoint, kwargs, _ = reduced
    assert inspect.signature(create_backup).parameters["backup_scope"].default == "full"
    kwargs.pop("backup_scope")
    with pytest.raises(AdmissionError, match="explicit --backup-scope"):
        create_backup(approval, destination, checkpoint, **kwargs)
    assert not destination.exists()
    receipt = create_backup(approval, destination, checkpoint)
    assert receipt["scope"] == "full"


def test_freeze_inventory_compares_catalog_hash(reduced):
    """The production inventory helper hashes real bytes and honors pinned hashes."""
    import time

    approval, destination, _, kwargs, reference = reduced
    contract = json.loads(kwargs["reference_contract"].read_text())
    frozen = destination.parent / "frozen.jsonl"
    with duckdb.connect(approval.target, read_only=True) as connection:
        result = freeze_reference_inventory(connection, contract, frozen, fingerprint(approval.target),
                                             time.monotonic() + 60)
        assert result["count"] == 1
        assert frozen.read_text() == kwargs["reference_manifest"].read_text()
        reference.write_bytes(b"changed")
        with pytest.raises(AdmissionError, match="catalog hash mismatch"):
            freeze_reference_inventory(connection, contract, destination.parent / "bad-frozen.jsonl",
                                       fingerprint(approval.target), time.monotonic() + 60)


def test_sql_deadline_interrupts_execute_before_any_fetch():
    """A blocking aggregate cannot hide the deadline until its first result."""
    import time

    started = time.monotonic()
    with (duckdb.connect() as connection,
          pytest.raises(AdmissionError, match="SQL deadline exceeded"), bounded_sql(connection, started + 0.1)):
        connection.execute("SELECT sum(i) FROM range(1000000000000) r(i)")
    assert time.monotonic() - started < 10


def test_reduced_receipt_and_real_restore(reduced):
    """Real copy/restore hashes and eight counts pass; reference contents stay put."""
    approval, destination, checkpoint, kwargs, reference = reduced
    before = sha256(Path(approval.target))
    receipt = create_backup(approval, destination, checkpoint, **kwargs)
    assert receipt["scope"] == REDUCED_SCOPE
    assert receipt["reference_verification"]["count"] == 1
    assert receipt["reference_verification"]["missing"] == receipt["reference_verification"]["mismatch"] == 0
    assert receipt["uncopied_reference_bytes"] == reference.stat().st_size
    assert receipt["catalog_sha256_before"] == receipt["catalog_sha256_after"] == before
    assert receipt["restore_check"]["sha256"] == before
    assert receipt["restore_check"]["admission_physical_tables"] == 0
    assert receipt["restore_check"]["bounded_query_rows"] == 1
    assert receipt["wal"] == {"before": "absent", "after": "absent"}
    assert str(reference) not in {i["source"] for i in backup_items(destination / "manifest.json")}
    assert preflight(replace(approval, backup_receipt=str(destination / "receipt.json")))["preflight"] == "passed"


def test_reduced_real_cli(reduced, monkeypatch):
    """The actual parser exposes opt-in and persists the same verified receipt."""
    approval, destination, checkpoint, kwargs, _ = reduced
    output = destination.parent / "cli-receipt.json"
    monkeypatch.setattr(sys, "argv", [
        "baostock18", "production-backup", "--production-target", approval.target,
        "--expected-catalog-fingerprint", approval.expected_catalog_fingerprint,
        "--window-started-at", approval.window_started_at, "--baseline", approval.baseline,
        "--approved-manifest-hash", approval.approved_manifest_hash,
        "--approved-batch-size", str(approval.approved_batch_size),
        "--checkpoint-receipt", str(checkpoint), "--destination", str(destination),
        "--backup-scope", REDUCED_SCOPE, "--reference-manifest", str(kwargs["reference_manifest"]),
        "--reference-contract", str(kwargs["reference_contract"]),
        "--max-backup-seconds", "60", "--output", str(output),
    ])
    main()
    assert json.loads(output.read_text())["scope"] == REDUCED_SCOPE


def test_real_restore_baseline_failure(reduced):
    """An actual restored DuckDB with the wrong expected count is refused."""
    import time

    approval, destination, _, _, _ = reduced
    baseline = json.loads(Path(approval.baseline).read_text())
    baseline["a_share_counts"]["bar_revision"] += 1
    with pytest.raises(AdmissionError, match="Restore a_share/admission counts"):
        production.restore_catalog(Path(approval.target), destination, baseline,
                                   sha256(Path(approval.target)), time.monotonic() + 60)


def test_reduced_production_import_staged_restore_verify_and_visibility(reduced):
    """Current staged backup retains every original production visibility gate."""
    approval, destination, checkpoint, kwargs, reference = reduced
    root = destination.parent
    create_backup(approval, destination, checkpoint, **kwargs)
    approval = replace(approval, backup_receipt=str(destination / "receipt.json"))
    manifest = prepare_manifest(root / "candidate.duckdb", end_date="2300-01-01")
    imported = import_candidate(manifest, approval.target, batch_size=approval.approved_batch_size, production=approval)
    (root / "raw").mkdir()
    raw = root / "raw/capture.json.gz"
    raw.write_bytes(b"synthetic capture object")
    next_approval = authorize(root, Path(approval.target), manifest, "after-import", backup=False)
    with duckdb.connect(approval.target, read_only=True) as connection:
        columns = connection.execute(
            "SELECT c.table_schema,c.table_name,c.column_name FROM information_schema.columns c "
            "JOIN duckdb_tables() t ON t.schema_name=c.table_schema AND t.table_name=c.table_name "
            "WHERE NOT t.internal ORDER BY 1,2,3"
        ).fetchall()
    contract = json.loads(kwargs["reference_contract"].read_text())
    contract.update(columns=columns, catalog_fingerprint=fingerprint(approval.target), json_root=str(root))
    contract["rules"].append({"schema": "baostock18_admission", "table": "row_lineage",
                              "path_column": "source_path", "hash_column": None, "root": str(root)})
    contract_path = root / "staged-contract.json"
    write_json(contract_path, contract)
    inventory = root / "staged-references.jsonl"
    inventory.write_text("".join(json.dumps({"path": str(p), "size": p.stat().st_size, "sha256": sha256(p)})
                                 + "\n" for p in sorted([reference, raw, root / "candidate.duckdb"])))
    staged = root / "staged-backup"
    staged_kwargs = {**kwargs, "reference_contract": contract_path, "reference_manifest": inventory,
                     "restore_admission_state": "staged_unpublished"}
    receipt = create_backup(next_approval, staged,
                            Path(next_approval.expected_catalog_fingerprint).with_name("checkpoint.json"),
                            **staged_kwargs)
    assert receipt["restore_check"]["visibility_counts"] == {
        "basis_admission": 0, "visibility_event": 0, "dataset_pointer": 0}
    next_approval = replace(next_approval, backup_receipt=str(staged / "receipt.json"))
    assert production.verify_production(next_approval)["verification"] == "passed"
    assert set_visible(next_approval.target, imported["snapshot_id"], basis="unadjusted",
                       expected_clean_rows=1, production=next_approval)["changed"]


@pytest.mark.parametrize("problem", ["missing", "hash", "omitted", "schema", "deadline", "restore"])
def test_reduced_failure_aborts_without_receipt_or_catalog_change(reduced, problem, monkeypatch):
    """Missing, corrupt, omitted, unknown, expired or unrestorable evidence aborts."""
    approval, destination, checkpoint, kwargs, reference = reduced
    before = fingerprint(approval.target), sha256(Path(approval.target))
    if problem == "missing":
        reference.unlink()
    elif problem == "hash":
        reference.write_bytes(b"modified payload")
    elif problem == "omitted":
        kwargs["reference_manifest"].write_text("")
    elif problem == "schema":
        value = json.loads(kwargs["reference_contract"].read_text())
        value["columns"] = []
        kwargs["reference_contract"].write_text(json.dumps(value))
    elif problem == "deadline":
        kwargs["max_seconds"] = 1e-12
    else:
        def fail(*args, **kwargs):
            raise AdmissionError("Restore read-only verification failed")
        monkeypatch.setattr(production, "restore_catalog", fail)
    with pytest.raises(AdmissionError):
        create_backup(approval, destination, checkpoint, **kwargs)
    assert not (destination / "receipt.json").exists()
    assert (fingerprint(approval.target), sha256(Path(approval.target))) == before

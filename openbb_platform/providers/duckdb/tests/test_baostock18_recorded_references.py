"""Recorded references remain explicit and fail closed with measurable sampling."""

import inspect
import json
import time
from dataclasses import replace

import duckdb
import pytest
from openbb_duckdb.baostock18 import recorded_references as recorded
from openbb_duckdb.baostock18.contract import AdmissionError
from openbb_duckdb.baostock18.driver import reference_contract
from openbb_duckdb.baostock18.manifest import sha256
from openbb_duckdb.baostock18.production import create_backup, preflight
from openbb_duckdb.baostock18.reference_backup import RECORDED_SCOPE, freeze_reference_inventory, verify_references

from .test_baostock18_reference_backup import reduced as reduced_fixture

reduced = reduced_fixture


def fixture_contract(c, root):
    """Bind a synthetic file table including a real recorded size."""
    return reference_contract(c, {}, reference_root=str(root), admission_root=str(root), json_root=[str(root)])


def test_missing_hash_recomputed_and_receipt(tmp_path):
    """No recorded hash must cause byte I/O and an explicit counted origin."""
    f = tmp_path / "file"
    f.write_bytes(b"actual payload")
    with duckdb.connect() as c:
        c.execute("CREATE TABLE refs(storage_key VARCHAR,sha256 VARCHAR,size_bytes BIGINT)")
        c.execute("INSERT INTO refs VALUES (?,NULL,?)", [str(f), f.stat().st_size])
        contract = fixture_contract(c, tmp_path)
        result = freeze_reference_inventory(c, contract, tmp_path / "inventory.jsonl", {},
                                           time.monotonic() + 30, scope=RECORDED_SCOPE)
    assert result["missing_hash_recomputed_files"] == result["recomputed_files"] == 1
    assert result["sample_recomputed_files"] == 1
    assert result["recorded_hash_comparisons"] == result["unread_bytes"] == 0
    assert result["recomputed_bytes"] == result["bytes"] == f.stat().st_size
    assert result["missing"] == result["size_mismatch"] == result["hash_mismatch"] == 0
    assert json.loads((tmp_path / "inventory.jsonl").read_text())["sha256"] == sha256(f)


@pytest.mark.parametrize("failure", ["size", "sample", "missing", "recorded_hash"])
def test_recorded_failure_aborts(reduced, failure):
    """No success backup receipt or admission is possible on any disagreement."""
    approval, destination, checkpoint, kwargs, reference = reduced
    if failure == "size":
        reference.write_bytes(b"wrong size")
    elif failure == "sample":
        reference.write_bytes(b"x" * reference.stat().st_size)
    elif failure == "missing":
        reference.unlink()
    else:
        item = json.loads(kwargs["reference_manifest"].read_text())
        item["sha256"] = "0" * 64
        kwargs["reference_manifest"].write_text(json.dumps(item) + "\n")
    with pytest.raises(AdmissionError):
        create_backup(approval, destination, checkpoint, **{**kwargs, "backup_scope": RECORDED_SCOPE})
    result = json.loads((destination / "reference-verification-summary.json").read_text())
    key = {"size": "size_mismatch", "sample": "hash_mismatch", "missing": "missing",
           "recorded_hash": "hash_mismatch"}[failure]
    assert result["status"] == "failed" and result[key] == 1
    assert not (destination / "receipt.json").exists()


def test_recorded_backup_receipt_preflight_and_default(reduced):
    """The real backup/read-only restore passes; forged or incomplete counts fail."""
    approval, destination, checkpoint, kwargs, _ = reduced
    receipt = create_backup(approval, destination, checkpoint, **{**kwargs, "backup_scope": RECORDED_SCOPE})
    result = receipt["reference_verification"]
    assert receipt["scope"] == RECORDED_SCOPE
    assert result["mode"] == RECORDED_SCOPE
    assert result["count"] == result["recorded_hash_comparisons"] == result["sample_recomputed_files"] == 1
    assert result["recomputed_files"] == 1 and result["missing_hash_recomputed_files"] == 0
    assert result["unread_bytes"] == 0
    assert result["sample_seed"] == recorded.SAMPLE_SEED
    assert receipt["restore_check"]["status"] == "passed"
    assert inspect.signature(create_backup).parameters["backup_scope"].default == "full"
    approval = replace(approval, backup_receipt=str(destination / "receipt.json"))
    assert preflight(approval)["preflight"] == "passed"
    del receipt["reference_verification"]["unread_bytes"]
    (destination / "receipt.json").write_text(json.dumps(receipt))
    with pytest.raises(AdmissionError, match="incomplete/inconsistent"):
        preflight(approval)


def test_sampling_limits_and_root_layer_coverage(tmp_path):
    """Selection is stable under reordered input and covers every defined stratum."""
    one, two = recorded.Sampler(), recorded.Sampler()
    paths = [(tmp_path / str(i % 2) / ("x/leaf.parquet" if i % 3 else "leaf.json") / str(i),
              tmp_path / str(i % 2)) for i in range(1200)]
    for path, root in paths:
        one.add(path, root)
    for path, root in reversed(paths):
        two.add(path, root)
    assert one.selected() == two.selected() and len(one.selected()) == 1000
    assert {p for _, p in one.strata.values()} <= one.selected()
    assert [recorded.sample_count(n) for n in (0, 5, 1200, 300000, 2000000)] == [0, 5, 1000, 1500, 5000]


def test_unsampled_bytes_not_read_but_size_checked(tmp_path, monkeypatch):
    """At scale, recorded files outside the sample get metadata checks only."""
    paths = []
    for i in range(1002):
        p = tmp_path / f"file-{i:04d}"
        p.write_bytes(b"payload")
        paths.append(p)
    with duckdb.connect() as c:
        c.execute("CREATE TABLE refs(storage_key VARCHAR,sha256 VARCHAR,size_bytes BIGINT)")
        c.executemany("INSERT INTO refs VALUES (?,?,?)", [(str(p), sha256(p), 7) for p in paths])
        contract = fixture_contract(c, tmp_path)
        inventory = tmp_path / "inventory.jsonl"
        frozen = freeze_reference_inventory(c, contract, inventory, {}, time.monotonic() + 30, scope=RECORDED_SCOPE)
        assert frozen["recorded_hash_comparisons"] == 1002 and frozen["recomputed_files"] == 1000
        assert frozen["unread_bytes"] == 14
        unsampled = next(p for p in paths if str(p) not in frozen["sample_paths"])
        hashed = []
        original = recorded.stable_hash

        def measured(path, deadline):
            hashed.append(str(path))
            return original(path, deadline)

        monkeypatch.setattr(recorded, "stable_hash", measured)
        destination = tmp_path / "verify"
        destination.mkdir()
        verified = verify_references(c, contract, inventory, destination, {}, time.monotonic() + 30, scope=RECORDED_SCOPE)
        assert str(unsampled) not in hashed and len(hashed) == 1000
        assert verified["unread_bytes"] == 14
        unsampled.write_bytes(b"bad size")
        destination = tmp_path / "bad"
        destination.mkdir()
        with pytest.raises(AdmissionError, match="size mismatch"):
            verify_references(c, contract, inventory, destination, {}, time.monotonic() + 30, scope=RECORDED_SCOPE)


def test_manifest_missing_hash_always_recomputed(tmp_path):
    """Nullable hash inventories cannot accidentally bypass the byte hash gate."""
    p = tmp_path / "file"
    p.write_bytes(b"payload")
    manifest = tmp_path / "inventory.jsonl"
    manifest.write_text(json.dumps({"path": str(p), "size": 7}) + "\n")
    with duckdb.connect() as c:
        c.execute("CREATE TABLE refs(path VARCHAR)")
        c.execute("INSERT INTO refs VALUES (?)", [str(p)])
        contract = fixture_contract(c, tmp_path)
        destination = tmp_path / "verify"
        destination.mkdir()
        result = verify_references(c, contract, manifest, destination, {}, time.monotonic() + 30, scope=RECORDED_SCOPE)
    assert result["missing_hash_recomputed_files"] == result["recomputed_files"] == 1


@pytest.mark.parametrize("failure", [None, "size", "hash", "member"])
def test_recorded_capture_directory_contract(tmp_path, failure):
    """Directory references keep exact membership and frozen member size/hash gates."""
    capture = tmp_path / "capture"
    capture.mkdir()
    member = capture / "page.body"
    member.write_bytes(b"page")
    directory_inventory = tmp_path / "directories.jsonl"
    directory_inventory.write_text(json.dumps({"directory": str(capture), "files": [
        {"path": str(member), "size": 4, "sha256": sha256(member)}]}) + "\n")
    frozen = {"inventory": str(directory_inventory), "inventory_sha256": sha256(directory_inventory)}
    with duckdb.connect() as c:
        c.execute("CREATE SCHEMA baostock18_admission")
        c.execute("CREATE TABLE baostock18_admission.row_lineage(source_path VARCHAR)")
        c.execute("INSERT INTO baostock18_admission.row_lineage VALUES ('capture')")
        contract = reference_contract(c, {}, reference_root=str(tmp_path), admission_root=str(tmp_path),
                                      json_root=[str(tmp_path)], capture_inventory=frozen)
        if failure == "size":
            member.write_bytes(b"longer")
        elif failure == "hash":
            member.write_bytes(b"evil")
        elif failure == "member":
            (capture / "added").write_bytes(b"added")
        if failure:
            with pytest.raises(AdmissionError, match="mismatch|membership"):
                freeze_reference_inventory(c, contract, tmp_path / "inventory.jsonl", {},
                                           time.monotonic() + 30, scope=RECORDED_SCOPE)
        else:
            result = freeze_reference_inventory(c, contract, tmp_path / "inventory.jsonl", {},
                                                time.monotonic() + 30, scope=RECORDED_SCOPE)
            assert result["directories"] == result["count"] == result["sample_recomputed_files"] == 1

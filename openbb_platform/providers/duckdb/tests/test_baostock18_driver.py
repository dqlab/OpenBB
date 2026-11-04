"""Regression evidence for persisted contracts and read-only ready replay."""

import hashlib
from dataclasses import replace

import duckdb
import pytest
from openbb_duckdb.baostock18 import import_candidate, set_visible
from openbb_duckdb.baostock18.contract import AdmissionError
from openbb_duckdb.baostock18.driver import reference_contract
from openbb_duckdb.baostock18.manifest import sha256
from openbb_duckdb.baostock18.production import capture_fingerprint, fingerprint, replay_production, write_json
from openbb_duckdb.baostock18.reference_backup import freeze_reference_inventory

from .test_baostock18_admission import make_candidate
from .test_baostock18_production import authorize, make_catalog


def test_in_memory_contract_matches_persisted_schema(tmp_path):
    """Tuple/list regression must fail here before an operator opens a window."""
    import time

    target = make_catalog(tmp_path / "catalog")
    observed = fingerprint(target)
    with duckdb.connect(str(target), read_only=True) as c:
        contract = reference_contract(c, observed, reference_root=str(tmp_path),
                                      admission_root=str(tmp_path), json_root=str(tmp_path))
        assert contract["columns"] and all(type(row) is list for row in contract["columns"])
        result = freeze_reference_inventory(c, contract, tmp_path / "references.jsonl", observed, time.monotonic() + 30)
    assert result["count"] == 0


@pytest.mark.parametrize("published", [False, True])
def test_replay_ready_never_opens_writer(tmp_path, published, monkeypatch):
    """Replay preserves the full catalog hash and rejects a changed candidate."""
    manifest = make_candidate(tmp_path / "candidate.duckdb", rows=2, quarantined=3)
    target = make_catalog(tmp_path / "catalog")
    first = authorize(tmp_path, target, manifest, "first")
    imported = import_candidate(manifest, target, production=first)
    second = authorize(tmp_path, target, manifest, "second")
    if published:
        set_visible(target, imported["snapshot_id"], basis="unadjusted", expected_clean_rows=2, production=second)
    fp = tmp_path / "replay-fingerprint.json"
    write_json(fp, capture_fingerprint(target, second.window_started_at))
    approval = replace(second, expected_catalog_fingerprint=str(fp), backup_receipt=None)
    before = fingerprint(target), sha256(target)
    real_connect = duckdb.connect

    def readonly_connect(*args, **kwargs):
        assert kwargs.get("read_only") is True
        return real_connect(*args, **kwargs)

    monkeypatch.setattr(duckdb, "connect", readonly_connect)
    result = replay_production(approval, manifest)
    assert result["new_batches"] == 0 and result["rows"] == 5 and result["read_only"]
    assert (fingerprint(target), sha256(target)) == before
    with pytest.raises(AdmissionError, match="differs"):
        replay_production(approval, dict(manifest, availability_basis="changed"))
    candidate = tmp_path / "candidate.duckdb"
    with candidate.open("ab") as stream:
        stream.write(b"changed")
    with pytest.raises(AdmissionError, match="hash mismatch"):
        replay_production(approval, manifest)


def test_replay_refuses_missing_snapshot(tmp_path):
    """A read-only replay cannot migrate or populate an empty target."""
    manifest = make_candidate(tmp_path / "candidate.duckdb")
    target = make_catalog(tmp_path / "catalog")
    approval = authorize(tmp_path, target, manifest, "first")
    before = hashlib.sha256(target.read_bytes()).hexdigest()
    with pytest.raises(duckdb.CatalogException):
        replay_production(approval, manifest)
    assert hashlib.sha256(target.read_bytes()).hexdigest() == before


def test_driver_uses_receipt_when_stdout_contains_progress(tmp_path, monkeypatch):
    """A successful import must not be mistaken for failed JSON parsing."""
    import subprocess

    from openbb_duckdb.baostock18.driver import Driver

    driver = Driver.__new__(Driver)
    driver.root, driver.commands = tmp_path, []

    def command(argv, *, stdout, stderr, check):
        stdout.write("Progress 100%\n{}\n")
        write_json(tmp_path / "import.json", {"rows": 50001, "batches": 2})
        return subprocess.CompletedProcess(argv, 0)

    monkeypatch.setattr(subprocess, "run", command)
    assert driver.cli("production-import", "import") == {"rows": 50001, "batches": 2}


def test_post_switch_abort_withdraws_current_visibility(tmp_path):
    """Emergency rollback is limited to this run's approved committed pointer."""
    from openbb_duckdb.baostock18.driver import Driver

    from .test_baostock18_admission import read

    manifest = make_candidate(tmp_path / "candidate.duckdb")
    target = make_catalog(tmp_path / "catalog")
    first = authorize(tmp_path, target, manifest, "first")
    imported = import_candidate(manifest, target, production=first)
    second = authorize(tmp_path, target, manifest, "second")
    set_visible(target, imported["snapshot_id"], basis="unadjusted", expected_clean_rows=1, production=second)
    driver = Driver.__new__(Driver)
    driver.target, driver.root = target, tmp_path
    driver.before_visibility = second.expected_catalog_fingerprint
    driver.config = {"approved_manifest_hash": imported["snapshot_id"]}
    driver.withdraw()
    assert read(target, basis="unadjusted") == []
    assert (tmp_path / "withdrawal.json").is_file()


def test_candidate_precheck_hashes_directory_members(tmp_path):
    """A relative capture directory freezes actual files, never a directory hash."""
    from openbb_duckdb.baostock18.driver import precheck_input_references

    manifest = make_candidate(tmp_path / "candidate.duckdb")
    write_json(tmp_path / "manifest.json", manifest)
    config = {"manifest": str(tmp_path / "manifest.json"), "admission_root": str(tmp_path),
              "output": str(tmp_path / "run"), "max_verify_seconds": 30}
    result = precheck_input_references(config)
    assert result["status"] == "failed" and result["missing"] == 1
    (tmp_path / "raw").mkdir()
    (tmp_path / "raw/capture.json.gz").mkdir()
    (tmp_path / "raw/capture.json.gz/authority.json").write_bytes(b"authority")
    (tmp_path / "raw/capture.json.gz/page-000.body").write_bytes(b"page")
    result = precheck_input_references(config, destination=tmp_path / "directories.jsonl")
    assert result["status"] == "passed" and result["directories"] == 1
    assert result["regular_files"] == 2 and result["bytes"] == 13
    assert result["root"] == result["resolved_admission_root"] == str(tmp_path)
    assert result["missing"] == result["changed"] == result["invalid"] == 0
    assert result["target_catalog_connections"] == 0


@pytest.mark.parametrize("bad", ["symlink", "fifo", "file", "escape", "unreadable"])
def test_candidate_directory_refuses_unsafe_members(tmp_path, bad, monkeypatch):
    """Reject nonregular/unreadable trees and locators escaping their root."""
    import os
    from pathlib import Path

    from openbb_duckdb.baostock18.driver import precheck_input_references

    manifest = make_candidate(tmp_path / "candidate.duckdb")
    write_json(tmp_path / "manifest.json", manifest)
    capture = tmp_path / "raw/capture.json.gz"
    capture.mkdir(parents=True)
    if bad == "symlink":
        (capture / "link").symlink_to(tmp_path / "candidate.duckdb")
    elif bad == "fifo":
        os.mkfifo(capture / "fifo")
    elif bad == "file":
        with duckdb.connect(str(tmp_path / "candidate.duckdb")) as c:
            c.execute("UPDATE capture_inventory SET relative='candidate.duckdb'")
    elif bad == "escape":
        with duckdb.connect(str(tmp_path / "candidate.duckdb")) as c:
            c.execute("UPDATE capture_inventory SET relative='../outside'")
    else:
        real_lstat = Path.lstat

        def deny(path):
            if path == capture:
                raise PermissionError("synthetic unreadable capture")
            return real_lstat(path)

        monkeypatch.setattr(Path, "lstat", deny)
    config = {"manifest": str(tmp_path / "manifest.json"), "admission_root": str(tmp_path),
              "output": str(tmp_path / "run"), "max_verify_seconds": 30}
    result = precheck_input_references(config)
    assert result["status"] == "failed" and result["target_catalog_connections"] == 0
    assert sum(result[k] for k in ("missing", "invalid", "changed", "nonregular", "unreadable")) == 1


@pytest.mark.parametrize("change", ["bytes", "added", "removed"])
def test_staged_directory_freeze_refuses_changed_capture(tmp_path, change):
    """A staged catalog remains bound to the pre-import members and byte hashes."""
    import time

    from openbb_duckdb.baostock18.driver import precheck_input_references

    manifest = make_candidate(tmp_path / "candidate.duckdb")
    write_json(tmp_path / "manifest.json", manifest)
    capture = tmp_path / "raw/capture.json.gz"
    capture.mkdir(parents=True)
    member = capture / "page-000.body"
    member.write_bytes(b"page")
    config = {"manifest": str(tmp_path / "manifest.json"), "admission_root": str(tmp_path),
              "output": str(tmp_path / "run"), "max_verify_seconds": 30}
    frozen = precheck_input_references(config, destination=tmp_path / "directories.jsonl")
    assert frozen["status"] == "passed"
    with duckdb.connect() as c:
        c.execute("CREATE SCHEMA baostock18_admission")
        c.execute("CREATE TABLE baostock18_admission.row_lineage(source_path VARCHAR)")
        c.execute("INSERT INTO baostock18_admission.row_lineage VALUES ('raw/capture.json.gz')")
        contract = reference_contract(c, {}, reference_root=str(tmp_path), admission_root=str(tmp_path),
                                      json_root=str(tmp_path), capture_inventory=frozen)
        result = freeze_reference_inventory(c, contract, tmp_path / "good.jsonl", {}, time.monotonic() + 30)
        assert result["count"] == 1 and result["bytes"] == 4
        if change == "bytes":
            member.write_bytes(b"evil")
        elif change == "added":
            (capture / "new").write_bytes(b"new")
        else:
            member.unlink()
        with pytest.raises(AdmissionError, match="mismatch|membership missing/changed"):
            freeze_reference_inventory(c, contract, tmp_path / "bad.jsonl", {}, time.monotonic() + 30)

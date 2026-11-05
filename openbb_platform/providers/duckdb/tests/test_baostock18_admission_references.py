"""The visibility reference gate covers the imported captures, independent of foreign data."""

# Fixed fixture SQL only.
# ruff: noqa: S608
import json
import time
from pathlib import Path

import duckdb
import pytest
from openbb_duckdb.baostock18 import import_candidate
from openbb_duckdb.baostock18.admission_references import verify_admission_references
from openbb_duckdb.baostock18.contract import AdmissionError
from openbb_duckdb.baostock18.driver import precheck_input_references
from openbb_duckdb.baostock18.manifest import sha256
from openbb_duckdb.baostock18.production import write_json
from openbb_duckdb.baostock18.recorded_references import Sampler

from .test_baostock18_admission import make_candidate


@pytest.fixture
def captures(tmp_path):
    """Build a real import with 1005 pinned members, including unsampled files."""
    manifest = make_candidate(tmp_path / "candidate.duckdb", rows=2, quarantined=1)
    write_json(tmp_path / "manifest.json", manifest)
    imported = import_candidate(manifest, tmp_path / "import.duckdb")
    directory = tmp_path / "raw/capture.json.gz"
    directory.mkdir(parents=True)
    sampler = Sampler()
    for number in range(1005):
        path = directory / f"member-{number:04}.body"
        path.write_bytes(b"immutable original")
        sampler.add(path, tmp_path)
    config = dict(manifest=str(tmp_path / "manifest.json"), admission_root=str(tmp_path),
                  output=str(tmp_path / "import-run"), max_verify_seconds=60,
                  approved_manifest_hash=imported["snapshot_id"])
    ledger = tmp_path / "pinned.jsonl"
    receipt = precheck_input_references(config, destination=ledger)
    write_json(tmp_path / "pinned-receipt.json", receipt)
    config.update(capture_inventory_receipt=str(tmp_path / "pinned-receipt.json"),
                  capture_inventory_sha256=sha256(ledger))
    selected = sampler.selected()
    return config, ledger, selected, tmp_path / "import.duckdb"


def run(captures, tmp_path):
    """Run the real gate, with no full-catalog reference projection."""
    config, _, _, target = captures
    output = tmp_path / "verify"
    output.mkdir()
    with duckdb.connect(str(target), read_only=True) as connection:
        return verify_admission_references(connection, config, output, time.monotonic()+60)


def test_pinned_sample_missing_hash_and_foreign_references(captures, tmp_path):
    """Unpinned files outside the sample are still hashed; foreign references are excluded."""
    config, ledger, selected, target = captures
    item = json.loads(ledger.read_text())
    unselected = next(member for member in item["files"] if member["path"] not in selected)
    unselected.pop("sha256")
    ledger.write_text(json.dumps(item)+"\n")
    receipt = json.loads((tmp_path / "pinned-receipt.json").read_text())
    receipt["inventory_sha256"] = sha256(ledger)
    (tmp_path / "pinned-receipt.json").write_text(json.dumps(receipt))
    config["capture_inventory_sha256"] = sha256(ledger)
    with duckdb.connect(str(target)) as connection:
        connection.execute("CREATE TABLE foreign_data AS SELECT '/absent/out-of-scope.parquet' AS source_path")
    result = run(captures, tmp_path)
    assert result["count"] == 1005 and result["bytes"] == 1005*len(b"immutable original")
    assert result["sample_recomputed_files"] == result["sample_target"] == 1000
    assert result["recomputed_files"] == 1001 and result["missing_hash_recomputed_files"] == 1
    assert result["recorded_hash_comparisons"] == 1004
    assert result["lineage_rows"] == 3 and result["unresolved_lineage_rows"] == 0
    assert len(Path(result["measured_inventory"]).read_text().splitlines()) == 1005


@pytest.mark.parametrize("bad", ["hash", "size", "missing", "symlink", "lineage", "ledger"])
def test_reference_failures_abort(captures, tmp_path, bad):
    """A sampled hash, any member type/size/membership, or any lineage defect fails."""
    _, ledger, selected, target = captures
    member = Path(min(selected))
    if bad == "hash":
        member.write_bytes(b"X"*len(b"immutable original"))
    elif bad == "size":
        member.write_bytes(b"different size")
    elif bad == "missing":
        member.rename(tmp_path / "retained-member.body")
    elif bad == "symlink":
        moved = tmp_path / "retained-member.body"
        member.rename(moved)
        member.symlink_to(moved)
    elif bad == "lineage":
        with duckdb.connect(str(target)) as connection:
            connection.execute("UPDATE baostock18_admission.row_lineage SET source_path='unbound/capture'")
    elif bad == "ledger":
        with ledger.open("a") as stream:
            stream.write(" ")
    with pytest.raises(AdmissionError):
        run(captures, tmp_path)
    assert not (tmp_path / "verify/receipt.json").exists()

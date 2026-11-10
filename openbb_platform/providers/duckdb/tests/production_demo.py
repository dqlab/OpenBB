"""Exercise the installed wheel's production CLI using a tiny local copied catalog.

Run from checkout root with the wheel install directory on PYTHONPATH:
python -m openbb_platform.providers.duckdb.tests.production_demo --output /new/absolute/run
"""

# Internal fixture/schema names only.
# ruff: noqa: S608

import argparse
import json
import subprocess
import sys
import time
from dataclasses import replace
from pathlib import Path

import openbb_duckdb
from openbb_duckdb.baostock18.manifest import sha256
from openbb_duckdb.baostock18.production import fingerprint, write_json
from openbb_duckdb.baostock18.store import utcnow

from .test_baostock18_admission import make_candidate, query, read
from .test_baostock18_production import authorize, make_catalog


def main():
    """Save command receipts and assert abort/preflight/success behavior."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    root = args.output
    if not root.is_absolute():
        parser.error("output must be absolute")
    root.mkdir(exist_ok=False)
    manifest = make_candidate(root / "candidate.duckdb", rows=3, quarantined=15_838)
    write_json(root / "candidate-manifest.json", manifest)
    target = make_catalog(root / "catalog")
    approval = authorize(root, target, manifest, "initial", backup=False)
    commands = []

    def invoke(command, approval, label, *extra, expected=0):
        common = ["--production-target", str(target), "--expected-catalog-fingerprint",
                  approval.expected_catalog_fingerprint, "--window-started-at", approval.window_started_at,
                  "--baseline", approval.baseline, "--approved-manifest-hash", approval.approved_manifest_hash,
                  "--approved-batch-size", str(approval.approved_batch_size), "--backup-receipt", approval.backup_receipt]
        if command == "production-fingerprint":
            common = ["--production-target", str(target), "--window-started-at", approval.window_started_at]
        elif command == "production-backup":
            common = common[:-2]
        argv = [sys.executable, "-m", "openbb_duckdb.baostock18", command, *common,
                "--output", str(root / (label + ".json")), *extra]
        started = time.monotonic()
        result = subprocess.run(argv, capture_output=True, text=True, check=False)  # noqa: S603
        item = {"argv": argv, "returncode": result.returncode, "stdout": result.stdout,
                "stderr": result.stderr, "seconds": time.monotonic() - started}
        commands.append(item)
        write_json(root / (label + "-command.json"), item)
        if result.returncode != expected:
            raise RuntimeError(item)
        return json.loads(result.stdout) if expected == 0 else item

    invoke("production-fingerprint", approval, "cli-fingerprint")
    approval = replace(approval, expected_catalog_fingerprint=str(root / "cli-fingerprint.json"))
    invoke("production-backup", approval, "cli-backup", "--destination", str(root / "initial" / "backup"),
           "--checkpoint-receipt", str(root / "initial" / "checkpoint.json"))
    before = fingerprint(target), sha256(target)
    pre = invoke("production-preflight", approval, "preflight")
    if (fingerprint(target), sha256(target)) != before:
        raise RuntimeError("Preflight modified catalog")
    record = Path(approval.expected_catalog_fingerprint)
    mismatch = json.loads(record.read_text())
    mismatch["fingerprint"]["size"] += 1
    write_json(root / "abort-fingerprint.json", mismatch)
    abort_approval = replace(approval, expected_catalog_fingerprint=str(root / "abort-fingerprint.json"))
    abort = invoke("production-import", abort_approval, "abort", "--manifest", str(root / "candidate-manifest.json"),
                   expected=2)
    if (fingerprint(target), sha256(target)) != before or (root / "abort.json").exists():
        raise RuntimeError("Aborted import modified catalog or wrote a success receipt")
    imported = invoke("production-import", approval, "import", "--manifest", str(root / "candidate-manifest.json"))
    if read(target, basis="unadjusted"):
        raise RuntimeError("Staging became visible")
    approval = authorize(root, target, manifest, "after-import")
    verified = invoke("production-verify", approval, "verify")
    visible = invoke("production-visible", approval, "visible", "--snapshot-id", imported["snapshot_id"],
                     "--basis", "unadjusted", "--expected-clean-rows", "3")
    approval = authorize(root, target, manifest, "replay-evidence")
    replay = invoke("production-import", approval, "replay", "--manifest", str(root / "candidate-manifest.json"))
    approval = authorize(root, target, manifest, "repeat-pointer-evidence")
    repeated = invoke("production-visible", approval, "repeat-pointer", "--snapshot-id", imported["snapshot_id"],
                      "--basis", "unadjusted", "--expected-clean-rows", "3")
    legacy = {t: query(target, f"SELECT count(*) FROM a_share.{t}")[0][0] for t in pre["a_share_counts"]}
    consumed = len(read(target, basis="unadjusted"))
    if not (imported["rows"] == 15_841 and imported["quarantine"] == 15_838
            and verified["clean_unadjusted"] == consumed == 3
            and replay["new_batches"] == 0 and repeated["changed"] is False
            and legacy == pre["a_share_counts"]):
        raise RuntimeError("Reconciliation failed")
    receipt = {"format": "baostock18-production-local-demo-v1", "completed_at": utcnow().isoformat(),
               "package": openbb_duckdb.__file__, "target": str(target), "local_copy_only": True,
               "production_catalog_writes": 0, "production_services_started_or_stopped": 0,
               "preflight": pre, "abort": {"returncode": abort["returncode"], "reason": abort["stderr"],
                                           "catalog_fingerprint_and_full_hash_unchanged": True},
               "import": imported, "verify": verified, "visible": visible, "replay": replay,
               "repeated_pointer": repeated, "consumed": consumed, "legacy_counts_after": legacy,
               "catalog_bytes_after": target.stat().st_size, "catalog_sha256_after": sha256(target),
               "commands": commands}
    write_json(root / "LOCAL_DEMO_RECEIPT.json", receipt)
    print(json.dumps({k: v for k, v in receipt.items() if k != "commands"}, indent=2))  # noqa: T201


if __name__ == "__main__":
    main()

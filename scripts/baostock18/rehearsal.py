"""Create a local production-shaped copy from saved DDL; run the shared driver.

No remote access, service changes, or production catalog connections.
"""

# This executable is an assertion-based offline acceptance runner.
# ruff: noqa: S608, S603, S101, T201
import argparse
import hashlib
import json
import subprocess
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import duckdb
from openbb_duckdb.baostock18.manifest import encoded, prepare_manifest, sha256
from openbb_duckdb.baostock18.production import A_SHARE_TABLES, fingerprint, inventory, load_json, write_json
from openbb_duckdb.baostock18.reader import read_bars
from openbb_duckdb.baostock18.store import utcnow


def main():
    """Build the saved production schema and execute the installed shared driver."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ddl", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--max-backup-seconds", type=float, required=True)
    parser.add_argument("--max-freeze-seconds", type=float, required=True)
    parser.add_argument("--max-verify-seconds", type=float, required=True)
    parser.add_argument("--backup-scope", default="catalog_with_reference_verification")
    parser.add_argument("--visibility", choices=("immediate", "deferred"), default="immediate")
    args = parser.parse_args()
    root = Path(args.output)
    root.mkdir(mode=0o700)
    ddl = json.loads(Path(args.ddl).read_text())
    if len(ddl["tables"]) < 79 or not ddl["views"]:
        raise ValueError("Full saved production DDL required")
    target = root / "catalog" / "dq_quant_invest_data.duckdb"
    target.parent.mkdir()
    reference_root = root / "data"
    reference_root.mkdir()
    raw = reference_root / "raw"
    raw.mkdir()
    capture = raw / "capture.json.gz"
    capture.write_bytes(b"local synthetic immutable capture")
    admission_root = root / "output"
    relative = "baostock_campaign_full18_joblevel/private/baostock-page-capture-v1/" + "a" * 64
    capture_directory = admission_root / relative
    capture_directory.mkdir(parents=True)
    for name, body in {
        "authority.json": b'{"synthetic":true}',
        "eligible.json": b'{"eligible":true}',
        "page-000.body": b"local synthetic immutable page",
        "page-000.fragments.jsonl": b'{"fragment":0}\n',
    }.items():
        (capture_directory / name).write_bytes(body)
    with duckdb.connect(str(target)) as c:
        for schema in ddl["schemas"]:
            # The saved inventory contains schema names.
            name = schema[0] if isinstance(schema, list) else schema
            if name != "main":
                c.execute('CREATE SCHEMA "' + name + '"')
        for _, _, sql in ddl["tables"]:
            c.execute(sql)
        pending = ddl["views"][:]
        while pending:
            left = []
            for entry in pending:
                try:
                    c.execute(entry[2])
                except duckdb.CatalogException:
                    left.append(entry)
            if len(left) == len(pending):
                raise ValueError("Unresolved production views")
            pending = left
        for entry in ddl.get("indexes", []):
            c.execute(entry[-1])
        # Full real column types and PKs; one coherent synthetic legacy row per table.
        c.execute(
            "INSERT INTO a_share.attempt(attempt_id,job_id,raw_key,raw_sha256) VALUES ('attempt','job',?,?)",
            [str(capture), sha256(capture)],
        )
        c.execute(
            "INSERT INTO a_share.bar_revision(snapshot_id,symbol,date,close) VALUES ('bars','SH.600000','2026-10-15',10)"
        )
        c.execute(
            "INSERT INTO a_share.bar_snapshot(snapshot_id,job_id,symbol,known_at,raw_key,raw_sha256,row_count) "
            "VALUES ('bars','job','SH.600000',current_timestamp,?,?,1)",
            [str(capture), sha256(capture)],
        )
        c.execute("INSERT INTO a_share.calendar_day VALUES ('reference','2026-10-15',true)")
        c.execute(
            "INSERT INTO a_share.factor_revision(snapshot_id,symbol,ex_date) VALUES ('bars','SH.600000','2026-10-15')"
        )
        c.execute("INSERT INTO a_share.instrument_version(snapshot_id,symbol) VALUES ('reference','SH.600000')")
        c.execute("INSERT INTO a_share.job(job_id,reference_id,symbol) VALUES ('job','reference','SH.600000')")
        c.execute(
            "INSERT INTO a_share.reference_snapshot(snapshot_id,raw_key,raw_sha256) VALUES ('reference',?,?)",
            [str(capture), sha256(capture)],
        )
        c.execute("UPDATE a_share.bar_snapshot SET quality_json=?",
                  [json.dumps({"raw_key": "raw/capture.json.gz", "raw_sha256": sha256(capture)})])
        baseline = {
            "format": "baostock18-production-baseline-v1",
            "tables": inventory(c),
            "a_share_counts": {t: c.execute("SELECT count(*) FROM a_share." + t).fetchone()[0] for t in A_SHARE_TABLES},
        }
        c.execute("CHECKPOINT")
    target.with_suffix(".market.lock").touch(exist_ok=False)
    target.with_suffix(".publisher.lock").touch(exist_ok=False)
    # Reuse the repository's established synthetic input factory, never a writer bypass.
    from openbb_platform.providers.duckdb.tests.test_baostock18_admission import make_candidate

    candidate = admission_root / "candidate.duckdb"
    make_candidate(candidate, rows=50001, quarantined=15838)
    # Production captures are relative directory locators, distinct from legacy raw files.
    with duckdb.connect(str(candidate)) as c:
        c.execute("UPDATE capture_inventory SET relative=?", [relative])
    manifest = prepare_manifest(candidate, end_date="2300-01-01")
    write_json(admission_root / "manifest.json", manifest)
    write_json(root / "baseline.json", baseline)
    config = {
        "target": str(target),
        "visibility": args.visibility,
        "output": str(root / "execution"),
        "manifest": str(admission_root / "manifest.json"),
        "baseline": str(root / "baseline.json"),
        "reference_root": str(reference_root),
        "admission_root": str(admission_root),
        "json_root": [str(reference_root), str(admission_root)],
        "window_started_at": utcnow().isoformat(),
        "approved_manifest_hash": hashlib.sha256(encoded(manifest).encode()).hexdigest(),
        "approved_batch_size": 50000,
        "max_backup_seconds": args.max_backup_seconds,
        "backup_scope": args.backup_scope,
        "max_freeze_seconds": args.max_freeze_seconds,
        "max_verify_seconds": args.max_verify_seconds,
        "expected_rows": 65839,
        "expected_clean_rows": 50001,
        "expected_quarantine": 15838,
        "expected_batches": 2,
        "reader_limit": 3,
        "metadata": [str(root / "baseline.json"), str(admission_root / "manifest.json")],
    }
    write_json(root / "driver-config.json", config)
    write_json(
        root / "fixture.json",
        {
            "ddl": args.ddl,
            "ddl_sha256": sha256(Path(args.ddl)),
            "tables": len(ddl["tables"]),
            "views": len(ddl["views"]),
            "a_share_counts": baseline["a_share_counts"],
            "input_rows": 65839,
        },
    )
    subprocess.run(
        [sys.executable, "-B", "-m", "openbb_duckdb.baostock18.driver", "--config", str(root / "driver-config.json")],
        check=True,
    )
    if args.visibility == "deferred":
        finish_deferred(root, config)


def finish_deferred(root, config):
    """Measure the real CLI chain on this local fixture and preserve every receipt."""
    from openbb_duckdb.baostock18.contract import SCHEMA
    from openbb_duckdb.baostock18.visibility import DEFERRED

    target = Path(config["target"])
    staged = load_json(root / "execution/receipt.json")
    assert staged["status"] == DEFERRED
    before_verify = fingerprint(target), sha256(target)
    subprocess.run(staged["next_commands"]["verify"], check=True)
    assert (fingerprint(target), sha256(target)) == before_verify
    verified_root = root / "execution/verification"
    verified = load_json(verified_root / "receipt.json")
    assert verified["status"] == "PASS"
    argv = load_json(verified_root / "approve-command.json")["argv"]
    for label in ("approve", "replay"):
        with (root / (label + ".stdout")).open("x") as out, (root / (label + ".stderr")).open("x") as err:
            subprocess.run(argv, check=True, stdout=out, stderr=err)
        if label == "approve":
            after_approve = fingerprint(target), sha256(target)
    assert (fingerprint(target), sha256(target)) == after_approve
    replay = load_json(root / "replay.stdout")
    assert replay["changed"] is False
    approval = load_json(verified_root / "approval.json")
    with duckdb.connect(str(target), read_only=True) as c:
        visible = c.execute(f"SELECT count(*) FROM {SCHEMA}.consumable").fetchone()[0]
        quarantine = c.execute(
            f"SELECT count(*) FROM {SCHEMA}.literal_version WHERE disposition='quarantine'"
        ).fetchone()[0]
        events = c.execute(f"SELECT count(*) FROM {SCHEMA}.visibility_event").fetchone()[0]
        sample = c.execute(f"SELECT symbol,date FROM {SCHEMA}.consumable LIMIT 1").fetchone()
    assert visible == config["expected_clean_rows"] and quarantine == config["expected_quarantine"] and events == 1
    at = datetime.fromisoformat(approval["known_at"])
    reader_args = dict(symbols=[sample[0]], start_date=date.fromisoformat(str(sample[1])),
                       end_date=date.fromisoformat(str(sample[1])), basis="unadjusted", limit=3)
    counts = {name: len(read_bars(target, **reader_args, as_of=when))
              for name, when in (("before", at - timedelta(microseconds=1)), ("at", at), ("current", None))}
    assert counts == {"before": 0, "at": 1, "current": 1}
    write_json(root / "DEFERRED_CHAIN_RECEIPT.json", {
        "status": "passed", "synthetic_fixture": True, "fixture": load_json(root / "fixture.json"),
        "driver_state": staged["status"], "imported": staged["imported"],
        "deferred_pointer_counts": staged["visibility_counts"], "verification": verified["status"],
        "verify_catalog_unchanged": True, "reference_verification": verified["reference_verification"],
        "approval": approval, "approve_command": argv, "replay_changed": replay["changed"],
        "visibility_events": events, "visible_rows": visible, "quarantined_rows": quarantine,
        "reader_counts": counts, "production_catalog_connections": 0, "service_operations": 0,
        "receipts": [{"path": str(p), "sha256": sha256(p)} for p in (
            root / "execution/receipt.json", verified_root / "receipt.json", verified_root / "approval.json")],
    })


if __name__ == "__main__":
    main()

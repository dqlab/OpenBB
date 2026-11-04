"""Explicit local CLI: python -m openbb_duckdb.baostock18 --help."""

import argparse
import json
from datetime import date, datetime
from pathlib import Path

from openbb_duckdb.baostock18 import import_candidate, prepare_manifest, read_bars, set_visible
from openbb_duckdb.baostock18.contract import AdmissionError
from openbb_duckdb.baostock18.production import (
    ProductionAdmission,
    capture_fingerprint,
    create_backup,
    load_json,
    preflight,
    verify_production,
    write_json,
)
from openbb_duckdb.baostock18.reference_backup import SCOPES


def main():
    """Explicit production gates; no default catalog or implicit activation."""
    parser = argparse.ArgumentParser(description="Offline BaoStock18 narrow-basis admission")
    commands = parser.add_subparsers(dest="command", required=True)
    manifest = commands.add_parser("manifest", help="Hash candidate and all local Parquet dependencies")
    manifest.add_argument("--candidate", required=True)
    manifest.add_argument("--output", required=True)
    manifest.add_argument("--symbols", nargs="+")
    manifest.add_argument("--start-date", default="1990-12-19", type=date.fromisoformat)
    manifest.add_argument("--end-date", default="2026-10-15", type=date.fromisoformat)
    manifest.add_argument("--evidence-file", action="append", default=[])
    stage = commands.add_parser("import", help="Stage into a dedicated offline store; never publish")
    stage.add_argument("--manifest", required=True)
    stage.add_argument("--target", required=True)
    stage.add_argument("--batch-size", type=int, default=50_000)
    visible = commands.add_parser("visible", help="Set/withdraw the OFFLINE consumption pointer")
    visible.add_argument("--target", required=True)
    visible.add_argument("--snapshot-id", help="Omit to record an explicit withdrawal")
    visible.add_argument("--basis", required=True)
    visible.add_argument("--expected-clean-rows", type=int)
    read = commands.add_parser("read", help="Read-only bounded query; no legacy fallback")
    read.add_argument("--database", required=True)
    read.add_argument("--symbols", nargs="+", required=True)
    read.add_argument("--start-date", required=True, type=date.fromisoformat)
    read.add_argument("--end-date", required=True, type=date.fromisoformat)
    read.add_argument("--basis", required=True)
    read.add_argument("--as-of", type=datetime.fromisoformat)
    read.add_argument("--fixed-qfq-basis-date", type=date.fromisoformat)
    read.add_argument("--limit", type=int, default=100_000)
    fingerprint = commands.add_parser("production-fingerprint", help="Read-only on-site window fingerprint")
    fingerprint.add_argument("--production-target", required=True)
    fingerprint.add_argument("--window-started-at", required=True)
    fingerprint.add_argument("--output", required=True)
    for name in ("backup", "preflight", "import", "verify", "visible"):
        command = commands.add_parser("production-" + name, help="Explicit production " + name)
        command.add_argument("--production-target", required=True)
        command.add_argument("--expected-catalog-fingerprint", required=True, help="Fresh fingerprint JSON file")
        command.add_argument("--window-started-at", required=True)
        command.add_argument("--baseline", required=True, help="Approved inventory and a_share eight-table counts")
        command.add_argument("--approved-manifest-hash", required=True, help="SHA256 of canonical manifest JSON")
        command.add_argument("--approved-batch-size", required=True, type=int)
        command.add_argument("--output", required=True, help="New receipt file (exclusive creation)")
        if name == "backup":
            command.add_argument("--backup-scope", choices=SCOPES, default="full")
            command.add_argument("--reference-manifest", help="Frozen sorted JSONL path/size/SHA256 inventory")
            command.add_argument("--reference-contract", help="Fingerprint-bound catalog enumeration contract")
            command.add_argument("--restore-admission-state", choices=("baseline", "staged_unpublished"),
                                 default="baseline", help="Initial schema absent; staged requires zero publication")
            command.add_argument("--destination", required=True)
            command.add_argument("--checkpoint-receipt", required=True)
            command.add_argument("--dependency", action="append", default=[])
            command.add_argument("--dependency-manifest", help="Sorted JSONL paths; copied in batches of 512")
            command.add_argument("--expected-files", type=int)
            command.add_argument("--expected-bytes", type=int)
            command.add_argument("--estimated-seconds", type=float)
            command.add_argument("--max-backup-seconds", type=float)
        else:
            command.add_argument("--backup-receipt", required=True)
        if name == "import":
            command.add_argument("--manifest", required=True)
        if name == "visible":
            command.add_argument("--snapshot-id")
            command.add_argument("--basis", required=True)
            command.add_argument("--expected-clean-rows", type=int)
    args = parser.parse_args()
    if args.command.startswith("production-"):
        try:
            result = production_command(args)
        except (AdmissionError, OSError) as exc:
            parser.exit(2, f"Admission refused: {exc}\n")
        print(json.dumps(result, default=str, ensure_ascii=False, indent=2))  # noqa: T201
        return
    if args.command == "manifest":
        result = prepare_manifest(args.candidate, symbols=args.symbols,
                                  start_date=str(args.start_date), end_date=str(args.end_date),
                                  evidence_files=args.evidence_file)
        with Path(args.output).open("x") as stream:
            json.dump(result, stream, ensure_ascii=False, indent=2)
    elif args.command == "import":
        result = import_candidate(
            json.loads(Path(args.manifest).read_text()), args.target, batch_size=args.batch_size,
        )
    elif args.command == "visible":
        result = set_visible(
            args.target, args.snapshot_id, basis=args.basis, expected_clean_rows=args.expected_clean_rows,
        )
    else:
        result = read_bars(args.database, symbols=args.symbols, start_date=args.start_date,
                           end_date=args.end_date, basis=args.basis, as_of=args.as_of,
                           fixed_qfq_basis_date=args.fixed_qfq_basis_date, limit=args.limit)
    print(json.dumps(result, default=str, ensure_ascii=False, indent=2))  # noqa: T201 - CLI output


def production_command(args):
    """Dispatch production commands with all approvals explicit, writing new receipts."""
    if Path(args.output).exists():
        raise AdmissionError("Output receipt already exists")
    if args.command == "production-fingerprint":
        result = capture_fingerprint(args.production_target, args.window_started_at)
    else:
        admission = ProductionAdmission(
            target=args.production_target, expected_catalog_fingerprint=args.expected_catalog_fingerprint,
            window_started_at=args.window_started_at, baseline=args.baseline,
            approved_manifest_hash=args.approved_manifest_hash, approved_batch_size=args.approved_batch_size,
            backup_receipt=getattr(args, "backup_receipt", None),
        )
        if args.command == "production-backup":
            result = create_backup(
                admission, args.destination, args.checkpoint_receipt, args.dependency,
                dependency_manifest=args.dependency_manifest,
                expected_files=args.expected_files, expected_bytes=args.expected_bytes,
                estimated_seconds=args.estimated_seconds, max_seconds=args.max_backup_seconds,
                backup_scope=args.backup_scope, reference_manifest=args.reference_manifest,
                reference_contract=args.reference_contract,
                restore_admission_state=args.restore_admission_state,
            )
        elif args.command == "production-preflight":
            result = preflight(admission)
        elif args.command == "production-import":
            result = import_candidate(load_json(args.manifest), args.production_target,
                                      batch_size=args.approved_batch_size, production=admission)
            result.update(mode="production", target=args.production_target)
        elif args.command == "production-verify":
            result = verify_production(admission)
        else:
            result = set_visible(args.production_target, args.snapshot_id, basis=args.basis,
                                 expected_clean_rows=args.expected_clean_rows, production=admission)
            result.update(mode="production", target=args.production_target)
    write_json(args.output, result)
    return result


if __name__ == "__main__":
    main()

"""Explicit local CLI: python -m openbb_duckdb.baostock18 --help."""

import argparse
import json
from datetime import date, datetime
from pathlib import Path

from openbb_duckdb.baostock18 import import_candidate, prepare_manifest, read_bars, set_visible


def main():
    """No default catalog, production mode, source requests, or implicit activation."""
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
    args = parser.parse_args()
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


if __name__ == "__main__":
    main()

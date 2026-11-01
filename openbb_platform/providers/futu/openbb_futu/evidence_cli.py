"""Capture bounded seed discovery or inspect coverage without changing it."""

import argparse
import json
import re
import sys
import uuid
from pathlib import Path

from openbb_futu.collector import TokenStore
from openbb_futu.coverage import report, write_json
from openbb_futu.discovery import discover
from openbb_futu.offline import replay, verify
from openbb_futu.read_budget import Budget


def main():
    """Run the evidence commands."""
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    discovery = commands.add_parser("discover", help="query explicit option/future seeds")
    discovery.add_argument("--seeds", required=True, type=Path)
    discovery.add_argument("--token-file", required=True, type=Path)
    discovery.add_argument("--output", required=True, type=Path)
    coverage = commands.add_parser("coverage", help="reconstruct coverage without writing")
    coverage.add_argument("--output", required=True, type=Path)
    coverage.add_argument("--run-id", required=True)
    replay_cmd = commands.add_parser("replay", help="archive offline JSONL frames")
    replay_cmd.add_argument("--input", required=True, type=Path)
    replay_cmd.add_argument("--output", required=True, type=Path)
    verify_cmd = commands.add_parser("verify", help="verify a sealed manifest without writing")
    verify_cmd.add_argument("--manifest", required=True, type=Path)
    for command in (coverage, verify_cmd):
        command.add_argument("--budget-bytes", type=int, default=64 * 1024 * 1024)
        command.add_argument("--budget-lines", type=int, default=100_000)
        command.add_argument("--budget-entries", type=int, default=100_000)
    args = parser.parse_args()
    if args.command in {"coverage", "verify"}:
        try:
            budget = Budget(bytes_limit=args.budget_bytes, lines_limit=args.budget_lines,
                            entries_limit=args.budget_entries)
        except ValueError as exc:
            parser.error(str(exc))
    if args.command in {"replay", "verify"}:
        try:
            result = replay(args.input, args.output) if args.command == "replay" else verify(args.manifest, budget=budget)
            print(json.dumps(result, ensure_ascii=False))  # noqa: T201
            raise SystemExit(1 if result.get("error") or result.get("status") == "failed" else 0)
        except (OSError, ValueError, KeyError, TypeError, RuntimeError) as exc:
            detail = f" errno={exc.errno}" if isinstance(exc, OSError) and exc.errno is not None else ""
            if type(exc) is RuntimeError and str(exc) == "futu archive disabled: secure storage unavailable":
                detail = "; archive disabled: secure storage unavailable"
            print("evidence operation failed: " + type(exc).__name__ + detail, file=sys.stderr)  # noqa: T201
            print(  # noqa: T201
                json.dumps({"status": "failed", "error": "invalid input or evidence"})
            )
            raise SystemExit(1) from None
    if args.command == "coverage":
        if not re.fullmatch(r"[0-9a-f]{32}", args.run_id):
            parser.error("invalid run ID")
        try:
            result = report(args.output, args.run_id, publish=False, budget=budget)
        except (OSError, ValueError, KeyError, TypeError):
            print(  # noqa: T201
                json.dumps({"status": "failed", "error": "invalid input or evidence"})
            )
            raise SystemExit(1) from None
        print(json.dumps(result, ensure_ascii=False))  # noqa: T201
        raise SystemExit(1 if result["evidence_status"] in {"failed", "unknown"} else 0)
    token = TokenStore(args.token_file).read()
    seeds = json.loads(args.seeds.read_text(encoding="utf-8"))
    result = discover(
        seeds,
        token["access_token"],
        args.output,
        secrets=(token["refresh_token"], token.get("client_secret", "")),
    )
    # A new result file is created in the private discovery directory.
    # Empty seed lists have no response directory and are not meaningful.
    if not result["requests"]:
        parser.error("at least one seed is required")
    result_path = Path(result["evidence_dir"]) / ("discovery-" + uuid.uuid4().hex + ".json")
    write_json(result_path, result)
    result["report_file"] = str(result_path)
    print(json.dumps(result, ensure_ascii=False))  # noqa: T201


if __name__ == "__main__":
    main()

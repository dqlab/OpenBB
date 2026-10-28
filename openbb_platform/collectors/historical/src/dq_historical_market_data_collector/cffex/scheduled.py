"""One bounded acquisition pass for the existing NAS job runner.

Keep deferred historical work distinct from a failed date. A runner may schedule
another bounded pass without treating a clean max-days boundary as unhealthy.
Actual failed dates, lock contention, interruption and malformed CLI output stay
nonzero; this adapter never marks a partial provider response successful.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path
from typing import Any


class AdapterError(ValueError):
    """A child process did not honor the machine-readable CLI contract."""


def invoke(command: str, config: Path, extra: list[str]) -> tuple[int, dict[str, Any]]:
    """Use the same interpreter and process group; preserve the CLI's exact exit code."""
    child = subprocess.run(
        [
            sys.executable,
            "-m",
            "dq_historical_market_data_collector.cffex.cli",
            command,
            "--config",
            str(config),
            *extra,
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if child.stderr:
        sys.stderr.write(child.stderr)
    try:
        report = json.loads(child.stdout)
    except (ValueError, TypeError) as exc:
        raise AdapterError("invalid_child_summary") from exc
    if not isinstance(report, dict):
        raise AdapterError("invalid_child_summary")
    if child.returncode == 0 and (
        report.get("status") != "complete" or report.get("command") != command
    ):
        raise AdapterError("child_exit_summary_mismatch")
    return child.returncode, report


def deferred_success(code: int, report: dict[str, Any]) -> bool:
    """Only a clean historical batch boundary is a scheduler-successful deferral."""
    return (
        code == 3
        and report.get("command") == "backfill"
        and report.get("status") == "partial"
        and type(report.get("deferred_days")) is int
        and report["deferred_days"] > 0
        and type(report.get("failed_days")) is int
        and report["failed_days"] == 0
        and type(report.get("unprocessed_days")) is int
        and report["unprocessed_days"] == 0
        and report.get("errors") == []
    )


def one_pass(command: str, config: Path, max_days: int) -> tuple[int, dict[str, Any]]:
    """Complete a bounded pass, releasing all journal locks before returning."""
    result: dict[str, Any] = {"command": command, "status": "failed"}
    try:
        code, calendar = invoke("calendar-refresh", config, [])
        result["calendar"] = calendar
        if code != 0:
            result["exit_code"] = code
        else:
            extra = ["--max-days", str(max_days)] if command == "backfill" else []
            code, report = invoke(command, config, extra)
            result["collection"] = report
            result["collector_exit_code"] = code
            if deferred_success(code, report):
                result["status"] = "deferred"
                code = 0
            elif code == 0:
                result["status"] = "complete"
            result["exit_code"] = code
    except (AdapterError, OSError) as exc:
        code = 2
        result.update(code="scheduler_cli_contract_failed", error_type=type(exc).__name__)
    return code, result


def main(argv: list[str] | None = None) -> int:
    """Run once or finish history in bounded passes with lock-free pause windows."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["backfill", "update"])
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--max-days", type=int, default=100)
    parser.add_argument("--until-complete", action="store_true")
    parser.add_argument("--pause-seconds", type=int, default=30)
    args = parser.parse_args(argv)
    if not 1 <= args.max_days <= 366:
        parser.error("max-days must be between 1 and 366")
    if args.until_complete and args.command != "backfill":
        parser.error("until-complete is only valid for backfill")
    if not 15 <= args.pause_seconds <= 3600:
        parser.error("pause-seconds must be between 15 and 3600")
    while True:
        code, result = one_pass(args.command, args.config, args.max_days)
        print(json.dumps(result, ensure_ascii=False, allow_nan=False), flush=True)
        if code != 0 or not args.until_complete or result["status"] != "deferred":
            return code
        # Child CLI exited and closed its lock; ongoing daily work can acquire
        # the journal between historical batches, rather than waiting hours.
        time.sleep(args.pause_seconds)


if __name__ == "__main__":
    raise SystemExit(main())

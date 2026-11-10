"""One calendar-first pass for the existing NAS runner; no production deployment here."""

from __future__ import annotations

import argparse
import json
import signal
import subprocess
import sys
import threading
from pathlib import Path
from typing import Any

from .config import load_config


class AdapterError(ValueError):
    pass


def invoke(command: str, config: Path, extra: list[str]) -> tuple[int, dict]:
    child = subprocess.Popen(  # pylint: disable=consider-using-with
        [
            sys.executable,
            "-m",
            "dq_historical_market_data_collector.etf_options.cli",
            command,
            "--config",
            str(config),
            *extra,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    previous = {}
    interrupted = threading.Event()

    def forward(sig, _):
        interrupted.set()
        if child.poll() is None:
            child.send_signal(sig)

    try:
        # NAS runners commonly signal only the adapter PID, not its process group.
        if threading.current_thread() is threading.main_thread():
            for signum in (signal.SIGTERM, signal.SIGINT):
                previous[signum] = signal.signal(signum, forward)
        stdout, stderr = child.communicate()
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)
    if stderr:
        sys.stderr.write(stderr)
    try:
        report = json.loads(stdout)
    except (ValueError, TypeError) as exc:
        raise AdapterError("invalid_child_summary") from exc
    if not isinstance(report, dict) or report.get("command") != command:
        raise AdapterError("invalid_child_summary")
    if interrupted.is_set():
        return 4, {**report, "status": "interrupted", "code": "cancelled"}
    if child.returncode == 0 and report.get("status") not in {"complete", "skipped"}:
        raise AdapterError("child_exit_summary_mismatch")
    if (
        command == "calendar-refresh"
        and child.returncode == 0
        and report.get("status") != "complete"
    ):
        raise AdapterError("invalid_calendar_summary")
    return child.returncode, report


def one_pass(config: Path, extra: list[str] | None = None) -> tuple[int, dict]:
    result: dict[str, Any] = {"command": "scheduled", "status": "failed"}
    try:
        code, calendar = invoke("calendar-refresh", config, [])
        result["calendar"] = calendar
        if code == 0:
            code, collection = invoke("collect", config, extra or [])
            result["collection"] = collection
            if code == 0:
                # Only delivery options reach the outbox CLI. Acquisition flags
                # must not trigger another provider request or date/calendar gate.
                delivery_extra = []
                extras = extra or []
                if "--date" in extras:
                    index = extras.index("--date")
                    delivery_extra = extras[index : index + 2]
                code, delivery = invoke("deliver", config, delivery_extra)
                result["delivery"] = delivery
                if code == 0:
                    result["status"] = collection["status"]
                result["central_publication_status"] = "not_requested"
            elif code != 4 and collection.get("status") != "interrupted":
                # Failed acquisition must never become success. Only drain an
                # already sealed outbox, without accepting incomplete new rows.
                try:
                    enabled = load_config(config).delivery is not None
                except (OSError, ValueError):
                    enabled = False
                if enabled:
                    delivery_code, delivery = invoke(
                        "deliver", config, ["--pending-only"]
                    )
                    result["delivery"] = delivery
                    result["delivery_exit_code"] = delivery_code
                    result["central_publication_status"] = "not_requested"
        result["exit_code"] = code
        return code, result
    except (AdapterError, OSError) as exc:
        result.update(
            exit_code=2,
            code="scheduler_cli_contract_failed",
            error_type=type(exc).__name__,
        )
        return 2, result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    args = parser.parse_args(argv)
    code, report = one_pass(args.config)
    print(json.dumps(report, ensure_ascii=False, allow_nan=False), flush=True)
    return code


if __name__ == "__main__":
    raise SystemExit(main())

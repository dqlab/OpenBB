"""Local CFFEX basket commands. Backfill/export end dates are inclusive."""

from __future__ import annotations

import argparse
import json
import signal
import subprocess
import sys
import threading
from datetime import UTC, datetime
from pathlib import Path

from openbb_collector_core.models import ProviderFailure
from openbb_collector_core.process import ProcessClient
from pydantic import ValidationError

from .config import ConfigurationError, iso_date, load_calendar, load_config
from .runtime import Collector, EvidenceStateError, NoSourceClient, refresh_calendar
from .source import CffexCsvClient
from .storage import Journal, JournalError, LockBusy, export_csv, status

EXIT_CODES = {"complete": 0, "failed": 1, "partial": 3, "interrupted": 4}


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__, prog="dq-cffex-daily-collector")
    commands = result.add_subparsers(dest="command", required=True)
    for name in ("backfill", "update", "status", "export", "calendar-refresh", "consume"):
        command = commands.add_parser(name)
        command.add_argument("--config", type=Path, required=True)
        if name in ("backfill", "export", "consume"):
            command.add_argument("--start-date", type=iso_date, help="Inclusive YYYY-MM-DD")
            command.add_argument("--end-date", type=iso_date, help="Inclusive YYYY-MM-DD")
        if name == "backfill":
            command.add_argument(
                "--max-days",
                type=int,
                help="Bound provider-request days; resume on next invocation",
            )
        if name == "backfill":
            command.add_argument(
                "--refresh", action="store_true", help="Re-fetch completed days for corrections"
            )
        if name == "update":
            command.add_argument(
                "--as-of", type=iso_date, help="Last eligible Shanghai date; no future override"
            )
        if name == "status":
            command.add_argument("--limit", type=int, default=100)
        if name == "export":
            command.add_argument("--output", type=Path, required=True)
        if name == "consume":
            command.add_argument(
                "--limit",
                type=int,
                help="Bound consumed days; captured evidence is never re-fetched",
            )
    return result


def emit(payload: dict) -> None:
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True, allow_nan=False), flush=True)


def _isolated_acquire(extra: list[str]) -> int:
    """Run the acquisition-only task in a fresh interpreter.

    This module imports the configuration, journal and storage modules for the
    collection commands. The acquire task is allowed to touch raw evidence only,
    so it must not inherit that import closure: the alias hands the request to
    ``cffex.acquire``, whose closure contains no configuration, journal, normalize,
    storage or central module, and passes its exit code through unchanged.
    """
    child = subprocess.run(
        [sys.executable, "-m", "dq_historical_market_data_collector.cffex.acquire", *extra],
        check=False,
    )
    return child.returncode


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if argv[:1] == ["acquire"]:
        return _isolated_acquire(argv[1:])
    args = parser().parse_args(argv)
    stop = threading.Event()
    previous = {}
    try:
        config = load_config(args.config)
        if args.command == "status":
            if not 1 <= args.limit <= 1000:
                raise ConfigurationError("invalid_status_limit")
            emit(status(config.root, args.limit))
            return 0
        if args.command == "export":
            if args.output.resolve() in {args.config.resolve(), config.calendar_file}:
                raise ConfigurationError("protected_export_path")
            count = export_csv(
                config.root,
                args.output,
                args.start_date.isoformat() if args.start_date else None,
                args.end_date.isoformat() if args.end_date else None,
                config.evidence,
            )
            emit(
                {
                    "status": "complete",
                    "command": "export",
                    "row_count": count,
                    "output": str(args.output.resolve()),
                    "end_inclusive": True,
                }
            )
            return 0
        if getattr(args, "max_days", None) is not None and args.max_days < 1:
            raise ConfigurationError("invalid_max_days")
        if getattr(args, "limit", None) is not None and args.limit < 1:
            raise ConfigurationError("invalid_max_days")
        # calendar-refresh intentionally needs no existing calendar file.
        calendar = (
            None if args.command == "calendar-refresh" else load_calendar(config.calendar_file)
        )
        if threading.current_thread() is threading.main_thread():
            for signum in (signal.SIGINT, signal.SIGTERM):
                previous[signum] = signal.signal(signum, lambda *_: stop.set())
        with Journal(config.root) as journal:
            # ``consume`` reads captured evidence only: the client refuses every
            # request, so a source call is a bug rather than a slow path. The daily
            # lane captures literal source bytes and therefore runs the transport
            # in this process; only the calendar lane still crosses the provider
            # process boundary.
            if args.command == "consume":
                client = NoSourceClient()
            elif args.command == "calendar-refresh":
                client = ProcessClient(stop)
            else:
                client = CffexCsvClient(stop)
            try:
                if args.command == "calendar-refresh":
                    report = refresh_calendar(config, journal, client, stop)
                else:
                    assert calendar is not None
                    collector = Collector(
                        config, journal, calendar, client, stop, clock=lambda: datetime.now(UTC)
                    )
                    if args.command == "backfill":
                        report = collector.backfill(
                            args.start_date, args.end_date, args.refresh, args.max_days
                        )
                    elif args.command == "consume":
                        report = collector.consume(args.start_date, args.end_date, args.limit)
                    else:
                        report = collector.update(args.as_of)
            finally:
                client.close()
        emit(report)
        return EXIT_CODES[report["status"]]
    except LockBusy:
        emit({"status": "error", "code": "writer_locked"})
        return 5
    except (EvidenceStateError, JournalError) as exc:
        # Unknown or corrupt private state: refuse with a stable code and no
        # fallback to the source.
        emit({"status": "error", "code": exc.code, "detail": exc.detail})
        return 2
    except ProviderFailure as exc:
        emit({"status": "error", "code": exc.code})
        return 4 if exc.code == "cancelled" else 1
    except KeyboardInterrupt:
        emit({"status": "interrupted", "code": "cancelled"})
        return 4
    except ConfigurationError as exc:
        emit({"status": "error", "code": exc.code})
        return 2
    except ValidationError as exc:
        emit(
            {
                "status": "error",
                "code": "invalid_configuration",
                "validation_errors": [
                    {"location": list(item["loc"]), "type": item["type"]}
                    for item in exc.errors(
                        include_url=False, include_context=False, include_input=False
                    )
                ],
            }
        )
        return 2
    except Exception as exc:
        # Never put vendor payloads, filesystem secrets, or credential text on stdout.
        emit(
            {
                "status": "error",
                "code": "invalid_configuration_or_runtime",
                "error_type": type(exc).__name__,
            }
        )
        return 2
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)


if __name__ == "__main__":
    raise SystemExit(main())

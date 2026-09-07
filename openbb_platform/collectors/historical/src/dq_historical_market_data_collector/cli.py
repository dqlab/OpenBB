"""Standalone, bounded command-line interface."""

from __future__ import annotations

import argparse
import importlib.resources
import importlib.util
import signal
import threading
from datetime import UTC, date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from openbb_collector_core.delivery import read_delivery_status

from . import __version__
from .config import load_config
from .delivery import deliver
from .planner import chunks
from .providers import validate_providers
from .runtime import Collector, emit_json
from .storage import Journal, read_records, read_status


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(prog="dq-historical-market-data-collector")
    result.add_argument("--version", action="version", version=__version__)
    commands = result.add_subparsers(dest="command", required=True)
    init = commands.add_parser("init-config", help="Write the packaged example without overwriting")
    init.add_argument("--output", type=Path, required=True)
    for name in (
        "validate-config",
        "plan",
        "doctor",
        "once",
        "run",
        "status",
        "query",
        "smoke",
        "deliver",
        "delivery-status",
    ):
        command = commands.add_parser(name)
        command.add_argument("--config", type=Path, required=True)
        if name in {"plan", "once", "smoke"}:
            command.add_argument("--as-of", type=date.fromisoformat, default=None)
        if name in {"plan", "status", "query"}:
            command.add_argument("--limit", type=int, default=100 if name != "status" else 20)
        if name in {"deliver", "delivery-status"}:
            command.add_argument("--limit", type=int, default=10 if name == "deliver" else 20)
        if name == "once":
            command.add_argument("--refresh", action="store_true")
        if name == "run":
            command.add_argument("--max-seconds", type=float)
        if name == "query":
            command.add_argument("--instrument", required=True)
            command.add_argument("--source")
            command.add_argument("--as-of", type=datetime.fromisoformat, required=True)
        if name == "smoke":
            command.add_argument("--require-source", action="append", required=True)
            command.add_argument("--min-rows", type=int, default=1)
            command.add_argument("--max-seconds", type=float, default=180)
    return result


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        if args.command == "init-config":
            example = (
                importlib.resources.files("dq_historical_market_data_collector")
                .joinpath(
                    "example.yaml",
                )
                .read_text(encoding="utf-8")
            )
            with args.output.open("x", encoding="utf-8") as stream:
                stream.write(example)
            emit_json({"status": "created"})
            return 0
        config = load_config(args.config)
        as_of = (
            getattr(args, "as_of", None) or datetime.now(ZoneInfo(config.schedule.timezone)).date()
        )
        if args.command == "validate-config":
            validate_providers(config)
            next(chunks(config, datetime.now(UTC).date()), None)
            emit_json({"status": "valid", "config_hash": config.fingerprint()})
            return 0
        if args.command == "doctor":
            validate_providers(config)
            modules = ["openbb_core"]
            optional = {"parquet": "pyarrow", "duckdb": "duckdb"}.get(config.storage.format)
            if optional:
                modules.append(optional)
            checks = {
                name: importlib.util.find_spec(name) is not None for name in sorted(set(modules))
            }
            emit_json({"modules": checks, "gateway_checked": False, "entitlements_checked": False})
            return 0 if all(checks.values()) else 1
        if args.command == "plan":
            if not 1 <= args.limit <= 1000:
                raise ValueError("Invalid limit")
            planned = []
            truncated = False
            for chunk in chunks(config, as_of):
                if len(planned) == args.limit:
                    truncated = True
                    break
                planned.append(chunk.payload())
            emit_json({"chunks": planned, "truncated": truncated, "end_exclusive": True})
            return 0
        if args.command == "status":
            emit_json({"sessions": read_status(config.storage.root, args.limit)})
            return 0
        if args.command == "query":
            emit_json(
                {
                    "records": read_records(
                        config.storage.root,
                        instrument_id=args.instrument,
                        source=args.source,
                        as_of=as_of,
                        limit=args.limit,
                    )
                }
            )
            return 0
        if args.command == "delivery-status":
            emit_json(read_delivery_status(config.storage.root, config.delivery, args.limit))
            return 0
        if args.command == "deliver":
            if config.delivery is None or not 1 <= args.limit <= 100:
                raise ValueError("Configure delivery and a limit of 1..100")
            with Journal(config.storage) as journal:
                journal.recover()
                result = deliver(config, journal, force=True, limit=args.limit)
            emit_json(result)
            return (
                1
                if (
                    result["counts"].get("pending")
                    or result.get("pending_output_sessions")
                    or result.get("batch_limit_reached")
                )
                or any(event["status"] == "pending" for event in result["events"])
                else 0
            )
        if args.command == "smoke":
            if not 0 < args.max_seconds <= 600:
                raise ValueError("smoke duration must be 0..600 seconds")
        validate_providers(config)
        stop = threading.Event()
        previous = {}

        def halt(*_: object) -> None:
            stop.set()

        for signum in (signal.SIGINT, signal.SIGTERM):
            previous[signum] = signal.signal(signum, halt)
        timer = None
        try:
            with Journal(config.storage) as journal:
                collector = Collector(
                    config, journal, stop=stop, emit=emit_json, config_path=args.config
                )
                try:
                    if args.command == "run":
                        collector.run(args.max_seconds)
                        return 0
                    if args.command == "smoke":
                        timer = threading.Timer(args.max_seconds, stop.set)
                        timer.start()
                        result = collector.acceptance(
                            as_of=as_of,
                            required_sources=args.require_source,
                            min_rows=args.min_rows,
                        )
                        return 0 if result["passed"] else 1
                    result = collector.collect(as_of=as_of, refresh=args.refresh)
                    return 0 if result["status"] == "complete" else 1
                finally:
                    collector.close()
        finally:
            if timer:
                timer.cancel()
            for signum, handler in previous.items():
                signal.signal(signum, handler)
    except (Exception, KeyboardInterrupt):
        # Validation and vendor exceptions may contain secrets or private filesystem paths.
        emit_json({"status": "error", "code": "invalid_configuration_or_runtime"})
        return 2

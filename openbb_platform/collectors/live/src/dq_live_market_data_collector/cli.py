"""Foreground service CLI; validation, preview, and status never connect to a provider."""

from __future__ import annotations

import argparse
import importlib.resources
import importlib.util
import json
import signal
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from openbb_collector_core.delivery import read_delivery_status
from openbb_collector_core.providers import package_metadata
from pydantic import ValidationError

from . import __version__
from .acceptance import assess, checkpoint, required_pairs
from .config import load_config
from .delivery import deliver
from .providers import validate_providers
from .schedule import window_on
from .service import Collector
from .storage import Journal, read_status


def emit(payload: dict[str, Any]) -> None:
    print(json.dumps(payload, sort_keys=True, allow_nan=False), flush=True)


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--version", action="version", version=__version__)
    subparsers = result.add_subparsers(dest="command", required=True)
    initialize = subparsers.add_parser("init-config")
    initialize.add_argument("--output", type=Path, required=True)
    for command in (
        "validate-config",
        "doctor",
        "plan",
        "run",
        "once",
        "status",
        "export",
        "deliver",
        "delivery-status",
        "smoke",
    ):
        child = subparsers.add_parser(command)
        child.add_argument("--config", required=True, help="YAML configuration file")
        if command == "plan":
            child.add_argument("--date", required=True, type=date.fromisoformat)
        if command == "run":
            child.add_argument("--max-seconds", type=float)
        if command == "smoke":
            child.add_argument("--max-seconds", type=float, default=120)
            child.add_argument("--require-source", action="append", required=True)
            child.add_argument("--min-samples", type=int, default=1)
        if command in {"status", "delivery-status", "deliver"}:
            child.add_argument("--limit", type=int, default=20)
        if command == "export":
            child.add_argument("--max-batches", type=int, default=100)
    return result


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        if args.command == "init-config":
            example = (
                importlib.resources.files("dq_live_market_data_collector")
                .joinpath("example.yaml")
                .read_text(encoding="utf-8")
            )
            with args.output.open("x", encoding="utf-8") as stream:
                stream.write(example)
            emit({"created": True})
            return 0
        config = load_config(args.config)
        if args.command == "validate-config":
            validate_providers(config)
            emit(
                {
                    "valid": True,
                    "config_hash": config.fingerprint(),
                    "instruments": len(config.instruments),
                    "collections": len(config.collections),
                    "format": config.storage.format,
                }
            )
        elif args.command == "doctor":
            validate_providers(config)
            modules = ["openbb_core"]
            if config.storage.format in {"parquet", "duckdb"}:
                modules.append({"parquet": "pyarrow", "duckdb": "duckdb"}[config.storage.format])
            missing = [module for module in modules if importlib.util.find_spec(module) is None]
            emit(
                {
                    "valid_config": True,
                    "missing_modules": missing,
                    "packages": {
                        name: package_metadata(source.provider, source.model)
                        for name, source in config.sources.items()
                    },
                    "provider_connection": "not_probed",
                    "entitlements": "not_probed",
                }
            )
            return 1 if missing else 0
        elif args.command == "plan":
            planned = []
            for name, collection in config.collections.items():
                window = window_on(collection.schedule, args.date)
                planned.append(
                    {
                        "collection": name,
                        "date": args.date.isoformat(),
                        "start_utc": window.start.isoformat() if window else None,
                        "end_utc": window.end.isoformat() if window else None,
                        "instrument_ids": collection.instrument_ids,
                        "sources": [collection.primary, *collection.secondary],
                        "frequency_seconds": collection.frequency_seconds,
                        "fields": collection.fields,
                    }
                )
            emit({"collections": planned, "calendar_basis": "operator_configuration"})
        elif args.command == "delivery-status":
            emit(read_delivery_status(config.storage.root, config.delivery, args.limit))
        elif args.command == "deliver":
            if config.delivery is None or not 1 <= args.limit <= 100:
                raise ValueError("Configure delivery and a limit of 1..100")
            with Journal(config.storage) as journal:
                journal.export_pending(100)
                now = datetime.now(UTC)
                for session in journal.running_sessions():
                    completed = datetime.fromisoformat(session["end"]) <= now
                    journal.finish_session(
                        session["id"],
                        "completed" if completed else "interrupted",
                        "session_closed" if completed else "delivery_recovery",
                    )
                journal.write_reports(now)
                result = deliver(config, journal, force=True, limit=args.limit)
            emit(result)
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
        elif args.command == "status":
            emit(read_status(config.storage.root, args.limit))
        else:
            if args.command == "run" and args.max_seconds is not None:
                if not 0 < args.max_seconds <= 31_536_000:
                    raise ValueError("max-seconds must be positive and at most one year")
            if args.command == "smoke":
                if not 0 < args.max_seconds <= 600 or not 1 <= args.min_samples <= 1000:
                    raise ValueError("Smoke duration must be at most 600s; minimum samples 1-1000")
                required_pairs(config, args.require_source)
            if args.command == "export" and not 1 <= args.max_batches <= 10000:
                raise ValueError("max-batches must be between 1 and 10000")
            if args.command != "export":
                validate_providers(config)
            with Journal(config.storage) as journal:
                if args.command == "export":
                    emit({"exported_records": journal.export_pending(args.max_batches)})
                    journal.write_reports(datetime.now(UTC))
                    if config.delivery:
                        emit(deliver(config, journal))
                else:
                    before = checkpoint(journal)
                    collector = Collector(config, journal, emit=emit)
                    previous = {}
                    for signum in (signal.SIGINT, signal.SIGTERM):
                        previous[signum] = signal.signal(signum, lambda *_: collector.stop.set())
                    try:
                        if args.command == "once":
                            collector.step()
                            collector.shutdown("bounded_run")
                            emit({"event": "collection_pass_complete"})
                        else:
                            collector.run(args.max_seconds)
                    finally:
                        collector.shutdown()
                        for signum, handler in previous.items():
                            signal.signal(signum, handler)
                    if args.command == "smoke":
                        report = assess(
                            journal, config, before, args.require_source, args.min_samples
                        )
                        emit(report)
                        return 0 if report["passed"] else 1
        return 0
    except ValidationError as exc:
        emit(
            {
                "error": "invalid_configuration",
                "issues": [
                    {"location": list(item["loc"]), "message": item["msg"]}
                    for item in exc.errors(include_input=False, include_url=False)
                ],
            }
        )
        return 2
    except (ValueError, OSError, RuntimeError) as exc:
        # Exception text may contain credentials, storage paths, or vendor URLs.
        emit({"error": type(exc).__name__, "operation": args.command})
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

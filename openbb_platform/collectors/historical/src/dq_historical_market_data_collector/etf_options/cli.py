# Keep acquisition-only imports free of provider/parsing/journal initialization.
# pylint: disable=import-outside-toplevel

"""Machine-readable CLI for same-day closing chains and diagnostic export.

Two kinds of command live here, and only one of them is acquisition:

``acquire``
    the acquisition-only task (:mod:`.acquire`): a literal plan and a private
    evidence root in, document bytes out, then exit.  It is dispatched before
    anything else is imported, so this path loads no configuration, no journal,
    no quality module and no catalog -- ``python -m ...etf_options.cli acquire``
    works with no config file at all.

``plan``
    writes the literal ``etf-acquire-plan-v2`` for one day from the configuration
    (:func:`.runtime.acquisition_plan`).  It fetches nothing: it is the hand-off
    between the two halves, and the plan it writes carries the identity block that
    makes the acquisition's evidence the collector's evidence.

``collect`` / ``calendar-refresh`` / ``status`` / ``export`` / ``deliver`` /
``delivery-status``
    the downstream commands, which do read configuration and the local journal.

``collect --acquire-plan PLAN`` adopts that plan's identity: the collector then
looks for exactly the locators the acquisition task wrote, and reuses a completed
capture with no source call (``0`` calls, same locator, same bytes).
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

EXIT_CODES = {"complete": 0, "skipped": 0, "failed": 1, "interrupted": 4}


def Collector(*args, **kwargs):
    """The collection class, resolved when a collection command actually runs.

    A module-level name so an operator or a test can substitute the worker
    boundary, without this module importing the runtime graph (and therefore
    ``.config``/``.storage``/``.quality``) on ``import``.
    """
    from .runtime import Collector as implementation

    return implementation(*args, **kwargs)


def _iso_date(value: str):
    """Deferred ``config.iso_date``: the acquisition command has no ``--date``."""
    from .config import iso_date

    return iso_date(value)


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(
        prog="dq-etf-option-chain-collector", description=__doc__
    )
    commands = result.add_subparsers(dest="command", required=True)
    acquire = commands.add_parser(
        "acquire", help="persist literal document bytes for a plan; parse nothing"
    )
    acquire.add_argument("--plan", type=Path, required=True)
    acquire.add_argument("--root", type=Path, required=True)
    for name in (
        "collect",
        "calendar-refresh",
        "status",
        "export",
        "deliver",
        "delivery-status",
        "plan",
    ):
        command = commands.add_parser(name)
        command.add_argument("--config", type=Path, required=True)
        if name in {"collect", "export", "deliver", "plan"}:
            command.add_argument("--date", type=_iso_date)
        if name == "collect":
            command.add_argument("--no-resume", action="store_true")
            command.add_argument(
                "--acquire-plan",
                type=Path,
                help="adopt this etf-acquire-plan-v2's identity and reuse its captures",
            )
            command.add_argument(
                "--approve-universe-change",
                action="store_true",
                help="Explicitly approve same-day removed contracts; audit in run",
            )
        if name == "plan":
            command.add_argument("--out", type=Path, required=True)
        if name == "status":
            command.add_argument("--limit", type=int, default=100)
        if name == "delivery-status":
            command.add_argument("--limit", type=int, default=20)
        if name == "deliver":
            command.add_argument("--limit", type=int, default=100)
            command.add_argument("--force", action="store_true")
            command.add_argument("--stage-only", action="store_true")
            command.add_argument("--pending-only", action="store_true")
        if name == "export":
            command.add_argument("--output", type=Path, required=True)
    return result


def emit(report: dict) -> None:
    print(
        json.dumps(report, sort_keys=True, ensure_ascii=False, allow_nan=False),
        flush=True,
    )


def _handoff(path: Path | None) -> dict | None:
    """Read a plan's identity block; a plan without one cannot be adopted."""
    if path is None:
        return None
    from .config import ConfigurationError

    try:
        plan = json.loads(Path(path).read_bytes())
    except (OSError, ValueError, UnicodeError) as exc:
        raise ConfigurationError("invalid_plan") from exc
    block = plan.get("identity") if isinstance(plan, dict) else None
    if not isinstance(block, dict):
        raise ConfigurationError("plan_without_identity")
    return block


def _write_plan(cfg, args) -> int:
    """Write the literal acquisition plan for one day.  Fetches nothing."""
    from datetime import datetime

    from .config import SHANGHAI, ConfigurationError
    from .runtime import acquisition_plan

    target = args.date or datetime.now(SHANGHAI).date()
    plan = acquisition_plan(cfg, target)
    payload = json.dumps(plan, sort_keys=True, indent=2) + "\n"
    if args.out.exists():
        # A plan is addressed by its identity block, so an existing plan with
        # different content is a different day, configuration or token -- refused
        # rather than silently overwritten, and an identical one is not rewritten.
        if args.out.read_text(encoding="utf-8") != payload:
            raise ConfigurationError("plan_conflict")
    else:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        with args.out.open("x", encoding="utf-8") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
    emit(
        {
            "command": "plan",
            "status": "complete",
            "date": target.isoformat(),
            "plan": str(args.out),
            "documents": len(plan["requests"]),
            "identity": plan["identity"],
        }
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    if args.command == "acquire":
        # Nothing above this line imported configuration, storage or the runtime.
        from .acquire import main as acquire_main

        return acquire_main(["--plan", str(args.plan), "--root", str(args.root)])
    return _run(args)


def _run(args: argparse.Namespace) -> int:
    import signal
    import threading

    from .config import ConfigurationError, load_calendar, load_config
    from .delivery import deliver, delivery_status
    from .storage import Journal, LockBusy, export_csv, status

    stop, previous = threading.Event(), {}
    try:
        cfg = load_config(args.config)
        if args.command == "plan":
            return _write_plan(cfg, args)
        if args.command == "status":
            emit(status(cfg.root, args.limit))
            return 0
        if args.command == "delivery-status":
            emit(delivery_status(cfg, args.limit))
            return 0
        if args.command == "export":
            if args.output.resolve() in {args.config.resolve(), cfg.calendar_file}:
                raise ConfigurationError("protected_export_path")
            count = export_csv(
                cfg.root, args.output, args.date.isoformat() if args.date else None
            )
            emit(
                {
                    "command": "export",
                    "status": "complete",
                    "row_count": count,
                    "output": str(args.output.resolve()),
                }
            )
            return 0
        if threading.current_thread() is threading.main_thread():
            for signum in (signal.SIGINT, signal.SIGTERM):
                previous[signum] = signal.signal(signum, lambda *_: stop.set())
        if args.command == "deliver":
            with Journal(cfg.root) as journal:
                report = deliver(
                    cfg,
                    journal,
                    day=args.date.isoformat() if args.date else None,
                    force=args.force,
                    send=not args.stage_only,
                    limit=args.limit,
                    pending_only=args.pending_only,
                    stop=stop,
                )
            emit(report)
            return 0 if report["status"] in {"complete", "skipped"} else 1
        calendar = (
            None
            if args.command == "calendar-refresh"
            else load_calendar(cfg.calendar_file)
        )
        handoff = _handoff(args.acquire_plan) if args.command == "collect" else None
        with Journal(cfg.root) as journal:
            collector = Collector(cfg, journal, calendar, stop=stop, handoff=handoff)
            original = None
            try:
                report = (
                    collector.refresh_calendar()
                    if args.command == "calendar-refresh"
                    else collector.collect(
                        args.date, not args.no_resume, args.approve_universe_change
                    )
                )
            except BaseException as exc:
                original = exc
                raise
            finally:
                try:
                    collector.close()
                except BaseException as cleanup:
                    if original is not None:
                        original.add_note(
                            f"ETF worker cleanup failed: {type(cleanup).__name__}"
                        )
                    else:
                        report["status"] = "failed"
                        report["response_complete"] = False
                        report["errors"].append({"code": "worker_cleanup_failure"})
                        try:
                            journal.abort_run(report["run_id"], report)
                        except BaseException as secondary:
                            cleanup.add_note(
                                f"ETF journal cleanup failed: {type(secondary).__name__}"
                            )
                        raise
            if stop.is_set() and report["status"] != "interrupted":
                report["status"] = "interrupted"
                report["response_complete"] = False
                journal.abort_run(report["run_id"], report)
        emit(report)
        return EXIT_CODES[report["status"]]
    except LockBusy:
        emit({"status": "failed", "code": "writer_locked", "command": args.command})
        return 5
    except (KeyboardInterrupt, InterruptedError):
        emit({"status": "interrupted", "code": "cancelled", "command": args.command})
        return 4
    except Exception as exc:
        emit(
            {
                "status": "failed",
                "code": (
                    exc.code
                    if isinstance(exc, ConfigurationError)
                    else "configuration_or_runtime_failure"
                ),
                "error_type": type(exc).__name__,
                "command": args.command,
            }
        )
        return 2
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)


if __name__ == "__main__":
    raise SystemExit(main())

"""Offline raw snapshot replay and read-only segment inspection."""

from __future__ import annotations

import argparse
import importlib
import importlib.metadata
import json
import math
import os
import queue
import signal
import subprocess
import sys
import threading
import time
from datetime import datetime
from datetime import timezone as tz
from pathlib import Path

from .tick_file import TickIngress, TickWriter, inspect_file
from .tick_source import load_source


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    replay = commands.add_parser("replay", help="record synthetic JSONL payloads")
    replay.add_argument("fixture", type=Path)
    replay.add_argument("--root", type=Path, required=True)
    replay.add_argument("--source-id", default="replay")
    replay.add_argument("--capacity", type=int, default=10000)
    replay.add_argument("--max-bytes", type=int, default=256 * 1024 * 1024)
    replay.add_argument("--sync-seconds", type=float, default=1.0)
    native = commands.add_parser(
        "native", help="explicitly connect an OpenCTP futures MdApi"
    )
    native.add_argument(
        "--profile",
        choices=("ctp_futures", "securities", "securities_options"),
        default="ctp_futures",
    )
    native.add_argument("--root", type=Path, required=True)
    native.add_argument("--front", required=True)
    native.add_argument("--broker", required=True)
    native.add_argument("--user-env", default="CTP_USER")
    native.add_argument("--password-env", default="CTP_PASSWORD")
    native.add_argument(
        "--instruments",
        type=Path,
        required=True,
        help="JSON array of raw instrument IDs",
    )
    native.add_argument("--source-id", default="ctp-futures")
    native.add_argument(
        "--environment",
        required=True,
        help="account environment label; does not imply live data",
    )
    native.add_argument("--duration", type=float, required=True)
    native.add_argument("--batch-size", type=int, default=1)
    native.add_argument("--interval", type=float, default=1.0)
    native.add_argument("--ack-timeout", type=float, default=10.0)
    native.add_argument("--attempts", type=int, default=3)
    native.add_argument("--capacity", type=int, default=10000)
    native.add_argument("--max-bytes", type=int, default=256 * 1024 * 1024)
    native.add_argument("--max-seconds", type=float, default=3600)
    native.add_argument("--sync-seconds", type=float, default=1.0)
    native.add_argument(
        "--startup-timeout",
        type=float,
        default=10.0,
        help="startup allowance in the supervisor's total hard time budget (seconds)",
    )
    native.add_argument("--shutdown-timeout", type=float, default=10.0)
    native.add_argument(
        "--md-module", help="explicit MdApi module override for isolated testing"
    )
    verify = commands.add_parser("verify", help="inspect complete JSONL prefixes")
    verify.add_argument("path", type=Path)
    args = parser.parse_args(argv)
    if args.command == "verify":
        paths = (
            list(args.path.rglob("part-*.jsonl*"))
            if args.path.is_dir()
            else [args.path]
        )
        paths = [
            p
            for p in paths
            if p.name.startswith("part-") and p.name.endswith((".jsonl", ".jsonl.open"))
        ]
        paths.sort(
            key=lambda p: (
                next((x for x in p.parts if x.startswith("session=")), str(p.parent)),
                p.name,
            )
        )
        results = [inspect_file(path) for path in paths]
        last: dict = {}
        parts: dict = {}
        damaged: dict = {}
        for path, result in zip(paths, results, strict=True):
            session = next(
                (p for p in path.parts if p.startswith("session=")), str(path.parent)
            )
            try:
                part = int(path.name.split(".")[0].removeprefix("part-"))
            except ValueError:
                part = -1
            prior_damage = damaged.get(session)
            boundary_error = None
            if part != parts.get(session, 0) + 1:
                boundary_error = "missing_or_duplicate_segment"
            parts[session] = part
            if result["first_seq"] is not None:
                expected = last.get(session, 0) + 1
                if result["first_seq"] != expected and not boundary_error:
                    boundary_error = "noncontiguous_segment_sequence"
                last[session] = result["last_seq"]
            result["boundary_error"] = boundary_error
            if boundary_error and not result["error"]:
                result["error"] = boundary_error
            result["segment_readable_bytes"] = result["valid_bytes"]
            result["session_trusted_bytes"] = (
                0 if prior_damage or boundary_error else result["valid_bytes"]
            )
            if (result["error"] or boundary_error) and not prior_damage:
                damaged[session] = {
                    "file": str(path),
                    "error": boundary_error or result["error"],
                }
            result["session_corruption"] = damaged.get(session)
        # Report the session's eventual corruption even on its earlier segments.
        for path, result in zip(paths, results, strict=True):
            session = next(
                (p for p in path.parts if p.startswith("session=")), str(path.parent)
            )
            result["session_corruption"] = damaged.get(session)
        print(json.dumps(results, ensure_ascii=False))
        return int(not paths or any(result["error"] for result in results))
    if args.command == "native":
        if os.environ.get("DQ_CTP_SUPERVISED_CHILD") == "1":
            return run_native(args)
        return supervise_native(args, argv)
    leftovers = [inspect_file(path) for path in sorted(args.root.rglob("*.jsonl.open"))]
    writer = TickWriter(args.root, args.source_id, max_bytes=args.max_bytes)
    ingress = TickIngress(
        writer,
        profile="replay",
        environment="synthetic",
        capacity=args.capacity,
        sync_seconds=args.sync_seconds,
    )
    events = []
    source = load_source("replay")(
        args.fixture, ingress.callback, lambda kind, data: events.append((kind, data))
    )
    stopped = threading.Event()
    previous = {}

    def stop_handler(signum, frame):
        del signum, frame
        stopped.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        previous[sig] = signal.signal(sig, stop_handler)
    try:
        source.start()
        while not (source.done.wait(0.05) or stopped.is_set() or ingress.stop.is_set()):
            pass
        source.stop_and_status()
        source_closed = source.close()
        source_status = source.status()
        if (
            source_status["state"] != "eof"
            or source.failed.is_set()
            or not source_closed
        ):
            ingress.coverage_lost = True
        writer_closed = ingress.close()
        ok = (
            source_status["state"] == "eof"
            and source_closed
            and writer_closed
            and not source.failed.is_set()
            and writer.written > 0
        )
        if source.failed.is_set():
            print(f"replay failed: {source.status()['error']}", file=sys.stderr)
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)
    print(
        json.dumps(
            {
                "session_id": writer.session_id,
                "ok": ok,
                "status": ingress.status(),
                "source": source.status(),
                "events": events,
                "previous_open_files": leftovers,
            },
            ensure_ascii=False,
        )
    )
    return 0 if ok else 1


def supervise_native(args, argv):
    """Bound the complete native and writer process, including blocked native calls."""
    if any(
        not math.isfinite(value) or value <= 0
        for value in (args.duration, args.shutdown_timeout, args.startup_timeout)
    ) or not math.isfinite(
        args.duration + args.shutdown_timeout + args.startup_timeout
    ):
        raise ValueError(
            "duration, startup and shutdown timeouts must be finite and positive"
        )
    env = dict(os.environ, DQ_CTP_SUPERVISED_CHILD="1")
    command = list(sys.argv[1:] if argv is None else argv)
    # Set the Linux death signal in a fresh interpreter, not in a post-fork
    # callback in this potentially multithreaded process. Check the parent
    # after prctl to close the race in which it died before registration.
    bootstrap = (
        "import ctypes,os,runpy,signal,sys; "
        "parent=int(sys.argv[1]); "
        "linux=sys.platform.startswith('linux'); "
        "failed=linux and ctypes.CDLL(None,use_errno=True).prctl(1,signal.SIGKILL)!=0; "
        "sys.exit(1) if failed or (linux and os.getppid()!=parent) else None; "
        "sys.argv=sys.argv[1:]; "
        'runpy.run_module("dq_live_market_data_collector.tick_cli",run_name="__main__")'
    )
    previous = {}
    with subprocess.Popen(
        [sys.executable, "-c", bootstrap, str(os.getpid()), *command],
        env=env,
        start_new_session=True,
    ) as child:
        try:
            # One hard budget covers interpreter startup, acquisition, and shutdown.
            deadline = (
                time.monotonic()
                + args.startup_timeout
                + args.duration
                + args.shutdown_timeout
            )

            def stop(signum, frame):
                nonlocal deadline
                del frame
                deadline = min(deadline, time.monotonic() + args.shutdown_timeout)
                if child.poll() is None:
                    child.send_signal(signum)

            for sig in (signal.SIGINT, signal.SIGTERM):
                previous[sig] = signal.signal(sig, stop)
            while child.poll() is None and time.monotonic() < deadline:
                time.sleep(0.02)
            if child.poll() is None:
                print(
                    "native child exceeded deadline; open segments may be incomplete",
                    file=sys.stderr,
                )
                child.kill()
                child.wait()
                return 1
            return child.returncode if child.returncode >= 0 else 1
        finally:
            if child.poll() is None:
                child.kill()
                child.wait()
            for sig, handler in previous.items():
                signal.signal(sig, handler)


def run_native(args):
    """Run a bounded opt-in session; secrets are read only from environment."""
    if args.profile != "ctp_futures":
        raise ValueError(f"unsupported source profile: {args.profile}")
    NativeSession = load_source("ctp")

    if args.duration <= 0 or args.shutdown_timeout <= 0:
        raise ValueError("duration and shutdown timeout must be positive")
    user, password = os.environ.get(args.user_env), os.environ.get(args.password_env)
    if not user or not password:
        raise ValueError("credential environment variables must be set")
    instruments = json.loads(args.instruments.read_text(encoding="utf-8"))
    if not isinstance(instruments, list) or any(
        not isinstance(x, str) for x in instruments
    ):
        raise ValueError("instruments must be a JSON array of strings")
    leftovers = [inspect_file(path) for path in sorted(args.root.rglob("*.jsonl.open"))]
    writer = TickWriter(
        args.root,
        args.source_id,
        max_bytes=args.max_bytes,
        max_seconds=args.max_seconds,
    )
    try:
        wrapper_version = (
            importlib.metadata.version("openctp-ctp")
            if not args.md_module
            else "synthetic"
        )
    except importlib.metadata.PackageNotFoundError:
        wrapper_version = "unknown"
    md_module = importlib.import_module(args.md_module or "openctp_ctp.thostmduserapi")
    api_version = "unknown"
    if not args.md_module:
        api_version = str(md_module.CThostFtdcMdApi.GetApiVersion())
    ingress = TickIngress(
        writer,
        profile="ctp_futures",
        environment=args.environment,
        capacity=args.capacity,
        sync_seconds=args.sync_seconds,
        wrapper_version=wrapper_version,
        native_api_version=api_version,
    )
    session_dir = args.root / ("run-" + writer.session_id)
    session_dir.mkdir(parents=True, exist_ok=False)
    flow = session_dir / "flow"
    flow.mkdir()
    events: queue.Queue = queue.Queue(maxsize=10000)
    event_failure = threading.Event()

    def on_event(kind, data):
        if kind == "disconnected":
            ingress.coverage_lost = True
        try:
            events.put_nowait(
                {
                    "event": kind,
                    "at_utc": datetime.now(tz.utc).isoformat(),  # noqa: UP017
                    **data,
                }
            )
        except queue.Full:
            event_failure.set()

    session = NativeSession(
        args.front,
        args.broker,
        user,
        password,
        str(flow) + os.sep,
        instruments,
        ingress.callback,
        on_event,
        batch_size=args.batch_size,
        interval=args.interval,
        ack_timeout=args.ack_timeout,
        attempts=args.attempts,
        md=md_module,
        on_raw_tick=ingress.callback,
    )
    stopped = threading.Event()
    previous = {}

    def stop_handler(signum, frame):
        del signum, frame
        stopped.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        previous[sig] = signal.signal(sig, stop_handler)
    ok = False
    try:
        with (session_dir / "events.jsonl").open("x", encoding="utf-8") as event_file:

            def drain():
                while True:
                    try:
                        event = events.get_nowait()
                    except queue.Empty:
                        break
                    event_file.write(
                        json.dumps(event, ensure_ascii=False, allow_nan=False) + "\n"
                    )
                event_file.flush()
                os.fsync(event_file.fileno())

            def status():
                data = {
                    "session_id": writer.session_id,
                    "updated_at_utc": datetime.now(tz.utc).isoformat(),  # noqa: UP017
                    "source": session.status(),
                    "ingress": ingress.status(),
                }
                temp = session_dir / "status.json.tmp"
                with temp.open("w", encoding="utf-8") as out:
                    json.dump(data, out, ensure_ascii=False)
                    out.flush()
                    os.fsync(out.fileno())
                os.replace(temp, session_dir / "status.json")

            on_event(
                "session_start",
                {"session_id": writer.session_id, "previous_open_files": leftovers},
            )
            session.start()
            deadline = time.monotonic() + args.duration
            while not (
                stopped.is_set()
                or session.failed.is_set()
                or ingress.stop.is_set()
                or event_failure.is_set()
                or time.monotonic() >= deadline
            ):
                stopped.wait(0.1)
                drain()
                status()
            on_event("session_stopping", {"signal": stopped.is_set()})
            shutdown_deadline = time.monotonic() + args.shutdown_timeout
            source_status = session.stop_and_status()
            source_closed = session.close(
                timeout=max(0, shutdown_deadline - time.monotonic())
            )
            writer_closed = ingress.close(
                timeout=max(0, shutdown_deadline - time.monotonic())
            )
            all_acknowledged = (
                source_status["connected"] is True
                and source_status["logged_in"] is True
                and bool(source_status["subscriptions"])
                and all(
                    item["state"] == "acknowledged"
                    and item.get("generation") == source_status["generation"]
                    for item in source_status["subscriptions"].values()
                )
            )
            ok = (
                all_acknowledged
                and source_closed
                and writer_closed
                and writer.written > 0
                and not session.failed.is_set()
                and not event_failure.is_set()
            )
            on_event(
                "session_end",
                {"ok": ok, "status": ingress.status(), "source": session.status()},
            )
            drain()
            status()
    except Exception as exc:
        ok = False
        print(f"native session failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        session.close(timeout=args.shutdown_timeout)
        ingress.close(timeout=args.shutdown_timeout)
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)
    print(
        json.dumps(
            {
                "session_id": writer.session_id,
                "ok": ok,
                "status": ingress.status(),
                "source": session.status(),
                "previous_open_files": leftovers,
            },
            ensure_ascii=False,
        )
    )
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())

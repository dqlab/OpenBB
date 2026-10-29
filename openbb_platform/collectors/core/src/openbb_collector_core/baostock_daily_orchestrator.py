"""Acquire pinned daily reference evidence and run a BaoStock daily campaign."""

import argparse
import fcntl
import json
import multiprocessing
import os
import time
import uuid
from datetime import date
from multiprocessing.reduction import DupFd
from pathlib import Path
from types import SimpleNamespace

from . import baostock_campaign as campaign
from . import baostock_daily as daily
from .baostock_daily_plan import _rows, shanghai_date


def _reference_worker(operation, params, output, connection, lock_handle, day, deadline):
    # A duplicate of the very same open file description retains flock after
    # parent death. It is released only when the source worker actually exits.
    lock_fd = lock_handle.detach()
    from openbb_baostock.utils import source_evidence as source

    def admit(kind):
        connection.send(kind)
        if not connection.recv():
            raise RuntimeError("source admission refused")
        if kind != "logout" and (shanghai_date() != day or time.monotonic() >= deadline):
            raise RuntimeError("source admission expired")

    original_login, original_logout = source.helpers.bs.login, source.helpers.bs.logout
    original_resolve = source.helpers.resolve_method

    def login():
        admit("login")
        return original_login()

    def logout():
        admit("logout")
        return original_logout()

    def resolve(method):
        original = original_resolve(method)

        def query(**kwargs):
            admit("query")
            result = original(**kwargs)
            if result is not None:
                old_next = result.next

                def next_page():
                    admit("page")
                    return old_next()

                result.next = next_page
            return result

        return query

    source.helpers.bs.login = login
    source.helpers.bs.logout = logout
    source.helpers.resolve_method = resolve
    try:
        daily.atomic_json(Path(output), source._query(operation, params))
    finally:
        connection.close()
        os.close(lock_fd)


def _durable_read(path):
    # A visible rename may have preceded a failed directory fsync. Flush both
    # the actual object and its directory before accepting its contents.
    with path.open("rb") as stream:
        os.fsync(stream.fileno())
    campaign.raw._sync(path.parent)
    return campaign.load_json(path)


def run(args):
    day = shanghai_date()
    if args.date and args.date != day:
        raise ValueError("date_must_be_current_shanghai_day")
    config = campaign.load_json(args.config)
    if (
        not date.fromisoformat(config["start_date"])
        <= date.fromisoformat(day)
        <= date.fromisoformat(config["end_date"])
    ):
        raise ValueError("outside_configured_date_range")
    args.work.mkdir(parents=True, exist_ok=True)
    with (args.work / ".orchestrator.lock").open("a+b") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        directory = args.work / "references" / day
        directory.mkdir(parents=True, exist_ok=True)
        for parent in reversed((directory.resolve(), *directory.resolve().parents)):
            campaign.raw._sync(parent)
        identity = dict(
            version="baostock-orchestrator-v1",
            date=day,
            config=config,
            root=str(args.root.resolve()),
            reference_budget=args.reference_budget,
            timeout=args.timeout,
            request_budget=args.request_budget,
            max_attempts=args.max_attempts,
            backoff_seconds=args.backoff_seconds,
            history_daily_budget=args.history_daily_budget,
        )
        identity_path = directory / "identity.json"
        if identity_path.exists():
            if _durable_read(identity_path) != identity:
                raise ValueError("reference_identity_conflict")
        else:
            daily.atomic_json(identity_path, identity)
        deadline = time.monotonic() + args.seconds
        specs = (
            (
                "calendar",
                "BaostockTradeDates",
                {"start_date": day, "end_date": day},
                ["calendar_date", "is_trading_day"],
            ),
            (
                "universe",
                "BaostockStockBasic",
                {},
                ["code", "code_name", "ipoDate", "outDate", "type", "status"],
            ),
        )
        retained = {}
        for name, operation, params, fields in specs:
            pinned = directory / (name + ".json")
            if pinned.exists():
                pointer = _durable_read(pinned)
                digest = pointer["sha256"]
                if (
                    not isinstance(digest, str)
                    or len(digest) != 64
                    or any(c not in "0123456789abcdef" for c in digest)
                ):
                    raise ValueError("invalid_reference_pointer")
                evidence_path = directory / ("evidence-" + digest + ".json")
                evidence = _durable_read(evidence_path)
                if campaign.digest(evidence) != digest:
                    raise ValueError("reference_evidence_conflict")
                _rows(evidence, operation, params, fields)
                retained[name] = evidence_path
                continue
            calls = list(directory.glob("call-*.json"))
            for call in calls:
                record = _durable_read(call)
                if (
                    set(record) != {"attempt", "kind", "status"}
                    or record["kind"] not in ("login", "query", "page", "logout")
                    or record["status"] != "unknown"
                ):
                    raise ValueError("invalid_reference_call_journal")
            # Legacy attempts have no per-call journal and therefore cannot be
            # safely priced. Preserve them and refuse further source calls.
            attempts = list(directory.glob("attempt-*.json"))
            if any("call_journal" not in _durable_read(p) for p in attempts):
                return dict(
                    status="partial", date=day, reason="legacy_reference_calls_unknown", source=name
                )
            if len(calls) >= args.reference_budget:
                return dict(
                    status="partial", date=day, reason="reference_budget_exhausted", source=name
                )
            if shanghai_date() != day or time.monotonic() >= deadline:
                return dict(
                    status="partial",
                    date=day,
                    reason="date_changed" if shanghai_date() != day else "time_budget",
                )
            token = str(time.time_ns()) + "-" + uuid.uuid4().hex
            attempt = directory / ("attempt-" + token + ".json")
            output = directory / ("result-" + token + ".json")
            # Reserve durably before starting a child, including crashes and timeouts.
            daily.atomic_json(
                attempt, dict(operation=operation, params=params, status="reserved", call_journal=1)
            )
            context = multiprocessing.get_context("spawn")
            parent, child = context.Pipe()
            process = context.Process(
                target=_reference_worker,
                args=(operation, params, str(output), child, DupFd(lock.fileno()), day, deadline),
            )
            process.start()
            child.close()
            limit = min(time.monotonic() + args.timeout, deadline)
            refusal = None
            while process.is_alive() and time.monotonic() < limit:
                if not parent.poll(min(0.05, max(0, limit - time.monotonic()))):
                    continue
                try:
                    kind = parent.recv()
                except EOFError:
                    break
                calls = list(directory.glob("call-*.json"))
                if len(calls) >= args.reference_budget:
                    refusal = "reference_budget_exhausted"
                elif kind != "logout" and shanghai_date() != day:
                    refusal = "date_changed"
                elif kind != "logout" and time.monotonic() >= deadline:
                    refusal = "time_budget"
                if refusal is None:
                    call = directory / (
                        "call-" + str(time.time_ns()) + "-" + uuid.uuid4().hex + ".json"
                    )
                    daily.atomic_json(call, dict(attempt=token, kind=kind, status="unknown"))
                parent.send(refusal is None)
            process.join(max(0, limit - time.monotonic()))
            parent.close()
            status = "process_failed"
            if process.is_alive():
                process.terminate()
                process.join()
                status = "timeout"
            elif process.exitcode == 0 and output.exists():
                try:
                    evidence = campaign.load_json(output)
                    rows = _rows(evidence, operation, params, fields)
                    if name == "calendar" and (
                        len(rows) != 1 or rows[0] not in ([day, "0"], [day, "1"])
                    ):
                        raise ValueError("unknown_trading_day")
                    digest = campaign.digest(evidence)
                    evidence_path = directory / ("evidence-" + digest + ".json")
                    if evidence_path.exists():
                        if campaign.digest(_durable_read(evidence_path)) != digest:
                            raise ValueError("reference_evidence_conflict")
                    else:
                        daily.atomic_json(evidence_path, evidence)
                    daily.atomic_json(pinned, {"sha256": digest})
                    retained[name] = evidence_path
                    status = "retained"
                except (ValueError, KeyError, OSError) as exc:
                    status = "invalid_evidence:" + str(exc)
            if refusal is not None:
                status = refusal
            elif shanghai_date() != day:
                status = "date_changed"
            daily.atomic_json(
                attempt,
                dict(
                    operation=operation,
                    params=params,
                    call_journal=1,
                    status=status,
                    result=str(output) if output.exists() else None,
                ),
            )
            if status != "retained":
                return dict(status="partial", date=day, reason=status, source=name)
        if shanghai_date() != day or time.monotonic() >= deadline:
            return dict(
                status="partial",
                date=day,
                reason="date_changed" if shanghai_date() != day else "time_budget",
            )
        os.environ["OPENBB_CAMPAIGN_DAILY_CALL_BUDGET"] = str(args.history_daily_budget)
        return daily.launch(
            SimpleNamespace(
                config=args.config,
                calendar=retained["calendar"],
                universe=retained["universe"],
                work=args.work,
                root=args.root,
                request_budget=args.request_budget,
                max_attempts=args.max_attempts,
                backoff_seconds=args.backoff_seconds,
                seconds=deadline - time.monotonic(),
                date=day,
                correct_closed=False,
                reason=None,
            )
        )


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("config", "work", "root"):
        parser.add_argument("--" + name, type=Path, required=True)
    for name in (
        "reference-budget",
        "request-budget",
        "history-daily-budget",
        "seconds",
        "timeout",
    ):
        parser.add_argument("--" + name, type=int, required=True)
    parser.add_argument("--max-attempts", type=int, default=3)
    parser.add_argument("--backoff-seconds", type=int, default=60)
    parser.add_argument("--date", help="Expected current Shanghai trading date")
    args = parser.parse_args(argv)
    try:
        if (
            min(
                args.reference_budget,
                args.request_budget,
                args.history_daily_budget,
                args.seconds,
                args.timeout,
                args.max_attempts,
            )
            <= 0
            or args.backoff_seconds < 0
        ):
            raise ValueError("invalid_budget_or_policy")
        result = run(args)
        print(json.dumps(result, sort_keys=True))
        return 0 if result["status"] in ("complete", "closed") else 1
    except (ValueError, OSError, KeyError) as exc:
        print(json.dumps(dict(status="refused", error=str(exc))))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

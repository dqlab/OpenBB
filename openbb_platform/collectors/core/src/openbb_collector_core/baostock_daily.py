"""Run a single Shanghai-day BaoStock campaign from retained source evidence."""

import argparse
import fcntl
import json
import os
import shutil
import tempfile
import time
from datetime import date, timedelta
from pathlib import Path

from . import baostock_campaign as campaign
from .baostock_daily_plan import daily_plan, shanghai_date, verify_campaign


def atomic_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(dir=path.parent, prefix=".daily-")
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(campaign.encode(value))
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(name, path)
        campaign.raw._sync(path.parent)
    finally:
        if os.path.exists(name):
            os.unlink(name)


def durable_json(path):
    with path.open("rb") as stream:
        os.fsync(stream.fileno())
    campaign.raw._sync(path.parent)
    return campaign.load_json(path)


def launch(args):
    execution_day = shanghai_date()
    day = args.date or execution_day
    if date.fromisoformat(day).isoformat() != day:
        raise ValueError("invalid_date")
    if day != execution_day and not args.correct_closed:
        raise ValueError("historical_correction_requires_authorization")
    config = campaign.load_json(args.config)
    start = date.fromisoformat(config["start_date"])
    end = date.fromisoformat(config["end_date"])
    if (
        start.isoformat() != config["start_date"]
        or end.isoformat() != config["end_date"]
        or start > end
    ):
        raise ValueError("invalid_date_range")
    if not (start <= date.fromisoformat(day) <= end):
        raise ValueError("outside_configured_date_range")
    calendar = campaign.load_json(args.calendar, 32 * 1024 * 1024)
    stock = campaign.load_json(args.universe, 32 * 1024 * 1024) if args.universe.exists() else None
    result = daily_plan(config, day, calendar, stock)
    args.work.mkdir(parents=True, exist_ok=True)
    with (args.work / ".daily.lock").open("a+b") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        days = args.work / "days"
        days.mkdir(exist_ok=True)
        for parent in reversed((days.resolve(), *days.resolve().parents)):
            campaign.raw._sync(parent)
        previous = []
        for item in days.iterdir():
            if not item.is_dir():
                continue
            try:
                parsed = date.fromisoformat(item.name)
            except ValueError:
                continue
            if parsed.isoformat() == item.name and item.name < day:
                previous.append(item.name)
        previous.sort()
        gaps = []
        if previous:
            cursor = date.fromisoformat(previous[-1]) + timedelta(days=1)
            while cursor < date.fromisoformat(day):
                gaps.append(cursor.isoformat())
                cursor += timedelta(days=1)
        directory = days / day
        identity = dict(
            date=day,
            config=config,
            calendar_sha256=campaign.digest(calendar),
            universe_sha256=campaign.digest(stock) if stock is not None else None,
            policy=dict(
                max_attempts=args.max_attempts,
                request_budget=args.request_budget,
                backoff_seconds=args.backoff_seconds,
            ),
            status=result["status"],
        )
        correction = directory / "closed-correction"
        was_existing = directory.exists()
        root_identity = str(args.root.resolve())
        if directory.exists():
            campaign.raw._sync(days)
            original = durable_json(directory / "identity.json")
            old_calendar = directory / "calendar.json"
            if (
                not old_calendar.is_file()
                or campaign.digest(durable_json(old_calendar)) != original["calendar_sha256"]
            ):
                raise ValueError("original_calendar_evidence_conflict")
            if correction.exists():
                campaign.raw._sync(directory)
                audit = durable_json(correction / "audit.json")
                if (
                    audit["original_identity"] != original
                    or audit["corrected_identity"] != identity
                    or audit["root"] != root_identity
                ):
                    raise ValueError("daily_identity_conflict")
                old_calendar = directory / "calendar.json"
                if (
                    not old_calendar.is_file()
                    or campaign.digest(campaign.load_json(old_calendar))
                    != original["calendar_sha256"]
                ):
                    raise ValueError("original_calendar_evidence_conflict")
                if (
                    campaign.digest(durable_json(correction / "calendar.json"))
                    != identity["calendar_sha256"]
                    or campaign.digest(durable_json(correction / "universe.json"))
                    != identity["universe_sha256"]
                ):
                    raise ValueError("daily_correction_evidence_conflict")
            elif original != identity:
                if not args.correct_closed or not args.reason:
                    raise ValueError("daily_identity_conflict")
                if original.get("status") != "closed" or identity["status"] != "planned":
                    raise ValueError("only_closed_to_open_correction_supported")
                if any(original.get(k) != identity.get(k) for k in ("date", "config", "policy")):
                    raise ValueError("daily_correction_scope_conflict")
                run_dir = args.root / campaign.VERSION / campaign.digest("daily:" + day)
                if run_dir.exists() or (directory / "manifest").exists():
                    raise ValueError("daily_correction_existing_campaign")
                old_calendar = directory / "calendar.json"
                if (
                    not old_calendar.is_file()
                    or campaign.digest(campaign.load_json(old_calendar))
                    != original["calendar_sha256"]
                ):
                    raise ValueError("original_calendar_evidence_conflict")
                stage = Path(tempfile.mkdtemp(prefix=".correction-", dir=directory))
                try:
                    audit = dict(
                        version="baostock-closed-correction-v1",
                        reason=args.reason,
                        original_identity=original,
                        corrected_identity=identity,
                        recorded_ns=time.time_ns(),
                        root=root_identity,
                        original_calendar_retained=(directory / "calendar.json").is_file(),
                    )
                    atomic_json(stage / "audit.json", audit)
                    atomic_json(stage / "calendar.json", calendar)
                    atomic_json(stage / "universe.json", stock)
                    campaign.compile_manifest(
                        result["plan"], stage / "manifest", identity["policy"]
                    )
                    os.rename(stage, correction)
                    campaign.raw._sync(directory)
                finally:
                    if stage.exists():
                        shutil.rmtree(stage)
            elif args.correct_closed:
                raise ValueError("daily_correction_not_needed")
        else:
            if day != execution_day or args.correct_closed:
                raise ValueError("correction_requires_existing_closed_day")
            stage = Path(tempfile.mkdtemp(prefix=".stage-", dir=days))
            try:
                atomic_json(stage / "identity.json", identity)
                atomic_json(stage / "calendar.json", calendar)
                if result["status"] == "planned":
                    atomic_json(
                        stage / "capture-root.json",
                        {"version": "baostock-daily-root-v1", "root": root_identity},
                    )
                    campaign.compile_manifest(
                        result["plan"], stage / "manifest", identity["policy"]
                    )
                os.rename(stage, directory)
                campaign.raw._sync(days)
            finally:
                if stage.exists():
                    shutil.rmtree(stage)
        if result["status"] == "closed":
            return dict(status="closed", date=day, gaps=gaps, gap_history_known=bool(previous))
        manifest = (correction if correction.exists() else directory) / "manifest"
        run_key, slot = "daily:" + day, day
        binding_file = directory / "capture-root.json"
        binding = {"version": "baostock-daily-root-v1", "root": root_identity}
        if binding_file.exists():
            if durable_json(binding_file) != binding:
                raise ValueError("daily_capture_root_conflict")
        else:
            if was_existing and not correction.exists():
                authority = (
                    args.root / campaign.VERSION / campaign.digest(run_key) / "authority.json"
                )
                if not authority.exists():
                    raise ValueError("legacy_daily_root_unproven")
                existing = campaign.load_json(authority)
                if (
                    existing.get("run") != run_key
                    or existing.get("slot") != slot
                    or existing.get("manifest") != campaign.load_json(manifest / "manifest.json")
                ):
                    raise ValueError("legacy_daily_root_unproven")
            campaign.raw._install(binding_file, campaign.encode(binding))
        deadline = time.monotonic() + args.seconds
        last = None
        while time.monotonic() < deadline and shanghai_date() == execution_day:
            last = campaign.run(
                manifest,
                args.root,
                run_key,
                slot,
                source_allowed=lambda: (
                    time.monotonic() < deadline and shanghai_date() == execution_day
                ),
            )
            if last["status"] == "complete":
                verified = verify_campaign(manifest, args.root, run_key, slot)
                return dict(
                    date=day, gaps=gaps, gap_history_known=bool(previous), campaign=last, **verified
                )
            if last["status"] != "partial" or last["daily_budget_exhausted"]:
                break
            # Avoid a hot loop during source backoff or a stalled retry.
            if last["source_calls"] == 0:
                break
        return dict(
            status="partial",
            date=day,
            gaps=gaps,
            gap_history_known=bool(previous),
            campaign=last,
            reason=(
                "date_changed"
                if shanghai_date() != execution_day
                else "time_budget"
                if time.monotonic() >= deadline
                else "campaign_stalled"
            ),
        )


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("config", "calendar", "universe", "work", "root"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--request-budget", type=int, required=True)
    parser.add_argument("--seconds", type=int, required=True)
    parser.add_argument("--max-attempts", type=int, default=3)
    parser.add_argument("--backoff-seconds", type=int, default=60)
    parser.add_argument("--correct-closed", action="store_true")
    parser.add_argument("--reason")
    parser.add_argument("--date", help="Explicit date for an authorized closed-day correction")
    args = parser.parse_args(argv)
    try:
        if args.correct_closed != bool(args.reason and args.reason.strip()):
            raise ValueError("correction_requires_flag_and_reason")
        if args.seconds <= 0:
            raise ValueError("invalid_time_budget")
        result = launch(args)
        print(json.dumps(result, sort_keys=True))
        return 0 if result["status"] in ("complete", "closed") else 1
    except (ValueError, OSError, KeyError) as exc:
        print(json.dumps(dict(status="refused", error=str(exc))))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

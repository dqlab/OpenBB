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


def launch(args):
    day = shanghai_date()
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
        if directory.exists():
            if campaign.load_json(directory / "identity.json") != identity:
                raise ValueError("daily_identity_conflict")
        else:
            stage = Path(tempfile.mkdtemp(prefix=".stage-", dir=days))
            try:
                atomic_json(stage / "identity.json", identity)
                if result["status"] == "planned":
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
        manifest = directory / "manifest"
        run_key, slot = "daily:" + day, day
        deadline = time.monotonic() + args.seconds
        last = None
        while time.monotonic() < deadline and shanghai_date() == day:
            last = campaign.run(manifest, args.root, run_key, slot)
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
                if shanghai_date() != day
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
    args = parser.parse_args(argv)
    try:
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

"""Configurable single-date raw acquisition and externally invoked daily scheduler."""
from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import sys
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, time
from pathlib import Path
from zoneinfo import ZoneInfo

from .raw_acquisition import capture_locator, capture_process, read_capture, validate_task


@dataclass(frozen=True)
class Source:
    provider: str
    model: str
    timeout_seconds: float = 180
    min_interval_seconds: float = 0.1
    max_response_bytes: int = 50_000_000
    max_rows: int = 1_000_000
    batch_size: int = 1
    credentials_env: dict[str, str] = field(default_factory=dict)

    def model_dump(self, *, mode="json"):
        return asdict(self)


def make_task(config, day, attempt=1):
    """Bind a literal request and date to a stable identity; attempts are revisions."""
    day = date.fromisoformat(day).isoformat()
    source = config["source"]
    if set(source) != {"provider", "model"}:
        raise ValueError("source requires provider and model")
    params = json.loads(json.dumps(config.get("params", {})))
    date_path = config.get("date_path", ["date"])
    if not isinstance(date_path, list) or not date_path or not all(
        isinstance(x, str) and x for x in date_path
    ):
        raise ValueError("invalid date_path")
    target = params
    for part in date_path[:-1]:
        target = target.setdefault(part, {})
        if not isinstance(target, dict):
            raise ValueError("invalid date_path")
    if date_path[-1] in target:
        raise ValueError("date parameter already supplied")
    target[date_path[-1]] = day
    identity = {"source": source, "params": params}
    digest = hashlib.sha256(json.dumps(identity, sort_keys=True, separators=(",", ":"),
                                       allow_nan=False).encode()).hexdigest()
    if type(attempt) is not int or attempt < 1:
        raise ValueError("invalid attempt")
    return validate_task({"version": "raw-acquisition-v1", "task_id": "daily:" + digest,
                          "generation": f"attempt-{attempt}", "source": source,
                          "requests": [{"request_id": "daily", "params": params}]})


def _is_etf(task):
    return task["source"] == {"provider": "baostock", "model": "BaostockDailyHistoryKETFSource"}


def _classify(state, task):
    """Keep raw transport status separate from the source verdict."""
    if not _is_etf(task):
        return state
    state = dict(state)
    state["transport_status"] = state["status"]
    verdict = None
    if state.get("complete") and state["status"] == "captured":
        try:
            rows = json.loads(state["responses"][0]["body"])["rows"]
            if len(rows) == 1 and isinstance(rows[0].get("evidence"), dict):
                evidence = rows[0]["evidence"]
                verdict = evidence.get("status")
                pages = evidence.get("pages")
                if verdict == "complete" and (type(pages) is not list or len(pages) != 1
                        or type(pages[0].get("rows")) is not list
                        or len(pages[0]["rows"]) >= 20000):
                    verdict = "partial"
        except (KeyError, TypeError, ValueError, IndexError, AttributeError):
            pass
    state["source_status"] = verdict if verdict in ("complete", "partial", "failed") else "unknown"
    if state["status"] == "captured" and verdict != "complete":
        state["status"] = "partial" if verdict == "partial" else "failed"
        state["reason"] = "source_" + state["source_status"]
    return state


def _page_state(state):
    terminal = state.get("terminal") or {}
    summary = terminal.get("summary") or {}
    return {**state, "transport_status": "captured" if summary else ("failed" if terminal else "interrupted"),
            "source_status": summary.get("status", "unknown"),
            "status": "captured" if state["complete"] else ("partial" if state["pages"] else "failed"),
            "reason": terminal.get("reason", "interrupted")}


def acquire(config, day, attempt=1, *, client=None):
    task = make_task(config, day, attempt)
    limits = config.get("limits", {})
    source = Source(**task["source"], **limits, credentials_env=config.get("credentials_env", {}))
    if (not 0 < source.timeout_seconds <= 3600 or not 0 <= source.min_interval_seconds <= 3600
            or type(source.max_rows) is not int or source.max_rows < 1
            or type(source.max_response_bytes) is not int
            or not 1 <= source.max_response_bytes <= 50_000_000 or source.batch_size != 1
            or type(source.credentials_env) is not dict
            or any(type(k) is not str or not k or type(v) is not str or not v
                   for k, v in source.credentials_env.items())):
        raise ValueError("invalid source limits")
    if _is_etf(task) and client is None:
        if limits or config.get("credentials_env"):
            raise ValueError("ETF page capture does not support generic limits or credentials_env")
        from . import baostock_page_capture as pages
        root = Path(config["root"])
        root.mkdir(parents=True, exist_ok=True, mode=0o700)
        with (root / ".daily-pages.lock").open("a+") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            path = root / pages.capture_locator(task)
            legacy = root / capture_locator(task["task_id"], task["generation"])
            if legacy.exists() and not path.exists():
                return _classify(read_capture(root, task["task_id"], task["generation"]), task)
            state = pages.recover_capture(root, task) if path.exists() else pages.capture_process(root, task)
            return _page_state(state)
    return _classify(capture_process(config["root"], task, source, client=client), task)


def schedule(config, now=None, *, client=None):
    """Execute at most one due date per invocation; evidence is the retry ledger."""
    schedule_config = config["schedule"]
    zone = ZoneInfo(schedule_config["timezone"])
    now = (now or datetime.now(zone)).astimezone(zone)
    calendar = json.loads(Path(schedule_config["calendar"]).read_text())
    start = date.fromisoformat(calendar["coverage_start"])
    end = date.fromisoformat(calendar["coverage_end"])
    day = now.date()
    if not start <= day <= end:
        raise ValueError("calendar coverage does not include today")
    days = [date.fromisoformat(x) for x in calendar["trading_dates"]]
    if any(not start <= d <= end for d in days) or len(days) != len(set(days)):
        raise ValueError("invalid trading dates")
    due = time.fromisoformat(schedule_config["time"])
    if due.tzinfo is not None:
        raise ValueError("schedule time must be local")
    attempts = schedule_config["max_attempts"]
    if type(attempts) is not int or not 1 <= attempts <= 10:
        raise ValueError("max_attempts must be 1..10")
    if day not in days or now.time().replace(tzinfo=None) < due:
        return {"status": "not_due"}
    root = Path(config["root"])
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    with (root / ".daily-schedule.lock").open("a+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        for attempt in range(1, attempts + 1):
            task = make_task(config, day.isoformat(), attempt)
            if _is_etf(task) and client is None:
                from . import baostock_page_capture as pages
                path = root / pages.capture_locator(task)
                if path.exists():
                    if (path / "authority.json").exists():
                        with (root / ".daily-pages.lock").open("a+") as page_lock:
                            fcntl.flock(page_lock, fcntl.LOCK_EX)
                            state = _page_state(pages.recover_capture(root, task))
                        if state["complete"] and state["status"] == "captured":
                            return state
                    continue
                legacy = root / capture_locator(task["task_id"], task["generation"])
                if legacy.exists():
                    if (legacy / "task.json").exists():
                        state = _classify(read_capture(root, task["task_id"], task["generation"]), task)
                        if state["complete"] and state["status"] == "captured":
                            return state
                    continue
            else:
                path = root / capture_locator(task["task_id"], task["generation"])
                if path.exists():
                    if (path / "task.json").exists():
                        state = _classify(read_capture(root, task["task_id"], task["generation"]), task)
                        if state["complete"] and state["status"] == "captured":
                            return state
                    continue
            return acquire(config, day.isoformat(), attempt, client=client)
        return {"status": "exhausted"}


def report(config, day, attempt):
    """Read-only structural report; never gates capture or asserts market completeness."""
    task = make_task(config, day, attempt)
    if _is_etf(task):
        from . import baostock_page_capture as pages
        path = Path(config["root"]) / pages.capture_locator(task)
        if path.exists():
            state = _page_state(pages.read_capture(config["root"], task))
            evidence = [pages.read_page(path, item["page"], decode=False) for item in state["pages"]]
            return {"task_id": task["task_id"], "generation": task["generation"],
                    "status": state["status"], "complete": state["complete"],
                    "transport_status": state["transport_status"], "source_status": state["source_status"],
                    "source_evidence": [{"status": item.get("status"), "reason": item.get("reason"),
                                         "page_count": len(item.get("pages", [])),
                                         "source_row_count": sum(len(p.get("rows") or []) for p in item.get("pages", []))}
                                        for item in evidence], "market_completeness": "not assessed"}
    state = _classify(read_capture(config["root"], task["task_id"], task["generation"]), task)
    rows = [json.loads(item["body"])["rows"] for item in state["responses"]
            if item["body"] is not None]
    evidence = [row["evidence"] for response in rows for row in response
                if isinstance(row, dict) and isinstance(row.get("evidence"), dict)]
    source_reports = [{"status": item.get("status"), "reason": item.get("reason"),
                       "page_count": len(item.get("pages", [])),
                       "source_row_count": sum(len(page.get("rows") or [])
                                               for page in item.get("pages", []))}
                      for item in evidence]
    return {"task_id": task["task_id"], "generation": task["generation"],
            "status": state["status"], "complete": state["complete"],
            **({"transport_status": state["transport_status"], "source_status": state["source_status"]}
               if _is_etf(task) else {}),
            "response_count": len(rows), "row_counts": [len(r) for r in rows],
            "source_evidence": source_reports, "market_completeness": "not assessed"}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("acquire", "schedule", "report"))
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--date", help="ISO date (required for acquire/report)")
    parser.add_argument("--attempt", type=int, default=1)
    args = parser.parse_args(argv)
    if args.command != "schedule" and not args.date:
        parser.error("--date is required")
    try:
        config = json.loads(args.config.read_text())
        if args.command == "schedule":
            result = schedule(config)
        elif args.command == "report":
            result = report(config, args.date, args.attempt)
        else:
            result = acquire(config, args.date, args.attempt)
        output = {k: result[k] for k in ("status", "complete", "reason", "locator", "transport_status", "source_status") if k in result}
        if args.command == "report":
            output = result
        print(json.dumps(output))
        return 0 if result["status"] in ("captured", "not_due") else 1
    except Exception as exc:
        print(f"daily raw: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

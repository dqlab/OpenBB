"""Generate explicit campaign tasks from a retained stock-basic SDK snapshot."""

from __future__ import annotations

import argparse
import json
import re
from datetime import date, timedelta
from pathlib import Path

from . import baostock_campaign as campaign


def make_plan(config, snapshot, *, daily_date=None):
    """Select current provider classifications, preserving literal source identity."""
    expected = {
        "security_type",
        "status",
        "start_date",
        "end_date",
        "window_days",
        "fields",
        "adjustflags",
        "generation",
    }
    if type(config) is not dict or set(config) != expected:
        raise ValueError("invalid_config")
    if any(
        type(config[k]) is not str or not config[k]
        for k in ("security_type", "status", "generation", "fields")
    ):
        raise ValueError("invalid_config_string")
    if daily_date is not None:
        config = {**config, "start_date": daily_date, "end_date": daily_date}
    start, end = date.fromisoformat(config["start_date"]), date.fromisoformat(config["end_date"])
    if (
        start.isoformat() != config["start_date"]
        or end.isoformat() != config["end_date"]
        or start > end
    ):
        raise ValueError("invalid_date_range")
    days = config["window_days"]
    flags = config["adjustflags"]
    if (
        type(days) is not int
        or not 1 <= days <= 36600
        or type(flags) is not list
        or not flags
        or len(set(flags)) != len(flags)
        or any(x not in ("1", "2", "3") for x in flags)
    ):
        raise ValueError("invalid_window_or_flags")
    if (
        type(snapshot) is not dict
        or snapshot.get("operation") != "query_stock_basic"
        or snapshot.get("params") != {}
        or snapshot.get("error_code") != "0"
    ):
        raise ValueError("invalid_stock_basic_snapshot")
    fields, rows = snapshot.get("fields"), snapshot.get("rows")
    if (
        fields != ["code", "code_name", "ipoDate", "outDate", "type", "status"]
        or type(rows) is not list
    ):
        raise ValueError("invalid_stock_basic_shape")
    codes = []
    seen = set()
    for row in rows:
        if type(row) is not list or len(row) != len(fields) or any(type(x) is not str for x in row):
            raise ValueError("invalid_stock_basic_row")
        code, _, _, _, security_type, status = row
        if (
            not re.fullmatch(r"(?:sh|sz)\.[0-9]{6}", code)
            or code in seen
            or security_type not in ("1", "2", "3", "4", "5")
            or status not in ("0", "1")
        ):
            raise ValueError("invalid_or_duplicate_stock_basic_identity")
        seen.add(code)
        if security_type == config["security_type"] and status == config["status"]:
            codes.append(code)
    if not codes:
        raise ValueError("empty_selection")
    tasks = []
    for code in sorted(codes):
        cursor = start
        while cursor <= end:
            last = min(end, cursor + timedelta(days=days - 1))
            for flag in flags:
                params = {
                    "code": code,
                    "start_date": cursor.isoformat(),
                    "end_date": last.isoformat(),
                    "frequency": "d",
                    "fields": config["fields"],
                    "adjustflag": flag,
                }
                task = {
                    "version": "raw-acquisition-v1",
                    "task_id": f"literal:{code}:{cursor}:{last}:{flag}",
                    "generation": config["generation"],
                    "source": {"provider": "baostock", "model": "BaostockHistoryKDataPlus"},
                    "requests": [{"request_id": flag, "params": params}],
                }
                campaign.tasks_check([task])
                tasks.append(task)
                if len(tasks) > campaign.MAX_PAGES * 8:
                    raise ValueError("plan_too_large")
            if last == end:
                break
            cursor = last + timedelta(days=1)
    return {"tasks": tasks}


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate_json_key: {key}")
        result[key] = value
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument(
        "--snapshot",
        type=Path,
        required=True,
        help="Retained single query_stock_basic SDK response",
    )
    parser.add_argument(
        "--plan", type=Path, required=True, help="New file; never overwrites an existing plan"
    )
    parser.add_argument(
        "--date", help="Explicit ISO date for a single-day plan; overrides both config dates"
    )
    args = parser.parse_args()
    config = json.loads(args.config.read_text(), object_pairs_hook=_unique_object)
    snapshot = json.loads(args.snapshot.read_text(), object_pairs_hook=_unique_object)
    plan = make_plan(config, snapshot, daily_date=args.date)
    with args.plan.open("x", encoding="utf-8") as out:
        json.dump(plan, out, ensure_ascii=False, indent=2)
        out.write("\n")


if __name__ == "__main__":
    main()

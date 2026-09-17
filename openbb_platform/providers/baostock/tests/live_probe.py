"""Opt-in bounded live checks; save source samples and a per-command outcome ledger.

Run only after installing the provider and rebuilding OpenBB:
python tests/live_probe.py --output /tmp/baostock-live
Each request runs in a separate process with a hard timeout. No retries occur.
"""

import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from openbb_baostock.utils.catalog import DATASETS


def parameters(kind):
    """Choose explicit, small windows or a single source snapshot per dataset."""
    window = {"start_date": "2024-01-01", "end_date": "2024-03-31"}
    code = {"code": "sz.000651"}
    return {
        "history": {
            **code,
            "start_date": "2024-01-02",
            "end_date": "2024-01-05",
            "fields": "date,code,open,high,low,close,volume,amount,adjustflag",
        },
        "date": {"date": "2024-01-02"},
        "basic": code,
        "range": window,
        "day": {"day": "2024-01-02"},
        "classification": {**code, "date": "2024-01-02"},
        "dividend": {**code, "year": 2023, "year_type": "report"},
        "code_range": {**code, **window},
        "quarter": {**code, "year": 2024, "quarter": 1},
        "reserve": {**window, "year_type": "0"},
        "month_range": {"start_date": "2024-01", "end_date": "2024-03"},
        "year_range": {"start_date": "2023", "end_date": "2023"},
    }[kind]


def probe_one(command, params, output):
    """Call the installed public interface and retain an unmodified sample."""
    from openbb import obb

    record = {
        "command": command,
        "params": params,
        "retrieved_at": datetime.now(timezone.utc).isoformat(),
    }
    try:
        result = getattr(obb.baostock, command)(provider="baostock", **params)
        rows = [row.model_dump() for row in result.results]
        record.update(status="ok" if rows else "empty", row_count=len(rows), sample=rows[:2])
    except Exception as exc:  # Report source failures without losing the rest of the inventory.
        record.update(status="error", error=str(exc))
    output.write_text(json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8")


def main():
    """Probe every catalogued query once, with a bounded process lifetime."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--timeout", type=float, default=15)
    parser.add_argument("--command")
    parser.add_argument("--params")
    args = parser.parse_args()
    if args.command:
        probe_one(args.command, json.loads(args.params), args.output)
        return
    args.output.mkdir(parents=True, exist_ok=True)
    results = []
    for dataset in DATASETS:
        params = parameters(dataset.parameters)
        if dataset.command in {"deposit_rate_data", "loan_rate_data"}:
            params = {"start_date": "2015-01-01", "end_date": "2015-12-31"}
        output = args.output / f"{dataset.command}.json"
        if output.exists():
            raise FileExistsError(f"Refusing to overwrite an existing source sample: {output}")
        command = [
            sys.executable,
            str(Path(__file__).resolve()),
            "--command",
            dataset.command,
            "--params",
            json.dumps(params),
            "--output",
            str(output),
        ]
        try:
            completed = subprocess.run(command, capture_output=True, text=True, timeout=args.timeout, check=False)  # noqa: S603
            if completed.returncode != 0:
                raise RuntimeError(completed.stderr[-2000:])
            record = json.loads(output.read_text(encoding="utf-8"))
        except (subprocess.TimeoutExpired, RuntimeError) as exc:
            record = {
                "command": dataset.command,
                "params": params,
                "status": "timeout" if isinstance(exc, subprocess.TimeoutExpired) else "error",
                "error": str(exc),
            }
            output.write_text(json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8")
        record["sdk_public"] = dataset.public
        results.append(record)
        print(dataset.command, record["status"], record.get("row_count", record.get("error", "")), flush=True)  # noqa: T201
    (args.output / "results.json").write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()

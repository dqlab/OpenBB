"""Retain independent BaoStock calendar and universe source attempts."""

import argparse
import json
import multiprocessing
import time
import uuid
from pathlib import Path

from .baostock_daily import atomic_json
from .baostock_daily_plan import shanghai_date


def _fetch(operation, params, path):
    from openbb_baostock.utils.source_evidence import _query

    atomic_json(Path(path), _query(operation, params))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--timeout", type=int, required=True)
    args = parser.parse_args(argv)
    if args.timeout <= 0:
        parser.error("timeout must be positive")
    day = shanghai_date()
    directory = args.output / day
    directory.mkdir(parents=True, exist_ok=True)
    results = {}
    for name, operation, params in (
        ("calendar", "BaostockTradeDates", {"start_date": day, "end_date": day}),
        ("universe", "BaostockStockBasic", {}),
    ):
        token = str(time.time_ns()) + "-" + uuid.uuid4().hex
        path = directory / (name + "-" + token + ".json")
        process = multiprocessing.get_context("spawn").Process(
            target=_fetch, args=(operation, params, str(path))
        )
        process.start()
        process.join(args.timeout)
        if process.is_alive():
            process.terminate()
            process.join()
            status = "timeout"
        elif process.exitcode != 0 or not path.exists():
            status = "process_failed"
        else:
            status = "retained"
        if status != "retained":
            path = directory / (name + "-" + token + "-failure.json")
            atomic_json(
                path,
                dict(
                    version="baostock-daily-source-attempt-v1",
                    date=day,
                    operation=operation,
                    params=params,
                    status=status,
                ),
            )
        results[name] = dict(status=status, path=str(path))
    print(json.dumps(dict(date=day, sources=results), sort_keys=True))
    return 0 if all(r["status"] == "retained" for r in results.values()) else 1


if __name__ == "__main__":
    raise SystemExit(main())

"""Opt-in live checks; save source samples and a per-command outcome ledger.

Run only after installing the provider:

    python tests/live_probe.py --output /tmp/akshare-live

Every case runs against the live AKShare source, so this is excluded from the
offline suite. Failures are recorded rather than raised, so one broken upstream
interface does not hide the others.
"""

import argparse
import asyncio
import json
import traceback
from datetime import datetime, timezone
from pathlib import Path

from openbb_akshare.models.futures_historical import AkshareFuturesHistoricalFetcher
from openbb_akshare.models.native import NATIVE_FETCHERS

CASES = (
    (
        "FuturesHistorical",
        AkshareFuturesHistoricalFetcher,
        {"symbol": "IF0", "start_date": "2026-10-01", "end_date": "2026-10-13"},
    ),
    ("AkshareCffexDaily", NATIVE_FETCHERS["AkshareCffexDaily"], {"date": "2026-09-18"}),
    ("AkshareFuturesHistDailyCffex", NATIVE_FETCHERS["AkshareFuturesHistDailyCffex"], {"date": "2026-09-18"}),
    ("AkshareCffexRankTable", NATIVE_FETCHERS["AkshareCffexRankTable"], {"date": "2026-09-18", "vars_list": "IF"}),
    ("AkshareFuturesZhDailySina", NATIVE_FETCHERS["AkshareFuturesZhDailySina"], {"symbol": "IF0"}),
    ("AkshareFuturesZhMinuteSina", NATIVE_FETCHERS["AkshareFuturesZhMinuteSina"], {"symbol": "IF0", "period": "5"}),
    ("AkshareStockZhIndexDaily", NATIVE_FETCHERS["AkshareStockZhIndexDaily"], {"symbol": "sh000300"}),
    ("AkshareFuturesMainSina", NATIVE_FETCHERS["AkshareFuturesMainSina"], {
        "symbol": "IF0", "start_date": "2026-09-01", "end_date": "2026-10-13",
    }),
    ("AkshareBondZhUsRate", NATIVE_FETCHERS["AkshareBondZhUsRate"], {"start_date": "2026-10-01"}),
)


def run_case(name: str, fetcher: type, params: dict) -> dict:
    """Run one fetcher and record its outcome without raising."""
    record: dict = {"command": name, "status": "unknown", "row_count": None, "error": None}
    try:
        query = fetcher.transform_query(params)
        data = asyncio.run(fetcher.aextract_data(query, None))
        rows = fetcher.transform_data(query, data)
        record["status"] = "ok"
        record["row_count"] = len(rows)
        record["sample"] = [row.model_dump() for row in rows[:2]]
    except Exception as exc:  # A live failure is data for the ledger, not a crash.
        record["status"] = "error"
        record["error"] = f"{type(exc).__name__}: {exc}"
        record["traceback"] = traceback.format_exc()
    return record


def main() -> int:
    """Run every live case and write the ledger and samples."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, help="Directory for the ledger and samples.")
    args = parser.parse_args()
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)

    records = [run_case(name, fetcher, params) for name, fetcher, params in CASES]
    ledger = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "record_count": len(records),
        "records": records,
    }
    (output / "ledger.json").write_text(json.dumps(ledger, indent=2, default=str), encoding="utf-8")
    for record in records:
        if record.get("sample"):
            (output / f"{record['command']}.json").write_text(
                json.dumps(record["sample"], indent=2, default=str), encoding="utf-8"
            )
    for record in records:
        print(record["command"], record["status"], record.get("row_count") or record.get("error"), flush=True)  # noqa: T201
    return 0 if all(record["status"] == "ok" for record in records) else 1


if __name__ == "__main__":
    raise SystemExit(main())

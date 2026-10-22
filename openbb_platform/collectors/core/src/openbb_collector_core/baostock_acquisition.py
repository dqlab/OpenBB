"""Raw-only BaoStock entry point. No DQ, catalog, calendar or job dependencies.

Each literal task uses an existing native OpenBB model. History is partitioned
by downstream into at most two responses/task before source side effects; the
50MB ProcessClient response budget is unchanged. Successful individual responses
are durable before the next request, including adjustment/factor failures.
"""

from __future__ import annotations

import argparse
import json
import signal
from dataclasses import asdict, dataclass
from pathlib import Path
from threading import Event

from .raw_acquisition import capture_process, plan_process_capture, validate_task

MODELS = frozenset(
    {"BaostockStockBasic", "BaostockTradeDates", "BaostockHistoryKDataPlus", "BaostockAdjustFactor"}
)


@dataclass(frozen=True)
class Source:
    model: str
    provider: str = "baostock"
    timeout_seconds: float = 180
    min_interval_seconds: float = 0.1
    max_response_bytes: int = 50_000_000
    max_rows: int = 1_000_000
    batch_size: int = 1

    def model_dump(self, *, mode="json"):
        return dict(asdict(self), credentials_env={})


def acquire(root: Path | str, tasks: list[dict], *, client=None, stop: Event | None = None):
    """Capture literal tasks only. No discovery, canonical status or ACK here.

    A caller-supplied client is caller-owned and exists for offline testing or
    process-client reuse. Production uses the existing killable ProcessClient
    and selected-provider QueryExecutor; no vendor SDK is imported here.
    """
    if type(tasks) is not list or not 1 <= len(tasks) <= 8:
        raise ValueError("invalid_baostock_task_bundle")
    prepared, seen = [], set()
    for supplied in tasks:
        task = validate_task(supplied)
        if task["source"]["provider"] != "baostock" or task["source"]["model"] not in MODELS:
            raise ValueError("unsupported_baostock_native_model")
        identity = (task["task_id"], task["generation"])
        if identity in seen:
            raise ValueError("duplicate_baostock_task")
        seen.add(identity)
        source = Source(task["source"]["model"])
        if not plan_process_capture(task, source)["fits"]:
            raise ValueError("partition_baostock_task_before_acquisition")
        prepared.append((task, source))
    results = []
    for task, source in prepared:
        result = capture_process(root, task, source, client=client, stop=stop)
        results.append(result)
        if not result["complete"]:
            break
    return results


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", required=True, type=Path)
    parser.add_argument("--root", required=True, type=Path)
    args = parser.parse_args(argv)
    if args.plan.stat().st_size > 2_097_152:
        parser.error("plan too large")
    plan = json.loads(args.plan.read_bytes())
    stop = Event()
    previous = {}
    for name in (signal.SIGTERM, signal.SIGINT):
        previous[name] = signal.signal(name, lambda *_: stop.set())
    try:
        result = acquire(args.root, plan["tasks"], stop=stop)
        print(
            json.dumps(
                [
                    dict(task_id=r["task"]["task_id"], status=r["status"], complete=r["complete"])
                    for r in result
                ]
            )
        )
        return (
            0
            if len(result) == len(plan["tasks"])
            and all(r["complete"] and r["status"] == "captured" for r in result)
            else 1
        )
    finally:
        for name, handler in previous.items():
            signal.signal(name, handler)


if __name__ == "__main__":
    raise SystemExit(main())

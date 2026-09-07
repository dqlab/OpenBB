"""Bounded OpenBB commands over the historical collector runtime."""

import math
import threading
from datetime import UTC, date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from dq_historical_market_data_collector.config import load_config
from dq_historical_market_data_collector.planner import chunks
from dq_historical_market_data_collector.providers import validate_providers
from dq_historical_market_data_collector.runtime import Collector
from dq_historical_market_data_collector.storage import Journal, read_records, read_status
from openbb_core.app.model.obbject import OBBject
from openbb_core.app.router import Router
from pydantic import AwareDatetime

from openbb_collection._utils import _safe_errors
from openbb_collection.live_router import router as live_router

router = Router(description="Historical bars and scheduled live market-data collection.")
router.include_router(live_router)


@router.command()
def validate_config(config_path: str) -> OBBject[dict]:
    """Validate a collector YAML file and its request budgets without fetching data.

    Parameters
    ----------
    config_path : str
        Path to a collector YAML file on the host running OpenBB. Relative storage
        paths resolve against the YAML directory, as with the collector CLI.
    """
    with _safe_errors():
        config = load_config(config_path)
        validate_providers(config)
        next(chunks(config, datetime.now(UTC).date()), None)
        return OBBject(results={"status": "valid", "config_hash": config.fingerprint()})


@router.command()
def plan(config_path: str, as_of: date | None = None, limit: int = 100) -> OBBject[dict]:
    """Preview bounded historical request chunks without fetching or writing data.

    Parameters
    ----------
    config_path : str
        Path to the collector YAML file on the OpenBB host.
    as_of : date, optional
        Exclusive planning cutoff; defaults to today in the configured schedule timezone.
    limit : int
        Maximum chunks to return, between 1 and 1000.
    """
    with _safe_errors():
        if not 1 <= limit <= 1000:
            raise ValueError("Invalid limit")
        config = load_config(config_path)
        cutoff = as_of or datetime.now(ZoneInfo(config.schedule.timezone)).date()
        planned = []
        truncated = False
        for chunk in chunks(config, cutoff):
            if len(planned) == limit:
                truncated = True
                break
            planned.append(chunk.payload())
        return OBBject(results={"chunks": planned, "truncated": truncated, "end_exclusive": True})


@router.command(methods=["POST"])
def once(
    config_path: str,
    as_of: date | None = None,
    refresh: bool = False,
    max_seconds: float = 180,
) -> OBBject[dict]:
    """Run one bounded collection session and return its durable session report.

    Parameters
    ----------
    config_path : str
        Path to the collector YAML file on the OpenBB host. Storage is relative to
        this file; the command acquires the collector's exclusive writer lock.
    as_of : date, optional
        Exclusive collection cutoff; defaults to today in the schedule timezone.
    refresh : bool
        Revisit completed chunks to observe provider corrections.
    max_seconds : float
        Cancellation deadline in seconds, greater than zero and at most 600.
        The collector finishes journal/export cleanup before returning. Interrupted
        and partial reports retain checkpoints for the next invocation.
    """
    with _safe_errors():
        if not math.isfinite(max_seconds) or not 0 < max_seconds <= 600:
            raise ValueError("Invalid duration")
        config = load_config(config_path)
        validate_providers(config)
        stop = threading.Event()
        with Journal(config.storage) as journal:
            collector = Collector(config, journal, stop=stop, config_path=Path(config_path))
            timer = threading.Timer(max_seconds, stop.set)
            timer.daemon = True
            try:
                timer.start()
                return OBBject(results=collector.collect(as_of=as_of, refresh=refresh))
            finally:
                stop.set()
                timer.cancel()
                collector.close()


@router.command()
def status(config_path: str, limit: int = 20) -> OBBject[dict]:
    """Read recent collector session reports without creating storage.

    Parameters
    ----------
    config_path : str
        Path to the collector YAML file on the OpenBB host.
    limit : int
        Maximum reports to return, between 1 and 1000.
    """
    with _safe_errors():
        config = load_config(config_path)
        return OBBject(results={"sessions": read_status(config.storage.root, limit)})


@router.command()
def query(
    config_path: str,
    instrument: str,
    as_of: AwareDatetime,
    source: str | None = None,
    limit: int = 100,
) -> OBBject[dict]:
    """Read collected revisions available at a timezone-aware observation cutoff.

    Parameters
    ----------
    config_path : str
        Path to the collector YAML file on the OpenBB host.
    instrument : str
        Configured instrument ID, rather than its provider symbol.
    as_of : datetime
        Timezone-aware cutoff for collector first-observed availability. Backfilled
        records do not establish availability at the original market event time.
    source : str, optional
        Configured source ID to filter; source identities remain separate.
    limit : int
        Maximum records to return, between 1 and 1000.
    """
    with _safe_errors():
        config = load_config(config_path)
        return OBBject(
            results={
                "records": read_records(
                    config.storage.root,
                    instrument_id=instrument,
                    as_of=as_of,
                    source=source,
                    limit=limit,
                )
            }
        )

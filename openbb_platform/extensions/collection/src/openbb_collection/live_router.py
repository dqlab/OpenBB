"""OpenBB commands for the independently installed live snapshot collector."""

import math
import threading
from datetime import date
from typing import TYPE_CHECKING

from openbb_core.app.model.abstract.error import OpenBBError
from openbb_core.app.model.obbject import OBBject
from openbb_core.app.router import Router

from openbb_collection._utils import _safe_errors

if TYPE_CHECKING:
    from dq_live_market_data_collector.config import CollectorConfig

router = Router(prefix="/live", description="Scheduled market quote snapshot collection.")


def _load_config(config_path: str) -> "CollectorConfig":
    try:
        from dq_live_market_data_collector.config import load_config
    except ModuleNotFoundError as exc:
        if exc.name == "dq_live_market_data_collector":
            raise OpenBBError(
                "Install dq-live-market-data-collector from "
                "openbb_platform/collectors/live in this environment."
            ) from None
        raise
    with _safe_errors():
        return load_config(config_path)


@router.command()
def validate_config(config_path: str) -> OBBject[dict]:
    """Validate a live collector configuration without connecting to a provider.

    Parameters
    ----------
    config_path : str
        Live collector YAML file on the OpenBB host. Relative storage roots resolve
        against this file's directory.
    """
    config = _load_config(config_path)
    with _safe_errors():
        from dq_live_market_data_collector.providers import validate_providers

        validate_providers(config)
    return OBBject(
        results={
            "valid": True,
            "config_hash": config.fingerprint(),
            "instruments": len(config.instruments),
            "collections": len(config.collections),
            "format": config.storage.format,
        }
    )


@router.command()
def plan(config_path: str, session_date: date) -> OBBject[dict]:
    """Preview configured quote-polling sessions on a local calendar date.

    Parameters
    ----------
    config_path : str
        Live collector YAML file on the OpenBB host.
    session_date : date
        Session start date in each collection's configured timezone. Holidays,
        overrides, overnight sessions and DST use the existing collector calendar.
    """
    config = _load_config(config_path)
    with _safe_errors():
        from dq_live_market_data_collector.schedule import window_on

        planned = []
        for name, collection in config.collections.items():
            window = window_on(collection.schedule, session_date)
            planned.append(
                {
                    "collection": name,
                    "date": session_date.isoformat(),
                    "start_utc": window.start.isoformat() if window else None,
                    "end_utc": window.end.isoformat() if window else None,
                    "instrument_ids": collection.instrument_ids,
                    "sources": [collection.primary, *collection.secondary],
                    "frequency_seconds": collection.frequency_seconds,
                    "fields": collection.fields,
                }
            )
        return OBBject(results={"collections": planned, "calendar_basis": "operator_configuration"})


@router.command(methods=["POST"])
def once(config_path: str, max_seconds: float = 180) -> OBBject[dict]:
    """Perform one scheduled live collection pass and return bounded journal status.

    Parameters
    ----------
    config_path : str
        Live collector YAML file on the OpenBB host. Existing committed polling
        slots are resumed; outside a configured session no quotes are requested.
    max_seconds : float
        Cancellation deadline greater than zero and at most 600 seconds. Worker
        cancellation is followed by shutdown, journal/export cleanup and reports.
        Use the CLI run command for continuous scheduling.
    """
    with _safe_errors():
        if not math.isfinite(max_seconds) or not 0 < max_seconds <= 600:
            raise ValueError("Invalid duration")
    config = _load_config(config_path)
    with _safe_errors():
        from dq_live_market_data_collector.providers import validate_providers
        from dq_live_market_data_collector.service import Collector
        from dq_live_market_data_collector.storage import Journal, read_status

        validate_providers(config)
        with Journal(config.storage) as journal:
            collector = Collector(config, journal)
            timer = threading.Timer(max_seconds, collector.stop.set)
            timer.daemon = True
            try:
                timer.start()
                collector.step()
                interrupted = collector.stop.is_set()
            finally:
                collector.stop.set()
                timer.cancel()
                collector.shutdown("bounded_run")
        return OBBject(
            results={
                "event": "collection_pass_complete",
                "interrupted": interrupted,
                **read_status(config.storage.root, 20),
            }
        )


@router.command()
def status(config_path: str, limit: int = 20) -> OBBject[dict]:
    """Read recent live sessions and pending output without creating storage.

    Parameters
    ----------
    config_path : str
        Live collector YAML file on the OpenBB host.
    limit : int
        Maximum sessions to return, between 1 and 100.
    """
    config = _load_config(config_path)
    with _safe_errors():
        from dq_live_market_data_collector.storage import read_status

        return OBBject(results=read_status(config.storage.root, limit))


@router.command(methods=["POST"])
def export(config_path: str, max_batches: int = 100) -> OBBject[dict]:
    """Replay bounded pending live output batches without fetching new quotes.

    Parameters
    ----------
    config_path : str
        Live collector YAML file on the OpenBB host. The writer lock protects the
        journal and existing output batch identities.
    max_batches : int
        Maximum pending batches to replay, between 1 and 10000.
    """
    with _safe_errors():
        if not 1 <= max_batches <= 10000:
            raise ValueError("Invalid batch limit")
    config = _load_config(config_path)
    with _safe_errors():
        from dq_live_market_data_collector.storage import Journal

        with Journal(config.storage) as journal:
            return OBBject(results={"exported_records": journal.export_pending(max_batches)})

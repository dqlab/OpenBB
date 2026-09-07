"""Live collection context mapped into configuration-selected providers."""

from __future__ import annotations

from typing import Any, Protocol

from openbb_collector_core import ProviderFailure, Response
from openbb_collector_core.process import ProcessClient
from openbb_collector_core.providers import request_parameters as map_parameters

from . import __version__
from .config import CollectorConfig, Instrument, Source

__all__ = ["OpenBBClient", "ProviderClient", "ProviderFailure", "Response", "request_parameters"]


class ProviderClient(Protocol):
    def fetch(self, source_id: str, source: Source, instrument: Instrument) -> Response: ...
    def reset(self) -> None: ...
    def close(self) -> None: ...


def request_parameters(source_id: str, source: Source, instrument: Instrument) -> dict[str, Any]:
    return map_parameters(
        source,
        {
            **instrument.model_dump(mode="json"),
            "symbol": instrument.source_symbols.get(source_id, instrument.symbol),
            "snapshot_wait_seconds": source.snapshot_wait_seconds,
            "requested_feed_type": source.requested_feed_type,
        },
    )


def validate_providers(config: CollectorConfig) -> None:
    """Validate selected provider models and parameters without requesting quotes."""
    for collection in config.collections.values():
        for source_id in [collection.primary, *collection.secondary]:
            for instrument_id in collection.instrument_ids:
                request_parameters(
                    source_id, config.sources[source_id], config.instruments[instrument_id]
                )


class OpenBBClient(ProcessClient):
    """Live engine facade over the shared bounded provider process."""

    collector_metadata = {"version": __version__, "distribution": "dq-live-market-data-collector"}

    def fetch(self, source_id: str, source: Source, instrument: Instrument) -> Response:
        return super().fetch(source_id, source, request_parameters(source_id, source, instrument))

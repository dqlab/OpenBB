"""Live collection context mapped into configuration-selected providers."""

from __future__ import annotations

from typing import Any, Protocol

from openbb_collector_core import ProviderFailure, Response
from openbb_collector_core.process import ProcessClient
from openbb_collector_core.providers import fetcher_for
from openbb_collector_core.providers import request_parameters as map_parameters

from . import __version__
from .config import CollectorConfig, Instrument, Source

__all__ = ["OpenBBClient", "ProviderClient", "ProviderFailure", "Response", "request_parameters"]


class ProviderClient(Protocol):
    def fetch(self, source_id: str, source: Source, instrument: Instrument) -> Response: ...
    def fetch_batch(
        self,
        source_id: str,
        source: Source,
        instruments: list[Instrument],
    ) -> list[Response]: ...
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
            source = config.sources[source_id]
            if source.batch_size > 1:
                fetcher = fetcher_for(source.provider, source.model)
                if source.batch_size > getattr(fetcher, "collection_batch_limit", 0):
                    raise ProviderFailure("unsupported_provider_batch")
            for instrument_id in collection.instrument_ids:
                request_parameters(
                    source_id, config.sources[source_id], config.instruments[instrument_id]
                )


class OpenBBClient(ProcessClient):
    """Live engine facade over the shared bounded provider process."""

    collector_metadata = {"version": __version__, "distribution": "dq-live-market-data-collector"}

    def fetch(self, source_id: str, source: Source, instrument: Instrument) -> Response:
        return super().fetch(source_id, source, request_parameters(source_id, source, instrument))

    def fetch_batch(
        self,
        source_id: str,
        source: Source,
        instruments: list[Instrument],
    ) -> list[Response]:
        return super().fetch_batch(
            source_id,
            source,
            [request_parameters(source_id, source, instrument) for instrument in instruments],
        )

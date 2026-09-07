"""Historical collection context mapped into configuration-selected providers."""

from __future__ import annotations

from datetime import UTC, datetime, time, timedelta
from typing import Any, Protocol
from zoneinfo import ZoneInfo

from openbb_collector_core import ProviderFailure, Response
from openbb_collector_core.process import ProcessClient
from openbb_collector_core.providers import request_parameters as map_parameters

from . import __version__
from .config import BAR_SECONDS, Collection, CollectorConfig, Instrument, Source
from .planner import Chunk, expected_times

__all__ = ["OpenBBClient", "ProviderClient", "ProviderFailure", "Response", "request_parameters"]


class ProviderClient(Protocol):
    def fetch(
        self,
        source_id: str,
        source: Source,
        instrument: Instrument,
        collection: Collection,
        chunk: Chunk,
    ) -> Response: ...
    def reset(self) -> None: ...
    def close(self) -> None: ...


def request_parameters(
    source_id: str, source: Source, instrument: Instrument, collection: Collection, chunk: Chunk
) -> dict[str, Any]:
    end = datetime.combine(chunk.end, time(), ZoneInfo(instrument.timezone)).astimezone(UTC)
    expected = expected_times(collection, instrument, chunk)
    if collection.frequency != "1d" and expected:
        final_bar = datetime.fromisoformat(max(expected))
        end = max(end, final_bar + timedelta(seconds=BAR_SECONDS[collection.frequency]))
    context = {
        **instrument.model_dump(mode="json"),
        **collection.model_dump(mode="json"),
        "symbol": instrument.source_symbols.get(source_id, instrument.symbol),
        "start_date": chunk.start.isoformat(),
        "end_date": chunk.end.isoformat(),
        "end_datetime": end.isoformat(),
        "adjustment": source.adjustment,
        "extended_hours": not collection.use_rth,
        "timeout": max(0.1, source.timeout_seconds - 1),
    }
    return map_parameters(source, context)


def validate_providers(config: CollectorConfig) -> None:
    """Validate every source/instrument query without credentials or network I/O."""
    for collection_id, collection in config.collections.items():
        for source_id in [collection.primary, *collection.secondary]:
            for instrument_id in collection.instrument_ids:
                chunk = Chunk(
                    collection_id,
                    instrument_id,
                    collection.start_date,
                    collection.start_date + timedelta(days=1),
                )
                request_parameters(
                    source_id,
                    config.sources[source_id],
                    config.instruments[instrument_id],
                    collection,
                    chunk,
                )


class OpenBBClient(ProcessClient):
    """Historical engine facade over the shared bounded provider process."""

    collector_metadata = {
        "version": __version__,
        "distribution": "dq-historical-market-data-collector",
    }

    def fetch(
        self,
        source_id: str,
        source: Source,
        instrument: Instrument,
        collection: Collection,
        chunk: Chunk,
    ) -> Response:
        return super().fetch(
            source_id, source, request_parameters(source_id, source, instrument, collection, chunk)
        )

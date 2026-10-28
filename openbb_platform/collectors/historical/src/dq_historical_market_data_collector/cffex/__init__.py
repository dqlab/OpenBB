"""CFFEX date-wide daily futures collection, independent of fixed-symbol engines.

The names below stay importable exactly as before (``from dq_historical_market_data_collector.cffex
import Collector``), but they are resolved on first access instead of at package import. That keeps
the acquisition-only task (:mod:`.acquire`) free of the configuration, journal and storage modules:
importing ``cffex.acquire`` must not drag a journal into a process that is only allowed to write raw
evidence. The eager import list this replaces is preserved verbatim in ``__all__``.
"""

from __future__ import annotations

from typing import Any

__all__ = [
    "Collector",
    "Config",
    "Journal",
    "Source",
    "TradingCalendar",
    "load_calendar",
    "load_config",
]

_CONFIG_NAMES = ("Config", "Source", "TradingCalendar", "load_calendar", "load_config")


def __getattr__(name: str) -> Any:
    if name in _CONFIG_NAMES:
        from . import config

        return getattr(config, name)
    if name == "Collector":
        from .runtime import Collector

        return Collector
    if name == "Journal":
        from .storage import Journal

        return Journal
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted(__all__)

"""Discover independently installed collection engines."""

from functools import lru_cache
from importlib.metadata import entry_points

from .models import CollectorExtension


@lru_cache
def get_collector(name: str) -> CollectorExtension:
    matches = list(entry_points(group="openbb_collector_extension", name=name))
    if len(matches) != 1:
        raise ValueError(f"Expected one installed collector named {name!r}")
    collector = matches[0].load()
    if not isinstance(collector, CollectorExtension) or collector.name != name:
        raise ValueError("Invalid collector extension")
    return collector

"""Compatibility module for :mod:`openbb_quantitative.commodity.analytics`."""

from typing import Any

import openbb_quantitative.commodity.analytics as _analytics

PUBLIC_FUNCTIONS = _analytics.PUBLIC_FUNCTIONS
european_option = _analytics.european_option
router = _analytics.router


def __getattr__(name: str) -> Any:
    """Delegate public dqlib commodity analytics bindings."""
    return getattr(_analytics, name)


def __dir__() -> list[str]:
    """Return the commodity analytics public names."""
    return dir(_analytics)


__all__ = _analytics.__all__

"""Compatibility module for :mod:`openbb_quantitative.equity.analytics`."""

from typing import Any

import openbb_quantitative.equity.analytics as _analytics

PUBLIC_FUNCTIONS = _analytics.PUBLIC_FUNCTIONS
american_option = _analytics.american_option
build_volatility_surface = _analytics.build_volatility_surface
european_option = _analytics.european_option
router = _analytics.router


def __getattr__(name: str) -> Any:
    """Delegate public dqlib equity analytics bindings."""
    return getattr(_analytics, name)


def __dir__() -> list[str]:
    """Return the equity analytics public names."""
    return dir(_analytics)


__all__ = _analytics.__all__

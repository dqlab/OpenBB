"""Compatibility module for :mod:`openbb_quantitative.foreign_exchange.analytics`."""

from typing import Any

import openbb_quantitative.foreign_exchange.analytics as _analytics

PUBLIC_FUNCTIONS = _analytics.PUBLIC_FUNCTIONS
atm_strike = _analytics.atm_strike
router = _analytics.router


def __getattr__(name: str) -> Any:
    """Delegate public dqlib foreign-exchange analytics bindings."""
    return getattr(_analytics, name)


def __dir__() -> list[str]:
    """Return the foreign-exchange analytics public names."""
    return dir(_analytics)


__all__ = _analytics.__all__

"""Compatibility module for :mod:`openbb_quantitative.risk.analytics`."""

from typing import Any

import openbb_quantitative.risk.analytics as _analytics

PUBLIC_FUNCTIONS = _analytics.PUBLIC_FUNCTIONS
expected_shortfall = _analytics.expected_shortfall
router = _analytics.router
value_at_risk = _analytics.value_at_risk


def __getattr__(name: str) -> Any:
    """Delegate public dqlib market-risk analytics bindings."""
    return getattr(_analytics, name)


def __dir__() -> list[str]:
    """Return the market-risk analytics public names."""
    return dir(_analytics)


__all__ = _analytics.__all__

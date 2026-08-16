"""Compatibility module for :mod:`openbb_quantitative.interest_rate.analytics`."""

from typing import Any

import openbb_quantitative.interest_rate.analytics as _analytics

PUBLIC_FUNCTIONS = _analytics.PUBLIC_FUNCTIONS
curve_analytics = _analytics.curve_analytics
router = _analytics.router
single_currency_curve = _analytics.single_currency_curve


def __getattr__(name: str) -> Any:
    """Delegate public dqlib interest-rate analytics bindings."""
    return getattr(_analytics, name)


def __dir__() -> list[str]:
    """Return the interest-rate analytics public names."""
    return dir(_analytics)


__all__ = _analytics.__all__

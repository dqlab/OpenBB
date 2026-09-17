"""Compatibility module for :mod:`openbb_quantitative.credit.analytics`."""

from typing import Any

import openbb_quantitative.credit.analytics as _analytics

PUBLIC_FUNCTIONS = _analytics.PUBLIC_FUNCTIONS
curve_analytics = _analytics.curve_analytics
router = _analytics.router


def __getattr__(name: str) -> Any:
    """Delegate public dqlib credit analytics bindings."""
    return getattr(_analytics, name)


def __dir__() -> list[str]:
    """Return the credit analytics public names."""
    return dir(_analytics)


__all__ = _analytics.__all__

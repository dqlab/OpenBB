"""Compatibility module for :mod:`openbb_quantitative.common.analytics`."""

from typing import Any

import openbb_quantitative.common.analytics as _analytics

PUBLIC_FUNCTIONS = _analytics.PUBLIC_FUNCTIONS
router = _analytics.router


def __getattr__(name: str) -> Any:
    """Delegate public dqlib analytics bindings to the common package."""
    return getattr(_analytics, name)


def __dir__() -> list[str]:
    """Return the common analytics public names."""
    return dir(_analytics)


__all__ = _analytics.__all__

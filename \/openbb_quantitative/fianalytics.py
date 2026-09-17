"""Compatibility module for :mod:`openbb_quantitative.fixed_income.analytics`."""

from typing import Any

import openbb_quantitative.fixed_income.analytics as _analytics

PUBLIC_FUNCTIONS = _analytics.PUBLIC_FUNCTIONS
fixed_coupon_bond_ytm = _analytics.fixed_coupon_bond_ytm
router = _analytics.router


def __getattr__(name: str) -> Any:
    """Delegate public dqlib fixed-income analytics bindings."""
    return getattr(_analytics, name)


def __dir__() -> list[str]:
    """Return the fixed-income analytics public names."""
    return dir(_analytics)


__all__ = _analytics.__all__

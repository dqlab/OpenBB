"""Typed models for equity analytics."""

from openbb_quantitative.common.models import (
    EuropeanOptionRequest,
    EuropeanOptionResult,
)


class EquityEuropeanOptionRequest(EuropeanOptionRequest):
    """Inputs for a native dqlib equity European option valuation."""


__all__ = ["EquityEuropeanOptionRequest", "EuropeanOptionResult"]

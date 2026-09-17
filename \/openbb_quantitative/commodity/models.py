"""Typed models for commodity analytics."""

from openbb_quantitative.common.models import (
    EuropeanOptionRequest,
    EuropeanOptionResult,
)


class CommodityEuropeanOptionRequest(EuropeanOptionRequest):
    """Inputs for a native dqlib commodity European option valuation."""


__all__ = ["CommodityEuropeanOptionRequest", "EuropeanOptionResult"]

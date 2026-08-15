"""Backward-compatible imports for typed domain request and response models."""

from openbb_quantitative.commodity.models import CommodityEuropeanOptionRequest
from openbb_quantitative.common.models import (
    Currency,
    EuropeanOptionRequest,
    EuropeanOptionResult,
    PayoffType,
)
from openbb_quantitative.credit.models import (
    CreditCurveAnalyticsRequest,
    CreditCurveAnalyticsResult,
    CreditCurvePillar,
    CreditCurvePoint,
)
from openbb_quantitative.equity.models import EquityEuropeanOptionRequest
from openbb_quantitative.fixed_income.models import (
    FixedCouponBondYtmRequest,
    FixedCouponBondYtmResult,
)
from openbb_quantitative.foreign_exchange.models import (
    FxAtmStrikeRequest,
    FxAtmStrikeResult,
)
from openbb_quantitative.interest_rate.models import (
    IrCurveAnalyticsRequest,
    IrCurveAnalyticsResult,
    IrCurvePillar,
    IrCurvePoint,
)
from openbb_quantitative.risk.models import (
    ExpectedShortfallResult,
    TailRiskRequest,
    ValueAtRiskResult,
)

__all__ = [
    "CommodityEuropeanOptionRequest",
    "CreditCurveAnalyticsRequest",
    "CreditCurveAnalyticsResult",
    "CreditCurvePillar",
    "CreditCurvePoint",
    "Currency",
    "EquityEuropeanOptionRequest",
    "EuropeanOptionRequest",
    "EuropeanOptionResult",
    "ExpectedShortfallResult",
    "FixedCouponBondYtmRequest",
    "FixedCouponBondYtmResult",
    "FxAtmStrikeRequest",
    "FxAtmStrikeResult",
    "IrCurveAnalyticsRequest",
    "IrCurveAnalyticsResult",
    "IrCurvePillar",
    "IrCurvePoint",
    "PayoffType",
    "TailRiskRequest",
    "ValueAtRiskResult",
]

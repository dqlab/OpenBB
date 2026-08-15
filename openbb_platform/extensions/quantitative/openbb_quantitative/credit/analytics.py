"""Typed OpenBB workflows and public dqlib credit analytics bindings."""

from typing import Any

from openbb_core.app.model.obbject import OBBject

from openbb_quantitative._dqlib import domain_dir, domain_getattr, execute_function
from openbb_quantitative._dqlib_api import install_public_functions
from openbb_quantitative.common.native import to_datetime, vector_values
from openbb_quantitative.credit.models import (
    CreditCurveAnalyticsRequest,
    CreditCurveAnalyticsResult,
    CreditCurvePoint,
)
from openbb_quantitative.dqlib_domain import create_domain_router

_DOMAIN = "cranalytics"
router = create_domain_router(_DOMAIN, "credit")

_FUNCTION_NAMES = (
    "create_cds_pricing_settings",
    "create_cr_mkt_data_set",
    "create_cr_risk_settings",
    "create_credit_par_curve",
    "credit_curve_builder",
    "credit_default_swap_pricer",
    "to_accrual_bias",
    "to_forwards_in_coupon_period",
    "to_numerical_fix",
)

PUBLIC_FUNCTIONS = install_public_functions(globals(), _DOMAIN, _FUNCTION_NAMES)


@router.command(
    methods=["POST"],
    operation_id="dqlib_credit_curve_analytics",
)
def curve_analytics(
    request: CreditCurveAnalyticsRequest,
) -> OBBject[CreditCurveAnalyticsResult]:
    """Construct a native dqlib credit curve and calculate curve measures."""
    as_of_date = to_datetime(request.as_of_date)
    pillar_dates = [to_datetime(pillar.date) for pillar in request.pillars]
    query_dates = [to_datetime(value) for value in request.query_dates]
    curve = execute_function(
        "analytics",
        "create_credit_curve",
        [
            as_of_date,
            pillar_dates,
            [pillar.hazard_rate for pillar in request.pillars],
        ],
        {
            "day_count": request.day_count,
            "interp_method": request.interpolation,
            "extrap_method": request.extrapolation,
            "curve_name": request.curve_name,
            "pillar_names": [pillar.name for pillar in request.pillars],
        },
    )
    size = len(query_dates)
    spreads = vector_values(
        execute_function("analytics", "get_credit_spread", [curve, query_dates]),
        size,
        "credit-spread calculation",
    )
    survival_probabilities = vector_values(
        execute_function("analytics", "get_survival_probability", [curve, query_dates]),
        size,
        "survival-probability calculation",
    )
    points = [
        CreditCurvePoint(
            date=value,
            credit_spread=spreads[index],
            survival_probability=survival_probabilities[index],
        )
        for index, value in enumerate(request.query_dates)
    ]
    return OBBject(
        results=CreditCurveAnalyticsResult(
            as_of_date=request.as_of_date,
            curve_name=request.curve_name,
            points=points,
        )
    )


def __getattr__(name: str) -> Any:
    """Resolve re-exported public attributes from dqlib.cranalytics."""
    return domain_getattr(_DOMAIN, name)


def __dir__() -> list[str]:
    """Return local and dqlib public credit names for discovery."""
    return sorted(set(globals()) | set(domain_dir(_DOMAIN)))


__all__ = [  # noqa: PLE0604
    *PUBLIC_FUNCTIONS,
    "PUBLIC_FUNCTIONS",
    "curve_analytics",
    "router",
]

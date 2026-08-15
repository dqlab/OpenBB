"""Typed OpenBB workflows and public dqlib interest-rate analytics bindings."""

from typing import Any

from openbb_core.app.model.obbject import OBBject

from openbb_quantitative._dqlib import (
    domain_dir,
    domain_getattr,
    execute_function,
)
from openbb_quantitative._dqlib_api import install_public_functions
from openbb_quantitative.common.native import to_datetime, vector_values
from openbb_quantitative.dqlib_domain import create_domain_router
from openbb_quantitative.interest_rate.models import (
    IrCurveAnalyticsRequest,
    IrCurveAnalyticsResult,
    IrCurvePoint,
)

_DOMAIN = "iranalytics"
router = create_domain_router(_DOMAIN, "interest_rate")

_FUNCTION_NAMES = (
    "create_cross_ccy_mkt_data_set",
    "create_ir_curve_build_settings",
    "create_ir_mkt_data_set",
    "create_ir_par_rate_curve",
    "create_ir_risk_settings",
    "create_xccy_ir_risk_settings",
    "cross_currency_swap_pricer",
    "ir_cross_ccy_curve_builder",
    "ir_single_ccy_curve_builder",
    "ir_vanilla_instrument_pricer",
)

PUBLIC_FUNCTIONS = install_public_functions(globals(), _DOMAIN, _FUNCTION_NAMES)


@router.command(
    methods=["POST"],
    operation_id="dqlib_interest_rate_curve_analytics",
)
def curve_analytics(
    request: IrCurveAnalyticsRequest,
) -> OBBject[IrCurveAnalyticsResult]:
    """Construct a native dqlib zero curve and calculate curve measures."""
    as_of_date = to_datetime(request.as_of_date)
    pillar_dates = [to_datetime(pillar.date) for pillar in request.pillars]
    query_dates = [to_datetime(value) for value in request.query_dates]
    curve = execute_function(
        "analytics",
        "create_ir_yield_curve",
        [
            as_of_date,
            request.currency,
            pillar_dates,
            [pillar.zero_rate for pillar in request.pillars],
        ],
        {
            "day_count": request.day_count,
            "interp_method": request.interpolation,
            "extrap_method": request.extrapolation,
            "compounding_type": request.compounding,
            "frequency": request.frequency,
            "curve_name": request.curve_name,
            "pillar_names": [pillar.name for pillar in request.pillars],
        },
    )
    size = len(query_dates)
    zero_rates = vector_values(
        execute_function("analytics", "get_zero_rate", [curve, query_dates]),
        size,
        "IR zero-rate calculation",
    )
    discount_factors = vector_values(
        execute_function("analytics", "get_discount_factor", [curve, query_dates]),
        size,
        "IR discount-factor calculation",
    )
    forward_rates = vector_values(
        execute_function(
            "analytics",
            "get_fwd_rate",
            [curve, query_dates, request.forward_tenor],
        ),
        size,
        "IR forward-rate calculation",
    )
    points = [
        IrCurvePoint(
            date=value,
            zero_rate=zero_rates[index],
            discount_factor=discount_factors[index],
            forward_rate=forward_rates[index],
        )
        for index, value in enumerate(request.query_dates)
    ]
    return OBBject(
        results=IrCurveAnalyticsResult(
            as_of_date=request.as_of_date,
            currency=request.currency,
            curve_name=request.curve_name,
            points=points,
        )
    )


def __getattr__(name: str) -> Any:
    """Resolve re-exported public attributes from dqlib.iranalytics."""
    return domain_getattr(_DOMAIN, name)


def __dir__() -> list[str]:
    """Return local and dqlib public interest-rate names for discovery."""
    return sorted(set(globals()) | set(domain_dir(_DOMAIN)))


__all__ = [  # noqa: PLE0604
    *PUBLIC_FUNCTIONS,
    "PUBLIC_FUNCTIONS",
    "curve_analytics",
    "router",
]

"""Lazy bridge to dqlib credit analytics."""

from typing import Any

from openbb_core.app.model.obbject import OBBject

from openbb_quantitative._dqlib import (
    domain_dir,
    domain_getattr,
    execute_function,
)
from openbb_quantitative._dqlib_typed import to_datetime, vector_values
from openbb_quantitative.dqlib_domain import create_domain_router
from openbb_quantitative.dqlib_models import (
    CreditCurveAnalyticsRequest,
    CreditCurveAnalyticsResult,
    CreditCurvePoint,
)

_DOMAIN = "cranalytics"
router = create_domain_router(_DOMAIN)


@router.command(
    methods=["POST"],
    operation_id="dqlib_cranalytics_curve_analytics",
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
        execute_function(
            "analytics", "get_survival_probability", [curve, query_dates]
        ),
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
    """Resolve a public attribute from dqlib.cranalytics."""
    return domain_getattr(_DOMAIN, name)


def __dir__() -> list[str]:
    """Return local and dqlib domain attributes for interactive discovery."""
    return sorted(set(globals()) | set(domain_dir(_DOMAIN)))

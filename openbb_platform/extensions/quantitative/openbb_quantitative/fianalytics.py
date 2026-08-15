"""Lazy bridge to dqlib fixed-income analytics."""

from typing import Any

from openbb_core.app.model.obbject import OBBject

from openbb_quantitative._dqlib import (
    domain_dir,
    domain_getattr,
    execute_function,
)
from openbb_quantitative._dqlib_typed import to_datetime
from openbb_quantitative.dqlib_domain import create_domain_router
from openbb_quantitative.dqlib_models import (
    FixedCouponBondYtmRequest,
    FixedCouponBondYtmResult,
)

_DOMAIN = "fianalytics"
router = create_domain_router(_DOMAIN)


@router.command(
    methods=["POST"],
    operation_id="dqlib_fianalytics_fixed_coupon_bond_ytm",
)
def fixed_coupon_bond_ytm(
    request: FixedCouponBondYtmRequest,
) -> OBBject[FixedCouponBondYtmResult]:
    """Build a fixed-coupon bond and calculate its native dqlib yield."""
    issue_date = to_datetime(request.issue_date)
    calculation_date = to_datetime(request.calculation_date)
    template = execute_function(
        "fimarket",
        "create_fixed_cpn_bond_template",
        [
            request.instrument_name,
            issue_date,
            request.settlement_days,
            issue_date,
            request.maturity,
            request.coupon_rate,
            request.currency,
            request.calendar,
        ],
        {
            "frequency": request.frequency,
            "day_count": request.day_count,
            "issue_price": request.issue_price,
            "interest_day_convention": "UNADJUSTED",
            "pay_day_convention": "UNADJUSTED",
        },
    )
    bond = execute_function(
        "fimarket", "build_fixed_cpn_bond", [request.nominal, template]
    )
    forward_curve = execute_function(
        "analytics",
        "create_flat_ir_yield_curve",
        [calculation_date, request.currency, request.curve_rate],
    )
    result = execute_function(
        _DOMAIN,
        "yield_to_maturity_calculator",
        [
            calculation_date,
            request.compounding,
            bond,
            forward_curve,
            request.price,
            request.price_type,
            request.frequency,
        ],
    )
    return OBBject(
        results=FixedCouponBondYtmResult(
            yield_to_maturity=float(result),
            price=request.price,
            price_type=request.price_type,
            currency=request.currency,
        )
    )


def __getattr__(name: str) -> Any:
    """Resolve a public attribute from dqlib.fianalytics."""
    return domain_getattr(_DOMAIN, name)


def __dir__() -> list[str]:
    """Return local and dqlib domain attributes for interactive discovery."""
    return sorted(set(globals()) | set(domain_dir(_DOMAIN)))

"""Typed OpenBB workflows and public dqlib fixed-income analytics bindings."""

from typing import Any

from openbb_core.app.model.obbject import OBBject

from openbb_quantitative._dqlib import (
    domain_dir,
    domain_getattr,
    execute_function,
)
from openbb_quantitative._dqlib_api import install_public_functions
from openbb_quantitative.common.native import to_datetime
from openbb_quantitative.dqlib_domain import create_domain_router
from openbb_quantitative.fixed_income.models import (
    FixedCouponBondYtmRequest,
    FixedCouponBondYtmResult,
)

_DOMAIN = "fianalytics"
router = create_domain_router(_DOMAIN, "fixed_income")

_FUNCTION_NAMES = (
    "build_bond_sprd_curve",
    "build_bond_yield_curve",
    "create_bond_curve_build_settings",
    "create_bond_par_curve",
    "create_fi_mkt_data_set",
    "create_fi_risk_settings",
    "fixed_cpn_bond_par_rate_calculator",
    "to_bond_quote_type",
    "vanilla_bond_pricer",
    "yield_to_maturity_calculator",
)

PUBLIC_FUNCTIONS = install_public_functions(globals(), _DOMAIN, _FUNCTION_NAMES)


@router.command(
    methods=["POST"],
    operation_id="dqlib_fixed_income_fixed_coupon_bond_ytm",
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
    bond = execute_function("fimarket", "build_fixed_cpn_bond", [request.nominal, template])
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
    """Resolve re-exported public attributes from dqlib.fianalytics."""
    return domain_getattr(_DOMAIN, name)


def __dir__() -> list[str]:
    """Return local and dqlib public fixed-income names for discovery."""
    return sorted(set(globals()) | set(domain_dir(_DOMAIN)))


__all__ = [  # noqa: PLE0604
    *PUBLIC_FUNCTIONS,
    "PUBLIC_FUNCTIONS",
    "fixed_coupon_bond_ytm",
    "router",
]

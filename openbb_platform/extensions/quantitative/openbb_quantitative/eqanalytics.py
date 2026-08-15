"""Lazy bridge to dqlib equity analytics."""

from typing import Any

from openbb_core.app.model.obbject import OBBject

from openbb_quantitative._dqlib import (
    domain_dir,
    domain_getattr,
    execute_function,
)
from openbb_quantitative._dqlib_typed import (
    build_option_settings,
    pricing_values,
    to_datetime,
)
from openbb_quantitative.dqlib_domain import create_domain_router
from openbb_quantitative.dqlib_models import (
    EquityEuropeanOptionRequest,
    EuropeanOptionResult,
)

_DOMAIN = "eqanalytics"
router = create_domain_router(_DOMAIN)


@router.command(
    methods=["POST"],
    operation_id="dqlib_eqanalytics_european_option",
)
def european_option(
    request: EquityEuropeanOptionRequest,
) -> OBBject[EuropeanOptionResult]:
    """Price an equity European option with native flat dqlib market data."""
    valuation_date = to_datetime(request.valuation_date)
    expiry_date = to_datetime(request.expiry_date)
    discount_curve = execute_function(
        "analytics",
        "create_flat_ir_yield_curve",
        [valuation_date, request.currency, request.discount_rate],
    )
    dividend_curve = execute_function(
        "analytics",
        "create_flat_dividend_curve",
        [valuation_date, request.carry_rate, f"{request.underlying}_DIVIDEND"],
    )
    volatility_surface = execute_function(
        "analytics",
        "create_flat_volatility_surface",
        [valuation_date, request.volatility, request.underlying],
    )
    instrument = execute_function(
        "market",
        "create_european_option",
        [
            request.payoff_type,
            expiry_date,
            expiry_date,
            request.strike,
            request.nominal,
            request.currency,
            "EQ_SPOT",
            request.currency,
            request.underlying,
        ],
    )
    quanto_volatility = execute_function(
        "analytics",
        "create_flat_vol_curve",
        [valuation_date, 0.0, f"{request.currency}{request.currency}"],
    )
    market_data = execute_function(
        _DOMAIN,
        "create_eq_mkt_data_set",
        [
            valuation_date,
            discount_curve,
            request.spot,
            volatility_surface,
            dividend_curve,
            discount_curve,
            quanto_volatility,
            0.0,
            request.underlying,
        ],
    )
    pricing, risk, scenario = build_option_settings(_DOMAIN, request.currency)
    response = execute_function(
        _DOMAIN,
        "eq_european_option_pricer",
        [instrument, valuation_date, market_data, pricing, risk, scenario],
    )
    present_value, cash_value, currency = pricing_values(
        response, "equity European option pricing"
    )
    return OBBject(
        results=EuropeanOptionResult(
            present_value=present_value,
            cash_value=cash_value,
            currency=currency,
        )
    )


def __getattr__(name: str) -> Any:
    """Resolve a public attribute from dqlib.eqanalytics."""
    return domain_getattr(_DOMAIN, name)


def __dir__() -> list[str]:
    """Return local and dqlib domain attributes for interactive discovery."""
    return sorted(set(globals()) | set(domain_dir(_DOMAIN)))

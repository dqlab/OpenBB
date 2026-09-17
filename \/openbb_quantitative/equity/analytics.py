"""Typed OpenBB workflows and public dqlib equity analytics bindings."""

from typing import Any

from openbb_core.app.model.obbject import OBBject

from openbb_quantitative._dqlib import (
    DQLibExecutionError,
    domain_dir,
    domain_getattr,
    execute_function,
)
from openbb_quantitative._dqlib_api import install_public_functions
from openbb_quantitative.common.native import (
    build_option_settings,
    pricing_values,
    to_datetime,
    vector_values,
)
from openbb_quantitative.dqlib_domain import create_domain_router
from openbb_quantitative.equity.models import (
    BuildEqVolatilitySurfaceRequest,
    BuildEqVolatilitySurfaceResult,
    EqRateCurveInput,
    EquityAmericanOptionRequest,
    EquityAmericanOptionResult,
    EquityEuropeanOptionRequest,
    EqVolatilitySurfacePoint,
    EuropeanOptionResult,
)

_DOMAIN = "eqanalytics"
router = create_domain_router(_DOMAIN, "equity")

_FUNCTION_NAMES = (
    "build_eq_index_dividend_curve",
    "create_eq_mkt_data_set",
    "create_eq_option_quote_matrix",
    "create_eq_risk_settings",
    "demo_load_vol_surface",
    "eq_airbag_option_pricer",
    "eq_american_option_pricer",
    "eq_asian_option_pricer",
    "eq_digital_option_pricer",
    "eq_double_barrier_option_pricer",
    "eq_double_shark_fin_option_pricer",
    "eq_double_touch_option_pricer",
    "eq_european_option_pricer",
    "eq_one_touch_option_pricer",
    "eq_phoenix_auto_callable_note_pricer",
    "eq_ping_pong_option_pricer",
    "eq_range_accrual_option_pricer",
    "eq_single_barrier_option_pricer",
    "eq_single_shark_fin_option_pricer",
    "eq_snowball_auto_callable_note_pricer",
    "eq_vol_surface_builder",
    "run_eq_pricing",
)

PUBLIC_FUNCTIONS = install_public_functions(globals(), _DOMAIN, _FUNCTION_NAMES)


def _build_flat_equity_market(
    request: EquityEuropeanOptionRequest | EquityAmericanOptionRequest,
) -> tuple[Any, Any]:
    """Build the native flat market shared by typed equity option pricers."""
    valuation_date = to_datetime(request.valuation_date)
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
    return valuation_date, market_data


def _build_rate_curve(
    request: BuildEqVolatilitySurfaceRequest,
    curve: EqRateCurveInput,
    curve_name: str,
) -> Any:
    """Convert a typed flat or pillar curve to a native dqlib IR curve."""
    as_of_date = to_datetime(request.as_of_date)
    if curve.flat_rate is not None:
        return execute_function(
            "analytics",
            "create_flat_ir_yield_curve",
            [as_of_date, request.currency, curve.flat_rate],
        )
    if curve.pillars is None:  # pragma: no cover - model validation
        raise DQLibExecutionError(f"The {curve_name} curve has no values.")
    return execute_function(
        "analytics",
        "create_ir_yield_curve",
        [
            as_of_date,
            request.currency,
            [to_datetime(pillar.date) for pillar in curve.pillars],
            [pillar.rate for pillar in curve.pillars],
        ],
        {
            "day_count": curve.day_count,
            "interp_method": curve.interpolation,
            "extrap_method": curve.extrapolation,
            "compounding_type": curve.compounding,
            "frequency": "ANNUAL",
            "curve_name": curve_name,
            "pillar_names": [pillar.name for pillar in curve.pillars],
        },
    )


def _sample_zero_rates(
    curve: Any,
    expiry_dates: list[Any],
    curve_name: str,
) -> list[float]:
    """Sample one native zero curve at every option expiry."""
    return vector_values(
        execute_function("analytics", "get_zero_rate", [curve, expiry_dates]),
        len(expiry_dates),
        f"equity {curve_name} curve sampling",
    )


def _build_carry_curve(
    request: BuildEqVolatilitySurfaceRequest,
    discount_curve: Any,
    expiry_dates: list[Any],
) -> Any:
    """Build the single carry curve accepted by dqlib 3.0.2.

    The native surface request has no repo field. When a distinct repo curve is
    supplied, its equivalent continuous dividend yield is r_discount - r_repo
    + q_dividend, preserving the requested equity forward carry.
    """
    dividend_zero_curve = _build_rate_curve(
        request,
        request.dividend_curve,
        f"{request.underlying}_DIVIDEND_INPUT",
    )
    dividend_rates = _sample_zero_rates(
        dividend_zero_curve,
        expiry_dates,
        "dividend",
    )
    effective_rates = dividend_rates
    if request.repo_curve is not None:
        repo_curve = _build_rate_curve(
            request,
            request.repo_curve,
            f"{request.underlying}_REPO",
        )
        discount_rates = _sample_zero_rates(
            discount_curve,
            expiry_dates,
            "discount",
        )
        repo_rates = _sample_zero_rates(repo_curve, expiry_dates, "repo")
        effective_rates = [
            discount_rate - repo_rate + dividend_rate
            for discount_rate, repo_rate, dividend_rate in zip(
                discount_rates,
                repo_rates,
                dividend_rates,
            )
        ]

    as_of_date = to_datetime(request.as_of_date)
    curve_name = f"{request.underlying}_EFFECTIVE_DIVIDEND"
    if len(effective_rates) == 1 or all(
        abs(value - effective_rates[0]) <= 1e-14
        for value in effective_rates[1:]
    ):
        return execute_function(
            "analytics",
            "create_flat_dividend_curve",
            [as_of_date, effective_rates[0], curve_name],
        )
    return execute_function(
        "analytics",
        "create_dividend_curve",
        [
            as_of_date,
            expiry_dates,
            effective_rates,
            "CONTINUOUS_DIVIDEND",
            "LINEAR_INTERP",
            "FLAT_EXTRAP",
            "ACT_365_FIXED",
            as_of_date,
        ],
        {
            "pillar_names": [value.isoformat() for value in request.expiry_dates],
            "curve_name": curve_name,
        },
    )


def _build_quote_matrix(
    request: BuildEqVolatilitySurfaceRequest,
    expiry_dates: list[Any],
) -> Any:
    """Group OpenBB option-chain rows into dqlib's nested smile vectors."""
    payoff_types: list[list[str]] = []
    option_prices: list[list[float]] = []
    option_strikes: list[list[float]] = []
    for expiry in request.expiry_dates:
        quotes = sorted(
            (
                quote
                for quote in request.option_chain
                if quote.expiry_date == expiry
            ),
            key=lambda quote: (quote.strike, quote.option_type),
        )
        payoff_types.append([quote.option_type for quote in quotes])
        option_prices.append([quote.market_price for quote in quotes])
        option_strikes.append([quote.strike for quote in quotes])
    return execute_function(
        _DOMAIN,
        "create_eq_option_quote_matrix",
        [
            request.build_settings.exercise_type,
            "SPOT_UNDERLYING_TYPE",
            to_datetime(request.as_of_date),
            expiry_dates,
            payoff_types,
            option_prices,
            option_strikes,
            request.underlying,
        ],
    )


@router.command(
    methods=["POST"],
    operation_id="dqlib_equity_build_volatility_surface",
)
def build_volatility_surface(
    request: BuildEqVolatilitySurfaceRequest,
) -> OBBject[BuildEqVolatilitySurfaceResult]:
    """Build and evaluate a native dqlib equity volatility surface."""
    expiry_dates = [to_datetime(value) for value in request.expiry_dates]
    discount_curve = _build_rate_curve(
        request,
        request.discount_curve,
        f"{request.underlying}_DISCOUNT",
    )
    carry_curve = _build_carry_curve(request, discount_curve, expiry_dates)
    quote_matrix = _build_quote_matrix(request, expiry_dates)
    pricing_settings = execute_function(
        "analytics",
        "create_pricing_settings",
        [],
        {
            "pricing_currency": request.currency,
            "inc_current": False,
            "pricing_method": request.build_settings.pricing_method,
        },
    )
    wing_strike_type = {
        "ABSOLUTE_STRIKE": "ABOSULTE_STRIKE",
    }.get(
        request.build_settings.wing_strike_type,
        request.build_settings.wing_strike_type,
    )
    surface = execute_function(
        _DOMAIN,
        "eq_vol_surface_builder",
        [
            to_datetime(request.as_of_date),
            request.build_settings.smile_method,
            wing_strike_type,
            request.build_settings.lower,
            request.build_settings.upper,
            quote_matrix,
            [request.underlying_price] * len(expiry_dates),
            discount_curve,
            carry_curve,
            pricing_settings,
            [
                request.build_settings.fixed_parameter_index,
                request.build_settings.fixed_parameter_value,
            ],
            request.underlying,
        ],
    )
    if isinstance(surface, str) or not hasattr(surface, "vol_smiles"):
        raise DQLibExecutionError(
            "dqlib failed to build the equity volatility surface."
        )

    strikes = request.output_strikes
    volatilities = vector_values(
        execute_function(
            "analytics",
            "get_volatility",
            [surface, expiry_dates, strikes],
        ),
        len(expiry_dates) * len(strikes),
        "equity volatility-surface evaluation",
    )
    points = [
        EqVolatilitySurfacePoint(
            expiry_date=expiry,
            strike=strike,
            volatility=volatilities[expiry_index * len(strikes) + strike_index],
        )
        for expiry_index, expiry in enumerate(request.expiry_dates)
        for strike_index, strike in enumerate(strikes)
    ]
    return OBBject(
        results=BuildEqVolatilitySurfaceResult(
            as_of_date=request.as_of_date,
            underlying=request.underlying,
            smile_method=request.build_settings.smile_method,
            wing_strike_type=request.build_settings.wing_strike_type,
            lower=request.build_settings.lower,
            upper=request.build_settings.upper,
            points=points,
        )
    )


@router.command(
    methods=["POST"],
    operation_id="dqlib_equity_european_option",
)
def european_option(
    request: EquityEuropeanOptionRequest,
) -> OBBject[EuropeanOptionResult]:
    """Price an equity European option with native flat dqlib market data."""
    valuation_date, market_data = _build_flat_equity_market(request)
    expiry_date = to_datetime(request.expiry_date)
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
    pricing, risk, scenario = build_option_settings(_DOMAIN, request.currency)
    response = execute_function(
        _DOMAIN,
        "eq_european_option_pricer",
        [instrument, valuation_date, market_data, pricing, risk, scenario],
    )
    present_value, cash_value, currency = pricing_values(response, "equity European option pricing")
    return OBBject(
        results=EuropeanOptionResult(
            present_value=present_value,
            cash_value=cash_value,
            currency=currency,
        )
    )


@router.command(
    methods=["POST"],
    operation_id="dqlib_equity_american_option",
)
def american_option(
    request: EquityAmericanOptionRequest,
) -> OBBject[EquityAmericanOptionResult]:
    """Price an equity American option with native flat dqlib market data."""
    valuation_date, market_data = _build_flat_equity_market(request)
    instrument = execute_function(
        "market",
        "create_american_option",
        [
            request.payoff_type,
            to_datetime(request.expiry_date),
            request.strike,
            request.settlement_days,
            request.nominal,
            request.currency,
            "EQ_SPOT",
            request.currency,
            request.underlying,
        ],
    )
    pricing, risk, scenario = build_option_settings(
        _DOMAIN,
        request.currency,
        request.pricing_method,
    )
    response = execute_function(
        _DOMAIN,
        "eq_american_option_pricer",
        [instrument, valuation_date, market_data, pricing, risk, scenario],
    )
    present_value, cash_value, currency = pricing_values(
        response,
        "equity American option pricing",
    )
    return OBBject(
        results=EquityAmericanOptionResult(
            present_value=present_value,
            cash_value=cash_value,
            currency=currency,
            pricing_method=request.pricing_method,
        )
    )


def __getattr__(name: str) -> Any:
    """Resolve re-exported public attributes from dqlib.eqanalytics."""
    return domain_getattr(_DOMAIN, name)


def __dir__() -> list[str]:
    """Return local and dqlib public equity names for discovery."""
    return sorted(set(globals()) | set(domain_dir(_DOMAIN)))


__all__ = [  # noqa: PLE0604
    *PUBLIC_FUNCTIONS,
    "PUBLIC_FUNCTIONS",
    "american_option",
    "build_volatility_surface",
    "european_option",
    "router",
]

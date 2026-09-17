"""Typed OpenBB workflows and public dqlib interest-rate analytics bindings."""

from datetime import date
from typing import Any

from openbb_core.app.model.obbject import OBBject

from openbb_quantitative._dqlib import (
    DQLibExecutionError,
    domain_dir,
    domain_getattr,
    execute_function,
)
from openbb_quantitative._dqlib_api import install_public_functions
from openbb_quantitative.common.native import to_datetime, vector_values
from openbb_quantitative.dqlib_domain import create_domain_router
from openbb_quantitative.interest_rate.models import (
    IrBuiltCurvePillar,
    IrBuiltYieldCurve,
    IrCurveAnalyticsRequest,
    IrCurveAnalyticsResult,
    IrCurveJacobian,
    IrCurvePoint,
    IrDepositTemplate,
    IrKnownYieldCurve,
    IrSingleCurrencyCurveBuildRequest,
    IrSingleCurrencyCurveBuildResult,
    IrVanillaSwapTemplate,
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


def _require_native_object(value: Any, operation: str) -> Any:
    """Reject the empty/string error values used by public dqlib wrappers."""
    if value is None or isinstance(value, (str, bytes)):
        raise DQLibExecutionError(f"dqlib failed to {operation}.")
    return value


def _register_ibor_indices(request: IrSingleCurrencyCurveBuildRequest) -> None:
    """Register the public static IBOR definitions required by swap templates."""
    for index in request.ibor_indices:
        created = execute_function(
            "irmarket",
            "create_ibor_index",
            [
                index.index_name,
                index.tenor,
                request.currency,
                index.calendars,
                index.start_delay,
            ],
            {
                "day_count": index.day_count,
                "interest_day_convention": index.interest_day_convention,
                "date_roll_convention": index.date_roll_convention,
                "ibor_type": index.ibor_type,
            },
        )
        if created is not True:
            raise DQLibExecutionError(
                f"dqlib failed to register IBOR index {index.index_name}."
            )


def _register_deposit_template(
    request: IrSingleCurrencyCurveBuildRequest,
    template: IrDepositTemplate,
) -> None:
    """Register one deposit template in dqlib's static object cache."""
    created = execute_function(
        "irmarket",
        "create_depo_template",
        [
            template.instrument_name,
            request.currency,
            template.calendar,
            template.start_delay,
        ],
        {
            "day_count": template.day_count,
            "interest_day_convention": template.interest_day_convention,
            "pay_day_offset": template.pay_day_offset,
            "pay_day_convention": template.pay_day_convention,
            "start_convention": template.start_convention,
        },
    )
    _require_native_object(
        created,
        f"register deposit template {template.instrument_name}",
    )


def _register_swap_template(
    request: IrSingleCurrencyCurveBuildRequest,
    template: IrVanillaSwapTemplate,
) -> None:
    """Build and register one fixed/floating vanilla swap template."""
    fixed = template.fixed_leg
    fixed_leg = _require_native_object(
        execute_function(
            "irmarket",
            "create_fixed_leg_definition",
            [request.currency, template.calendar, fixed.frequency],
            {
                "day_count": fixed.day_count,
                "interest_day_convention": fixed.interest_day_convention,
                "stub_policy": fixed.stub_policy,
                "broken_period_type": fixed.broken_period_type,
                "pay_day_offset": fixed.pay_day_offset,
                "pay_day_convention": fixed.pay_day_convention,
                "notional_exchange": fixed.notional_exchange,
            },
        ),
        f"create the fixed leg for {template.instrument_name}",
    )
    floating = template.floating_leg
    floating_leg = _require_native_object(
        execute_function(
            "irmarket",
            "create_floating_leg_definition",
            [
                request.currency,
                template.reference_index,
                template.calendar,
                template.fixing_calendars,
                floating.frequency,
                floating.fixing_frequency,
            ],
            {
                "day_count": floating.day_count,
                "payment_discount_method": floating.payment_discount_method,
                "rate_calc_method": floating.rate_calculation_method,
                "spread": floating.spread,
                "interest_day_convention": floating.interest_day_convention,
                "stub_policy": floating.stub_policy,
                "broken_period_type": floating.broken_period_type,
                "pay_day_offset": floating.pay_day_offset,
                "pay_day_convention": floating.pay_day_convention,
                "fixing_day_convention": floating.fixing_day_convention,
                "fixing_mode": floating.fixing_mode,
                "fixing_day_offset": floating.fixing_day_offset,
                "notional_exchange": floating.notional_exchange,
            },
        ),
        f"create the floating leg for {template.instrument_name}",
    )
    created = execute_function(
        "irmarket",
        "create_ir_vanilla_swap_template",
        [
            template.instrument_name,
            template.start_delay,
            fixed_leg,
            floating_leg,
            template.start_convention,
        ],
    )
    _require_native_object(
        created,
        f"register swap template {template.instrument_name}",
    )


def _register_instrument_templates(
    request: IrSingleCurrencyCurveBuildRequest,
) -> None:
    """Register every validated deposit and vanilla-swap template."""
    _register_ibor_indices(request)
    for template in request.instrument_templates:
        if isinstance(template, IrDepositTemplate):
            _register_deposit_template(request, template)
        elif isinstance(template, IrVanillaSwapTemplate):
            _register_swap_template(request, template)


def _build_known_curve(
    request: IrSingleCurrencyCurveBuildRequest,
    curve: IrKnownYieldCurve,
) -> Any:
    """Convert an explicitly supplied zero curve to a native yield curve."""
    return _require_native_object(
        execute_function(
            "analytics",
            "create_ir_yield_curve",
            [
                to_datetime(request.as_of_date),
                request.currency,
                [to_datetime(pillar.date) for pillar in curve.pillars],
                [pillar.zero_rate for pillar in curve.pillars],
            ],
            {
                "day_count": curve.day_count,
                "interp_method": curve.interpolation,
                "extrap_method": curve.extrapolation,
                "compounding_type": curve.compounding,
                "frequency": curve.frequency,
                "curve_name": curve.curve_name,
                "pillar_names": [pillar.name for pillar in curve.pillars],
            },
        ),
        f"create known curve {curve.curve_name}",
    )


def _build_target_inputs(
    request: IrSingleCurrencyCurveBuildRequest,
) -> tuple[list[Any], list[Any]]:
    """Convert target quotes and curve managers to native protobuf objects."""
    par_curves: list[Any] = []
    build_settings: list[Any] = []
    for target in request.targets:
        par_curves.append(
            _require_native_object(
                execute_function(
                    _DOMAIN,
                    "create_ir_par_rate_curve",
                    [
                        to_datetime(request.as_of_date),
                        request.currency,
                        target.native_par_curve_name,
                        [quote.instrument_name for quote in target.quotes],
                        [quote.instrument_type for quote in target.quotes],
                        [quote.term for quote in target.quotes],
                        [quote.factor for quote in target.quotes],
                        [quote.quote for quote in target.quotes],
                    ],
                ),
                f"create par-rate curve {target.native_par_curve_name}",
            )
        )
        build_settings.append(
            _require_native_object(
                execute_function(
                    _DOMAIN,
                    "create_ir_curve_build_settings",
                    [
                        target.curve_name,
                        target.discount_curves
                        or {request.currency: target.curve_name},
                        target.forward_curves,
                        target.use_on_tn_fx_swap,
                    ],
                ),
                f"create build settings for {target.curve_name}",
            )
        )
    return par_curves, build_settings


def _native_date(value: Any, operation: str) -> date:
    """Translate dqlib's public Date protobuf to a Python date."""
    try:
        return date(value.year, value.month, value.day)
    except (AttributeError, TypeError, ValueError) as exc:
        raise DQLibExecutionError(
            f"dqlib returned an invalid date for {operation}."
        ) from exc


def _curve_pillars(native_curve: Any, curve_name: str) -> list[IrBuiltCurvePillar]:
    """Translate the calibrated native term-structure pillars."""
    try:
        term_structure = native_curve.curve.curve
        native_dates = list(term_structure.pillar_date)
        names = list(term_structure.pillar_name)
        rates = vector_values(
            term_structure.pillar_values,
            len(native_dates),
            f"{curve_name} calibrated pillars",
        )
    except AttributeError as exc:
        raise DQLibExecutionError(
            f"dqlib returned an invalid target curve for {curve_name}."
        ) from exc
    if names and len(names) != len(native_dates):
        raise DQLibExecutionError(
            f"dqlib returned mismatched pillar names for {curve_name}."
        )
    names = names or [""] * len(native_dates)
    return [
        IrBuiltCurvePillar(
            date=_native_date(value, f"{curve_name} calibrated pillars"),
            name=names[index],
            zero_rate=rates[index],
        )
        for index, value in enumerate(native_dates)
    ]


def _curve_jacobians(native_curve: Any, curve_name: str) -> list[IrCurveJacobian]:
    """Translate optional native calibration Jacobian matrices."""
    results: list[IrCurveJacobian] = []
    for jacobian in getattr(native_curve, "jacobians", []):
        matrix = getattr(jacobian, "matrix", None)
        if matrix is None:
            raise DQLibExecutionError(
                f"dqlib returned an invalid Jacobian for {curve_name}."
            )
        values = [float(value) for value in matrix.data]
        if len(values) != matrix.rows * matrix.cols:
            raise DQLibExecutionError(
                f"dqlib returned an invalid Jacobian size for {curve_name}."
            )
        results.append(
            IrCurveJacobian(
                name=jacobian.name,
                rows=matrix.rows,
                columns=matrix.cols,
                values=values,
            )
        )
    return results


def _curve_points(
    request: IrSingleCurrencyCurveBuildRequest,
    native_curve: Any,
    curve_name: str,
) -> list[IrCurvePoint]:
    """Calculate typed zero, discount, and forward values for one curve."""
    query_dates = [to_datetime(value) for value in request.query_dates]
    size = len(query_dates)
    zero_rates = vector_values(
        execute_function("analytics", "get_zero_rate", [native_curve, query_dates]),
        size,
        f"{curve_name} zero-rate calculation",
    )
    discount_factors = vector_values(
        execute_function(
            "analytics", "get_discount_factor", [native_curve, query_dates]
        ),
        size,
        f"{curve_name} discount-factor calculation",
    )
    forward_rates = vector_values(
        execute_function(
            "analytics",
            "get_fwd_rate",
            [native_curve, query_dates, request.forward_tenor],
        ),
        size,
        f"{curve_name} forward-rate calculation",
    )
    return [
        IrCurvePoint(
            date=value,
            zero_rate=zero_rates[index],
            discount_factor=discount_factors[index],
            forward_rate=forward_rates[index],
        )
        for index, value in enumerate(request.query_dates)
    ]


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


@router.command(
    methods=["POST"],
    operation_id="dqlib_interest_rate_single_currency_curve",
)
def single_currency_curve(
    request: IrSingleCurrencyCurveBuildRequest,
) -> OBBject[IrSingleCurrencyCurveBuildResult]:
    """Build and query native dqlib single-currency yield curves."""
    _register_instrument_templates(request)
    par_curves, build_settings = _build_target_inputs(request)
    other_curves = [
        _build_known_curve(request, curve) for curve in request.other_curves
    ]
    response = execute_function(
        _DOMAIN,
        "ir_single_ccy_curve_builder",
        [
            to_datetime(request.as_of_date),
            [target.curve_name for target in request.targets],
            build_settings,
            par_curves,
            request.day_count,
            request.compounding,
            request.frequency,
            other_curves,
        ],
        {
            "building_method": request.building_method,
            "calc_jacobian": request.calculate_jacobian,
            "shift": request.shift,
            "finite_difference_method": request.finite_difference_method,
            "threading_mode": request.threading_mode,
        },
    )
    if response is None or isinstance(response, (str, bytes)):
        raise DQLibExecutionError(
            "dqlib failed to build the single-currency yield curves."
        )
    try:
        native_curves = list(response)
    except TypeError as exc:
        raise DQLibExecutionError(
            "dqlib returned invalid single-currency yield curves."
        ) from exc
    if len(native_curves) != len(request.targets):
        raise DQLibExecutionError(
            f"dqlib returned {len(native_curves)} target curves; "
            f"expected {len(request.targets)}."
        )

    curves_by_name: dict[str, Any] = {}
    for native_curve in native_curves:
        try:
            curve_name = str(native_curve.curve.curve.name)
        except AttributeError as exc:
            raise DQLibExecutionError(
                "dqlib returned a target curve without a name."
            ) from exc
        if not curve_name or curve_name in curves_by_name:
            raise DQLibExecutionError(
                "dqlib returned missing or duplicate target curve names."
            )
        curves_by_name[curve_name] = native_curve

    expected_names = {target.curve_name for target in request.targets}
    if set(curves_by_name) != expected_names:
        raise DQLibExecutionError(
            "dqlib target curve names do not match the requested curves."
        )
    curves = [
        IrBuiltYieldCurve(
            curve_name=target.curve_name,
            currency=request.currency,
            pillars=_curve_pillars(
                curves_by_name[target.curve_name], target.curve_name
            ),
            points=_curve_points(
                request,
                curves_by_name[target.curve_name],
                target.curve_name,
            ),
            jacobians=_curve_jacobians(
                curves_by_name[target.curve_name], target.curve_name
            ),
        )
        for target in request.targets
    ]
    return OBBject(
        results=IrSingleCurrencyCurveBuildResult(
            as_of_date=request.as_of_date,
            building_method=request.building_method,
            curves=curves,
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
    "single_currency_curve",
]

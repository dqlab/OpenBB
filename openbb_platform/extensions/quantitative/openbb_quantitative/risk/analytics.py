"""Typed OpenBB workflows and public dqlib market-risk analytics bindings."""

from typing import Any

from openbb_core.app.model.obbject import OBBject

from openbb_quantitative._dqlib import (
    DQLibExecutionError,
    domain_dir,
    domain_getattr,
    execute_function,
)
from openbb_quantitative._dqlib_api import install_public_functions
from openbb_quantitative.common.native import checked_response
from openbb_quantitative.dqlib_domain import create_domain_router
from openbb_quantitative.risk.models import (
    ExpectedShortfallResult,
    TailRiskRequest,
    ValueAtRiskResult,
)

_DOMAIN = "mktrisk"
router = create_domain_router(_DOMAIN, "risk")

_FUNCTION_NAMES = (
    "calculate_expected_short_fall",
    "calculate_inst_cleansed_returns",
    "calculate_inst_raw_returns",
    "calculate_profit_loss_distribution",
    "calculate_risk_factor_change",
    "calculate_tier_n_initial_margin",
    "calculate_tier_n_initial_margin_rate",
    "calculate_tier_p_initial_margin",
    "calculate_total_initial_margin",
    "calculate_value_at_risk",
    "create_portfolio",
    "create_trading_position",
    "generate_hist_sim_scenarios",
    "generate_stressed_scenarios",
    "get_im_backtesting_result",
    "identify_stress_dates",
    "run_data_cleansing_engine",
    "run_initial_margin_backtesting_engine",
    "run_initial_margin_engine",
    "simulate_risk_factor",
)

PUBLIC_FUNCTIONS = install_public_functions(globals(), _DOMAIN, _FUNCTION_NAMES)


def _profit_loss_vector(samples: list[float]) -> Any:
    """Build the native dqlib vector required by tail-risk analytics."""
    return execute_function(_DOMAIN, "dqCreateProtoVector", [samples])


def _tail_risk_runtime() -> tuple[Any, Any]:
    """Load protobuf types and the working dqlib request function lazily."""
    try:
        from dqlib.processrequest import process_request  # noqa: PLC0415
        from dqlibproto import dqproto  # noqa: PLC0415
    except Exception as exc:  # pylint: disable=broad-exception-caught
        raise DQLibExecutionError("The dqlib 3.0.2 tail-risk runtime is unavailable.") from exc
    return dqproto, process_request


def _tail_risk_request(
    profit_loss_samples: Any,
    probability: float,
    antithetic: bool,
    *,
    request_name: str,
    input_class_name: str,
    output_class_name: str,
    mirrored_field: str,
) -> Any:
    """Run a native request around broken dqlib 3.0.2 risk wrappers."""
    try:
        dqproto, process_request = _tail_risk_runtime()
        vector = (
            _profit_loss_vector(profit_loss_samples)
            if isinstance(profit_loss_samples, list)
            else profit_loss_samples
        )
        pb_input = getattr(dqproto, input_class_name)()
        pb_input.profit_loss_samples.CopyFrom(vector)
        pb_input.probability = probability
        setattr(pb_input, mirrored_field, antithetic)

        response = process_request(request_name, pb_input.SerializeToString())
        if response is None:
            raise DQLibExecutionError(f"dqlib returned no response for {request_name}.")

        pb_output = getattr(dqproto, output_class_name)()
        pb_output.ParseFromString(response)
        if hasattr(pb_output, "success") and not pb_output.success:
            raise DQLibExecutionError(f"dqlib reported failure for {request_name}.")
        return pb_output
    except DQLibExecutionError:
        raise
    except Exception as exc:  # pylint: disable=broad-exception-caught
        raise DQLibExecutionError(f"dqlib operation failed: {_DOMAIN}.{request_name}") from exc


@router.command(
    methods=["POST"],
    operation_id="dqlib_risk_value_at_risk",
)
def value_at_risk(request: TailRiskRequest) -> OBBject[ValueAtRiskResult]:
    """Calculate dqlib value at risk from profit-and-loss samples."""
    response = checked_response(
        _tail_risk_request(
            request.profit_loss_samples,
            request.probability,
            request.antithetic,
            request_name="CALCULATE_VALUE_AT_RISK",
            input_class_name="CalculateValueAtRiskInput",
            output_class_name="CalculateValueAtRiskOutput",
            mirrored_field="calc_var_mirrored",
        ),
        "value-at-risk calculation",
    )
    mirrored = float(response.value_at_risk_mirrored) if request.antithetic else None
    return OBBject(
        results=ValueAtRiskResult(
            probability=request.probability,
            value_at_risk=float(response.value_at_risk),
            value_at_risk_mirrored=mirrored,
        )
    )


@router.command(
    methods=["POST"],
    operation_id="dqlib_risk_expected_shortfall",
)
def expected_shortfall(
    request: TailRiskRequest,
) -> OBBject[ExpectedShortfallResult]:
    """Calculate dqlib expected shortfall from profit-and-loss samples."""
    response = checked_response(
        _tail_risk_request(
            request.profit_loss_samples,
            request.probability,
            request.antithetic,
            request_name="CALCULATE_EXPECTED_SHORT_FALL",
            input_class_name="CalculateExpectedShortfallInput",
            output_class_name="CalculateExpectedShortfallOutput",
            mirrored_field="calc_es_mirrored",
        ),
        "expected-shortfall calculation",
    )
    mirrored = float(response.expected_shortfall_mirrored) if request.antithetic else None
    return OBBject(
        results=ExpectedShortfallResult(
            probability=request.probability,
            expected_shortfall=float(response.expected_shortfall),
            expected_shortfall_mirrored=mirrored,
        )
    )


def __getattr__(name: str) -> Any:
    """Resolve re-exported public attributes from dqlib.mktrisk."""
    return domain_getattr(_DOMAIN, name)


def __dir__() -> list[str]:
    """Return local and dqlib public market-risk names for discovery."""
    return sorted(set(globals()) | set(domain_dir(_DOMAIN)))


__all__ = [  # noqa: PLE0604
    *PUBLIC_FUNCTIONS,
    "PUBLIC_FUNCTIONS",
    "expected_shortfall",
    "router",
    "value_at_risk",
]

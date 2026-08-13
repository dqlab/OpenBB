"""Lazy bridge to dqlib market risk analytics."""

from typing import Any

from openbb_core.app.model.obbject import OBBject

from openbb_quantitative._dqlib import (
    DQLibExecutionError,
    domain_dir,
    domain_getattr,
    execute_function,
    to_jsonable,
)
from openbb_quantitative.dqlib_domain import create_domain_router
from openbb_quantitative.models import DQLibScalarResult

_DOMAIN = "mktrisk"
router = create_domain_router(_DOMAIN)


def _result(function: str, args: list[Any]) -> OBBject[DQLibScalarResult]:
    """Execute one market-risk function and serialize its result."""
    result = execute_function(_DOMAIN, function, args)
    return OBBject(results=DQLibScalarResult(value=to_jsonable(result)))


def _profit_loss_vector(samples: list[float]) -> Any:
    """Build the native dqlib vector required by tail-risk analytics."""
    return execute_function(_DOMAIN, "dqCreateProtoVector", [samples])


def _tail_risk_runtime() -> tuple[Any, Any]:
    """Load protobuf types and the working dqlib request function lazily."""
    try:
        from dqlib.processrequest import process_request  # noqa: PLC0415
        from dqlibproto import dqproto  # noqa: PLC0415
    except Exception as exc:  # pylint: disable=broad-exception-caught
        raise DQLibExecutionError(
            "The dqlib 3.0.2 tail-risk runtime is unavailable."
        ) from exc
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
            raise DQLibExecutionError(
                f"dqlib returned no response for {request_name}."
            )

        pb_output = getattr(dqproto, output_class_name)()
        pb_output.ParseFromString(response)
        if hasattr(pb_output, "success") and not pb_output.success:
            message = getattr(pb_output, "err_msg", "")
            raise DQLibExecutionError(
                message or f"dqlib reported failure for {request_name}."
            )
        return pb_output
    except DQLibExecutionError:
        raise
    except Exception as exc:  # pylint: disable=broad-exception-caught
        raise DQLibExecutionError(
            f"dqlib operation failed: {_DOMAIN}.{request_name}"
        ) from exc


@router.command(
    methods=["POST"],
    operation_id="dqlib_mktrisk_risk_factor_change",
)
def risk_factor_change(
    values: list[float],
    change_type: str = "RELATIVE",
) -> OBBject[DQLibScalarResult]:
    """Calculate historical dqlib risk-factor changes."""
    return _result("calculate_risk_factor_change", [values, change_type])


@router.command(
    methods=["POST"],
    operation_id="dqlib_mktrisk_simulate_risk_factor",
)
def simulate_risk_factor(
    changes: list[float],
    base: float,
    change_type: str = "RELATIVE",
) -> OBBject[DQLibScalarResult]:
    """Simulate dqlib risk-factor levels from historical changes."""
    return _result("simulate_risk_factor", [changes, change_type, base])


@router.command(
    methods=["POST"],
    operation_id="dqlib_mktrisk_value_at_risk",
)
def value_at_risk(
    profit_loss_samples: list[float],
    probability: float = 0.99,
    antithetic: bool = False,
) -> OBBject[DQLibScalarResult]:
    """Calculate dqlib value at risk from profit-and-loss samples."""
    result = _tail_risk_request(
        profit_loss_samples,
        probability,
        antithetic,
        request_name="CALCULATE_VALUE_AT_RISK",
        input_class_name="CalculateValueAtRiskInput",
        output_class_name="CalculateValueAtRiskOutput",
        mirrored_field="calc_var_mirrored",
    )
    return OBBject(results=DQLibScalarResult(value=to_jsonable(result)))


@router.command(
    methods=["POST"],
    operation_id="dqlib_mktrisk_expected_shortfall",
)
def expected_shortfall(
    profit_loss_samples: list[float],
    probability: float = 0.99,
    antithetic: bool = False,
) -> OBBject[DQLibScalarResult]:
    """Calculate dqlib expected shortfall from profit-and-loss samples."""
    result = _tail_risk_request(
        profit_loss_samples,
        probability,
        antithetic,
        request_name="CALCULATE_EXPECTED_SHORT_FALL",
        input_class_name="CalculateExpectedShortfallInput",
        output_class_name="CalculateExpectedShortfallOutput",
        mirrored_field="calc_es_mirrored",
    )
    return OBBject(results=DQLibScalarResult(value=to_jsonable(result)))


def __getattr__(name: str) -> Any:
    """Resolve a public attribute from dqlib.mktrisk."""
    return domain_getattr(_DOMAIN, name)


def __dir__() -> list[str]:
    """Return local and dqlib domain attributes for interactive discovery."""
    return sorted(set(globals()) | set(domain_dir(_DOMAIN)))

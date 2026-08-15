"""Lazy bridge to dqlib market risk analytics."""

from typing import Any

from openbb_core.app.model.obbject import OBBject

from openbb_quantitative._dqlib import (
    DQLibExecutionError,
    domain_dir,
    domain_getattr,
    execute_function,
)
from openbb_quantitative._dqlib_typed import checked_response
from openbb_quantitative.dqlib_domain import create_domain_router
from openbb_quantitative.dqlib_models import (
    ExpectedShortfallResult,
    TailRiskRequest,
    ValueAtRiskResult,
)

_DOMAIN = "mktrisk"
router = create_domain_router(_DOMAIN)


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
            raise DQLibExecutionError(
                f"dqlib reported failure for {request_name}."
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
    operation_id="dqlib_mktrisk_value_at_risk",
)
def value_at_risk(
    request: TailRiskRequest,
) -> OBBject[ValueAtRiskResult]:
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
    mirrored = (
        float(response.value_at_risk_mirrored) if request.antithetic else None
    )
    return OBBject(
        results=ValueAtRiskResult(
            probability=request.probability,
            value_at_risk=float(response.value_at_risk),
            value_at_risk_mirrored=mirrored,
        )
    )


@router.command(
    methods=["POST"],
    operation_id="dqlib_mktrisk_expected_shortfall",
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
    mirrored = (
        float(response.expected_shortfall_mirrored)
        if request.antithetic
        else None
    )
    return OBBject(
        results=ExpectedShortfallResult(
            probability=request.probability,
            expected_shortfall=float(response.expected_shortfall),
            expected_shortfall_mirrored=mirrored,
        )
    )


def __getattr__(name: str) -> Any:
    """Resolve a public attribute from dqlib.mktrisk."""
    return domain_getattr(_DOMAIN, name)


def __dir__() -> list[str]:
    """Return local and dqlib domain attributes for interactive discovery."""
    return sorted(set(globals()) | set(domain_dir(_DOMAIN)))

"""Shared conversion helpers for typed native dqlib operations."""

from collections.abc import Sequence
from datetime import date, datetime
from typing import Any

from openbb_quantitative._dqlib import DQLibExecutionError, execute_function


def to_datetime(value: date) -> datetime:
    """Convert an OpenBB date to the datetime required by dqlib 3.0.2."""
    return datetime.combine(value, datetime.min.time())


def vector_values(value: Any, expected: int, operation: str) -> list[float]:
    """Translate a native dqlib vector and validate its result cardinality."""
    data = getattr(value, "data", value)
    if isinstance(data, Sequence) and not isinstance(data, (str, bytes)):
        result = [float(item) for item in data]
    else:
        try:
            result = [float(item) for item in data]
        except (TypeError, ValueError) as exc:
            raise DQLibExecutionError(
                f"dqlib returned an invalid vector for {operation}."
            ) from exc
    if len(result) != expected:
        raise DQLibExecutionError(
            f"dqlib returned {len(result)} values for {operation}; "
            f"expected {expected}."
        )
    return result


def checked_response(response: Any, operation: str) -> Any:
    """Validate the common success contract used by dqlib protobuf outputs."""
    if response is None:
        raise DQLibExecutionError(f"dqlib returned no response for {operation}.")
    if hasattr(response, "success") and not response.success:
        raise DQLibExecutionError(f"dqlib reported failure for {operation}.")
    return response


def pricing_values(response: Any, operation: str) -> tuple[float, float | None, str]:
    """Translate common native pricing results to stable OpenBB scalar fields."""
    output = checked_response(response, operation)
    results = getattr(output, "results", None)
    if results is None or not hasattr(results, "present_value"):
        raise DQLibExecutionError(
            f"dqlib returned invalid pricing results for {operation}."
        )
    listed_fields = {
        descriptor.name for descriptor, _value in results.ListFields()
    }
    cash_value = (
        float(results.cash_value) if "cash_value" in listed_fields else None
    )
    return float(results.present_value), cash_value, str(results.currency)


def build_option_settings(domain: str, currency: str) -> tuple[Any, Any, Any]:
    """Build native model-free pricing, risk, and no-scenario settings."""
    risk_functions = {
        "cmanalytics": "create_cm_risk_settings",
        "eqanalytics": "create_eq_risk_settings",
    }
    if domain not in risk_functions:
        raise DQLibExecutionError(
            f"Typed option settings are unavailable for dqlib.{domain}."
        )
    pricing = execute_function(
        "analytics",
        "create_pricing_settings",
        [],
        {
            "pricing_currency": currency,
            "inc_current": False,
            "pricing_method": "ANALYTICAL",
        },
    )
    risk = execute_function(
        domain,
        risk_functions[domain],
        [
            execute_function("analytics", "create_ir_curve_risk_settings"),
            execute_function("analytics", "create_price_risk_settings"),
            execute_function("analytics", "create_vol_risk_settings"),
            execute_function("analytics", "create_price_vol_risk_settings"),
            execute_function("analytics", "create_dividend_curve_risk_settings"),
            execute_function("analytics", "create_theta_risk_settings"),
        ],
    )
    scenario = execute_function(
        "analytics",
        "create_scn_analysis_settings",
        ["NO_SCN_ANALYSIS", 0.0, 0.0, 0, 0, 0.0, 0.0, 0],
    )
    return pricing, risk, scenario

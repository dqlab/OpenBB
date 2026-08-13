"""Optional bridge to the proprietary dqlib Python plugin."""

from __future__ import annotations

import base64
import inspect
import platform
import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import fields, is_dataclass
from datetime import date, datetime
from enum import Enum
from importlib import import_module, metadata
from types import ModuleType
from typing import Any

DQLIB_RELEASE_VERSION = "3.0.2"
DQLIB_RELEASE_TAG = "dqlib-plugin-python-3.0.2"
DQLIB_RELEASE_URL = (
    "https://github.com/dqlab/dqlibpy/releases/tag/"
    f"{DQLIB_RELEASE_TAG}"
)
DQLIB_WHEEL_NAME = "dqlib-3.0.2-cp312-cp312-linux_x86_64.whl"
DQLIB_DOMAINS = frozenset(
    {
        "analytics",
        "cmanalytics",
        "cmmarket",
        "cranalytics",
        "crmarket",
        "datetime",
        "eqanalytics",
        "fianalytics",
        "fimarket",
        "fxanalytics",
        "fxmarket",
        "iranalytics",
        "irmarket",
        "market",
        "mktrisk",
        "numerics",
        "staticdata",
        "utility",
    }
)

_SAFE_FUNCTION_PREFIXES = (
    "build",
    "calculate",
    "create",
    "date_generator",
    "generate",
    "get",
    "identify",
    "implied_vol",
    "dqcreateproto",
    "run",
    "schedule",
    "simulate",
    "to_",
    "year_frac",
)
_SAFE_FUNCTION_SUFFIXES = ("_builder", "_calculator", "_pricer")
_DENIED_FUNCTION_PREFIXES = ("load_",)
_DENIED_FUNCTION_PARTS = ("demo", "_file", "process_request")


class DQLibUnavailableError(ImportError):
    """Raised when the optional dqlib runtime cannot be loaded."""


class DQLibExecutionError(RuntimeError):
    """Raised when a dqlib operation cannot be executed safely."""


class DQLibSerializationError(TypeError):
    """Raised when a dqlib result cannot be represented in an OpenBB response."""


def _qualified_name(domain: str | None) -> str:
    """Return the dqlib import name for a validated domain."""
    if domain is None:
        return "dqlib"
    if domain not in DQLIB_DOMAINS:
        raise ValueError(f"Unknown dqlib domain: {domain}")
    return f"dqlib.{domain}"


def load_module(domain: str | None = None) -> ModuleType:
    """Load dqlib or one of its domain modules lazily."""
    module_name = _qualified_name(domain)
    try:
        return import_module(module_name)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        raise DQLibUnavailableError(
            "dqlib is unavailable. Install the authenticated dqlib 3.0.2 "
            "release wheel for CPython 3.12 on Linux x86_64 and configure "
            f"its runtime license. See {DQLIB_RELEASE_URL}."
        ) from exc


def is_available() -> bool:
    """Return whether the dqlib package and native runtime can be imported."""
    try:
        load_module()
    except DQLibUnavailableError:
        return False
    return True


def installed_version() -> str | None:
    """Return the installed dqlib distribution version, when available."""
    try:
        return metadata.version("dqlib")
    except metadata.PackageNotFoundError:
        return None


def is_supported_runtime() -> bool:
    """Return whether this interpreter matches the released wheel tags."""
    machine = platform.machine().lower()
    return (
        sys.version_info[:2] == (3, 12)
        and sys.platform.startswith("linux")
        and machine in {"amd64", "x86_64"}
    )


def get_status() -> dict[str, bool | str | None]:
    """Return sanitized dqlib integration status for OpenBB."""
    available = is_available()
    version = installed_version()
    supported = is_supported_runtime()

    if available and version == DQLIB_RELEASE_VERSION:
        message = "dqlib is ready."
    elif available:
        message = (
            "dqlib is loaded, but its installed version differs from the "
            f"validated {DQLIB_RELEASE_VERSION} release."
        )
    elif not supported:
        message = (
            "The published dqlib wheel requires CPython 3.12 on Linux x86_64."
        )
    else:
        message = (
            "Install the authenticated dqlib release wheel and configure its "
            "runtime license."
        )

    return {
        "available": available,
        "supported_runtime": supported,
        "release_version": DQLIB_RELEASE_VERSION,
        "installed_version": version,
        "release_url": DQLIB_RELEASE_URL,
        "message": message,
    }


def _is_safe_function(name: str, value: Any) -> bool:
    """Return whether a public callable is allowed through the OpenBB gateway."""
    lowered = name.lower()
    return (
        name.isidentifier()
        and not name.startswith("_")
        and callable(value)
        and not lowered.startswith(_DENIED_FUNCTION_PREFIXES)
        and not any(part in lowered for part in _DENIED_FUNCTION_PARTS)
        and (
            lowered.startswith(_SAFE_FUNCTION_PREFIXES)
            or lowered.endswith(_SAFE_FUNCTION_SUFFIXES)
        )
    )


def get_function(domain: str, name: str) -> Callable[..., Any]:
    """Return an allowlisted callable from a dqlib analytics domain."""
    module = load_module(domain)
    value = getattr(module, name, None)
    if not _is_safe_function(name, value):
        raise DQLibExecutionError(
            f"dqlib function is unavailable through OpenBB: {domain}.{name}"
        )
    return value


def list_functions(domain: str) -> list[dict[str, str]]:
    """Describe analytics functions exposed for a dqlib domain."""
    module = load_module(domain)
    result: list[dict[str, str]] = []
    for name in sorted(dir(module)):
        value = getattr(module, name)
        if not _is_safe_function(name, value):
            continue
        try:
            signature = str(inspect.signature(value))
        except (TypeError, ValueError):
            signature = "(...)"
        doc = inspect.getdoc(value) or ""
        result.append(
            {
                "name": name,
                "signature": signature,
                "description": doc.splitlines()[0] if doc else "",
            }
        )
    return result


def _resolve_reference(reference: str, values: Mapping[str, Any]) -> Any:
    """Resolve a pipeline step reference with optional dotted traversal."""
    parts = reference.split(".")
    if not parts[0] or parts[0] not in values:
        raise DQLibExecutionError(f"Unknown dqlib pipeline reference: {reference}")

    value = values[parts[0]]
    for part in parts[1:]:
        if isinstance(value, Mapping):
            if part not in value:
                raise DQLibExecutionError(
                    f"Unknown dqlib pipeline reference: {reference}"
                )
            value = value[part]
        elif isinstance(value, Sequence) and not isinstance(
            value, (str, bytes, bytearray)
        ):
            try:
                value = value[int(part)]
            except (IndexError, TypeError, ValueError) as exc:
                raise DQLibExecutionError(
                    f"Unknown dqlib pipeline reference: {reference}"
                ) from exc
        elif hasattr(value, part) and not part.startswith("_"):
            value = getattr(value, part)
        else:
            raise DQLibExecutionError(
                f"Unknown dqlib pipeline reference: {reference}"
            )
    return value


def resolve_value(value: Any, pipeline_values: Mapping[str, Any]) -> Any:
    """Resolve references and typed JSON tokens in a dqlib argument value."""
    if isinstance(value, Mapping):
        if set(value) == {"$ref"}:
            return _resolve_reference(str(value["$ref"]), pipeline_values)
        if set(value) == {"$date"}:
            return date.fromisoformat(str(value["$date"]))
        if set(value) == {"$datetime"}:
            return datetime.fromisoformat(str(value["$datetime"]))
        if set(value) == {"$bytes"}:
            try:
                return base64.b64decode(str(value["$bytes"]), validate=True)
            except ValueError as exc:
                raise DQLibExecutionError("Invalid base64 dqlib argument.") from exc
        return {
            str(key): resolve_value(item, pipeline_values)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [resolve_value(item, pipeline_values) for item in value]
    if isinstance(value, tuple):
        return tuple(resolve_value(item, pipeline_values) for item in value)
    return value


def _execute_mktrisk_compatibility(
    function: str,
    args: Sequence[Any] | None,
    kwargs: Mapping[str, Any] | None,
) -> Any:
    """Execute dqlib 3.0.2 tail-risk functions with their working primitives."""
    from openbb_quantitative.mktrisk import _tail_risk_request  # noqa: PLC0415

    names = ("profit_loss_samples", "probability", "antithetic")
    parameters: dict[str, Any] = {
        "probability": 0.99,
        "antithetic": False,
    }
    values = list(args or ())
    if len(values) > len(names):
        raise DQLibExecutionError(
            f"Too many arguments for dqlib.mktrisk.{function}."
        )
    parameters.update(zip(names, values))
    parameters.update(dict(kwargs or {}))
    unknown = set(parameters) - set(names)
    if unknown or "profit_loss_samples" not in parameters:
        raise DQLibExecutionError(
            f"Invalid arguments for dqlib.mktrisk.{function}."
        )

    metadata = {
        "calculate_value_at_risk": (
            "CALCULATE_VALUE_AT_RISK",
            "CalculateValueAtRiskInput",
            "CalculateValueAtRiskOutput",
            "calc_var_mirrored",
        ),
        "calculate_expected_short_fall": (
            "CALCULATE_EXPECTED_SHORT_FALL",
            "CalculateExpectedShortfallInput",
            "CalculateExpectedShortfallOutput",
            "calc_es_mirrored",
        ),
    }
    request_name, input_name, output_name, mirrored_field = metadata[function]
    return _tail_risk_request(
        parameters["profit_loss_samples"],
        float(parameters["probability"]),
        bool(parameters["antithetic"]),
        request_name=request_name,
        input_class_name=input_name,
        output_class_name=output_name,
        mirrored_field=mirrored_field,
    )



def execute_function(
    domain: str,
    function: str,
    args: Sequence[Any] | None = None,
    kwargs: Mapping[str, Any] | None = None,
) -> Any:
    """Execute one allowlisted dqlib function."""
    if domain == "mktrisk" and function in {
        "calculate_value_at_risk",
        "calculate_expected_short_fall",
    }:
        return _execute_mktrisk_compatibility(function, args, kwargs)
    callable_ = get_function(domain, function)
    try:
        return callable_(*(args or ()), **dict(kwargs or {}))
    except Exception as exc:  # pylint: disable=broad-exception-caught
        raise DQLibExecutionError(
            f"dqlib operation failed: {domain}.{function}"
        ) from exc


def _protobuf_to_dict(value: Any) -> dict[str, Any] | None:
    """Convert a protobuf message without importing protobuf eagerly."""
    if not hasattr(value, "DESCRIPTOR") or not hasattr(value, "ListFields"):
        return None
    try:
        from google.protobuf.json_format import MessageToDict  # noqa: PLC0415

        return MessageToDict(value, preserving_proto_field_name=True)
    except (ImportError, TypeError, ValueError):
        return None


def to_jsonable(value: Any, *, _seen: set[int] | None = None) -> Any:  # noqa: PLR0911
    """Convert dqlib, protobuf, NumPy, and Python values to JSON-safe data."""
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, Enum):
        return to_jsonable(value.value, _seen=_seen)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, (bytes, bytearray, memoryview)):
        return {
            "encoding": "base64",
            "data": base64.b64encode(bytes(value)).decode("ascii"),
        }

    seen = _seen if _seen is not None else set()
    value_id = id(value)
    if value_id in seen:
        raise DQLibSerializationError("Cyclic dqlib result cannot be serialized.")
    seen.add(value_id)
    try:
        protobuf_value = _protobuf_to_dict(value)
        if protobuf_value is not None:
            return to_jsonable(protobuf_value, _seen=seen)
        if is_dataclass(value) and not isinstance(value, type):
            return {
                field.name: to_jsonable(getattr(value, field.name), _seen=seen)
                for field in fields(value)
            }
        if hasattr(value, "model_dump") and callable(value.model_dump):
            return to_jsonable(value.model_dump(mode="json"), _seen=seen)
        if hasattr(value, "_asdict") and callable(value._asdict):
            return to_jsonable(value._asdict(), _seen=seen)
        if isinstance(value, Mapping):
            return {
                str(key): to_jsonable(item, _seen=seen)
                for key, item in value.items()
            }
        if isinstance(value, Sequence) and not isinstance(
            value, (str, bytes, bytearray)
        ):
            return [to_jsonable(item, _seen=seen) for item in value]
        if hasattr(value, "tolist") and callable(value.tolist):
            return to_jsonable(value.tolist(), _seen=seen)
        if hasattr(value, "item") and callable(value.item):
            return to_jsonable(value.item(), _seen=seen)
        if hasattr(value, "__dict__"):
            public = {
                key: item
                for key, item in vars(value).items()
                if not key.startswith("_")
            }
            if public:
                return to_jsonable(public, _seen=seen)
    finally:
        seen.discard(value_id)

    raise DQLibSerializationError(
        f"Unsupported dqlib result type: {type(value).__name__}"
    )


def execute_pipeline(
    steps: Sequence[Mapping[str, Any]], outputs: Sequence[str] | None = None
) -> dict[str, Any]:
    """Execute dqlib operations while retaining native intermediate objects."""
    values: dict[str, Any] = {}
    for step in steps:
        step_id = str(step.get("id", ""))
        if not step_id or not step_id.isidentifier():
            raise DQLibExecutionError(
                "Each dqlib pipeline step requires an identifier-safe id."
            )
        if step_id in values:
            raise DQLibExecutionError(f"Duplicate dqlib pipeline id: {step_id}")

        domain = str(step.get("domain", ""))
        function = str(step.get("function", ""))
        args = resolve_value(step.get("args", []), values)
        kwargs = resolve_value(step.get("kwargs", {}), values)
        if not isinstance(args, Sequence) or isinstance(args, (str, bytes)):
            raise DQLibExecutionError(f"Pipeline args must be a list: {step_id}")
        if not isinstance(kwargs, Mapping):
            raise DQLibExecutionError(f"Pipeline kwargs must be an object: {step_id}")
        values[step_id] = execute_function(domain, function, args, kwargs)

    selected = (
        list(outputs)
        if outputs is not None
        else [next(reversed(values))]
        if values
        else []
    )
    return {reference: _resolve_reference(reference, values) for reference in selected}


def domain_getattr(domain: str, name: str) -> Any:
    """Resolve an attribute from a lazy dqlib domain bridge."""
    if name.startswith("__"):
        raise AttributeError(name)
    return getattr(load_module(domain), name)


def domain_dir(domain: str) -> list[str]:
    """Return public names for a dqlib domain without breaking optional installs."""
    try:
        module = load_module(domain)
    except DQLibUnavailableError:
        return []
    return [name for name in dir(module) if not name.startswith("_")]

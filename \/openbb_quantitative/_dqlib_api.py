"""Explicit bindings for the installed dqlib 3.0.2 public Python API."""

from collections.abc import Callable, Iterable, MutableMapping
from typing import Any

from openbb_quantitative._dqlib import (
    DQLibExecutionError,
    DQLibUnavailableError,
    load_module,
)


def native_function(domain: str, name: str) -> Callable[..., Any]:
    """Create a lazy binding to one explicitly declared public dqlib function."""

    def function(*args: Any, **kwargs: Any) -> Any:
        module = load_module(domain)
        callable_ = getattr(module, name, None)
        if not callable(callable_):
            raise DQLibExecutionError(f"Installed dqlib does not expose {domain}.{name}.")
        try:
            return callable_(*args, **kwargs)
        except DQLibUnavailableError:
            raise
        except Exception as exc:  # pylint: disable=broad-exception-caught
            raise DQLibExecutionError(f"dqlib operation failed: {domain}.{name}") from exc

    function.__name__ = name
    function.__qualname__ = name
    function.__doc__ = f"Call the public dqlib 3.0.2 function ``{domain}.{name}`` lazily."
    return function


def install_public_functions(
    namespace: MutableMapping[str, Any],
    domain: str,
    names: Iterable[str],
) -> tuple[str, ...]:
    """Install an audited set of lazy public functions into a module namespace."""
    installed: list[str] = []
    for name in names:
        if not name.isidentifier() or name.startswith("_"):
            raise ValueError(f"Invalid public dqlib function name: {name}")
        if name in namespace:
            raise ValueError(f"Duplicate dqlib function binding: {domain}.{name}")
        function = native_function(domain, name)
        function.__module__ = str(namespace.get("__name__", __name__))
        namespace[name] = function
        installed.append(name)
    return tuple(installed)

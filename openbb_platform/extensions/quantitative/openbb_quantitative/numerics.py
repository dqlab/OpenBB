"""Lazy bridge to dqlib numerical utilities."""

from typing import Any

from openbb_quantitative._dqlib import domain_dir, domain_getattr

_DOMAIN = "numerics"


def __getattr__(name: str) -> Any:
    """Resolve a public attribute from dqlib.numerics."""
    return domain_getattr(_DOMAIN, name)


def __dir__() -> list[str]:
    """Return local and dqlib domain attributes for interactive discovery."""
    return sorted(set(globals()) | set(domain_dir(_DOMAIN)))

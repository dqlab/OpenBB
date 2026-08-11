"""Lazy bridge to dqlib equity analytics."""

from typing import Any

from openbb_quantitative._dqlib import domain_dir, domain_getattr

_DOMAIN = "eqanalytics"


def __getattr__(name: str) -> Any:
    """Resolve a public attribute from dqlib.eqanalytics."""
    return domain_getattr(_DOMAIN, name)


def __dir__() -> list[str]:
    """Return local and dqlib domain attributes for interactive discovery."""
    return sorted(set(globals()) | set(domain_dir(_DOMAIN)))

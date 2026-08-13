"""Lazy bridge to dqlib commodity analytics."""

from typing import Any

from openbb_quantitative._dqlib import domain_dir, domain_getattr
from openbb_quantitative.dqlib_domain import create_domain_router

_DOMAIN = "cmanalytics"
router = create_domain_router(_DOMAIN)


def __getattr__(name: str) -> Any:
    """Resolve a public attribute from dqlib.cmanalytics."""
    return domain_getattr(_DOMAIN, name)


def __dir__() -> list[str]:
    """Return local and dqlib domain attributes for interactive discovery."""
    return sorted(set(globals()) | set(domain_dir(_DOMAIN)))

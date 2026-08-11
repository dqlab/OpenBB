"""Optional bridge to the proprietary dqlib Python plugin."""

from __future__ import annotations

import platform
import sys
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


class DQLibUnavailableError(ImportError):
    """Raised when the optional dqlib runtime cannot be loaded."""


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


def domain_getattr(domain: str, name: str) -> Any:
    """Resolve an attribute from a lazy dqlib domain bridge."""
    return getattr(load_module(domain), name)


def domain_dir(domain: str) -> list[str]:
    """Return public names for a dqlib domain without breaking optional installs."""
    try:
        module = load_module(domain)
    except DQLibUnavailableError:
        return []
    return [name for name in dir(module) if not name.startswith("_")]

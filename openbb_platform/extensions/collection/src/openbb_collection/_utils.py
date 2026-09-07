"""Shared error handling for collector commands."""

from collections.abc import Iterator
from contextlib import contextmanager

from openbb_core.app.model.abstract.error import OpenBBError


@contextmanager
def _safe_errors() -> Iterator[None]:
    # Configuration and vendor exceptions may contain credentials or private paths.
    try:
        yield
    except Exception:
        raise OpenBBError("invalid_configuration_or_runtime") from None

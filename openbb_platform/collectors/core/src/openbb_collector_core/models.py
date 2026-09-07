"""Shared envelopes and engine registration, independent of any provider."""

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any


class ProviderFailure(RuntimeError):
    """Safe diagnostic code; vendor exception text must not enter reports."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


@dataclass
class Response:
    rows: list[dict[str, Any]]
    warning_count: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class CollectorExtension:
    """An installed engine's configuration loader and runtime factory."""

    name: str
    load_config: Callable[..., Any]
    factory: Callable[..., Any]

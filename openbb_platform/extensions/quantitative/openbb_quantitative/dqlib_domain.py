"""Shared OpenBB router factory for executable dqlib domains."""

from typing import Any

from openbb_core.app.model.obbject import OBBject
from openbb_core.app.router import Router

from openbb_quantitative._dqlib import (
    execute_function,
    resolve_value,
    to_jsonable,
)
from openbb_quantitative.models import DQLibCallResult


def call_dqlib(
    domain: str,
    function: str,
    args: list[Any] | None = None,
    kwargs: dict[str, Any] | None = None,
) -> DQLibCallResult:
    """Execute and serialize one dqlib function for an OpenBB command."""
    resolved_args = resolve_value(args or [], {})
    resolved_kwargs = resolve_value(kwargs or {}, {})
    result = execute_function(domain, function, resolved_args, resolved_kwargs)
    return DQLibCallResult(
        domain=domain,
        function=function,
        result=to_jsonable(result),
    )


def create_domain_router(domain: str) -> Router:
    """Create the /call command for one validated dqlib domain."""
    router = Router(
        prefix=f"/{domain}",
        description=f"Executable dqlib {domain} analytics.",
    )

    @router.command(
        methods=["POST"],
        operation_id=f"dqlib_{domain}_call",
    )
    def call(
        function: str,
        args: list[Any] | None = None,
        kwargs: dict[str, Any] | None = None,
    ) -> OBBject[DQLibCallResult]:
        """Execute an allowlisted dqlib function in this analytics domain."""
        return OBBject(results=call_dqlib(domain, function, args, kwargs))

    return router

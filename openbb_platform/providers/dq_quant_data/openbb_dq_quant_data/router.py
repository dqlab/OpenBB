"""OpenBB router for canonical API governance queries."""

from collections.abc import Callable, Coroutine
from typing import Any
from urllib.parse import quote

from fastapi import Request, Response
from fastapi.responses import JSONResponse
from fastapi.routing import APIRoute
from openbb_core.app.model.command_context import CommandContext
from openbb_core.app.model.obbject import OBBject
from openbb_core.app.router import Router

from openbb_dq_quant_data.client import DQAPIClient
from openbb_dq_quant_data.native import NativeAPIClient, NativeQueryError

router = Router(description="DQ canonical fundamentals, quality and lineage.")


class _NativeQueryAPIRoute(APIRoute):
    """Preserve public API rejection status outside the OpenBB command wrapper."""

    def get_route_handler(self) -> Callable[[Request], Coroutine[Any, Any, Response]]:
        handler = super().get_route_handler()

        async def handle(request: Request) -> Response:
            try:
                return await handler(request)
            except NativeQueryError as exc:
                headers = {"Retry-After": exc.retry_after} if exc.retry_after else None
                return JSONResponse(
                    status_code=exc.status_code,
                    content={"detail": str(exc)},
                    headers=headers,
                )

        return handle


@router.command(methods=["POST"], route_class_override=_NativeQueryAPIRoute)
def native_query(cc: CommandContext, request: dict[str, Any]) -> OBBject[list[dict]]:
    """Query bounded native v2 records and retain exact values and provenance.

    Decimal and nanosecond strings, nulls, source quality and availability
    metadata remain unchanged. The server validates dataset, fields and clocks.
    """
    result = NativeAPIClient(cc.user_settings.credentials.model_dump()).native_query(
        request
    )
    return OBBject(results=result["data"], extra=result["meta"])


def _read(
    cc: CommandContext, path: str, params: dict | None = None
) -> OBBject[list[dict]]:
    result = DQAPIClient(cc.user_settings.credentials.model_dump()).get_result(
        path, params
    )
    return OBBject(results=result["data"], extra=result["meta"])


@router.command()
def fundamental_facts(
    cc: CommandContext,
    symbol: str,
    start_date: str | None = None,
    end_date: str | None = None,
) -> OBBject[list[dict]]:
    """Read canonical long-form fundamental facts using configured DQ credentials."""
    return _read(
        cc,
        "/api/v1/fundamentals/facts",
        {
            "symbols": symbol,
            "start_date": start_date,
            "end_date": end_date,
        },
    )


@router.command()
def quality(cc: CommandContext, dataset: str | None = None) -> OBBject[list[dict]]:
    """Read canonical publication quality status."""
    return _read(cc, "/api/v1/quality/status", {"dataset": dataset})


@router.command()
def lineage(cc: CommandContext, dataset: str) -> OBBject[list[dict]]:
    """Read dataset lineage and retain API provenance in extra."""
    return _read(cc, f"/api/v1/lineage/{quote(dataset, safe='')}")

"""Opt-in history SDK commands; old DQ commands keep their existing connections."""

from collections.abc import Callable, Coroutine
from typing import Any

from fastapi import Request, Response
from fastapi.responses import JSONResponse
from fastapi.routing import APIRoute
from openbb_core.app.model.command_context import CommandContext
from openbb_core.app.model.obbject import OBBject
from openbb_core.app.router import Router

from openbb_dq_quant_data.history import read_history
from openbb_dq_quant_data.native import NativeQueryError

router = Router(
    description="Explicit frozen native history, with no automatic fallback."
)


class _HistoryAPIRoute(APIRoute):
    """Keep upstream rejection status through the OpenBB REST wrapper."""

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


@router.command(route_class_override=_HistoryAPIRoute)
def describe(cc: CommandContext, release_id: str) -> OBBject[dict]:
    """Read discrete UTC dates, exclusions, source identities and lab quality.

    Date boundaries do not imply continuous coverage. No realtime/chain/as-of
    completeness is claimed. Requires OPENBB_DQ_HISTORY_CONFIG.
    """
    return OBBject(
        results=read_history(
            cc.user_settings.credentials.model_dump(), release_id, "describe"
        )
    )


@router.command(methods=["POST"], route_class_override=_HistoryAPIRoute)
def scopes(cc: CommandContext, release_id: str, request: dict) -> OBBject[dict]:
    """Discover a bounded instrument prefix within one explicit UTC manifest day.

    Request: day, instrument_prefix, after_instrument, limit (1..32).
    Continue with next_after_instrument until the native response is empty.
    """
    return OBBject(
        results=read_history(
            cc.user_settings.credentials.model_dump(), release_id, "scopes", request
        )
    )


@router.command(methods=["POST"], route_class_override=_HistoryAPIRoute)
def coverage(cc: CommandContext, release_id: str, request: dict) -> OBBject[dict]:
    """Inspect one exact instrument/UTC day including exclusions and unknown clocks.

    Request: instrument, day, fields, optional limit. An excluded scope cannot be
    paged. Coverage outside the frozen membership is an error, never missing data.
    """
    return OBBject(
        results=read_history(
            cc.user_settings.credentials.model_dump(), release_id, "coverage", request
        )
    )


@router.command(methods=["POST"], route_class_override=_HistoryAPIRoute)
def pages(cc: CommandContext, release_id: str, request: dict) -> OBBject[dict]:
    """Read one authenticated frozen page, retaining the complete native envelope.

    Request: instrument, day, fields, limit (1..2048), optional cursor. Keep the
    release, instrument, UTC day and field order unchanged for subsequent cursors.
    Stop at meta.next_cursor=null. Decimal/ns strings, nulls, source and quality
    remain exact. 422/excluded scopes require calling the old interface separately.
    There is no automatic pagination, fallback, retry, as-of or latest projection.
    """
    return OBBject(
        results=read_history(
            cc.user_settings.credentials.model_dump(), release_id, "pages", request
        )
    )

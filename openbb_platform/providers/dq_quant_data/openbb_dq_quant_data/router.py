"""OpenBB router for canonical API governance queries."""

from urllib.parse import quote

from openbb_core.app.model.command_context import CommandContext
from openbb_core.app.model.obbject import OBBject
from openbb_core.app.router import Router

from openbb_dq_quant_data.client import DQAPIClient

router = Router(description="DQ canonical fundamentals, quality and lineage.")


def _read(cc: CommandContext, path: str, params: dict | None = None) -> OBBject[list[dict]]:
    result = DQAPIClient(cc.user_settings.credentials.model_dump()).get_result(path, params)
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

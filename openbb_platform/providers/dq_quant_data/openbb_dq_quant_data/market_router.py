"""OpenBB SDK access to the governed local market store."""

from datetime import datetime
from typing import Literal

from openbb_core.app.model.obbject import OBBject
from openbb_core.app.router import Router

from openbb_dq_quant_data.market import MarketOptionsChainsData, _query, project_chain

router = Router(description="Versioned archived market data, captures and availability.")


@router.command()
def status(dataset: str | None = None, config_path: str | None = None) -> OBBject[dict]:
    """Read collected, archived and published watermarks for saved datasets."""
    return OBBject(results=_query("status", config_path, dataset=dataset))


@router.command()
def instruments(
    query: str | None = None,
    as_of: datetime | str | None = None,
    limit: int = 1000,
    config_path: str | None = None,
) -> OBBject[dict]:
    """Resolve exact provider identities and versioned instrument terms."""
    return OBBject(
        results=_query("instruments", config_path, query=query, as_of=as_of, limit=limit)
    )


@router.command()
def captures(
    start: datetime | str,
    end: datetime | str,
    dataset: str = "spx_option_chains",
    symbol: str = "SPX",
    limit: int = 100,
    publication_id: str | None = None,
    config_path: str | None = None,
) -> OBBject[dict]:
    """List saved option captures in a bounded UTC window."""
    return OBBject(
        results=_query(
            "captures",
            config_path,
            dataset=dataset,
            symbol=symbol,
            start=start,
            end=end,
            limit=limit,
            publication_id=publication_id,
        )
    )


@router.command()
def chain(
    symbol: str = "SPX",
    dataset: str = "spx_option_chains",
    capture_id: str | None = None,
    publication_id: str | None = None,
    as_of: datetime | str | None = None,
    availability: Literal["central", "collector"] = "central",
    layer: Literal["gold", "silver"] = "gold",
    expiration: str | None = None,
    option_type: Literal["call", "put"] | None = None,
    max_rows: int = 100000,
    allow_incomplete: bool = False,
    config_path: str | None = None,
) -> OBBject[MarketOptionsChainsData]:
    """Read one complete option capture, preserving source units and publication metadata."""
    values = locals().copy()
    values.pop("config_path")
    return project_chain(_query("chain", config_path, **values))


@router.command()
def history(
    dataset: str,
    instruments: list[str],
    start: datetime | str,
    end: datetime | str,
    as_of: datetime | str | None = None,
    availability: Literal["central", "collector"] = "central",
    layer: Literal["gold", "silver"] = "gold",
    revisions: Literal["latest", "all"] = "latest",
    publication_id: str | None = None,
    limit: int = 10000,
    cursor: str | None = None,
    frequency: str | None = None,
    what_to_show: str | None = None,
    session_scope: str | None = None,
    adjustment: str | None = None,
    config_path: str | None = None,
) -> OBBject[dict]:
    """Read bounded quotes or bar revisions with a publication-pinned cursor."""
    values = locals().copy()
    values.pop("config_path")
    return OBBject(results=_query("history", config_path, **values))


@router.command()
def basket(
    dataset: str,
    instruments: list[str],
    as_of: datetime | str,
    max_age_seconds: int = 900,
    max_skew_seconds: int = 300,
    availability: Literal["central", "collector"] = "central",
    layer: Literal["gold", "silver"] = "gold",
    publication_id: str | None = None,
    config_path: str | None = None,
) -> OBBject[dict]:
    """Select one quote per contract and report missing members, age and timing spread."""
    values = locals().copy()
    values.pop("config_path")
    return OBBject(results=_query("basket", config_path, **values))

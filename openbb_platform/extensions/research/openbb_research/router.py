"""Public bounded research workflows. No server paths or executable configuration."""

from typing import Literal

from openbb_core.app.model.obbject import OBBject
from openbb_core.app.router import Router
from openbb_core.provider.standard_models.alpha_research import BtRequest, QlibRequest, RunRequest

router = Router(description="Recorded Lab research runs, artifacts and offline HTML reports.")


@router.command(methods=["POST"])
async def run(request: RunRequest) -> OBBject[dict]:
    """Run a bounded snapshot-to-evaluation workflow and record complete or failed status."""
    from openbb_research.workflow import run as execute

    return OBBject(results=await execute(request))


@router.command()
def runs(limit: int = 20) -> OBBject[dict]:
    """List recorded runs with explicit pagination/scan bounds."""
    from openbb_research.workflow import runs as listing

    return OBBject(results=listing(limit))


@router.command()
def result(manifest_id: str) -> OBBject[dict]:
    """Read and verify an immutable manifest by opaque ID."""
    from openbb_research.workflow import result as read

    return OBBject(results=read(manifest_id))


@router.command(methods=["POST"])
def report(manifest_id: str, format: Literal["html"] = "html") -> OBBject[dict]:
    """Create a self-contained HTML report without publishing it."""
    from openbb_research.report import report as render

    return OBBject(results=render(manifest_id))


@router.command(methods=["POST"])
def qlib_train(request: QlibRequest) -> OBBject[dict]:
    """Fit a purged chronological Qlib ridge model using saved factors and labels."""
    try:
        from openbb_qlib import train
    except ImportError as exc:
        raise ValueError(
            "Install the separate openbb-qlib package to use research.qlib_train"
        ) from exc
    return OBBject(results=train(request).model_dump(mode="json"))


@router.command(methods=["POST"])
def bt_backtest(request: BtRequest) -> OBBject[dict]:
    """Run a synthetic next-open, long-only bt portfolio with an explicit cost ledger."""
    try:
        from openbb_bt import backtest
    except ImportError as exc:
        raise ValueError(
            "Install the separate openbb-bt package to use research.bt_backtest"
        ) from exc
    return OBBject(results=backtest(request).model_dump(mode="json"))

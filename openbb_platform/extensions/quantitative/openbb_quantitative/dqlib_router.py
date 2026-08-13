"""OpenBB command tree for the optional proprietary dqlib runtime."""

from typing import Any

from openbb_core.app.model.obbject import OBBject
from openbb_core.app.router import Router

from openbb_quantitative._dqlib import (
    execute_pipeline,
    get_status,
    list_functions,
    to_jsonable,
)
from openbb_quantitative.analytics import router as analytics_router
from openbb_quantitative.cmanalytics import router as cmanalytics_router
from openbb_quantitative.cmmarket import router as cmmarket_router
from openbb_quantitative.cranalytics import router as cranalytics_router
from openbb_quantitative.crmarket import router as crmarket_router
from openbb_quantitative.datetime import router as datetime_router
from openbb_quantitative.dqlib_domain import call_dqlib
from openbb_quantitative.eqanalytics import router as eqanalytics_router
from openbb_quantitative.fianalytics import router as fianalytics_router
from openbb_quantitative.fimarket import router as fimarket_router
from openbb_quantitative.fxanalytics import router as fxanalytics_router
from openbb_quantitative.fxmarket import router as fxmarket_router
from openbb_quantitative.iranalytics import router as iranalytics_router
from openbb_quantitative.irmarket import router as irmarket_router
from openbb_quantitative.market import router as market_router
from openbb_quantitative.mktrisk import router as mktrisk_router
from openbb_quantitative.models import (
    DQLibCallResult,
    DQLibFunctionModel,
    DQLibPipelineResult,
    DQLibPipelineStep,
    DQLibStatusModel,
)
from openbb_quantitative.numerics import router as numerics_router
from openbb_quantitative.staticdata import router as staticdata_router
from openbb_quantitative.utility import router as utility_router

router = Router(
    prefix="/dqlib",
    description="Executable proprietary dqlib cross-asset analytics.",
)

for domain_router in (
    analytics_router,
    cmanalytics_router,
    cmmarket_router,
    cranalytics_router,
    crmarket_router,
    datetime_router,
    eqanalytics_router,
    fianalytics_router,
    fimarket_router,
    fxanalytics_router,
    fxmarket_router,
    iranalytics_router,
    irmarket_router,
    market_router,
    mktrisk_router,
    numerics_router,
    staticdata_router,
    utility_router,
):
    router.include_router(domain_router)


@router.command(methods=["GET"])
def status() -> OBBject[DQLibStatusModel]:
    """Return dqlib release, runtime compatibility, and availability status."""
    return OBBject(results=DQLibStatusModel.model_validate(get_status()))


@router.command(methods=["GET"])
def functions(domain: str) -> OBBject[list[DQLibFunctionModel]]:
    """List callable dqlib analytics available in one installed domain."""
    results = [
        DQLibFunctionModel.model_validate(item) for item in list_functions(domain)
    ]
    return OBBject(results=results)


@router.command(methods=["POST"])
def call(
    domain: str,
    function: str,
    args: list[Any] | None = None,
    kwargs: dict[str, Any] | None = None,
) -> OBBject[DQLibCallResult]:
    """Execute one allowlisted dqlib analytics function."""
    return OBBject(results=call_dqlib(domain, function, args, kwargs))


@router.command(methods=["POST"])
def pipeline(
    steps: list[DQLibPipelineStep],
    outputs: list[str] | None = None,
) -> OBBject[DQLibPipelineResult]:
    """Execute a native-object dqlib workflow and return selected outputs."""
    raw_steps = [step.model_dump() for step in steps]
    raw_outputs = execute_pipeline(raw_steps, outputs)
    serialized = to_jsonable(raw_outputs)
    return OBBject(
        results=DQLibPipelineResult.model_validate({"outputs": serialized})
    )

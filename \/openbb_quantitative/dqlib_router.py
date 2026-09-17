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
from openbb_quantitative.cmmarket import router as cmmarket_router
from openbb_quantitative.commodity.analytics import router as commodity_router
from openbb_quantitative.common.analytics import router as common_router
from openbb_quantitative.credit.analytics import router as credit_router
from openbb_quantitative.crmarket import router as crmarket_router
from openbb_quantitative.datetime import router as datetime_router
from openbb_quantitative.dqlib_domain import call_dqlib
from openbb_quantitative.equity.analytics import router as equity_router
from openbb_quantitative.fimarket import router as fimarket_router
from openbb_quantitative.fixed_income.analytics import router as fixed_income_router
from openbb_quantitative.foreign_exchange.analytics import (
    router as foreign_exchange_router,
)
from openbb_quantitative.fxmarket import router as fxmarket_router
from openbb_quantitative.interest_rate.analytics import router as interest_rate_router
from openbb_quantitative.irmarket import router as irmarket_router
from openbb_quantitative.market import router as market_router
from openbb_quantitative.models import (
    DQLibCallResult,
    DQLibFunctionModel,
    DQLibPipelineResult,
    DQLibPipelineStep,
    DQLibStatusModel,
)
from openbb_quantitative.numerics import router as numerics_router
from openbb_quantitative.risk.analytics import router as risk_router
from openbb_quantitative.staticdata import router as staticdata_router
from openbb_quantitative.utility import router as utility_router

router = Router(
    prefix="/dqlib",
    description="Executable proprietary dqlib cross-asset analytics.",
)

for domain_router in (
    common_router,
    commodity_router,
    cmmarket_router,
    credit_router,
    crmarket_router,
    datetime_router,
    equity_router,
    fixed_income_router,
    fimarket_router,
    foreign_exchange_router,
    fxmarket_router,
    interest_rate_router,
    irmarket_router,
    market_router,
    risk_router,
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
    steps: list[dict[str, Any]],
    outputs: list[str] | None = None,
) -> OBBject[DQLibPipelineResult]:
    """Execute a native-object dqlib workflow and return selected outputs."""
    raw_steps = [
        DQLibPipelineStep.model_validate(step).model_dump() for step in steps
    ]
    raw_outputs = execute_pipeline(raw_steps, outputs)
    serialized = to_jsonable(raw_outputs)
    return OBBject(
        results=DQLibPipelineResult.model_validate({"outputs": serialized})
    )

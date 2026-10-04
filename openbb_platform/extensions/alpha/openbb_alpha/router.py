"""Native alpha commands; providers own calculation, workflows own composition."""

from openbb_core.app.model.command_context import CommandContext
from openbb_core.app.model.obbject import OBBject
from openbb_core.app.provider_interface import ExtraParams, ProviderChoices, StandardParams
from openbb_core.app.query import Query
from openbb_core.app.router import Router
from openbb_core.provider.standard_models.alpha_research import DatasetInput, LabelRequest
from pydantic import BaseModel

router = Router(description="Versioned Lab alpha factors, independent labels and diagnostics.")


@router.command()
def catalog() -> OBBject[list[dict]]:
    """List supported versioned factors and complete-window semantics."""
    from openbb_alpha.catalog import catalog as definitions

    return OBBject(results=[s.model_dump(mode="json") for s in definitions()])


@router.command()
def capabilities() -> OBBject[list[dict]]:
    """Inspect installed engine capabilities without importing optional libraries."""
    from openbb_alpha.catalog import capabilities as inspect

    return OBBject(results=inspect())


@router.command(methods=["POST"])
def register(dataset: DatasetInput) -> OBBject[dict]:
    """Pin a bounded saved or synthetic Lab panel; never invoke collection."""
    from openbb_alpha.datasets import register as pin

    return OBBject(results=pin(dataset).model_dump(mode="json"))


@router.command(model="AlphaCompute", methods=["POST"])
async def compute(
    cc: CommandContext,
    provider_choices: ProviderChoices,
    standard_params: StandardParams,
    extra_params: ExtraParams,
) -> OBBject[BaseModel]:
    """Compute allowlisted factors using the selected native calculation provider."""
    return await OBBject.from_query(Query(**locals()))


@router.command(methods=["POST"])
def labels(request: LabelRequest) -> OBBject[dict]:
    """Construct independent next-open h-session labels from a pinned dataset."""
    from openbb_alpha.labels import labels as construct

    return OBBject(results=construct(request))


@router.command(model="AlphaEvaluate", methods=["POST"])
async def evaluate(
    cc: CommandContext,
    provider_choices: ProviderChoices,
    standard_params: StandardParams,
    extra_params: ExtraParams,
) -> OBBject[BaseModel]:
    """Evaluate identified factor/label artifacts using Alphalens diagnostics."""
    return await OBBject.from_query(Query(**locals()))

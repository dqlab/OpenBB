"""BaoStock-specific commands exposing the full source data API."""

from openbb_core.app.model.command_context import CommandContext
from openbb_core.app.model.obbject import OBBject
from openbb_core.app.provider_interface import ExtraParams, ProviderChoices, StandardParams
from openbb_core.app.query import Query
from openbb_core.app.router import Router
from pydantic import BaseModel

from openbb_baostock.utils.catalog import DATASETS, Dataset

router = Router(description="Complete BaoStock datasets, preserving source column names and units.")


def register_command(dataset: Dataset) -> None:
    """Register one typed, provider-backed command from the explicit API inventory."""

    async def command(
        cc: CommandContext,
        provider_choices: ProviderChoices,
        standard_params: StandardParams,
        extra_params: ExtraParams,
    ) -> OBBject[BaseModel]:
        return await OBBject.from_query(Query(**locals()))

    command.__name__ = dataset.command
    command.__qualname__ = dataset.command
    command.__doc__ = (
        dataset.description
        + "\n\nSource: baostock."
        + dataset.module
        + "."
        + dataset.method
        + ". Returns all source columns as strings, including blank values, without unit conversion."
    )
    globals()[dataset.command] = router.command(model=dataset.model)(command)


for _dataset in DATASETS:
    register_command(_dataset)

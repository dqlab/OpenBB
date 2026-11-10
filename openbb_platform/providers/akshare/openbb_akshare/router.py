# OpenBB and collector callback signatures retain protocol parameters.
# pylint: disable=unused-argument

"""AKShare-specific commands exposing the full source data API."""

from openbb_core.app.model.command_context import CommandContext
from openbb_core.app.model.obbject import OBBject
from openbb_core.app.provider_interface import (
    ExtraParams,
    ProviderChoices,
    StandardParams,
)
from openbb_core.app.query import Query
from openbb_core.app.router import Router
from pydantic import BaseModel

from openbb_akshare.utils.catalog import DATASETS, Dataset

router = Router(
    description="Complete AKShare datasets, preserving source column names and units."
)


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
        + "\n\nSource: akshare."
        + dataset.method
        + ". Returns all source columns, including blank values, without unit conversion."
    )
    globals()[dataset.command] = router.command(model=dataset.model)(command)


for _dataset in DATASETS:
    register_command(_dataset)


@router.command(model="AkshareEtfOptionContracts")
async def etf_option_contracts(
    cc: CommandContext,
    provider_choices: ProviderChoices,
    standard_params: StandardParams,
    extra_params: ExtraParams,
) -> OBBject[BaseModel]:
    """Fetch the SSE/SZSE ETF option universe with explicit source provenance."""
    return await OBBject.from_query(Query(**locals()))


@router.command(model="AkshareEtfOptionQuotes")
async def etf_option_quotes(
    cc: CommandContext,
    provider_choices: ProviderChoices,
    standard_params: StandardParams,
    extra_params: ExtraParams,
) -> OBBject[BaseModel]:
    """Bounded Sina bid/ask and five-level quote batch, including error rows."""
    return await OBBject.from_query(Query(**locals()))


@router.command(model="AkshareCffexOptionExpiries")
async def cffex_option_expiries(
    cc: CommandContext,
    provider_choices: ProviderChoices,
    standard_params: StandardParams,
    extra_params: ExtraParams,
) -> OBBject[BaseModel]:
    """Discover all Sina-displayed maturities for an explicit IO, HO or MO product."""
    return await OBBject.from_query(Query(**locals()))


@router.command(model="AkshareCffexOptionQuotes")
async def cffex_option_quotes(
    cc: CommandContext,
    provider_choices: ProviderChoices,
    standard_params: StandardParams,
    extra_params: ExtraParams,
) -> OBBject[BaseModel]:
    """Fetch native bid/ask and sizes for one maturity; event clocks remain unknown."""
    return await OBBject.from_query(Query(**locals()))

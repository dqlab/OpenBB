"""CTP registration; native sessions do not require loading the provider registry."""

from importlib import import_module


def __getattr__(name):
    """Load the OpenBB provider only when its entry point is requested."""
    if name != "ctp_provider":
        raise AttributeError(name)
    provider_type = import_module("openbb_core.provider.abstract.provider").Provider
    provider = provider_type(
        name="ctp",
        website="https://github.com/openctp/openctp",
        description="CTP market data source",
        credentials=[],
        fetcher_dict={},
    )
    globals()[name] = provider
    return provider

"""FUTUNN HTTP/WebSocket provider registration."""

from openbb_core.provider.abstract.provider import Provider

from openbb_futu.quote import FutuEquityQuoteFetcher

futu_provider = Provider(
    name="futu",
    website="https://open.futunn.com",
    description="FUTUNN Open API market snapshots",
    credentials=["access_token"],
    fetcher_dict={"EquityQuote": FutuEquityQuoteFetcher},
)

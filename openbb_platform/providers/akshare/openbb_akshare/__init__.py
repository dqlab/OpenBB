"""AKShare provider for the OpenBB Platform."""

from openbb_core.provider.abstract.provider import Provider

from openbb_akshare.models.futures_historical import AkshareFuturesHistoricalFetcher
from openbb_akshare.models.native import NATIVE_FETCHERS

akshare_provider = Provider(
    name="akshare",
    website="https://akshare.akfamily.xyz",
    description=(
        "AKShare China market data: CFFEX, SHFE, DCE, CZCE, INE, and GFEX futures, "
        "plus the broader AKShare interface surface."
    ),
    credentials=None,
    fetcher_dict={
        "FuturesHistorical": AkshareFuturesHistoricalFetcher,
        **NATIVE_FETCHERS,
    },
    repr_name="AKShare",
)

"""BaoStock provider for the OpenBB Platform."""

from openbb_core.provider.abstract.provider import Provider

from openbb_baostock.models.equity_historical import BaostockEquityHistoricalFetcher
from openbb_baostock.models.equity_profile import BaostockEquityProfileFetcher
from openbb_baostock.models.equity_search import BaostockEquitySearchFetcher
from openbb_baostock.models.index_constituents import BaostockIndexConstituentsFetcher
from openbb_baostock.models.index_historical import BaostockIndexHistoricalFetcher
from openbb_baostock.models.native import NATIVE_FETCHERS

baostock_provider = Provider(
    name="baostock",
    website="https://www.baostock.com",
    description=(
        "BaoStock securities, financial indicators, corporate actions, market classifications, and macroeconomic data."
    ),
    credentials=None,
    fetcher_dict={
        "EquityHistorical": BaostockEquityHistoricalFetcher,
        "EquityInfo": BaostockEquityProfileFetcher,
        "EquitySearch": BaostockEquitySearchFetcher,
        "IndexHistorical": BaostockIndexHistoricalFetcher,
        "IndexConstituents": BaostockIndexConstituentsFetcher,
        **NATIVE_FETCHERS,
    },
    repr_name="BaoStock",
)

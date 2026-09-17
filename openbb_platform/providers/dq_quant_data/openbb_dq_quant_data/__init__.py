"""OpenBB provider registration for dq-quant-invest-data."""

from openbb_core.provider.abstract.provider import Provider

from openbb_dq_quant_data.futures_curve import DQFuturesCurveFetcher
from openbb_dq_quant_data.models import (
    DQCalendarDividendFetcher,
    DQCalendarSplitsFetcher,
    DQCompanyFilingsFetcher,
    DQEquityHistoricalFetcher,
    DQEquityInfoFetcher,
    DQEquitySearchFetcher,
    DQEtfHistoricalFetcher,
    DQFredSeriesFetcher,
)
from openbb_dq_quant_data.options_chains import DQOptionsChainsFetcher

dq_quant_data_provider = Provider(
    name="dq_quant_data",
    website="https://github.com/dqlab/dq-quant-invest-data",
    description="Canonical point-in-time data from dq-quant-invest-data.",
    credentials=["api_url", "api_key"],
    fetcher_dict={
        "OptionsChains": DQOptionsChainsFetcher,
        "FuturesCurve": DQFuturesCurveFetcher,
        "EquityHistorical": DQEquityHistoricalFetcher,
        "EtfHistorical": DQEtfHistoricalFetcher,
        "EquitySearch": DQEquitySearchFetcher,
        "EquityInfo": DQEquityInfoFetcher,
        "CalendarDividend": DQCalendarDividendFetcher,
        "CalendarSplits": DQCalendarSplitsFetcher,
        "CompanyFilings": DQCompanyFilingsFetcher,
        "FredSeries": DQFredSeriesFetcher,
    },
)

__all__ = ["dq_quant_data_provider"]

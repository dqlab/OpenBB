"""Stooq provider module."""

from openbb_core.provider.abstract.provider import Provider
from openbb_stooq.models.currency_historical import StooqCurrencyHistoricalFetcher
from openbb_stooq.models.equity_historical import StooqEquityHistoricalFetcher
from openbb_stooq.models.index_historical import StooqIndexHistoricalFetcher

stooq_provider = Provider(
    name="stooq",
    website="https://stooq.com",
    description=(
        "Stooq provides historical market data for global equities, ETFs, indices, "
        "and currency pairs through its CSV download service."
    ),
    credentials=["api_key"],
    fetcher_dict={
        "CurrencyHistorical": StooqCurrencyHistoricalFetcher,
        "EquityHistorical": StooqEquityHistoricalFetcher,
        "EtfHistorical": StooqEquityHistoricalFetcher,
        "IndexHistorical": StooqIndexHistoricalFetcher,
    },
    repr_name="Stooq",
    instructions=(
        "Open https://stooq.com/q/d/?s=aapl.us&get_apikey, complete the "
        "verification, and copy the apikey value from the CSV download link."
    ),
)

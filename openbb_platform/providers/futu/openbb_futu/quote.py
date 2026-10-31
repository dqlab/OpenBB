"""OpenBB equity quote snapshot mapping."""

import re

from openbb_core.provider.abstract.fetcher import Fetcher
from openbb_core.provider.standard_models.equity_quote import (
    EquityQuoteData,
    EquityQuoteQueryParams,
)

from openbb_futu.client import snapshot


class FutuEquityQuoteQueryParams(EquityQuoteQueryParams):
    """Use FUTUNN market-prefixed codes, comma separated (up to 400)."""


class FutuEquityQuoteData(EquityQuoteData):
    """FUTUNN snapshot; bid/ask sizes are omitted because lot units are unverified."""


class FutuEquityQuoteFetcher(
    Fetcher[FutuEquityQuoteQueryParams, list[FutuEquityQuoteData]]
):
    """Retrieve bounded current snapshots."""

    @staticmethod
    def transform_query(params):
        """Handle transform query."""
        query = FutuEquityQuoteQueryParams(**params)
        codes = query.symbol.split(",")  # pylint: disable=no-member  # Pydantic field
        if any(not re.fullmatch(r"[A-Z]{2}\.[A-Z0-9._-]+", x) for x in codes):
            raise ValueError("symbols must use FUTUNN MARKET.CODE format")
        return query

    @staticmethod
    def extract_data(query, credentials, **kwargs):
        """Handle extract data."""
        del kwargs
        return snapshot(
            query.symbol.split(","), (credentials or {}).get("futu_access_token", "")
        )

    @staticmethod
    def transform_data(query, data, **kwargs):
        """Handle transform data."""
        del query, kwargs
        mapping = {
            "code": "symbol",
            "last_price": "last_price",
            "open_price": "open",
            "high_price": "high",
            "low_price": "low",
            "prev_close_price": "prev_close",
            "volume": "volume",
            "bid_price": "bid",
            "ask_price": "ask",
        }
        return [
            FutuEquityQuoteData.model_validate(
                {dst: row[src] for src, dst in mapping.items() if src in row}
            )
            for row in data
        ]

"""Lossless envelope of the yfinance SDK return, not network-wire bytes.

The captured value is exactly what the configured client call returned
(``yfinance.Ticker(symbol).get_info()``). No field whitelist, rename, fallback,
identity selection or exception-to-empty conversion happens here: the typed
``EquityInfo`` projection stays in ``openbb_yfinance.models.equity_profile`` and
business validation stays downstream.
"""

import asyncio
import math
from typing import Any, Literal

from openbb_core.provider.abstract.data import Data
from openbb_core.provider.abstract.fetcher import Fetcher
from openbb_core.provider.abstract.query_params import QueryParams
from pydantic import ConfigDict, Field


class YFinanceEquityProfileSourceQueryParams(QueryParams):
    """Literal single-symbol source request; no business identity normalization."""

    model_config = ConfigDict(extra="forbid", strict=True)
    symbol: str = Field(min_length=1, max_length=256, pattern=r"^[^,]+$")


class YFinanceEquityProfileSourceData(Data):
    """The SDK return value, including empty, null and business-invalid shapes."""

    evidence_format: Literal["yfinance-profile-extraction-v1"] = (
        "yfinance-profile-extraction-v1"
    )
    payload: Any


def _plain(value: Any, depth: int = 0) -> None:
    """Reject protocol coercion before the dispatcher's generic serializer."""
    if depth > 32:
        raise ValueError("non_json_source")
    if type(value) is dict:
        if any(type(key) is not str for key in value):
            raise ValueError("non_json_source")
        for child in value.values():
            _plain(child, depth + 1)
    elif type(value) is list:
        for child in value:
            _plain(child, depth + 1)
    elif type(value) not in (str, int, float, bool, type(None)) or (
        type(value) is float and not math.isfinite(value)
    ):
        raise ValueError("non_json_source")


class YFinanceEquityProfileSourceFetcher(
    Fetcher[
        YFinanceEquityProfileSourceQueryParams, list[YFinanceEquityProfileSourceData]
    ]
):
    """Preserve the client's own return without invoking the typed profile model."""

    require_credentials = False

    @staticmethod
    def transform_query(params: dict) -> YFinanceEquityProfileSourceQueryParams:
        """Validate only literal request structure."""
        return YFinanceEquityProfileSourceQueryParams(**params)

    @staticmethod
    async def aextract_data(query, credentials, **kwargs):
        """Return one verbatim SDK value per literal request.

        A transport or client exception propagates; a successful empty, null or
        unknown-only mapping is returned as the evidence it actually is.
        """
        # pylint: disable=import-outside-toplevel
        from yfinance import Ticker

        symbol = query.symbol
        return [await asyncio.to_thread(lambda: Ticker(symbol).get_info())]

    @staticmethod
    def transform_data(query, data, **kwargs) -> list[YFinanceEquityProfileSourceData]:
        """Envelope JSON without coercing its payload."""
        _plain(data)
        return [YFinanceEquityProfileSourceData(payload=data)]

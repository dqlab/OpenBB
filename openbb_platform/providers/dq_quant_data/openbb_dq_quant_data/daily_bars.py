"""OpenBB stock, ETF and index daily bars from retained platform snapshots."""

from datetime import datetime
from typing import Any, Literal

from openbb_core.app.model.abstract.error import OpenBBError
from openbb_core.provider.abstract.annotated_result import AnnotatedResult
from openbb_core.provider.abstract.fetcher import Fetcher
from openbb_core.provider.standard_models.equity_historical import (
    EquityHistoricalData,
    EquityHistoricalQueryParams,
)
from openbb_core.provider.standard_models.etf_historical import (
    EtfHistoricalData,
    EtfHistoricalQueryParams,
)
from openbb_core.provider.standard_models.index_historical import (
    IndexHistoricalData,
    IndexHistoricalQueryParams,
)
from pydantic import BaseModel, Field, field_validator

from openbb_dq_quant_data.client import DQAPIClient


class DailyParams(BaseModel):
    interval: Literal["1d"] = Field(default="1d", description="Retained daily session bars.")
    as_of: datetime | None = Field(
        default=None, description="Return revisions known by this UTC-offset timestamp."
    )
    adjustment: Literal[
        "source", "unadjusted", "splits_only", "splits_and_dividends", "forward", "backward"
    ] = Field(
        default="source",
        description="Source convention by default; unsupported adjustments fail explicitly.",
    )
    max_rows: int = Field(default=100000, ge=1, le=100000)

    @field_validator("as_of")
    @classmethod
    def aware_as_of(cls, value):
        if value is not None and (value.tzinfo is None or value.utcoffset() is None):
            raise ValueError("as_of requires a UTC offset")
        return value


class DQEquityHistoricalQueryParams(DailyParams, EquityHistoricalQueryParams):
    """Daily stocks, including retained A-shares under SH./SZ. symbols."""

    __json_schema_extra__ = {"symbol": {"multiple_items_allowed": True}}


class DQEtfHistoricalQueryParams(DailyParams, EtfHistoricalQueryParams):
    """Daily ETF session bars."""

    __json_schema_extra__ = {"symbol": {"multiple_items_allowed": True}}


class DQIndexHistoricalQueryParams(DailyParams, IndexHistoricalQueryParams):
    """Daily cash-index levels."""

    __json_schema_extra__ = {"symbol": {"multiple_items_allowed": True}}


class DQEquityHistoricalData(EquityHistoricalData):
    """Source OHLCV plus adjustment, snapshot, availability and quality fields."""

    symbol: str | None = None
    adj_close: float | None = None
    dividend: float | None = None
    split_ratio: float | None = None


class DQEtfHistoricalData(EtfHistoricalData):
    """ETF bars preserving missing volume and adjustment metadata."""

    symbol: str | None = None
    volume: float | None = None
    adj_close: float | None = None


class DQIndexHistoricalData(IndexHistoricalData):
    """Cash-index levels; source volume is not index trading volume."""


def _read(query, credentials, asset_class):
    params = query.model_dump(exclude={"interval", "symbol"}, exclude_none=True)
    params.update(symbols=query.symbol, asset_class=asset_class)
    return DQAPIClient(credentials).get_market_result("daily-bars", params)


def _project(data: dict, model):
    output = []
    for source in data["data"]:
        row = dict(source)
        if row.get("date") is None or any(
            row.get(key) is None for key in ("open", "high", "low", "close")
        ):
            raise OpenBBError("DQ API returned an invalid daily bar; refusing silent row loss.")
        if "adjusted_close" in row:
            row["adj_close"] = row["adjusted_close"]
        output.append(model.model_validate(row))
    return AnnotatedResult(result=output, metadata=data["meta"])


class DQEquityHistoricalFetcher(
    Fetcher[DQEquityHistoricalQueryParams, list[DQEquityHistoricalData]]
):
    require_credentials = False

    @staticmethod
    def transform_query(params: dict[str, Any]) -> DQEquityHistoricalQueryParams:
        return DQEquityHistoricalQueryParams(**params)

    @staticmethod
    def extract_data(
        query: DQEquityHistoricalQueryParams, credentials: dict | None, **kwargs
    ) -> dict:
        return _read(query, credentials, "stock")

    @staticmethod
    def transform_data(
        query: DQEquityHistoricalQueryParams, data: dict, **kwargs
    ) -> AnnotatedResult[list[DQEquityHistoricalData]]:
        return _project(data, DQEquityHistoricalData)


class DQEtfHistoricalFetcher(Fetcher[DQEtfHistoricalQueryParams, list[DQEtfHistoricalData]]):
    require_credentials = False

    @staticmethod
    def transform_query(params: dict[str, Any]) -> DQEtfHistoricalQueryParams:
        return DQEtfHistoricalQueryParams(**params)

    @staticmethod
    def extract_data(query: DQEtfHistoricalQueryParams, credentials: dict | None, **kwargs) -> dict:
        return _read(query, credentials, "etf")

    @staticmethod
    def transform_data(
        query: DQEtfHistoricalQueryParams, data: dict, **kwargs
    ) -> AnnotatedResult[list[DQEtfHistoricalData]]:
        return _project(data, DQEtfHistoricalData)


class DQIndexHistoricalFetcher(Fetcher[DQIndexHistoricalQueryParams, list[DQIndexHistoricalData]]):
    require_credentials = False

    @staticmethod
    def transform_query(params: dict[str, Any]) -> DQIndexHistoricalQueryParams:
        return DQIndexHistoricalQueryParams(**params)

    @staticmethod
    def extract_data(
        query: DQIndexHistoricalQueryParams, credentials: dict | None, **kwargs
    ) -> dict:
        return _read(query, credentials, "index")

    @staticmethod
    def transform_data(
        query: DQIndexHistoricalQueryParams, data: dict, **kwargs
    ) -> AnnotatedResult[list[DQIndexHistoricalData]]:
        return _project(data, DQIndexHistoricalData)

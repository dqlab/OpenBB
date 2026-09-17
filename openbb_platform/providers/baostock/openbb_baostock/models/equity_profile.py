"""BaoStock basic stock profiles."""

from datetime import date
from typing import Any

from openbb_core.provider.abstract.fetcher import Fetcher
from openbb_core.provider.standard_models.equity_info import EquityInfoData, EquityInfoQueryParams
from openbb_core.provider.utils.errors import EmptyDataError
from pydantic import Field, field_validator

from openbb_baostock.models.equity_search import BaostockEquitySearchData
from openbb_baostock.utils.helpers import normalize_symbol, normalize_symbols, query_batch, transform_stock


class BaostockEquityProfileQueryParams(EquityInfoQueryParams):
    """BaoStock profile query."""

    __json_schema_extra__ = {"symbol": {"multiple_items_allowed": True}}

    @field_validator("symbol", mode="before")
    @classmethod
    def to_upper(cls, value: str) -> str:
        """Normalize exchange-qualified symbols."""
        return normalize_symbols(value)


class BaostockEquityProfileData(EquityInfoData):
    """Basic listing data; BaoStock does not supply full company profiles here."""

    __alias_dict__ = BaostockEquitySearchData.__alias_dict__

    ipo_date: date | None = Field(default=None, description="Listing date.")
    delisting_date: date | None = Field(default=None, description="Delisting date, when available.")
    security_type: str = Field(description="BaoStock security type; 1 denotes a stock.")
    is_active: bool | None = Field(default=None, description="Whether the security is currently listed.")


class BaostockEquityProfileFetcher(Fetcher[BaostockEquityProfileQueryParams, list[BaostockEquityProfileData]]):
    """Fetch stock listing profiles without credentials."""

    require_credentials = False

    @staticmethod
    def transform_query(params: dict[str, Any]) -> BaostockEquityProfileQueryParams:
        """Validate requested stock symbols."""
        return BaostockEquityProfileQueryParams(**params)

    @staticmethod
    async def aextract_data(
        query: BaostockEquityProfileQueryParams, credentials: dict[str, str] | None, **kwargs: Any
    ) -> list[dict[str, Any]]:
        """Fetch every requested stock or report a missing profile."""
        symbols = query.symbol.split(",")
        batches = await query_batch("query_stock_basic", [{"code": symbol.lower()} for symbol in symbols])
        records = []
        for symbol, batch in zip(symbols, batches):
            stocks = [row for row in batch if row.get("type") == "1" and normalize_symbol(row["code"]) == symbol]
            if not stocks:
                raise EmptyDataError(f"No BaoStock stock profile for {symbol}.")
            records.extend(stocks)
        return records

    @staticmethod
    def transform_data(
        query: BaostockEquityProfileQueryParams, data: list[dict[str, Any]], **kwargs: Any
    ) -> list[BaostockEquityProfileData]:
        """Map basic listing metadata to the standard profile model."""
        return [BaostockEquityProfileData.model_validate(transform_stock(row)) for row in data]

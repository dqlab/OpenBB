"""BaoStock stock search."""

import re
from contextlib import suppress
from datetime import date
from typing import Any

from openbb_core.provider.abstract.fetcher import Fetcher
from openbb_core.provider.standard_models.equity_search import EquitySearchData, EquitySearchQueryParams
from pydantic import Field, field_validator

from openbb_baostock.utils.helpers import normalize_symbol, query_batch, transform_stock


class BaostockEquitySearchQueryParams(EquitySearchQueryParams):
    """Search BaoStock by company name or an exact/partial symbol."""

    limit: int = Field(default=100, ge=1, le=10000, description="Maximum number of results to return.")

    @field_validator("query")
    @classmethod
    def validate_query(cls, value: str) -> str:
        """Reject SDK protocol delimiters in free-text queries."""
        if re.search(r"[\x00-\x1f,]", value):
            raise ValueError("Search text cannot contain commas or control characters.")
        return value.strip()


class BaostockEquitySearchData(EquitySearchData):
    """BaoStock stock listing metadata."""

    __alias_dict__ = {
        "symbol": "code",
        "name": "code_name",
        "ipo_date": "ipoDate",
        "delisting_date": "outDate",
        "security_type": "type",
        "is_active": "status",
    }
    stock_exchange: str | None = Field(default=None, description="SSE or SZSE exchange identifier.")
    ipo_date: date | None = Field(default=None, description="Listing date.")
    delisting_date: date | None = Field(default=None, description="Delisting date, when available.")
    security_type: str = Field(description="BaoStock security type; 1 denotes a stock.")
    is_active: bool | None = Field(default=None, description="Whether the security is currently listed.")


class BaostockEquitySearchFetcher(Fetcher[BaostockEquitySearchQueryParams, list[BaostockEquitySearchData]]):
    """Search stocks, including delisted records returned by BaoStock."""

    require_credentials = False

    @staticmethod
    def transform_query(params: dict[str, Any]) -> BaostockEquitySearchQueryParams:
        """Validate search parameters."""
        return BaostockEquitySearchQueryParams(**params)

    @staticmethod
    async def aextract_data(
        query: BaostockEquitySearchQueryParams, credentials: dict[str, str] | None, **kwargs: Any
    ) -> list[dict[str, Any]]:
        """Use server-side name/exact-code search, or filter partial codes locally."""
        params = {}
        if query.is_symbol:
            # Partial numeric symbols are matched against the stock list.
            with suppress(ValueError):
                params["code"] = normalize_symbol(query.query).lower()
        elif query.query:
            params["code_name"] = query.query
        batches = await query_batch("query_stock_basic", [params])
        records = [row for row in batches[0] if row.get("type") == "1"]
        if query.is_symbol and "code" not in params:
            records = [row for row in records if query.query.upper() in row["code"].upper()]
        return sorted(records, key=lambda row: row["code"])[: query.limit]

    @staticmethod
    def transform_data(
        query: BaostockEquitySearchQueryParams, data: list[dict[str, Any]], **kwargs: Any
    ) -> list[BaostockEquitySearchData]:
        """Preserve listing dates and status in typed results."""
        return [BaostockEquitySearchData.model_validate(transform_stock(row)) for row in data]

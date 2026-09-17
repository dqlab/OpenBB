"""BaoStock constituents for the three index membership APIs."""

from datetime import date as dateType
from typing import Any

from openbb_core.provider.abstract.fetcher import Fetcher
from openbb_core.provider.standard_models.index_constituents import IndexConstituentsData, IndexConstituentsQueryParams
from pydantic import Field, field_validator

from openbb_baostock.utils.helpers import normalize_symbol, query_batch

INDEX_METHODS = {
    "HS300": "query_hs300_stocks",
    "SH.000300": "query_hs300_stocks",
    "SZ.399300": "query_hs300_stocks",
    "SZ50": "query_sz50_stocks",
    "SH.000016": "query_sz50_stocks",
    "ZZ500": "query_zz500_stocks",
    "SH.000905": "query_zz500_stocks",
    "SZ.399905": "query_zz500_stocks",
}


class BaostockIndexConstituentsQueryParams(IndexConstituentsQueryParams):
    """Query dated or latest CSI 300, SSE 50, or CSI 500 membership."""

    date: dateType | None = Field(default=None, description="Source membership snapshot date; omitted means latest.")

    @field_validator("symbol", mode="before")
    @classmethod
    def validate_index(cls, value: str) -> str:
        """Resolve explicit index aliases without guessing numeric codes."""
        value = value.strip().upper()
        value = value if value in INDEX_METHODS else normalize_symbol(value)
        if value not in INDEX_METHODS:
            raise ValueError("BaoStock constituents support HS300, SZ50, and ZZ500 only.")
        return value


class BaostockIndexConstituentsData(IndexConstituentsData):
    """Constituent identifiers with the source membership update date."""

    __alias_dict__ = {"symbol": "code", "name": "code_name", "date": "updateDate"}
    date: dateType | None = Field(default=None, description="Membership update date supplied by BaoStock.")


class BaostockIndexConstituentsFetcher(
    Fetcher[BaostockIndexConstituentsQueryParams, list[BaostockIndexConstituentsData]]
):
    """Provide BaoStock membership through the standard index command."""

    require_credentials = False

    @staticmethod
    def transform_query(params: dict[str, Any]) -> BaostockIndexConstituentsQueryParams:
        """Validate the selected index and optional date."""
        return BaostockIndexConstituentsQueryParams(**params)

    @staticmethod
    async def aextract_data(
        query: BaostockIndexConstituentsQueryParams, credentials: dict[str, str] | None, **kwargs: Any
    ) -> list[dict[str, Any]]:
        """Fetch one membership snapshot using the corresponding source API."""
        params = {"date": query.date.isoformat()} if query.date else {}
        return (await query_batch(INDEX_METHODS[query.symbol], [params]))[0]

    @staticmethod
    def transform_data(
        query: BaostockIndexConstituentsQueryParams, data: list[dict[str, Any]], **kwargs: Any
    ) -> list[BaostockIndexConstituentsData]:
        """Normalize constituent identifiers and preserve the source date."""
        return [
            BaostockIndexConstituentsData.model_validate(
                {
                    **{key: None if value == "" else value for key, value in row.items()},
                    "code": normalize_symbol(row["code"]),
                }
            )
            for row in data
        ]

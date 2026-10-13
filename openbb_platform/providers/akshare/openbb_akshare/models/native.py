"""Complete AKShare query surface with validated inputs and lossless source rows."""

from datetime import date as dateType
from typing import Any, Literal

from openbb_core.provider.abstract.data import Data
from openbb_core.provider.abstract.fetcher import Fetcher
from openbb_core.provider.abstract.query_params import QueryParams
from pydantic import ConfigDict, Field, create_model

from openbb_akshare.utils.catalog import DATASETS, Dataset
from openbb_akshare.utils.helpers import (
    normalize_period,
    normalize_symbol,
    normalize_vars_list,
    query_frame,
    source_date,
)


class AkshareNativeQuery(QueryParams):
    """Shared validation without assigning one dataset's fields to another."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)


class AkshareNativeData(Data):
    """A source row with the exact AKShare column names and values.

    AKShare datasets carry their own units and identifier conventions. Preserve
    every column, blank, and identifier instead of applying guesses based on
    field names. ``OBBject.to_df()`` exposes these columns directly. Normalized
    endpoints use separate models.
    """

    model_config = ConfigDict(extra="allow", alias_generator=None)
    __pydantic_extra__: dict[str, Any] = Field(init=False)

    def model_dump(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        """Serialize only columns actually supplied by AKShare."""
        kwargs.setdefault("exclude_unset", True)
        return super().model_dump(*args, **kwargs)


def query_fields(kind: str) -> dict[str, Any]:
    """Define the explicit parameter schema for each dataset family."""
    fields: dict[str, Any] = {}
    if kind in {"date", "rank"}:
        fields["date"] = (
            dateType,
            Field(
                description=(
                    "Trading date in YYYY-MM-DD. Required because AKShare's own"
                    " default is a hardcoded historical date."
                )
            ),
        )
    if kind == "rank":
        fields["vars_list"] = (
            str | None,
            Field(
                default=None,
                description=(
                    "Comma-separated product codes to rank, for example 'IF,T'."
                    " Omit to use the source default list."
                ),
            ),
        )
    if kind in {"symbol", "symbol_period"}:
        fields["symbol"] = (
            str,
            Field(description="China futures root such as IF0 or RB0, or a dated contract such as IF2610."),
        )
    if kind == "symbol_period":
        fields["period"] = (
            Literal["1", "5", "15", "30", "60"],
            Field(default="5", description="Intraday bar frequency in minutes."),
        )
    return fields


def source_parameters(query: Any) -> dict[str, Any]:
    """Serialize a validated query into AKShare call arguments."""
    params = query.model_dump(exclude_none=True)
    if "date" in params:
        params["date"] = source_date(params["date"])
    if "symbol" in params:
        params["symbol"] = normalize_symbol(params["symbol"])
    if "period" in params:
        params["period"] = normalize_period(params["period"])
    if "vars_list" in params:
        params["vars_list"] = normalize_vars_list(params["vars_list"])
        if params["vars_list"] is None:
            del params["vars_list"]
    return params


def make_fetcher(dataset: Dataset) -> type[Fetcher]:
    """Create independent query and data models for one registered AKShare interface."""
    query_model = create_model(
        f"{dataset.model}QueryParams",
        __base__=AkshareNativeQuery,
        __module__=__name__,
        __doc__=dataset.description,
        **query_fields(dataset.parameters),
    )
    # Declare source identity columns so OpenBB can distinguish a single scalar
    # row from a model of arrays when converting to a dataframe.
    data_model = create_model(
        f"{dataset.model}Data",
        __base__=AkshareNativeData,
        __module__=__name__,
        **{
            name: (Any, Field(default=None, description=f"Source {name}, preserved without conversion."))
            for name in dataset.identifiers
        },
    )
    if dataset.parameters == "rank":
        # OpenBB joins list parameters into a comma-separated string and rejects
        # the value unless the provider declares that multiple items are allowed.
        query_model.__json_schema_extra__ = {"vars_list": {"multiple_items_allowed": True}}

    class NativeFetcher(Fetcher[query_model, list[data_model]]):
        """Fetch a complete AKShare dataset while preserving every source column."""

        require_credentials = False

        @staticmethod
        def transform_query(params: dict[str, Any]) -> Any:
            """Validate this dataset's explicit parameter schema."""
            return query_model(**params)

        @staticmethod
        async def aextract_data(query: Any, credentials: dict[str, str] | None, **kwargs: Any) -> list[dict[str, Any]]:
            """Read every row; a successful empty dataset remains an empty list."""
            return await query_frame(dataset.method, source_parameters(query))

        @staticmethod
        def transform_data(query: Any, data: list[dict[str, Any]], **kwargs: Any) -> list[Any]:
            """Preserve all source columns and exact values, including blanks."""
            return [data_model.model_validate(row) for row in data]

    NativeFetcher.__name__ = f"{dataset.model}Fetcher"
    NativeFetcher.__qualname__ = NativeFetcher.__name__
    globals()[query_model.__name__] = query_model
    globals()[data_model.__name__] = data_model
    globals()[NativeFetcher.__name__] = NativeFetcher
    return NativeFetcher


NATIVE_FETCHERS = {dataset.model: make_fetcher(dataset) for dataset in DATASETS}

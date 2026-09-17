"""Complete BaoStock query surface with validated inputs and lossless source rows."""

import re
from datetime import date as dateType
from typing import Any, Literal

from openbb_core.provider.abstract.data import Data
from openbb_core.provider.abstract.fetcher import Fetcher
from openbb_core.provider.abstract.query_params import QueryParams
from pydantic import ConfigDict, Field, create_model, field_validator, model_validator

from openbb_baostock.utils.catalog import DATASETS, Dataset
from openbb_baostock.utils.helpers import normalize_symbol, query_batch


class BaostockNativeQuery(QueryParams):
    """Shared validation without assigning one dataset's fields to another."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    @field_validator("*", mode="before")
    @classmethod
    def validate_source_input(cls, value: Any, info: Any) -> Any:
        """Validate SDK protocol text, exchange identity, and optional blank inputs."""
        if isinstance(value, str):
            if re.search(r"[\x00-\x1f]", value):
                raise ValueError("BaoStock query text cannot contain control characters.")
            value = value.strip()
            if info.field_name != "fields" and "," in value:
                raise ValueError("BaoStock native queries accept one value per parameter.")
            if value == "":
                value = None
        if value is not None and info.field_name == "code":
            return normalize_symbol(value).lower()
        if value is not None and info.field_name == "fields":
            if not re.fullmatch(r"[A-Za-z][A-Za-z0-9]*(,[A-Za-z][A-Za-z0-9]*)*", value):
                raise ValueError("fields must be a comma-separated list of BaoStock field names.")
            if len(set(value.split(","))) != len(value.split(",")):
                raise ValueError("fields must not contain duplicates.")
        return value

    @model_validator(mode="after")
    def validate_range(self) -> "BaostockNativeQuery":
        """Keep daily, monthly, and annual range semantics separate."""
        start, end = getattr(self, "start_date", None), getattr(self, "end_date", None)
        if start is not None and end is not None and start > end:
            raise ValueError("start_date must be on or before end_date.")
        return self


class BaostockNativeData(Data):
    """A source row with the exact BaoStock column names and string values.

    BaoStock's financial and macroeconomic fields have dataset-specific units.
    Preserve every column, blank, identifier, publication date, and decimal
    string instead of applying guesses based on field names. OBBject.to_df()
    exposes these columns directly. Normalized endpoints use separate models.
    """

    model_config = ConfigDict(extra="allow", alias_generator=None)
    __pydantic_extra__: dict[str, Any] = Field(init=False)

    def model_dump(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        """Serialize only columns actually supplied by BaoStock."""
        kwargs.setdefault("exclude_unset", True)
        return super().model_dump(*args, **kwargs)


def query_fields(kind: str) -> dict[str, Any]:
    """Define the complete native signature for each family of SDK queries."""
    fields: dict[str, Any] = {}
    if kind in {"history", "code_range", "quarter", "dividend", "basic", "classification"}:
        optional = kind in {"basic", "classification"}
        fields["code"] = (
            str | None if optional else str,
            Field(default=None if optional else ..., description="Exchange-qualified BaoStock code, such as sh.600000."),
        )
    if kind in {"history", "code_range", "range", "reserve"}:
        for name, label in (("start_date", "First"), ("end_date", "Last")):
            fields[name] = (
                dateType | None,
                Field(default=None, description=f"{label} inclusive date. Omit to use BaoStock's default."),
            )
    if kind in {"month_range", "year_range"}:
        pattern = r"^[0-9]{4}-(0[1-9]|1[0-2])$" if kind == "month_range" else r"^[0-9]{4}$"
        for name in ("start_date", "end_date"):
            fields[name] = (
                str | None,
                Field(
                    default=None,
                    pattern=pattern,
                    description="Inclusive YYYY-MM period." if kind == "month_range" else "Inclusive YYYY year.",
                ),
            )
    if kind in {"date", "classification", "day"}:
        fields["day" if kind == "day" else "date"] = (
            dateType | None,
            Field(default=None, description="Source snapshot date. Omit to use BaoStock's latest/default snapshot."),
        )
    if kind in {"quarter", "dividend"}:
        fields["year"] = (
            int | None,
            Field(default=None, ge=1, le=9999, description="Reporting year; BaoStock defaults to the current year."),
        )
    if kind == "quarter":
        fields["quarter"] = (
            int | None,
            Field(
                default=None,
                ge=1,
                le=4,
                description="Reporting quarter, 1 through 4; defaults to the source's current quarter.",
            ),
        )
    if kind == "dividend":
        fields["year_type"] = (
            Literal["report", "operate"],
            Field(
                default="report",
                description="Filter by proposal-announcement year (report) or ex-dividend year (operate).",
            ),
        )
    if kind == "reserve":
        fields["year_type"] = (
            Literal["0", "1"],
            Field(default="0", description="0 selects announcement date; 1 selects effective date."),
        )
    if kind == "basic":
        fields["code_name"] = (
            str | None,
            Field(default=None, description="Security name; BaoStock supports fuzzy name matching."),
        )
    if kind == "history":
        fields.update(
            fields=(
                str,
                Field(description="Comma-separated source fields, for example date,code,open,high,low,close,volume."),
            ),
            frequency=(
                Literal["d", "w", "m", "5", "15", "30", "60"],
                Field(default="d", description="BaoStock bar frequency."),
            ),
            adjustflag=(
                Literal["1", "2", "3"],
                Field(default="3", description="1 = backward adjusted; 2 = forward adjusted; 3 = unadjusted."),
            ),
        )
    return fields


def sdk_parameters(query: BaostockNativeQuery) -> dict[str, Any]:
    """Omit absent parameters and serialize source dates without changing periods."""
    return {
        "yearType" if key == "year_type" else key: value.isoformat() if isinstance(value, dateType) else value
        for key, value in query.model_dump(exclude_none=True).items()
    }


def make_fetcher(dataset: Dataset) -> type[Fetcher]:
    """Create independent query/data models for one explicitly registered API."""
    query_model = create_model(
        f"{dataset.model}QueryParams",
        __base__=BaostockNativeQuery,
        __module__=__name__,
        __doc__=dataset.description,
        **query_fields(dataset.parameters),
    )
    # Declare source identity fields so OpenBB can distinguish a single scalar
    # row from a model of arrays when converting to a dataframe. Absent columns
    # stay unset; source-selected history fields need not include an identifier.
    if dataset.method == "query_trade_dates":
        columns = ("calendar_date", "is_trading_day")
    elif dataset.module == "macroscopic.economic_data":
        columns = ("pubDate", "statYear", "statMonth")
    else:
        columns = ("code",)
    data_model = create_model(
        f"{dataset.model}Data",
        __base__=BaostockNativeData,
        __module__=__name__,
        **{
            name: (Any, Field(default=None, description=f"Source {name}, preserved without conversion."))
            for name in columns
        },
    )
    if dataset.parameters == "history":
        query_model.__json_schema_extra__ = {"fields": {"multiple_items_allowed": True}}

    class NativeFetcher(Fetcher[query_model, list[data_model]]):
        """Fetch a complete source dataset using the shared serialized SDK session."""

        require_credentials = False

        @staticmethod
        def transform_query(params: dict[str, Any]) -> Any:
            """Validate this dataset's explicit parameter schema."""
            return query_model(**params)

        @staticmethod
        async def aextract_data(query: Any, credentials: dict[str, str] | None, **kwargs: Any) -> list[dict[str, Any]]:
            """Read every page; a successful empty dataset remains an empty list."""
            return (await query_batch(dataset.method, [sdk_parameters(query)]))[0]

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

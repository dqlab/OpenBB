"""DuckDB Equity Historical Price Model."""

from datetime import date
from math import isfinite
from typing import Any, Literal
from warnings import warn

from openbb_core.app.model.abstract.error import OpenBBError
from openbb_core.provider.abstract.fetcher import Fetcher
from openbb_core.provider.standard_models.equity_historical import (
    EquityHistoricalData,
    EquityHistoricalQueryParams,
)
from openbb_core.provider.utils.descriptions import DATA_DESCRIPTIONS
from openbb_duckdb.utils.helpers import query_historical_data
from pydantic import Field


class DuckDBEquityHistoricalQueryParams(EquityHistoricalQueryParams):
    """DuckDB Equity Historical Price Query."""

    __json_schema_extra__ = {
        "symbol": {"multiple_items_allowed": True},
        "interval": {"choices": ["1d"]},
    }

    database_path: str | None = Field(
        default=None,
        description=("Path to the local DuckDB database. Overrides the configured duckdb_database_path credential."),
    )
    table: str = Field(
        default="equity_historical",
        min_length=1,
        description="Qualified DuckDB table or view containing equity prices.",
    )
    interval: Literal["1d"] = Field(default="1d", description="Daily bars; stored sessions are not resampled.")
    adjustment: Literal["splits_only", "splits_and_dividends", "unadjusted", "forward", "backward"] = Field(
        default="splits_only",
        description=(
            "Select stored split-only, total-adjusted, unadjusted, forward or backward OHLC; source basis must match."
        ),
    )
    include_actions: bool = Field(default=True, description="Include stored dividends and split ratios.")
    extended_hours: Literal[False] = Field(default=False, description="Daily regular-session bars only.")
    limit: int = Field(
        default=100000,
        ge=1,
        le=1000000,
        description="Maximum rows; overflow raises instead of truncating.",
    )


class DuckDBEquityHistoricalData(EquityHistoricalData):
    """DuckDB Equity Historical Price Data."""

    symbol: str | None = Field(
        default=None,
        description=DATA_DESCRIPTIONS.get("symbol", ""),
    )
    open: float | None = Field(default=None, description="Source opening price; may be absent during a suspension.")
    high: float | None = Field(default=None, description="Source high price; may be absent during a suspension.")
    low: float | None = Field(default=None, description="Source low price; may be absent during a suspension.")
    close: float | None = Field(default=None, description="Source close price; may be absent during a suspension.")
    prev_close: float | None = Field(default=None, description="Previous close in the selected source price basis.")
    trade_status: Literal[0, 1] | None = Field(default=None, description="Source flag: 0 suspended, 1 trading.")
    dividend: float | None = Field(
        default=None,
        description="Stored dividend amount, with source adjustment basis preserved.",
    )
    split_ratio: float | None = Field(
        default=None,
        description="Stored split ratio; zero denotes no split in the Yahoo source.",
    )
    price_adjustment: str | None = Field(default=None, description="Selected price adjustment mode.")


class DuckDBEquityHistoricalFetcher(
    Fetcher[
        DuckDBEquityHistoricalQueryParams,
        list[DuckDBEquityHistoricalData],
    ]
):
    """Fetch historical equity prices from DuckDB."""

    require_credentials = False

    @staticmethod
    def transform_query(
        params: dict[str, Any],
    ) -> DuckDBEquityHistoricalQueryParams:
        """Transform the query parameters."""
        from dateutil.relativedelta import relativedelta

        params = dict(params)
        if params.get("start_date") is None:
            params["start_date"] = date.today() - relativedelta(years=1)
        if params.get("end_date") is None:
            params["end_date"] = date.today()
        query = DuckDBEquityHistoricalQueryParams(**params)
        if query.start_date > query.end_date:
            raise OpenBBError("start_date must not exceed end_date.")
        return query

    @staticmethod
    def extract_data(
        query: DuckDBEquityHistoricalQueryParams,
        credentials: dict[str, str] | None,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        """Return historical equity prices from DuckDB."""
        return query_historical_data(
            database_path=query.database_path,
            table=query.table,
            symbol=query.symbol,
            start_date=query.start_date,
            end_date=query.end_date,
            credentials=credentials,
            required_columns={"open", "high", "low", "close"},
            limit=query.limit,
        )

    @staticmethod
    def transform_data(
        query: DuckDBEquityHistoricalQueryParams,
        data: list[dict[str, Any]],
        **kwargs: Any,
    ) -> list[DuckDBEquityHistoricalData]:
        """Transform the data to the standard format."""
        results = []
        seen = set()
        for record in data:
            item = dict(record)
            key = (str(item.get("symbol", "")).upper(), str(item["date"]))
            if key in seen:
                raise OpenBBError("Daily relation has duplicate symbol/date rows; select a single revision upstream.")
            seen.add(key)
            if item.get("trade_status") != 0 and any(item.get(f) is None for f in ("open", "high", "low", "close")):
                raise OpenBBError("Missing OHLC without an explicit source suspension flag.")
            item["prev_close"] = item.get("prev_close", item.get("preclose"))
            base = item.get("base_adjustment")
            if query.adjustment == "unadjusted" and base != "unadjusted":
                raise OpenBBError("The stored relation does not declare unadjusted source prices.")
            if query.adjustment == "splits_only" and base not in {None, "splits_only"}:
                raise OpenBBError("The stored relation is not split-only; choose its explicit source adjustment.")
            if query.adjustment in {"splits_and_dividends", "forward", "backward"}:
                prefix = "adjusted" if query.adjustment == "splits_and_dividends" else query.adjustment
                for field in ["open", "high", "low", "close"]:
                    column = prefix + "_" + field
                    value = item.get(column)
                    if prefix == "adjusted":
                        value = item.get(column, item.get("adj_" + field))
                    suspended_missing = (
                        prefix in {"forward", "backward"}
                        and column in item
                        and item.get("trade_status") == 0
                        and value is None
                    )
                    if not suspended_missing and (value is None or not isfinite(float(value)) or float(value) <= 0):
                        raise OpenBBError("Requested adjusted OHLC is unavailable; collect adjusted daily prices first.")
                    item[field] = value
                if prefix in {"forward", "backward"}:
                    item["prev_close"] = item.get(prefix + "_preclose")
                item["vwap"] = None  # A stored ordinary VWAP cannot inherit a different price adjustment.
            item["price_adjustment"] = query.adjustment
            if not query.include_actions:
                for name in [
                    "dividend",
                    "split_ratio",
                    "dividends",
                    "stock_splits",
                    "dividend_amount",
                    "split_coefficient",
                ]:
                    item.pop(name, None)
            results.append(DuckDBEquityHistoricalData.model_validate(item))
        requested = {s.strip().upper() for s in query.symbol.split(",")}
        found = {key[0] for key in seen}
        for symbol in sorted(requested - found):
            warn(f"Data for '{symbol}' was not found in DuckDB.")
        return results

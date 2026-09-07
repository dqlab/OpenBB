"""Explicitly bounded history and quote models for scheduled data acquisition."""

from datetime import date
from typing import Any, Literal

from openbb_core.provider.abstract.annotated_result import AnnotatedResult
from openbb_core.provider.abstract.data import Data
from openbb_core.provider.abstract.fetcher import Fetcher
from openbb_core.provider.abstract.query_params import QueryParams
from pydantic import AwareDatetime, ConfigDict, Field, StrictInt, model_validator

from openbb_ibkr.utils.bounded import bounded_history, bounded_quote
from openbb_ibkr.utils.client import IbkrClient

SEC_TYPES = {
    "stock": "STK",
    "etf": "STK",
    "index": "IND",
    "future": "FUT",
    "option": "OPT",
    "future_option": "FOP",
    "forex": "CASH",
    "bond": "BOND",
    "fund": "FUND",
    "crypto": "CRYPTO",
    "cfd": "CFD",
    "commodity": "CMDTY",
}
BAR_SIZES = {
    "1m": "1 min",
    "5m": "5 mins",
    "15m": "15 mins",
    "30m": "30 mins",
    "1h": "1 hour",
    "1d": "1 day",
}


class MarketContractQueryParams(QueryParams):
    """One explicit contract; derivatives require a previously qualified identifier."""

    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)
    symbol: str = Field(min_length=1, max_length=100, pattern=r"^[^,\s]+$")
    asset_type: str
    currency: str = Field(pattern=r"^[A-Z]{3}$")
    exchange: str = Field(default="SMART", min_length=1, max_length=80)
    primary_exchange: str | None = None
    con_id: StrictInt | None = Field(default=None, gt=0)
    host: str | None = None
    port: StrictInt | None = Field(default=None, ge=1, le=65535)
    client_id: StrictInt | None = Field(default=None, ge=0, le=2147483637)
    feed_type: Literal["live", "delayed"] = "live"

    @model_validator(mode="after")
    def valid_contract(self):
        if self.asset_type not in SEC_TYPES:
            raise ValueError("Unsupported asset type")
        if self.asset_type in {"future", "option", "future_option"} and not self.con_id:
            raise ValueError("Derivatives require an explicit qualified con_id")
        return self

    def contract_parameters(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "sec_type": SEC_TYPES[self.asset_type],
            "currency": self.currency,
            "exchange": self.exchange,
            "primary_exchange": self.primary_exchange,
            "con_id": self.con_id,
        }

    def configure(self, credentials: dict[str, str] | None) -> None:
        credentials = credentials or {}
        connection = {
            key: getattr(self, key) if getattr(self, key) is not None else credentials.get("ibkr_" + key)
            for key in ("host", "port", "client_id")
        }
        IbkrClient.configure(**connection, read_only=True, delayed=self.feed_type == "delayed")


class IbkrMarketQuoteQueryParams(MarketContractQueryParams):
    """Quote subscription wait, followed by cancellation."""

    snapshot_wait_seconds: float = Field(default=4, ge=0.7, le=30)


class IbkrMarketHistoricalQueryParams(MarketContractQueryParams):
    """Date-exclusive request with an explicit timezone-aware gateway end."""

    start_date: date
    end_date: date
    end_datetime: AwareDatetime
    interval: Literal["1m", "5m", "15m", "30m", "1h", "1d"] = "1d"
    what_to_show: Literal["TRADES", "MIDPOINT", "BID", "ASK"] = "TRADES"
    use_rth: bool = True
    timeout: float = Field(default=59, ge=0.1, le=300)

    @model_validator(mode="after")
    def valid_span(self):
        span = (self.end_date - self.start_date).days
        if not 1 <= span <= (1 if self.interval == "1m" else 7):
            raise ValueError("Historical span exceeds supported bar-size duration")
        if self.asset_type in {"option", "future_option"} and self.interval == "1d":
            raise ValueError("Option history supports intraday bars only")
        return self

    def request_parameters(self) -> dict[str, Any]:
        from datetime import timezone

        return {
            **self.contract_parameters(),
            "end": self.end_datetime.astimezone(timezone.utc).strftime("%Y%m%d-%H:%M:%S"),
            "duration": f"{(self.end_date - self.start_date).days + 1} D",
            "bar_size": BAR_SIZES[self.interval],
            "what_to_show": self.what_to_show,
            "use_rth": self.use_rth,
            "timeout": self.timeout,
        }


class IbkrMarketData(Data):
    """Provider records retain raw fields and qualified identity for reconciliation."""

    symbol: str
    asset_type: str
    currency: str
    con_id: int
    sec_type: str


def _transform(query: MarketContractQueryParams, response) -> AnnotatedResult[list[IbkrMarketData]]:
    rows = []
    for row in response.rows:
        # Both bounded endpoints must prove identity before returning records.
        if (
            row.get("symbol") != query.symbol
            or row.get("currency") != query.currency
            or row.get("sec_type") != SEC_TYPES[query.asset_type]
            or not isinstance(row.get("con_id"), int)
            or isinstance(row.get("con_id"), bool)
            or row["con_id"] <= 0
            or (query.con_id and row["con_id"] != query.con_id)
        ):
            raise ValueError("contract_identity_mismatch")
        rows.append(IbkrMarketData(**row, asset_type=query.asset_type))
    return AnnotatedResult(result=rows, metadata=response.metadata)


class IbkrMarketQuoteFetcher(Fetcher[IbkrMarketQuoteQueryParams, list[IbkrMarketData]]):
    """Bounded quote acquisition through the provider registry."""

    require_credentials = False
    close_collection = staticmethod(IbkrClient.disconnect)

    @staticmethod
    def transform_query(params: dict) -> IbkrMarketQuoteQueryParams:
        return IbkrMarketQuoteQueryParams(**params)

    @staticmethod
    def extract_data(query: IbkrMarketQuoteQueryParams, credentials: dict | None, **kwargs):
        query.configure(credentials)
        return bounded_quote(
            IbkrClient,
            {
                **query.contract_parameters(),
                "delayed": query.feed_type == "delayed",
            },
            query.snapshot_wait_seconds,
        )

    @staticmethod
    def transform_data(query: IbkrMarketQuoteQueryParams, data, **kwargs):
        return _transform(query, data)


class IbkrMarketHistoricalFetcher(Fetcher[IbkrMarketHistoricalQueryParams, list[IbkrMarketData]]):
    """Bounded history with provider-owned pacing span and overlap requirements."""

    require_credentials = False
    collection_max_days = {"1m": 1, "default": 7}
    collection_overlap_days = 1
    close_collection = staticmethod(IbkrClient.disconnect)

    @staticmethod
    def transform_query(params: dict) -> IbkrMarketHistoricalQueryParams:
        return IbkrMarketHistoricalQueryParams(**params)

    @staticmethod
    def extract_data(query: IbkrMarketHistoricalQueryParams, credentials: dict | None, **kwargs):
        query.configure(credentials)
        return bounded_history(IbkrClient, query.request_parameters())

    @staticmethod
    def transform_data(query: IbkrMarketHistoricalQueryParams, data, **kwargs):
        return _transform(query, data)

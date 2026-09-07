"""Bounded market-data requests owned by the IBKR provider.

These calls preserve the gateway's qualified identity and diagnostics without
exposing vendor message text. They never submit orders.
"""

from dataclasses import dataclass, field
from datetime import date, datetime
from math import isfinite
from typing import Any


@dataclass
class Response:
    rows: list[dict[str, Any]]
    metadata: dict[str, Any] = field(default_factory=dict)


def json_value(value: Any) -> Any:
    return value.isoformat() if isinstance(value, (date, datetime)) else value


def historical_error_category(message: str) -> str:
    """Classify vendor text without retaining credentials, URLs or host addresses."""
    message = message.lower()
    for fragment, category in (
        ("different ip", "competing_session"),
        ("competing", "competing_session"),
        ("permission", "missing_entitlement"),
        ("subscrib", "missing_entitlement"),
        ("subscription", "missing_entitlement"),
        ("pacing", "pacing"),
        ("no data", "no_data"),
        ("no security definition", "invalid_contract"),
        ("invalid", "invalid_request"),
    ):
        if fragment in message:
            return category
    return "historical_service_error"


def bounded_history(client: Any, request: dict[str, Any]) -> Response:
    """Use dqlab's contract and persistent event-loop helpers with an explicit end."""

    def collect() -> Response:
        ib = client._ensure_connected()
        codes: set[int] = set()
        categories: set[str] = set()

        def error(request_id: int, code: int, message: str = "", *_: Any) -> None:
            if request_id >= 0 and isinstance(code, int):
                codes.add(code)
                categories.add(historical_error_category(message))

        ib.errorEvent += error
        try:
            contract = client.build_contract(
                **{
                    k: request[k]
                    for k in (
                        "symbol",
                        "sec_type",
                        "currency",
                        "exchange",
                        "primary_exchange",
                        "con_id",
                    )
                }
            )
            contract = client._qualify_contract(ib, contract)
            identity = {
                "symbol": contract.symbol,
                "sec_type": contract.secType,
                "currency": contract.currency,
                "con_id": contract.conId,
            }
            if any(identity[k] != request[k] for k in ("symbol", "sec_type", "currency")):
                raise ValueError("contract_identity_mismatch")
            if request["con_id"]:
                if identity["con_id"] != request["con_id"]:
                    raise ValueError("contract_identity_mismatch")
            elif identity["symbol"] != request["symbol"]:
                raise ValueError("contract_identity_mismatch")
            bars = ib.reqHistoricalData(
                contract,
                endDateTime=request["end"],
                durationStr=request["duration"],
                barSizeSetting=request["bar_size"],
                whatToShow=request["what_to_show"],
                useRTH=request["use_rth"],
                formatDate=2,
                keepUpToDate=False,
                timeout=request["timeout"],
            )
            rows = [
                {
                    **identity,
                    "date": json_value(bar.date),
                    "open": bar.open,
                    "high": bar.high,
                    "low": bar.low,
                    "close": bar.close,
                    "volume": bar.volume,
                    "wap": getattr(bar, "wap", getattr(bar, "average", None)),
                    "bar_count": bar.barCount,
                }
                for bar in bars
            ]
            return Response(
                rows,
                metadata={
                    "adapter": "dqlab_client_bounded_historical",
                    "qualified_contract": identity,
                    "gateway_error_codes": sorted(codes),
                    "gateway_error_categories": sorted(categories),
                    "gateway_error_scope": "provider_call_interval",
                    "historical_publication_time": "unknown",
                },
            )
        finally:
            ib.errorEvent -= error

    return client._run(collect)


MARKET_DATA_TYPES = {1: "live", 2: "frozen", 3: "delayed", 4: "delayed_frozen"}


def quote_record(row: dict[str, Any], ticker: Any) -> dict[str, Any]:
    """Keep decoded evidence while representing IBKR's unavailable quotes as null.

    The ticker's marketDataType is reset before subscription callbacks are pumped;
    its default value of 1 must never be used as evidence of live delivery.
    """
    raw = dict(row)
    for name, attribute in (("bid_size", "bidSize"), ("ask_size", "askSize")):
        size = getattr(ticker, attribute, None)
        raw[name] = float(size) if type(size) in (int, float) and isfinite(size) else None
    result = dict(row)
    unavailable = []
    for name in ("bid", "ask"):
        # Futures can trade at negative prices. A -1 price with positive size
        # remains a price; the zero-size sentinel denotes an unavailable quote.
        if raw.get(name) == -1 and (row.get("sec_type") not in {"FUT", "FOP", "CMDTY"} or raw[name + "_size"] == 0):
            result[name] = None
            unavailable.append(name)
    observed = getattr(ticker, "marketDataType", None)
    if type(observed) is not int or observed not in MARKET_DATA_TYPES:
        observed = None
    return {
        **result,
        "raw_quote": raw,
        "market_data_type": observed,
        "feed_type": MARKET_DATA_TYPES.get(observed, "unknown"),
        "unavailable_fields": unavailable,
    }


def bounded_quote(client: Any, request: dict[str, Any], wait_seconds: float) -> Response:
    """Bounded quote acquisition using the pinned dqlab client's own event loop.

    Its public quote helper cancels after 0.7s, before some gateways deliver data.
    Reuse its contract/normalization helpers with a configurable subscription wait.
    Error codes cover this call interval, including possible earlier callbacks.
    """

    def collect() -> Response:
        ib = client._ensure_connected()
        codes: set[int] = set()

        def error(request_id: int, code: int, *_: Any) -> None:
            if request_id >= 0 and isinstance(code, int):
                codes.add(code)

        ib.errorEvent += error
        contract = None
        subscribed = False
        try:
            delayed = bool(request.get("delayed", False))
            ib.reqMarketDataType(3 if delayed else 1)
            contract = client.build_contract(
                **{
                    key: value
                    for key, value in request.items()
                    if key in {"symbol", "sec_type", "exchange", "currency", "primary_exchange", "con_id"}
                }
            )
            contract = client._qualify_contract(ib, contract)
            ticker = ib.reqMktData(contract, "", False, False)
            subscribed = True
            # reqMktData sends the request synchronously; callbacks are pumped
            # by sleep below. Clear the library's default or cached feed type.
            ticker.marketDataType = 0
            ib.sleep(wait_seconds)
            row = quote_record(client._normalise_quote(contract, ticker, delayed), ticker)
            return Response(
                rows=[row],
                metadata={
                    "gateway_error_codes": sorted(codes),
                    "gateway_error_scope": "provider_call_interval",
                    "adapter": "dqlab_client_bounded_quote",
                    "snapshot_wait_seconds": wait_seconds,
                    "quote_schema_version": 2,
                    "feed_type_basis": "gateway_callback" if row["market_data_type"] else "unknown",
                    "unavailable_fields": row["unavailable_fields"],
                    "size_unit": "provider_defined",
                },
            )
        finally:
            try:
                if subscribed:
                    ib.cancelMktData(contract)
            finally:
                ib.errorEvent -= error

    return client._run(collect)

"""Bounded subscriptions sharing one IB connection, event loop and cutoff."""

import contextlib
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from .bounded import Response, quote_ready, quote_record

# ib_insync TickData stores local packet receipt, not exchange event time.
TICK_FIELDS = {
    0: "bid_size",
    1: "bid",
    2: "ask",
    3: "ask_size",
    4: "last",
    5: "last_size",
    8: "volume",
    9: "close",
    66: "bid",
    67: "ask",
    68: "last",
    69: "bid_size",
    70: "ask_size",
    71: "last_size",
    74: "volume",
    75: "close",
}
CONNECTION_ERRORS = {1100, 1101, 1102, 1300, 2110}


def bounded_quotes_batch(client: Any, requests: list[dict[str, Any]], wait_seconds: float) -> list[Response]:
    """Qualify first, subscribe at <=20/s, freeze fresh updates at a common cutoff.

    Results retain request order; incomplete contracts never borrow another
    contract's values or error codes. All subscriptions are cancelled on exit.
    """
    if not 1 <= len(requests) <= 40 or not 0.7 <= wait_seconds <= 30:
        raise ValueError("invalid_batch_limits")
    delayed = bool(requests[0].get("delayed", False))
    if any(bool(request.get("delayed", False)) != delayed for request in requests):
        raise ValueError("inconsistent_batch_feed")

    def collect() -> list[Response]:
        ib = client._ensure_connected()
        batch_id = uuid.uuid4().hex
        started = datetime.now(timezone.utc)
        states = [{"codes": set(), "fields": {}, "row": None, "status": "no_fresh_quote"} for _ in requests]
        subscriptions = []
        by_request = {}
        connection_codes = set()
        disconnected = False
        cutoff_mono = None

        def error(request_id, code, *_):
            nonlocal disconnected
            if type(code) is not int:
                return
            if code in CONNECTION_ERRORS:
                connection_codes.add(code)
                disconnected = True
            elif request_id in by_request:
                by_request[request_id]["codes"].add(code)

        def disconnect(*_):
            nonlocal disconnected
            disconnected = True

        ib.errorEvent += error
        ib.disconnectedEvent += disconnect
        try:
            contracts = [
                client.build_contract(
                    **{
                        key: value
                        for key, value in request.items()
                        if key in {"symbol", "sec_type", "exchange", "currency", "primary_exchange", "con_id"}
                    }
                )
                for request in requests
            ]
            # ib_insync qualifies concurrently using its existing request throttle.
            qualified = ib.qualifyContracts(*contracts)
            qualified_ids = {id(contract) for contract in qualified}
            identities = set()
            ib.reqMarketDataType(3 if delayed else 1)
            subscription_start = datetime.now(timezone.utc)
            for index, (contract, request, state) in enumerate(zip(contracts, requests, states, strict=True)):
                identity = (contract.symbol, contract.secType, contract.currency, contract.conId)
                expected = (request["symbol"], request["sec_type"], request["currency"])
                if (
                    id(contract) not in qualified_ids
                    or identity[:3] != expected
                    or not contract.conId
                    or (request.get("con_id") and contract.conId != request["con_id"])
                ):
                    state["status"] = "contract_qualification_failed"
                    continue
                if contract.conId in identities:
                    state["status"] = "duplicate_contract"
                    continue
                identities.add(contract.conId)
                if disconnected or not ib.isConnected():
                    disconnected = True
                    break
                try:
                    ticker = ib.reqMktData(contract, "", False, False)
                except Exception:
                    state["status"] = "subscription_failed"
                    with contextlib.suppress(Exception):
                        ib.cancelMktData(contract)
                    continue
                # Fresh contract objects normally create fresh tickers, but do
                # not depend on library defaults or cached subscription values.
                for name in ("bid", "ask", "last", "bidSize", "askSize", "lastSize", "close", "volume"):
                    setattr(ticker, name, float("nan"))
                ticker.time = None
                ticker.marketDataType = 0
                ticker.ticks.clear()
                request_id = ib.wrapper.ticker2ReqId["mktData"][ticker]
                by_request[request_id] = state
                state["subscription_requested_at"] = datetime.now(timezone.utc).isoformat()
                state["request_id"] = request_id

                def update(ticker, state=state, contract=contract):
                    if cutoff_mono is not None and time.monotonic() > cutoff_mono:
                        return
                    for tick in ticker.ticks:
                        field = TICK_FIELDS.get(tick.tickType)
                        if field:
                            state["fields"][field] = tick.time.isoformat()
                    state["row"] = quote_record(client._normalise_quote(contract, ticker, delayed), ticker)

                ticker.updateEvent += update
                subscriptions.append((contract, ticker, update))
                # Pace subscription messages, not one full observation wait per
                # contract. Existing IB client throttling also remains enabled.
                if index < len(contracts) - 1:
                    ib.sleep(0.05)
            subscribed_at = datetime.now(timezone.utc)
            cutoff_mono = time.monotonic() + wait_seconds
            cutoff_at = subscribed_at + timedelta(seconds=wait_seconds)
            while not disconnected and ib.isConnected():
                remaining = cutoff_mono - time.monotonic()
                if remaining <= 0:
                    break
                ib.sleep(min(0.05, remaining))
            if not ib.isConnected():
                disconnected = True
            completed_at = datetime.now(timezone.utc)
            results = []
            for request, state in zip(requests, states, strict=True):
                row = state["row"]
                required = {"bid", "ask"} if request["sec_type"] in {"FUT", "FOP", "CMDTY"} else {"last"}
                ready = bool(row and quote_ready(row) and required <= state["fields"].keys())
                status = "disconnected" if disconnected else "complete" if ready else state["status"]
                # Empty/partial results are archived with evidence and use the
                # collector's normal retry policy; no previous quote is reused.
                results.append(
                    Response(
                        rows=[row] if ready and not disconnected else [],
                        metadata={
                            "adapter": "dqlab_client_bounded_quote_batch",
                            "batch_id": batch_id,
                            "batch_size": len(requests),
                            "batch_started_at": started.isoformat(),
                            "batch_subscription_started_at": subscription_start.isoformat(),
                            "batch_subscribed_at": subscribed_at.isoformat(),
                            "batch_cutoff_at": cutoff_at.isoformat(),
                            "batch_completed_at": completed_at.isoformat(),
                            "batch_status": status,
                            "snapshot_wait_seconds": wait_seconds,
                            "subscription_requested_at": state.get("subscription_requested_at"),
                            "gateway_request_id": state.get("request_id"),
                            "gateway_error_codes": sorted(state["codes"]),
                            "gateway_error_scope": "subscription_request_id",
                            "connection_error_codes": sorted(connection_codes),
                            "field_received_at": state["fields"],
                            "field_timestamp_semantics": "provider_receive",
                            "field_age_at_cutoff_seconds": {
                                name: (cutoff_at - datetime.fromisoformat(receipt)).total_seconds()
                                for name, receipt in state["fields"].items()
                            },
                            "partial_quote": row if not ready else None,
                            "quote_schema_version": 2,
                            "feed_type_basis": "gateway_callback" if row and row["market_data_type"] else "unknown",
                            "actual_feed_type": row["feed_type"] if row else "unknown",
                            "size_unit": "provider_defined",
                            "event_time": None,
                        },
                    )
                )
            return results
        finally:
            for contract, ticker, callback in subscriptions:
                ticker.updateEvent -= callback
                with contextlib.suppress(Exception):
                    ib.cancelMktData(contract)
            ib.errorEvent -= error
            ib.disconnectedEvent -= disconnect

    return client._run(collect)

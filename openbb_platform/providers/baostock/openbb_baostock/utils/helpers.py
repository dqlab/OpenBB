"""BaoStock SDK sessions, result validation, and shared normalization."""

import asyncio
import logging
import re
from datetime import date, datetime, timedelta
from threading import Lock
from typing import Any
from zoneinfo import ZoneInfo

import baostock as bs
from baostock.common import context
from baostock.common.contants import BAOSTOCK_PER_PAGE_COUNT
from openbb_core.app.model.abstract.error import OpenBBError
from openbb_core.provider.utils.errors import EmptyDataError

from openbb_baostock.utils.catalog import resolve_method

INTERVALS = {"1d": "d", "1W": "w", "1M": "m", "5m": "5", "15m": "15", "30m": "30", "60m": "60"}
ADJUSTMENTS = {"unadjusted": "3", "forward": "2", "backward": "1"}
SHANGHAI = ZoneInfo("Asia/Shanghai")
_SESSION_LOCK = Lock()
_LOGGER = logging.getLogger(__name__)


def normalize_symbol(symbol: str) -> str:
    """Preserve the exchange instead of guessing ambiguous numeric tickers."""
    value = symbol.strip().upper()
    if re.fullmatch(r"(SH|SZ)\.[0-9]{6}", value):
        return value
    match = re.fullmatch(r"([0-9]{6})\.(SH|SS|SZ)", value)
    if match:
        code, exchange = match.groups()
        return f"{'SH' if exchange == 'SS' else exchange}.{code}"
    raise ValueError("Use an exchange-qualified BaoStock symbol, such as sh.600000, sz.000001, or 600000.SH.")


def normalize_symbols(value: str) -> str:
    """Normalize and deduplicate a comma-separated symbol list in request order."""
    return ",".join(dict.fromkeys(normalize_symbol(symbol) for symbol in value.split(",")))


def with_default_dates(params: dict[str, Any]) -> dict[str, Any]:
    """Default to one year ending at the requested end date or today's Shanghai date."""
    result = dict(params)
    end = result.get("end_date") or datetime.now(SHANGHAI).date()
    if isinstance(end, str):
        end = date.fromisoformat(end)
    result["end_date"] = end
    result["start_date"] = result.get("start_date") or end - timedelta(days=365)
    return result


def check_result(result: Any, operation: str) -> None:
    """Raise provider errors, including failures that occur during pagination."""
    code = getattr(result, "error_code", None)
    if code != "0":
        message = getattr(result, "error_msg", "No response from BaoStock")
        raise OpenBBError(f"BaoStock {operation} failed ({code}): {message}")


def read_result(result: Any, operation: str) -> list[dict[str, Any]]:
    """Read all pages without silently returning a truncated or malformed response."""
    check_result(result, operation)
    fields = list(result.fields)
    if not fields or len(set(fields)) != len(fields):
        raise OpenBBError(f"BaoStock {operation} returned invalid field names.")
    records = []
    while True:
        has_next = result.next()
        check_result(result, operation)
        if not has_next:
            # SDK 0.9.3 leaves error_code at zero when a page transport fails.
            # A successfully exhausted final page is short or has been replaced
            # with an empty page. A full, unchanged page means data was truncated.
            if len(getattr(result, "data", [])) == BAOSTOCK_PER_PAGE_COUNT:
                raise OpenBBError(f"BaoStock {operation} stopped at a full page; response may be incomplete.")
            break
        values = result.get_row_data()
        check_result(result, operation)
        if len(values) != len(fields):
            raise OpenBBError(f"BaoStock {operation} returned a row with unexpected field count.")
        records.append(dict(zip(fields, values)))
    return records


def _query_batch(method: str, requests: list[dict[str, Any]]) -> list[list[dict[str, Any]]]:
    """Serialize the SDK's process-global connection for the entire session."""
    with _SESSION_LOCK:
        logged_in = False
        try:
            check_result(bs.login(), "login")
            logged_in = True
            return [read_result(resolve_method(method)(**params), method) for params in requests]
        except OSError as exc:
            raise OpenBBError(f"BaoStock connection failed: {exc}") from exc
        finally:
            try:
                if logged_in:
                    check_result(bs.logout(), "logout")
            except Exception as exc:  # SDK cleanup must preserve the original error.
                _LOGGER.warning("BaoStock logout failed: %s", exc)
            finally:
                # The SDK can return early from logout without closing its socket.
                connection = getattr(context, "default_socket", None)
                if connection is not None:
                    try:
                        connection.close()
                    except OSError:
                        _LOGGER.debug("BaoStock socket was already closed.", exc_info=True)
                    context.default_socket = None


async def query_batch(method: str, requests: list[dict[str, Any]]) -> list[list[dict[str, Any]]]:
    """Run blocking SDK work off the event loop; cancellation retains the session lock."""
    return await asyncio.to_thread(_query_batch, method, requests)


def historical_fields(interval: str, is_index: bool = False) -> str:
    """Select only fields supported at the requested frequency."""
    if interval.endswith("m"):
        return "date,time,code,open,high,low,close,volume,amount,adjustflag"
    fields = "date,code,open,high,low,close,volume,amount,adjustflag"
    if interval == "1d":
        fields += ",preclose,pctChg"
        if not is_index:
            fields += ",turn,tradestatus,peTTM,pbMRQ,psTTM,pcfNcfTTM,isST"
    elif not is_index:
        fields += ",turn,pctChg"
    return fields


async def fetch_history(query: Any, is_index: bool = False) -> list[dict[str, Any]]:
    """Fetch a bounded date window for each symbol in one SDK session."""
    symbols = query.symbol.split(",")
    requests = [
        {
            "code": symbol.lower(),
            "fields": historical_fields(query.interval, is_index),
            "start_date": query.start_date.isoformat(),
            "end_date": query.end_date.isoformat(),
            "frequency": INTERVALS[query.interval],
            "adjustflag": ADJUSTMENTS[query.adjustment],
        }
        for symbol in symbols
    ]
    batches = await query_batch("query_history_k_data_plus", requests)
    records = []
    for symbol, batch in zip(symbols, batches):
        if not batch:
            raise EmptyDataError(f"No BaoStock history for {symbol} in the requested date range.")
        for row in batch:
            if normalize_symbol(row.get("code", "")) != symbol:
                raise OpenBBError(f"BaoStock returned an unexpected symbol for {symbol}.")
        records.extend(batch)
    return records


def transform_history(query: Any, data: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Preserve missing values, adjustments, and Shanghai intraday timestamps."""
    records = []
    seen = set()
    for raw in data:
        row = {key: None if value == "" else value for key, value in raw.items()}
        symbol = normalize_symbol(row["code"])
        day = date.fromisoformat(row["date"])
        if not query.start_date <= day <= query.end_date:
            raise OpenBBError(f"BaoStock returned a date outside the requested range: {day}.")
        timestamp: date | datetime = day
        if query.interval.endswith("m"):
            timestamp = datetime.strptime(row.pop("time"), "%Y%m%d%H%M%S%f").replace(tzinfo=SHANGHAI)
            if timestamp.date() != day:
                raise OpenBBError("BaoStock intraday time does not match its trading date.")
        key = (symbol, timestamp)
        if key in seen:
            raise OpenBBError(f"BaoStock returned duplicate history for {symbol} at {timestamp}.")
        seen.add(key)
        flag = row.pop("adjustflag", None)
        if flag != ADJUSTMENTS[query.adjustment]:
            raise OpenBBError(f"BaoStock returned an unexpected adjustment flag: {flag}.")
        row.update(date=timestamp, code=symbol, adjustment=query.adjustment)
        for field in ("pctChg", "turn"):
            if row.get(field) is not None:
                row[field] = float(row[field]) / 100
        records.append(row)
    return sorted(records, key=lambda item: (item["date"], item["code"]))


def transform_stock(row: dict[str, Any]) -> dict[str, Any]:
    """Map basic stock identifiers without fabricating unavailable profile fields."""
    result = {key: None if value == "" else value for key, value in row.items()}
    result["code"] = normalize_symbol(result["code"])
    result["stock_exchange"] = "SSE" if result["code"].startswith("SH.") else "SZSE"
    return result

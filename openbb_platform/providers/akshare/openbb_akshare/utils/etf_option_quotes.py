"""Bounded batch transport for the Sina quote source used by AKShare.

AKShare's single-contract parser labels only the first 43 fields. Preserve the
complete response (including the observed appended expiration/type fields) and
never silently truncate, skip a missing symbol, or fabricate an empty quote.
"""

from __future__ import annotations

import csv
import math
import re
from datetime import datetime, timezone
from io import StringIO
from typing import Any
from zoneinfo import ZoneInfo

from openbb_core.provider.utils.helpers import make_request

SHANGHAI = ZoneInfo("Asia/Shanghai")
SYMBOL = re.compile(r"[0-9]{8}")
ASSIGNMENT = re.compile(r'var\s+hq_str_CON_OP_([0-9]{8})\s*=\s*"([^"\r\n]*)"\s*;')
FIELDS = (
    "买量",
    "买价",
    "最新价",
    "卖价",
    "卖量",
    "持仓量",
    "涨幅",
    "行权价",
    "昨收价",
    "开盘价",
    "涨停价",
    "跌停价",
    "申卖价五",
    "申卖量五",
    "申卖价四",
    "申卖量四",
    "申卖价三",
    "申卖量三",
    "申卖价二",
    "申卖量二",
    "申卖价一",
    "申卖量一",
    "申买价一",
    "申买量一 ",
    "申买价二",
    "申买量二",
    "申买价三",
    "申买量三",
    "申买价四",
    "申买量四",
    "申买价五",
    "申买量五",
    "行情时间",
    "主力合约标识",
    "状态码",
    "标的证券类型",
    "标的股票",
    "期权合约简称",
    "振幅",
    "最高价",
    "最低价",
    "成交量",
    "成交额",
)
NUMERIC = {
    "bid": "买价",
    "ask": "卖价",
    "bid_size": "买量",
    "ask_size": "卖量",
    "last": "最新价",
    "open_interest": "持仓量",
    "strike": "行权价",
    "volume": "成交量",
    "turnover": "成交额",
    "open": "开盘价",
    "high": "最高价",
    "low": "最低价",
    "previous_close": "昨收价",
}
LEVELS = {1: "一", 2: "二", 3: "三", 4: "四", 5: "五"}
for _level, _label in LEVELS.items():
    NUMERIC.update(
        {
            f"bid_{_level}": f"申买价{_label}",
            f"ask_{_level}": f"申卖价{_label}",
            f"bid_size_{_level}": f"申买量{_label}" + (" " if _level == 1 else ""),
            f"ask_size_{_level}": f"申卖量{_label}",
        }
    )


def validate_symbols(value: str) -> list[str]:
    """Reject malformed/duplicate tokens before making any network request."""
    if not isinstance(value, str):
        raise ValueError("symbols must be a comma-separated string")
    symbols = value.split(",")
    if not 1 <= len(symbols) <= 50 or any(not SYMBOL.fullmatch(s) for s in symbols):
        raise ValueError("Expected 1..50 exact eight-digit option symbols")
    if len(set(symbols)) != len(symbols):
        raise ValueError("Duplicate option symbols")
    return symbols


def number(value: str) -> float | None:
    """Preserve documented absent-side sentinels, reject nonfinite/malformed data."""
    if value in {"", "-", "--"}:
        return None
    result = float(value)
    if not math.isfinite(result):
        raise ValueError("nonfinite_quote")
    return result


def parse_quote(symbol: str, payload: str, captured_at: str) -> dict[str, Any]:
    """Return a diagnostic row on schema failures, retaining the full raw payload."""
    row: dict[str, Any] = {
        "symbol": symbol,
        "quote_source": "sina",
        "fetched_at": captured_at,
        "raw_payload": payload,
        "quote_error": None,
    }
    if not payload:
        row["quote_error"] = "empty_quote"
        return row
    try:
        values = next(csv.reader(StringIO(payload), strict=True))
        row["source_field_count"] = len(values)
        # Both the SDK-prefix layout and the currently observed appended layout
        # are explicit. New/truncated layouts fail closed, never zip-truncate.
        if len(values) not in {43, 51}:
            raise ValueError("unexpected_quote_width")
        fields = dict(zip(FIELDS, values[:43], strict=True))
        row["raw_fields"] = fields
        row["raw_extra_fields"] = values[43:]
        row.update({key: number(fields[name]) for key, name in NUMERIC.items()})
        stamp = datetime.strptime(fields["行情时间"], "%Y-%m-%d %H:%M:%S")
        row["source_time"] = stamp.replace(tzinfo=SHANGHAI).isoformat()
        underlying = fields["标的股票"]
        if not re.fullmatch(r"[0-9]{6}", underlying):
            raise ValueError("invalid_quote_underlying")
        row.update(
            {
                "underlying": underlying,
                "contract_name": fields["期权合约简称"],
                "source_status": fields["状态码"],
            }
        )
        # Appended fields are retained as opaque source evidence until specified.
    except (ValueError, csv.Error, StopIteration) as exc:
        # The raw source remains available even if parsing failed part way through.
        row["quote_error"] = (
            str(exc)
            if str(exc)
            in {
                "unexpected_quote_width",
                "nonfinite_quote",
                "invalid_quote_underlying",
                "invalid_quote_option_type",
                "invalid_quote_expiration",
            }
            else "malformed_quote"
        )
    return row


def parse_batch(text: str, symbols: list[str], captured_at: str) -> list[dict[str, Any]]:
    """Account for every requested contract and reject unrecognized response text."""
    matches = list(ASSIGNMENT.finditer(text))
    residue = ASSIGNMENT.sub("", text).strip()
    if residue:
        raise ValueError("unexpected_quote_response")
    found: dict[str, str] = {}
    for match in matches:
        symbol, payload = match.groups()
        if symbol not in symbols or symbol in found:
            raise ValueError("unexpected_or_duplicate_quote_symbol")
        found[symbol] = payload
    rows = []
    for symbol in symbols:
        if symbol not in found:
            rows.append(
                {
                    "symbol": symbol,
                    "quote_source": "sina",
                    "fetched_at": captured_at,
                    "quote_error": "missing_quote",
                    "raw_payload": None,
                }
            )
        else:
            rows.append(parse_quote(symbol, found[symbol], captured_at))
    return rows


def fetch_quotes(symbols: str) -> list[dict[str, Any]]:
    """Batch the identical source behind AKShare's option_sse_spot_price_sina."""
    tokens = validate_symbols(symbols)
    response = make_request(
        "https://hq.sinajs.cn/list=" + ",".join("CON_OP_" + s for s in tokens),
        headers={"Referer": "https://stock.finance.sina.com.cn/", "User-Agent": "Mozilla/5.0"},
        timeout=25,
    )
    response.raise_for_status()
    if len(response.content) > 1_000_000:
        raise ValueError("quote_response_byte_limit")
    text = response.content.decode("gb18030", errors="strict")
    stamp = datetime.now(timezone.utc).isoformat()
    return parse_batch(text, tokens, stamp)

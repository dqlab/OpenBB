# OpenBB and collector callback signatures retain protocol parameters.
# pylint: disable=unused-argument

"""ETF option models and self-contained source adapters.

Current source universes retain exact identifiers, nullable unobserved contract
terms and raw fields. Quote transport and master transport use separate OpenBB
HTTP helpers so acquisition can capture each response before decoding it.
"""

from __future__ import annotations

import asyncio
import csv
import json
import math
import re
from datetime import date, datetime, timezone
from io import BytesIO, StringIO
from typing import Any, Literal
from zipfile import BadZipFile, ZipFile
from zoneinfo import ZoneInfo

import pandas as pd
from openbb_core.provider.abstract.data import Data
from openbb_core.provider.abstract.fetcher import Fetcher
from openbb_core.provider.abstract.query_params import QueryParams
from openbb_core.provider.utils.helpers import make_request
from pydantic import ConfigDict, Field, field_validator

make_contract_request = make_request
make_quote_request = make_request

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
        if len(values) == 51:
            if values[45] not in {"C", "P"}:
                raise ValueError("invalid_quote_option_type")
            if not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", values[46]):
                raise ValueError("invalid_quote_expiration")
            row["expiration"] = date.fromisoformat(values[46]).isoformat()
            row["option_type"] = "call" if values[45] == "C" else "put"
            row["adjustment_marker"] = values[43]
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


def parse_batch(
    text: str, symbols: list[str], captured_at: str
) -> list[dict[str, Any]]:
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
    response = make_quote_request(
        "https://hq.sinajs.cn/list=" + ",".join("CON_OP_" + s for s in tokens),
        headers={
            "Referer": "https://stock.finance.sina.com.cn/",
            "User-Agent": "Mozilla/5.0",
        },
        timeout=25,
    )
    response.raise_for_status()
    if len(response.content) > 1_000_000:
        raise ValueError("quote_response_byte_limit")
    text = response.content.decode("gb18030", errors="strict")
    stamp = datetime.now(timezone.utc).isoformat()
    return parse_batch(text, tokens, stamp)


SSE_CATEGORIES = {
    "510050": "50ETF",
    "510300": "300ETF",
    "510500": "500ETF",
    "588000": "科创50",
    "588080": "科创板50",
}


SSE_URL = "https://query.sse.com.cn/commonQuery.do"


SZSE_URL = "https://www.sse.org.cn/api/report/ShowReport"


SINA_API = (
    "https://stock.finance.sina.com.cn/futures/api/openapi.php/StockOptionService."
)


SINA_HEADERS = {
    "Referer": "https://stock.finance.sina.com.cn/",
    "User-Agent": "Mozilla/5.0",
}


JSON_BYTE_LIMIT = 4_000_000


XLSX_BYTE_LIMIT = 8_000_000


DISCOVERY_BYTE_LIMIT = 100_000


MAX_CONTRACTS = 5_000


TYPE_MAP = {
    "C": "call",
    "P": "put",
    "认购": "call",
    "认沽": "put",
    "购": "call",
    "沽": "put",
}


def _request(url: str, *, limit: int, **kwargs: Any) -> bytes:
    """Bound every request and propagate HTTP errors without source substitution."""
    response = make_contract_request(url, timeout=25, **kwargs)
    response.raise_for_status()
    if len(response.content) > limit:
        raise ValueError("contract_response_byte_limit")
    return response.content


def _json_request(url: str, **kwargs: Any) -> dict[str, Any]:
    """Decode JSON without executing vendor JavaScript or ignoring broken bytes."""
    value = json.loads(_request(url, limit=JSON_BYTE_LIMIT, **kwargs).decode("utf-8"))
    if not isinstance(value, dict):
        raise ValueError("invalid_contract_json")
    return value


def _identifier(value: Any, width: int) -> str:
    if not isinstance(value, str) or not re.fullmatch(rf"[0-9]{{{width}}}", value):
        raise ValueError("invalid_contract_identifier")
    return value


def _underlying(value: Any) -> str:
    """Only accept one source-explicit six-digit identifier, never a trade-code guess."""
    if not isinstance(value, str):
        raise ValueError("invalid_source_underlying")
    found = re.findall(r"(?<![0-9])[0-9]{6}(?![0-9])", value)
    if len(found) != 1:
        raise ValueError("ambiguous_source_underlying")
    return found[0]


def _number(value: Any, *, nullable: bool = False) -> float | None:
    if value is None or value in ("", "-", "--"):
        if nullable:
            return None
        raise ValueError("missing_contract_number")
    if isinstance(value, bool):
        raise ValueError("invalid_contract_number")
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("invalid_contract_number") from exc
    if not math.isfinite(result) or result <= 0:
        raise ValueError("nonfinite_or_nonpositive_contract_number")
    return result


def _date(value: Any, *, nullable: bool = False) -> str | None:
    """Explicit date layouts only; no month-to-expiry/calendar approximation."""
    if value is None or value == "":
        if nullable:
            return None
        raise ValueError("missing_contract_date")
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, str):
        if re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", value):
            return date.fromisoformat(value).isoformat()
        if re.fullmatch(r"[0-9]{8}", value):
            return datetime.strptime(value, "%Y%m%d").date().isoformat()
    raise ValueError("invalid_contract_date")


def _trade_code(value: Any, underlying: str, option_type: str) -> str | None:
    if value in (None, "", "-", "--"):
        return None
    if not isinstance(value, str) or value != value.strip():
        raise ValueError("invalid_contract_trade_code")
    # Cross-check an explicit code's identity, never use its encoded strike/unit.
    match = re.match(r"([0-9]{6})([CP])", value)
    if match and (match[1] != underlying or TYPE_MAP[match[2]] != option_type):
        raise ValueError("conflicting_contract_trade_code")
    return value


def _normalized(
    row: dict[str, Any], *, exchange: str, keys: dict[str, str]
) -> dict[str, Any]:
    required = ("symbol", "underlying", "option_type", "strike", "expiration")
    if any(keys[key] not in row for key in required):
        raise ValueError("missing_contract_fields")
    underlying = _underlying(row[keys["underlying"]])
    subtype = row[keys["option_type"]]
    if not isinstance(subtype, str) or subtype not in TYPE_MAP:
        raise ValueError("invalid_contract_option_type")
    option_type = TYPE_MAP[subtype]
    unit = _number(row.get(keys["contract_unit"]), nullable=True)
    return {
        "symbol": _identifier(row[keys["symbol"]], 8),
        "exchange": exchange,
        "underlying": underlying,
        "option_type": option_type,
        "strike": _number(row[keys["strike"]]),
        "expiration": _date(row[keys["expiration"]]),
        "contract_unit": unit,
        "contract_trade_code": _trade_code(
            row.get(keys["contract_trade_code"]), underlying, option_type
        ),
        "metadata_source": "official",
        "metadata_origin": "sse_official" if exchange == "SSE" else "szse_official",
        "source_date": _date(row.get(keys["source_date"]), nullable=exchange == "SSE"),
        "source_row": dict(row),
        "universe_scope": f"{exchange.lower()}_official_current_contracts",
        # A successful source response is not independent exchange-universe verification.
        "official_universe_verified": False,
        "metadata_complete": unit is not None,
    }


def _validate_master(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not rows or len(rows) > MAX_CONTRACTS:
        raise ValueError("empty_or_oversize_contract_universe")
    symbols = [row["symbol"] for row in rows]
    if len(set(symbols)) != len(symbols):
        raise ValueError("duplicate_contract_identifier")
    dates = {
        row["source_date"]
        for row in rows
        if row["metadata_source"] == "official" and row["source_date"] is not None
    }
    if len(dates) > 1:
        raise ValueError("mixed_contract_source_dates")
    return rows


def parse_sse_official(value: dict[str, Any]) -> list[dict[str, Any]]:
    """Map source-named SSE headers, not positional SDK rename guesses."""
    raw = value.get("result")
    if not isinstance(raw, list) or not all(isinstance(row, dict) for row in raw):
        raise ValueError("invalid_sse_contract_result")
    keys = {
        "symbol": "SECURITY_ID",
        "underlying": "SECURITYNAMEBYID",
        "option_type": "CALL_OR_PUT",
        "strike": "EXERCISE_PRICE",
        "expiration": "EXPIRE_DATE",
        "contract_unit": "CONTRACT_UNIT",
        "contract_trade_code": "CONTRACT_ID",
        "source_date": "TRADE_DATE",
    }
    return _validate_master(
        [_normalized(row, exchange="SSE", keys=keys) for row in raw]
    )


def parse_szse_official(content: bytes) -> list[dict[str, Any]]:
    """Parse binary XLSX as strings to retain codes and source adjustment columns."""
    if len(content) > XLSX_BYTE_LIMIT:
        raise ValueError("contract_response_byte_limit")
    try:
        with ZipFile(BytesIO(content)) as archive:
            if sum(info.file_size for info in archive.infolist()) > 50_000_000:
                raise ValueError("contract_workbook_expansion_limit")
    except BadZipFile as exc:
        raise ValueError("invalid_contract_workbook") from exc
    frame = pd.read_excel(
        BytesIO(content), header=None, dtype=str, keep_default_na=False
    )
    if frame.empty:
        raise ValueError("empty_contract_workbook")
    headers = frame.iloc[0].tolist()
    if any(not isinstance(h, str) or not h or h != h.strip() for h in headers) or len(
        set(headers)
    ) != len(headers):
        raise ValueError("invalid_or_duplicate_contract_headers")
    keys = {
        "symbol": "合约编码",
        "underlying": "标的证券简称(代码)",
        "option_type": "合约类型",
        "strike": "行权价",
        "expiration": "到期日",
        "contract_unit": "合约单位",
        "contract_trade_code": "合约代码",
        "source_date": "交易日期",
    }
    if not set(keys.values()).issubset(headers):
        raise ValueError("missing_contract_fields")
    records = [
        dict(zip(headers, values, strict=True))
        for values in frame.iloc[1:].itertuples(index=False, name=None)
    ]
    return _validate_master(
        [_normalized(row, exchange="SZSE", keys=keys) for row in records]
    )


def parse_discovery(text: str, name: str) -> list[str]:
    """Require one complete expected assignment and nonempty trailing-comma ID list."""
    match = re.fullmatch(
        r"\s*var\s+hq_str_" + re.escape(name) + r'\s*=\s*"([^"\r\n]*)"\s*;\s*', text
    )
    if not match or not re.fullmatch(r"(?:CON_OP_[0-9]{8},)+", match[1]):
        raise ValueError("invalid_or_empty_contract_discovery")
    symbols = [token.removeprefix("CON_OP_") for token in match[1].split(",")[:-1]]
    if len(set(symbols)) != len(symbols):
        raise ValueError("duplicate_contract_discovery")
    return symbols


def _sina_data(method: str, underlying: str, **params: Any) -> dict[str, Any]:
    value = _json_request(
        SINA_API + method,
        headers=SINA_HEADERS,
        params={
            "exchange": "null",
            "cate": SSE_CATEGORIES[underlying],
            **params,
        },
    )
    result = value.get("result")
    if (
        not isinstance(result, dict)
        or not isinstance(result.get("status"), dict)
        or result["status"].get("code") != 0
    ):
        raise ValueError("invalid_sina_discovery_status")
    data = result.get("data")
    if not isinstance(data, dict) or data.get("stockId") != underlying:
        raise ValueError("sina_discovery_underlying_mismatch")
    return data


def _sina_contracts(underlyings: list[str]) -> list[dict[str, Any]]:
    frozen: dict[str, dict[str, Any]] = {}
    for underlying in underlyings:
        category = _sina_data("getStockName", underlying)
        inventory = category.get("cateList")
        if not isinstance(inventory, list) or not all(
            isinstance(v, str) for v in inventory
        ):
            raise ValueError("invalid_sina_category_inventory")
        if not set(SSE_CATEGORIES.values()).issubset(inventory):
            raise ValueError("incomplete_sina_category_inventory")
        raw_months = category.get("contractMonth")
        if not isinstance(raw_months, list) or not raw_months or len(raw_months) > 24:
            raise ValueError("invalid_sina_contract_months")
        for month in raw_months:
            if not isinstance(month, str) or not re.fullmatch(
                r"[0-9]{4}-(?:0[1-9]|1[0-2])", month
            ):
                raise ValueError("invalid_sina_contract_month")
        for month in dict.fromkeys(raw_months):
            expiry = _sina_data("getRemainderDay", underlying, date=month)
            expire_day = _date(expiry.get("expireDay"))
            if not expire_day or expire_day[:7] != month:
                raise ValueError("sina_expiry_month_mismatch")
            for direction, option_type in (("UP", "call"), ("DOWN", "put")):
                name = f"OP_{direction}_{underlying}{month[2:4]}{month[5:7]}"
                text = _request(
                    "https://hq.sinajs.cn/list=" + name,
                    limit=DISCOVERY_BYTE_LIMIT,
                    headers=SINA_HEADERS,
                ).decode("gb18030", errors="strict")
                symbols = parse_discovery(text, name)
                evidence = {
                    "underlying": underlying,
                    "option_type": option_type,
                    "month": month,
                    "expiration": expire_day,
                    "category": category,
                    "expiry_validation": expiry,
                    "discovery": {
                        "name": name,
                        "symbols": symbols,
                        "raw_response": text,
                    },
                }
                for symbol in symbols:
                    if symbol in frozen:
                        raise ValueError("duplicate_contract_across_discovery_lists")
                    frozen[symbol] = evidence
                if len(frozen) > MAX_CONTRACTS:
                    raise ValueError("oversize_contract_universe")
    # Discovery is frozen before fetching quotes; never silently replace a changed list.
    output = []
    symbols = list(frozen)
    for start in range(0, len(symbols), 50):
        requested = symbols[start : start + 50]
        quotes = fetch_quotes(",".join(requested))
        received = [row.get("symbol") for row in quotes]
        if len(received) != len(requested) or set(received) != set(requested):
            raise ValueError("partial_or_duplicate_contract_quotes")
        by_symbol = {row["symbol"]: row for row in quotes}
        for symbol in requested:
            quote, evidence = by_symbol[symbol], frozen[symbol]
            if quote.get("quote_error") or quote.get("source_field_count") != 51:
                raise ValueError("invalid_contract_metadata_quote")
            if any(
                quote.get(key) != evidence[key]
                for key in ("underlying", "option_type", "expiration")
            ):
                raise ValueError("contract_quote_discovery_mismatch")
            expiration = _date(quote.get("expiration"))
            if not expiration or expiration[:7] != evidence["month"]:
                raise ValueError("contract_quote_month_mismatch")
            source_time = quote.get("source_time")
            if not isinstance(source_time, str):
                raise ValueError("missing_contract_quote_date")
            source_date = _date(source_time[:10])
            output.append(
                {
                    "symbol": _identifier(symbol, 8),
                    "exchange": "SSE",
                    "underlying": _identifier(quote.get("underlying"), 6),
                    "option_type": quote["option_type"],
                    "strike": _number(quote.get("strike")),
                    "expiration": expiration,
                    "contract_unit": None,
                    "contract_trade_code": None,
                    "metadata_source": "sina",
                    "metadata_origin": "sina_sse_discovery_and_quotes",
                    "source_date": source_date,
                    # Only this category/month/direction's discovery, not the entire universe.
                    "source_row": {
                        "quote": quote,
                        "category": evidence["category"],
                        "discovery": evidence["discovery"],
                        "expiry_validation": evidence["expiry_validation"],
                    },
                    "universe_scope": "sina_discovered_sse_current_contracts",
                    "official_universe_verified": False,
                    "metadata_complete": False,
                    "adjustment_marker": quote.get("adjustment_marker"),
                }
            )
    return _validate_master(output)


def fetch_contracts(
    exchange: str, source: str, underlyings: str | None = None
) -> list[dict[str, Any]]:
    """Typed fetcher boundary: explicit exchange/vendor and exact optional ETF filter."""
    if exchange not in {"SSE", "SZSE"} or source not in {"official", "sina"}:
        raise ValueError("Unsupported ETF contract exchange/source")
    if source == "sina" and exchange != "SSE":
        raise ValueError(
            "SZSE Sina contract discovery is not verified; use source='official'"
        )
    selected = None
    if underlyings is not None:
        if not isinstance(underlyings, str):
            raise ValueError("underlyings must be a comma-separated string")
        selected = underlyings.split(",")
        for token in selected:
            _identifier(token, 6)
        if len(set(selected)) != len(selected):
            raise ValueError("Duplicate underlying identifiers")
    if source == "sina":
        selected = list(SSE_CATEGORIES) if selected is None else selected
        if any(token not in SSE_CATEGORIES for token in selected):
            raise ValueError("Unverified SSE Sina underlying")
        return _sina_contracts(selected)
    if exchange == "SSE":
        value = _json_request(
            SSE_URL,
            params={
                "isPagination": "false",
                "expireDate": "",
                "securityId": "",
                "sqlId": "SSE_ZQPZ_YSP_GGQQZSXT_XXPL_DRHY_SEARCH_L",
            },
            headers={"Referer": "https://www.sse.com.cn/", "User-Agent": "Mozilla/5.0"},
        )
        rows = parse_sse_official(value)
    else:
        content = _request(
            SZSE_URL,
            limit=XLSX_BYTE_LIMIT,
            params={
                "SHOWTYPE": "xlsx",
                "CATALOGID": "option_drhy",
                "TABKEY": "tab1",
            },
        )
        rows = parse_szse_official(content)
    if selected is not None:
        available = {row["underlying"] for row in rows}
        if not set(selected).issubset(available):
            raise ValueError("Requested underlying missing from contract source")
        rows = [row for row in rows if row["underlying"] in selected]
    return rows


class AkshareEtfOptionContractsQueryParams(QueryParams):
    """Explicit universe provenance; blocked official sources never silently fall back."""

    model_config = ConfigDict(extra="forbid")
    exchange: Literal["SSE", "SZSE"]
    source: Literal["official", "sina"] = "official"
    underlyings: str | None = Field(
        default=None, description="Comma-separated ETF codes."
    )
    __json_schema_extra__ = {"underlyings": {"multiple_items_allowed": True}}

    @field_validator("underlyings")
    @classmethod
    def exact_underlyings(cls, value: str | None) -> str | None:
        """Do not repair invalid or duplicate underlying identifiers."""
        if value is not None:
            tokens = value.split(",")
            if not tokens or len(set(tokens)) != len(tokens):
                raise ValueError("Invalid underlying list")
            if any(not re.fullmatch(r"[0-9]{6}", token) for token in tokens):
                raise ValueError("Expected exact six-digit ETF codes")
        return value


class AkshareEtfOptionContractsData(Data):
    """One current source contract with nullable unobserved adjustment metadata."""

    model_config = ConfigDict(extra="allow")
    symbol: str
    exchange: Literal["SSE", "SZSE"]
    underlying: str
    option_type: Literal["call", "put"]
    strike: float
    expiration: str
    contract_unit: float | None = None
    contract_trade_code: str | None = None
    metadata_source: str
    source_date: str | None = None
    source_row: dict[str, Any] = Field(default_factory=dict)


class AkshareEtfOptionQuotesQueryParams(QueryParams):
    """At most fifty exact option contract identifiers in one source request."""

    model_config = ConfigDict(extra="forbid")
    symbols: str
    __json_schema_extra__ = {"symbols": {"multiple_items_allowed": True}}

    @field_validator("symbols")
    @classmethod
    def exact_symbols(cls, value: str) -> str:
        """Validate before spawning source I/O."""
        validate_symbols(value)
        return value


class AkshareEtfOptionQuotesData(Data):
    """A quote or explicit per-contract diagnostic, preserving raw source fields."""

    model_config = ConfigDict(extra="allow")
    symbol: str
    quote_error: str | None = None
    source_time: str | None = None
    bid: float | None = None
    ask: float | None = None
    bid_size: float | None = None
    ask_size: float | None = None
    last: float | None = None
    open_interest: float | None = None
    volume: float | None = None
    turnover: float | None = None
    underlying: str | None = None
    strike: float | None = None
    contract_name: str | None = None
    source_status: str | None = None
    raw_fields: dict[str, Any] = Field(default_factory=dict)


class AkshareEtfOptionContractsFetcher(
    Fetcher[AkshareEtfOptionContractsQueryParams, list[AkshareEtfOptionContractsData]]
):
    """Discover an explicit current universe, not arbitrary historical contracts."""

    require_credentials = False

    @staticmethod
    def transform_query(params: dict[str, Any]) -> AkshareEtfOptionContractsQueryParams:
        """Validate the declared source policy and ETF identifiers."""
        return AkshareEtfOptionContractsQueryParams(**params)

    @staticmethod
    async def aextract_data(
        query: AkshareEtfOptionContractsQueryParams,
        credentials: dict[str, str] | None,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        """Keep discovery and transport at the provider boundary."""
        return await asyncio.to_thread(
            fetch_contracts, query.exchange, query.source, query.underlyings
        )

    @staticmethod
    def transform_data(
        query: AkshareEtfOptionContractsQueryParams,
        data: list[dict[str, Any]],
        **kwargs: Any,
    ) -> list[AkshareEtfOptionContractsData]:
        """Validate identity and retain unobserved fields as null."""
        return [AkshareEtfOptionContractsData.model_validate(row) for row in data]


class AkshareEtfOptionQuotesFetcher(
    Fetcher[AkshareEtfOptionQuotesQueryParams, list[AkshareEtfOptionQuotesData]]
):
    """Batch transport over the same Sina source as AKShare's per-contract quote."""

    require_credentials = False

    @staticmethod
    def transform_query(params: dict[str, Any]) -> AkshareEtfOptionQuotesQueryParams:
        """Validate the exact requested symbol set."""
        return AkshareEtfOptionQuotesQueryParams(**params)

    @staticmethod
    async def aextract_data(
        query: AkshareEtfOptionQuotesQueryParams,
        credentials: dict[str, str] | None,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        """Fetch off the event loop; the collector worker enforces hard deadlines."""
        return await asyncio.to_thread(fetch_quotes, query.symbols)

    @staticmethod
    def transform_data(
        query: AkshareEtfOptionQuotesQueryParams,
        data: list[dict[str, Any]],
        **kwargs: Any,
    ) -> list[AkshareEtfOptionQuotesData]:
        """Retain explicit error rows instead of silently dropping contracts."""
        return [AkshareEtfOptionQuotesData.model_validate(row) for row in data]

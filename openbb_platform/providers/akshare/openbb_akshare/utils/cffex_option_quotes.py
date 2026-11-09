# Keep acquisition-only imports free of provider/parsing/journal initialization.
# pylint: disable=import-outside-toplevel
# Evidence schemas require exact built-in types, including rejection of bool-as-int.
# pylint: disable=unidiomatic-typecheck

"""Bounded native Sina CFFEX discovery and quotes, matching AKShare's source.

Displayed expiries are not an exchange-universe assertion. The quote source has
no event clock or contract unit: acquisition time must never substitute for either.
Blocking HTTP belongs off the event loop; collector workers own hard deadlines.
"""

from __future__ import annotations

import base64
import hashlib
import json
import math
import re
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, cast
from urllib.parse import urlencode

from openbb_core.provider.utils.helpers import make_request

DISCOVERY_BASE = "https://stock.finance.sina.com.cn/futures/view/optionsCffexDP.php"
DISCOVERY_URLS = {
    "IO": DISCOVERY_BASE,
    "HO": DISCOVERY_BASE + "/ho/cffex",
    "MO": DISCOVERY_BASE + "/mo/cffex",
}
QUOTE_URL = "https://stock.finance.sina.com.cn/futures/api/openapi.php/OptionService.getOptionData"
PRODUCT_INFO = {
    "IO": ("sh000300", "沪深300指数"),
    "HO": ("sh000016", "上证50指数"),
    "MO": ("sh000852", "中证1000指数"),
}
EXPIRY = re.compile(r"(io|ho|mo)([0-9]{2})(0[1-9]|1[0-2])")
CONTRACT = re.compile(r"((?:io|ho|mo)[0-9]{2}(?:0[1-9]|1[0-2]))([CP])([1-9][0-9]*)")
NUMERIC = re.compile(r"[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?")
MAX_BYTES = 1_000_000
MAX_SIDE_ROWS = 1000
MAX_EXPIRIES = 24
QUOTE_FIELDS = ("bid_size", "bid", "last", "ask", "ask_size", "open_interest", "change")


def validate_product(product: str) -> str:
    """Require an explicit exact upper-case family, without normalization."""
    if not isinstance(product, str) or product not in PRODUCT_INFO:
        raise ValueError("Expected exact product IO, HO or MO")
    return product


def validate_expiry_symbol(expiry_symbol: str) -> str:
    """Reject invalid or malicious tokens before source I/O."""
    if not isinstance(expiry_symbol, str) or not EXPIRY.fullmatch(expiry_symbol):
        raise ValueError("Expected exact lowercase (io|ho|mo)YYMM with valid month")
    return expiry_symbol


def _raw_bytes(raw: bytes) -> None:
    if not isinstance(raw, bytes) or not 0 < len(raw) <= MAX_BYTES:
        raise ValueError("invalid_or_oversized_source_bytes")


def _provenance(raw: bytes, url: str, fetched_at: str) -> dict[str, Any]:
    digest = hashlib.sha256(raw).hexdigest()
    return {
        "source_sha256": digest,
        "fetched_at": fetched_at,
        "raw_evidence": {
            "url": url,
            "sha256": digest,
            "encoding": "base64",
            "content_base64": base64.b64encode(raw).decode("ascii"),
        },
    }


def parse_expiries(raw: bytes, product: str, fetched_at: str) -> list[dict[str, Any]]:
    """Read the exact option_suffix list, not the page's selected/main contract."""
    validate_product(product)
    _raw_bytes(raw)
    # Keep the optional HTML parser lazy so quote-only use does not import it.
    from bs4 import BeautifulSoup

    document = BeautifulSoup(raw.decode("gb18030", errors="strict"), "html.parser")
    containers = document.select("#option_suffix")
    if len(containers) != 1:
        raise ValueError("missing_or_duplicate_expiry_container")
    items = containers[0].select("li")
    if not 1 <= len(items) <= MAX_EXPIRIES:
        raise ValueError("invalid_expiry_row_count")
    tokens: set[str] = set()
    rows = []
    provenance = _provenance(raw, DISCOVERY_URLS[product], fetched_at)
    for item in items:
        token = cast(str, item.get("data-value"))
        validate_expiry_symbol(token)
        if (
            item.get_text().strip() != token
            or token[:2] != product.lower()
            or token in tokens
        ):
            raise ValueError("foreign_duplicate_or_malformed_expiry")
        tokens.add(token)
        rows.append(
            {
                "product": product,
                "expiry_symbol": token,
                "universe_scope": "sina_displayed_expiries",
                "source_sha256": provenance["source_sha256"],
                "fetched_at": fetched_at,
            }
        )
    rows[0]["raw_evidence"] = provenance["raw_evidence"]
    return rows


def _number(
    value: Any,
    *,
    required: bool = False,
    nonnegative: bool = True,
    integral: bool = False,
) -> float | None:
    if value is None or (isinstance(value, str) and value in {"", "-", "--"}):
        if required:
            raise ValueError("missing_required_number")
        return None
    if isinstance(value, bool) or not isinstance(value, (str, int, float)):
        raise ValueError("malformed_numeric")
    if isinstance(value, str) and (len(value) > 64 or not NUMERIC.fullmatch(value)):
        raise ValueError("malformed_numeric")
    try:
        number = float(value)
    except (OverflowError, ValueError) as exc:
        raise ValueError("malformed_numeric") from exc
    if (
        not math.isfinite(number)
        or (nonnegative and number < 0)
        or (integral and not number.is_integer())
    ):
        raise ValueError("nonfinite_or_invalid_numeric")
    return number


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate_json_key")
        result[key] = value
    return result


def _bad_constant(value: str) -> None:
    raise ValueError(f"nonfinite_json_constant: {value}")


def _finite_json_float(value: str) -> float:
    result = float(value)
    if not math.isfinite(result):
        raise ValueError("nonfinite_json_number")
    return result


def _source_data(raw: bytes) -> dict[str, Any]:
    _raw_bytes(raw)
    try:
        payload = json.loads(
            raw.decode("utf-8", errors="strict"),
            object_pairs_hook=_unique_object,
            parse_constant=_bad_constant,
            parse_float=_finite_json_float,
        )
    except (UnicodeError, json.JSONDecodeError, RecursionError) as exc:
        raise ValueError("malformed_quote_json") from exc
    if not isinstance(payload, dict) or not isinstance(payload.get("result"), dict):
        raise ValueError("missing_quote_result")
    result = payload["result"]
    status = result.get("status")
    if (
        not isinstance(status, dict)
        or type(status.get("code")) is not int
        or status["code"] != 0
    ):
        raise ValueError("unsuccessful_source_status")
    data = result.get("data")
    if not isinstance(data, dict):
        raise ValueError("missing_quote_data")
    return data


def _source_info(info: Any, expiry_symbol: str) -> tuple[dict[str, Any], str]:
    product = expiry_symbol[:2].upper()
    relate, name = PRODUCT_INFO[product]
    if (
        not isinstance(info, dict)
        or info.get("relate") != relate
        or info.get("name") != name
    ):
        raise ValueError("source_underlying_mismatch")
    if "exchange" in info and info["exchange"] != "cffex":
        raise ValueError("source_exchange_mismatch")
    expiration = info.get("expire_date")
    if not isinstance(expiration, str) or not re.fullmatch(
        r"[0-9]{4}-[0-9]{2}-[0-9]{2}", expiration
    ):
        raise ValueError("invalid_source_expiration")
    parsed = date.fromisoformat(expiration)
    if parsed.year != 2000 + int(expiry_symbol[2:4]) or parsed.month != int(
        expiry_symbol[4:6]
    ):
        raise ValueError("source_expiration_month_mismatch")
    return info, expiration


def _parse_side(values: Any, side: str, expiry_symbol: str) -> list[dict[str, Any]]:
    if not isinstance(values, list) or not 1 <= len(values) <= MAX_SIDE_ROWS:
        raise ValueError("missing_or_excessive_side_rows")
    width, letter = (9, "C") if side == "up" else (8, "P")
    rows = []
    symbols: set[str] = set()
    strikes: set[Decimal] = set()
    for fields in values:
        if not isinstance(fields, list) or len(fields) != width:
            raise ValueError("unexpected_quote_row_width")
        symbol = fields[-1]
        match = (
            CONTRACT.fullmatch(symbol)
            if isinstance(symbol, str) and len(symbol) <= 64
            else None
        )
        if not match or match[1] != expiry_symbol or match[2] != letter:
            raise ValueError("invalid_or_foreign_native_id")
        strike = _number(match[3], required=True, integral=True)
        strike_key = Decimal(match[3])
        if strike is None or strike <= 0 or symbol in symbols or strike_key in strikes:
            raise ValueError("duplicate_or_invalid_strike_id")
        if side == "up":
            _number(fields[7], required=True, integral=True)
            if Decimal(str(fields[7])) != strike_key:
                raise ValueError("native_id_strike_mismatch")
        symbols.add(symbol)
        strikes.add(strike_key)
        row: dict[str, Any] = {
            key: _number(
                value,
                nonnegative=key != "change",
                integral=key in {"bid_size", "ask_size", "open_interest"},
            )
            for key, value in zip(QUOTE_FIELDS, fields[:7], strict=True)
        }
        diagnostics = ["source_time_unavailable", "contract_unit_unavailable"]
        for key in QUOTE_FIELDS[:6]:
            if row[key] is None:
                diagnostics.append("missing_" + key)
            elif row[key] == 0:
                diagnostics.append("zero_" + key)
        row.update(
            {
                "symbol": symbol,
                "option_type": "call" if side == "up" else "put",
                "strike": strike,
                "raw_fields": {"side": side, "values": fields},
                "quote_diagnostics": diagnostics,
            }
        )
        rows.append(row)
    return rows


def parse_quotes(
    raw: bytes, expiry_symbol: str, fetched_at: str
) -> list[dict[str, Any]]:
    """Parse sides independently and reject structurally incomplete responses whole."""
    validate_expiry_symbol(expiry_symbol)
    data = _source_data(raw)
    info, expiration = _source_info(data.get("info"), expiry_symbol)
    calls = _parse_side(data.get("up"), "up", expiry_symbol)
    puts = _parse_side(data.get("down"), "down", expiry_symbol)
    prefix_length = len(expiry_symbol) + 1
    call_strikes = {Decimal(row["symbol"][prefix_length:]) for row in calls}
    put_strikes = {Decimal(row["symbol"][prefix_length:]) for row in puts}
    if call_strikes != put_strikes:
        raise ValueError("unmatched_call_put_strike_membership")
    rows = calls + puts
    params = _quote_params(expiry_symbol)
    provenance = _provenance(raw, QUOTE_URL + "?" + urlencode(params), fetched_at)
    for row in rows:
        row.update(
            {
                "product": expiry_symbol[:2].upper(),
                "expiry_symbol": expiry_symbol,
                "exchange": "CFFEX",
                "underlying_symbol": info["relate"][2:],
                "underlying_source_symbol": info["relate"],
                "expiration": expiration,
                "contract_unit": None,
                "source_time": None,
                "source_info": info,
                "source_sha256": provenance["source_sha256"],
                "fetched_at": fetched_at,
            }
        )
    rows[0]["raw_evidence"] = provenance["raw_evidence"]
    return rows


def _quote_params(expiry_symbol: str) -> dict[str, str]:
    return {
        "type": "futures",
        "product": expiry_symbol[:2],
        "exchange": "cffex",
        "pinzhong": expiry_symbol,
    }


def _request_bytes(url: str, params: dict[str, str] | None = None) -> tuple[bytes, str]:
    response = make_request(
        url,
        params=params or {},
        headers={"Referer": DISCOVERY_BASE, "User-Agent": "Mozilla/5.0"},
        timeout=30,
        preferences={"request_timeout": 30},
        stream=True,
    )
    try:
        response.raise_for_status()
        if response.status_code != 200:
            raise ValueError("unexpected_source_http_status")
        chunks = []
        size = 0
        for chunk in response.iter_content(chunk_size=65536):
            size += len(chunk)
            if size > MAX_BYTES:
                raise ValueError("source_response_byte_limit")
            chunks.append(chunk)
        raw = b"".join(chunks)
        _raw_bytes(raw)
        return raw, datetime.now(timezone.utc).isoformat()
    finally:
        response.close()


def fetch_expiries(product: str) -> list[dict[str, Any]]:
    """Fetch the requested family's displayed expiry list with original-byte evidence."""
    validate_product(product)
    raw, stamp = _request_bytes(DISCOVERY_URLS[product])
    return parse_expiries(raw, product, stamp)


def fetch_quotes(expiry_symbol: str) -> list[dict[str, Any]]:
    """Fetch one literal maturity from the same native endpoint used by AKShare."""
    validate_expiry_symbol(expiry_symbol)
    raw, stamp = _request_bytes(QUOTE_URL, _quote_params(expiry_symbol))
    return parse_quotes(raw, expiry_symbol, stamp)

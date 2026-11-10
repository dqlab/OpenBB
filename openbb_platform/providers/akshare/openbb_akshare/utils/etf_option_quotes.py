"""Bounded transport and compatibility exports for the current ETF quote parser."""

from datetime import datetime, timezone
from typing import Any

from openbb_core.provider.utils.helpers import get_requests_session, make_request
from requests.auth import HTTPBasicAuth

from openbb_akshare.models.etf_options import (
    FIELDS,
    parse_batch,
    parse_quote,
    validate_symbols,
)

__all__ = ["FIELDS", "fetch_quotes", "parse_batch", "parse_quote", "validate_symbols"]


def _reject_unsafe_response(response: Any, **_kwargs: Any) -> Any:
    """Run before Requests resolves redirects or consumes response content."""
    if 300 <= response.status_code < 400:
        response.close()
        raise ValueError("quote_redirect_unsupported")
    encoding = response.headers.get("Content-Encoding", "").strip().lower()
    if encoding not in {"", "identity"}:
        response.close()
        raise ValueError("quote_content_encoding_unsupported")
    return response


def _read_quote_body(url: str) -> bytes:
    """Read identity-encoded bytes with a bounded encoded and decoded size."""
    limit = 1_000_000
    with get_requests_session() as session:
        auth = session.auth
        basic_tuple = (
            type(auth) is tuple
            and len(auth) == 2
            and all(type(value) in (str, bytes) for value in auth)
        )
        if auth is not None and type(auth) is not HTTPBasicAuth and not basic_tuple:
            raise ValueError("quote_auth_unsupported")
        if session.hooks.get("response"):
            raise ValueError("quote_response_hooks_unsupported")
        session.hooks["response"].append(_reject_unsafe_response)
        with make_request(
            url,
            session=session,
            headers={
                "Referer": "https://stock.finance.sina.com.cn/",
                "User-Agent": "Mozilla/5.0",
                "Accept-Encoding": "identity",
            },
            timeout=25,
            stream=True,
            allow_redirects=False,
        ) as response:
            response.raise_for_status()
            # No content decoder is enabled: identity bytes are both encoded and
            # decoded bytes. Read at most one byte beyond the limit.
            body = response.raw.read(limit + 1, decode_content=False)
            if len(body) > limit:
                raise ValueError("quote_response_byte_limit")
            return body


def fetch_quotes(symbols: str) -> list[dict[str, Any]]:
    """Batch the identical source behind AKShare's option_sse_spot_price_sina."""
    tokens = validate_symbols(symbols)
    body = _read_quote_body(
        "https://hq.sinajs.cn/list=" + ",".join("CON_OP_" + s for s in tokens)
    )
    text = body.decode("gb18030", errors="strict")
    stamp = datetime.now(timezone.utc).isoformat()
    return parse_batch(text, tokens, stamp)

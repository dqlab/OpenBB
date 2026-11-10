# Keep acquisition-only imports free of provider/parsing/journal initialization.
# pylint: disable=import-outside-toplevel
# Evidence schemas require exact built-in types, including rejection of bool-as-int.
# pylint: disable=unidiomatic-typecheck
# OpenBB and collector callback signatures retain protocol parameters.
# pylint: disable=unused-argument

"""Transport boundary for the ETF option chain: bytes, before any decode.

This module is the **only** place in the lane that performs an HTTP call, and it
performs it deliberately one layer below the provider adapter:

```
openbb_akshare.models.etf_options.make_quote_request(url)      # the provider's own helper
        -> requests.Response
        -> response.content                                  # bytes, untouched
        -> SourceBody(body=..., status=..., media_type=...)   # nothing parsed
```

The provider's own `fetch_quotes` decodes ``gb18030``, splits CSV fields, turns
text into floats and derives ``source_time``/``strike``/``expiration`` before the
collector can see anything.  Capturing *after* that parse cannot be a literal
capture of the response, so this lane calls the same transport helper the
provider calls and captures its body instead.

What this module does **not** do: decode, parse, date, unit or numeric handling,
judgement of any kind.  Byte bounds and the declared stop flag are transport
concerns and stay here.

Which requests are single documents is a property of the source, not a choice:

* ``AkshareEtfOptionQuotes``         -> one ``hq.sinajs.cn`` GET per block;
* ``AkshareEtfOptionContracts`` with ``source: official`` -> one SSE JSON GET or
  one SZSE XLSX GET;
* ``AkshareEtfOptionContracts`` with ``source: sina`` -> the provider aggregates
  dozens of GETs (category index, per-month expiry validation, two discovery
  lists per month, then quote batches) into one model response.  There is no
  single literal body to store, so this client declares it cannot address that
  request as a document and the collector falls back to the provider envelope,
  labelled ``provider-transformed``. The lane README documents this limitation.
"""

from __future__ import annotations

import threading
from typing import Any
from urllib.parse import urlencode

# URLs and headers are spelled exactly as the provider spells them.  A test
# records the URL the provider's own helpers pass to the transport and asserts
# equality, so the two spellings cannot drift without a red test.
SINA_QUOTE_URL = "https://hq.sinajs.cn/list="
SINA_HEADERS = {
    "Referer": "https://stock.finance.sina.com.cn/",
    "User-Agent": "Mozilla/5.0",
}
SSE_URL = "https://query.sse.com.cn/commonQuery.do"
SSE_PARAMS = {
    "isPagination": "false",
    "expireDate": "",
    "securityId": "",
    "sqlId": "SSE_ZQPZ_YSP_GGQQZSXT_XXPL_DRHY_SEARCH_L",
}
SSE_HEADERS = {"Referer": "https://www.sse.com.cn/", "User-Agent": "Mozilla/5.0"}
SZSE_URL = "https://www.sse.org.cn/api/report/ShowReport"
SZSE_PARAMS = {"SHOWTYPE": "xlsx", "CATALOGID": "option_drhy", "TABKEY": "tab1"}
SSE_MEDIA_TYPE = "application/json"
SZSE_MEDIA_TYPE = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
QUOTE_MEDIA_TYPE = "text/plain; charset=gb18030"

QUOTE_KIND = "quote-batch"
SSE_MASTER_KIND = "sse-official-master"
SZSE_MASTER_KIND = "szse-official-master"
KINDS = (QUOTE_KIND, SSE_MASTER_KIND, SZSE_MASTER_KIND)


class SourceBody:
    """A literal transport response: bytes plus where they came from.

    ``evidence_kind``/``transformation`` are recorded by the acquisition layer;
    they are declared here so a caller cannot relabel bytes as something else.
    """

    __slots__ = (
        "body",
        "status",
        "media_type",
        "url",
        "evidence_kind",
        "transformation",
    )

    def __init__(
        self,
        body: bytes,
        *,
        status: int | None,
        media_type: str,
        url: str,
    ) -> None:
        if type(body) is not bytes:
            raise TypeError("transport body must be bytes")
        self.body = body
        self.status = status
        self.media_type = media_type
        self.url = url
        self.evidence_kind = "raw-bytes"
        self.transformation = None


def quote_batch_url(symbols: list[str]) -> str:
    return SINA_QUOTE_URL + ",".join("CON_OP_" + symbol for symbol in symbols)


def sse_master_url() -> str:
    return SSE_URL + "?" + urlencode(SSE_PARAMS)


def szse_master_url() -> str:
    return SZSE_URL + "?" + urlencode(SZSE_PARAMS)


def quote_transport():
    """The provider's own transport helper for the quote endpoint, per call."""
    from openbb_akshare.models import etf_options as provider

    return provider.make_quote_request


def contract_transport():
    """The provider's own transport helper for the master endpoints, per call."""
    from openbb_akshare.models import etf_options as provider

    return provider.make_contract_request


def plan_kind(model: str, request: dict[str, Any]) -> str | None:
    """Which single-document kind this request is, or ``None`` (aggregate)."""
    if model == "AkshareEtfOptionQuotes":
        return QUOTE_KIND
    if model == "AkshareEtfOptionContracts":
        if request.get("source") == "official":
            return (
                SSE_MASTER_KIND
                if request.get("exchange") == "SSE"
                else SZSE_MASTER_KIND
            )
        # Sina discovery is an aggregate of many responses; not addressable as one.
        return None
    return None


def _status_of(response: Any) -> int | None:
    value = getattr(response, "status_code", None)
    return value if type(value) is int else None


def _media_type_of(response: Any, default: str) -> str:
    headers = getattr(response, "headers", None)
    try:
        value = headers.get("Content-Type") if headers is not None else None
    except AttributeError:  # pragma: no cover - defensive, headers is a Mapping
        value = None
    if isinstance(value, str) and value.strip():
        return value.strip()[:128]
    return default


class EtfChainClient:
    """Document-addressed transport client: one body per request, nothing parsed."""

    capability = "documents"

    def __init__(self, stop: threading.Event | None = None) -> None:
        self.stop = stop or threading.Event()
        self.calls = 0

    def plan_kind(self, model: str, request: dict[str, Any]) -> str | None:
        return plan_kind(model, request)

    def _request(
        self, url: str, *, timeout: float, headers: dict, params: dict | None, kind: str
    ):
        if self.stop.is_set():
            raise RuntimeError("cancelled")
        self.calls += 1
        transport = quote_transport() if kind == QUOTE_KIND else contract_transport()
        kwargs: dict[str, Any] = {"headers": dict(headers)}
        if params is not None:
            kwargs["params"] = dict(params)
        response = transport(url, timeout=timeout, **kwargs)
        return response

    def fetch(self, source_id: str, source: Any, request: dict[str, Any]) -> SourceBody:
        """Perform exactly one GET and return its body; never decode it."""
        kind = self.plan_kind(source.model, request)
        if kind is None:
            raise ValueError("aggregate_request_not_addressable")
        if kind == QUOTE_KIND:
            symbols = str(request.get("symbols", "")).split(",")
            url = quote_batch_url(symbols)
            response = self._request(
                url,
                timeout=source.timeout_seconds,
                headers=SINA_HEADERS,
                params=None,
                kind=kind,
            )
            default = QUOTE_MEDIA_TYPE
        elif kind == SSE_MASTER_KIND:
            url = SSE_URL
            response = self._request(
                url,
                timeout=source.timeout_seconds,
                headers=SSE_HEADERS,
                params=SSE_PARAMS,
                kind=kind,
            )
            default = SSE_MEDIA_TYPE
        else:
            url = SZSE_URL
            response = self._request(
                url,
                timeout=source.timeout_seconds,
                headers={},
                params=SZSE_PARAMS,
                kind=kind,
            )
            default = SZSE_MEDIA_TYPE
        body = getattr(response, "content", None)
        if type(body) is not bytes:
            raise ValueError("transport_without_bytes")
        if len(body) > source.max_response_bytes:
            # Refused before the bytes become evidence: an oversize body is a
            # protocol refusal, not a captured payload.
            raise ValueError("response_limit")
        return SourceBody(
            body,
            status=_status_of(response),
            media_type=_media_type_of(response, default),
            url=url,
        )

    def close(self) -> None:
        return None


class NoSourceClient:
    """A client that refuses every request; used where acquisition must not run."""

    capability = "none"

    def fetch(self, *_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("this path must not perform a source call")

    def plan_kind(self, *_args: Any, **_kwargs: Any) -> str | None:
        return None

    def close(self) -> None:
        return None

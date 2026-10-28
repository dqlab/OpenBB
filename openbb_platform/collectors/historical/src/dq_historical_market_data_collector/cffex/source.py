"""Literal CFFEX daily-CSV bytes, taken at the transport layer.

The OpenBB provider adapter parses the exchange CSV inside the provider process and
then stamps every row with the *requested* trading date
(``openbb_akshare/utils/cffex.py``, read-only here). A capture that happens after
that adapter can therefore never show a date the row itself carried: the stamp is
already in the payload.

This module keeps the capture upstream of that parser. It performs the same HTTP
GET through the provider's own transport helper -- ``make_request``, looked up on
the provider module at call time so the offline guard and any transport test see
the real entry point -- and returns ``response.content`` untouched. No character
decoding, no CSV reading, no column mapping, no date stamping and no numeric
conversion happens here. Those run in :mod:`dq_historical_market_data_collector.cffex.parse`,
downstream of the immutable store.

What is captured, precisely: the HTTP response body bytes as the transport helper
returns them (after any content-encoding the transport applied), together with the
HTTP status code and the media type. It is **not** a socket-level capture: TLS
termination, HTTP framing and content decoding have already happened, and response
headers are not stored. ``REPORT.md`` states that limit in the same words.
"""

from __future__ import annotations

import re
import time
from dataclasses import dataclass
from datetime import date
from typing import Any

from openbb_collector_core.models import ProviderFailure

SOURCE_LAYOUT = "cffex-rtj-csv-v1"
SOURCE_ENCODING = "gbk"
DAILY_CSV_URL = "http://www.cffex.com.cn/sj/hqsj/rtj/{year:04d}{month:02d}/{day:02d}/{stamp}_1.csv"
DEFAULT_TIMEOUT = 30
_STAMP = re.compile(r"[0-9]{8}\Z")


def daily_csv_url(day: date) -> str:
    """The exchange URL for one session, from the requested date alone.

    Identical to the template the provider adapter uses; ``test_cffex_daily.py``
    proves the two agree by recording the URL the provider passes to the (trapped)
    transport, so the two spellings can never drift apart silently.
    """
    if type(day) is not date:
        raise ValueError("invalid_source_date")
    return DAILY_CSV_URL.format(
        year=day.year, month=day.month, day=day.day, stamp=day.strftime("%Y%m%d")
    )


def transport():
    """The provider's transport helper, resolved per call (never cached)."""
    from openbb_akshare.utils import cffex as provider

    return provider.make_request


@dataclass(frozen=True)
class SourceBody:
    """One response at the transport layer: status plus the body bytes verbatim."""

    status: int
    body: bytes
    media_type: str | None = None

    def __post_init__(self) -> None:
        if type(self.status) is not int or not 100 <= self.status <= 599:
            raise ValueError("invalid_source_status")
        if type(self.body) is not bytes:
            raise ValueError("invalid_source_body_type")


def fetch_body(
    day: date,
    *,
    timeout: float = DEFAULT_TIMEOUT,
    fetch=None,
) -> SourceBody:
    """One literal GET. Raises only for transport failures; the body is untouched."""
    sender = fetch if fetch is not None else transport()
    response = sender(daily_csv_url(day), timeout=timeout)
    content = getattr(response, "content", None)
    if type(content) is not bytes:
        raise ValueError("invalid_source_body_type")
    headers = getattr(response, "headers", None) or {}
    media_type = None
    if hasattr(headers, "get"):
        media_type = headers.get("Content-Type") or None
    status = getattr(response, "status_code", None)
    return SourceBody(status, content, media_type)


class CffexCsvClient:
    """The daily lane's source client: literal bytes, no parser, no date.

    Pacing, cancellation and the byte budget are the only policies here; every
    decision about the payload belongs downstream of the store. A non-2xx status
    is captured with its body and rejected by the acquisition verdict.
    """

    def __init__(self, stop: Any = None, clock=time.monotonic, sender=None) -> None:
        self.stop = stop
        self.clock = clock
        self.sender = sender
        self.calls: list[dict[str, Any]] = []
        self.statuses: list[int] = []
        self._last: float | None = None

    def fetch_one(self, source: Any, day: date) -> SourceBody:
        interval = float(getattr(source, "min_interval_seconds", 0) or 0)
        if self._last is not None and interval > 0:
            wait = interval - (self.clock() - self._last)
            if wait > 0:
                if self.stop is not None:
                    if self.stop.wait(wait):
                        raise ProviderFailure("cancelled")
                else:
                    time.sleep(wait)
        if self.stop is not None and self.stop.is_set():
            raise ProviderFailure("cancelled")
        self._last = self.clock()
        self.calls.append({"date": day.isoformat()})
        try:
            result = fetch_body(
                day,
                timeout=float(getattr(source, "timeout_seconds", DEFAULT_TIMEOUT)),
                fetch=self.sender,
            )
        except ProviderFailure:
            raise
        except (ValueError, TypeError):
            raise
        except Exception as exc:  # requests/urllib3/OSError: no bytes exist to capture
            self.statuses.append(0)
            raise ProviderFailure("source_transport_unavailable") from exc
        budget = getattr(source, "max_response_bytes", None)
        if type(budget) is int and len(result.body) > budget:
            self.statuses.append(result.status)
            raise ValueError("adapter_response_limit_exceeded")
        self.statuses.append(result.status)
        return result

    def fetch(self, source_id: str, source: Any, request: dict[str, Any]) -> SourceBody:
        del source_id
        return self.fetch_one(source, date.fromisoformat(request["date"]))

    def close(self) -> None:
        return None

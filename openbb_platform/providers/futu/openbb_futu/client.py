"""Bounded FUTUNN REST calls. Credentials and server messages are never logged."""

import http.client
import json
import os
import re
import uuid
import zlib
from dataclasses import dataclass, field
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any
from urllib.parse import urlparse

import requests
from openbb_core.provider.utils.helpers import make_request

from openbb_futu.coordination import Owner, RequestBudget, coordination_directory

BASE = "https://webapi.futunn.com"


def _base():
    value = os.environ.get("FUTU_TEST_HTTP_BASE", BASE)
    parsed = urlparse(value)
    if value != BASE and not (
        parsed.scheme == "http"
        and parsed.hostname in {"127.0.0.1", "localhost", "::1"}
        and not parsed.username
        and not parsed.password
    ):
        raise FutuError("invalid_endpoint")
    return value.rstrip("/")


class FutuError(Exception):
    """A sanitized failure with an actionable category."""

    def __init__(
        self,
        category: str,
        *,
        request_id=None,
        started_at_utc=None,
        http_status=None,
        received_bytes=None,
        encoded_bytes=None,
        decoded_bytes=None,
    ):
        self.category = category
        self.request_id = request_id
        self.started_at_utc = started_at_utc
        self.http_status = http_status
        self.received_bytes = received_bytes
        self.encoded_bytes = encoded_bytes
        self.decoded_bytes = decoded_bytes
        super().__init__(f"FUTUNN request failed: {category}")


@dataclass(repr=False)
class OAuthTokens:
    """Handle OAuthTokens."""

    access_token: str = field(repr=False)
    refresh_token: str = field(repr=False)
    expires_in: int

    def __repr__(self):
        """Return a redacted representation."""
        return "OAuthTokens(<redacted>)"


def _post(path: str, *, owner: Owner | None = None, **kwargs: Any) -> dict:
    if http.client.HTTPConnection.debuglevel or http.client.HTTPSConnection.debuglevel:
        raise FutuError("unsafe_http_debug")
    directory = coordination_directory()
    try:
        with owner.borrow(directory) if owner else Owner(directory):
            if RequestBudget(directory).acquire() > 0:
                raise FutuError("local_budget_exhausted")
            try:
                response = make_request(
                    _base() + path,
                    method="POST",
                    timeout=10,
                    allow_redirects=False,
                    **kwargs,
                )
            except Exception:
                raise FutuError("transport") from None
    except FutuError:
        raise
    except Exception:
        raise FutuError("coordination") from None
    status = response.status_code
    if 300 <= status < 400:
        raise FutuError("redirect")
    if status == 401:
        raise FutuError("authentication")
    if status == 403:
        raise FutuError("permission")
    if status == 429:
        raise FutuError("rate_limit")
    if not 200 <= status < 300:
        raise FutuError("transport")
    try:
        result = response.json()
    except Exception:
        raise FutuError("invalid_response") from None
    if not isinstance(result, dict):
        raise FutuError("invalid_response")
    return result


def refresh_tokens(
    client_id: str, refresh_token: str, *, owner: Owner | None = None
) -> OAuthTokens:
    """Exchange a refresh token; callers must securely persist rotated tokens."""
    if not client_id or not refresh_token:
        raise FutuError("not_configured")
    result = _post(
        "/oauth2/token",
        owner=owner,
        data={
            "grant_type": "refresh_token",
            "client_id": client_id,
            "refresh_token": refresh_token,
        },
    )
    try:
        if not isinstance(result["access_token"], str) or not result["access_token"]:
            raise ValueError
        rotated = result.get("refresh_token") or refresh_token
        expires = int(result["expires_in"])
        if not isinstance(rotated, str) or expires <= 0:
            raise ValueError
        return OAuthTokens(result["access_token"], rotated, expires)
    except (KeyError, ValueError, TypeError):
        raise FutuError("authentication") from None


def snapshot(symbols: list[str], access_token: str) -> list[dict]:
    """Handle snapshot."""
    if not access_token:
        raise FutuError("not_configured")
    if not symbols or len(symbols) > 400 or len(set(symbols)) != len(symbols):
        raise ValueError("supply 1 to 400 distinct symbols")
    result = _post(
        "/api/v1.0/quote/snapshot",
        headers={"Authorization": "Bearer " + access_token},
        json={"code_list": symbols},
    )
    if result.get("ret_code") != 0:
        # Application error codes have not been verified; do not guess their meaning.
        raise FutuError("provider_error")
    data = result.get("data")
    rows = data.get("snapshot_list") if isinstance(data, dict) else None
    if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
        raise FutuError("invalid_response")
    codes = [row.get("code") for row in rows]
    if len(codes) != len(symbols) or set(codes) != set(symbols):
        raise FutuError("partial_or_no_data")
    return rows


def capabilities() -> dict:
    """Report documented integration separately from unobserved account access."""
    return {
        "api_family": "futunn_http_ws",
        "sdk": "not_required",
        "snapshot": "documented",
        "account_entitlement": "not_tested",
        "websocket": "not_tested",
        "live_verified": False,
        "discovery_complete": False,
    }


def _raw_snapshot_reserved(
    symbols, access_token, *, timeout=10, max_bytes=1024 * 1024, secrets=(), request
):
    """Capture a bounded, inspected response without changing parsed fetcher behavior."""
    if (
        not access_token
        or not symbols
        or len(symbols) > 400
        or len(set(symbols)) != len(symbols)
    ):
        raise FutuError("invalid_request")
    if not 0 < timeout <= 60 or not 1 <= max_bytes <= 1024 * 1024:
        raise FutuError("invalid_request")
    request_id, started = request
    if http.client.HTTPConnection.debuglevel or http.client.HTTPSConnection.debuglevel:
        raise FutuError(
            "unsafe_http_debug", request_id=request_id, started_at_utc=started
        )

    class NoRedirectSession(requests.Session):
        def __init__(self):
            super().__init__()
            self.trust_env = False

        def resolve_redirects(self, *args, **kwargs):
            # Session.send otherwise consumes redirect bodies to populate response.next.
            del args, kwargs
            return iter(())

    session = NoRedirectSession()
    try:
        response = make_request(
            _base() + "/api/v1.0/quote/snapshot",
            method="POST",
            headers={"Authorization": "Bearer " + access_token},
            json={"code_list": symbols},
            timeout=timeout,
            stream=True,
            allow_redirects=False,
            session=session,
        )
        try:
            body = bytearray()
            status = response.status_code
            encoded = 0
            decoded = 0

            def fail(category):
                raise FutuError(
                    category,
                    request_id=request_id,
                    started_at_utc=started,
                    http_status=status,
                    received_bytes=decoded,
                    encoded_bytes=encoded,
                    decoded_bytes=decoded,
                )

            encoding = (
                response.headers.get("Content-Encoding", "identity").lower().strip()
            )
            if encoding not in {"identity", "gzip"}:
                fail("unsupported_content_encoding")
            decompressor = (
                zlib.decompressobj(16 + zlib.MAX_WBITS) if encoding == "gzip" else None
            )
            while True:
                # Read at most one byte past the remaining entity budget.
                chunk = response.raw.read(
                    min(65536, max_bytes - encoded + 1), decode_content=False
                )
                if not chunk:
                    break
                encoded += len(chunk)
                if encoded > max_bytes:
                    fail("encoded_response_too_large")
                if decompressor:
                    if decompressor.eof:
                        fail("invalid_content_encoding")
                    try:
                        data = decompressor.decompress(chunk, max_bytes - decoded + 1)
                    except zlib.error:
                        fail("invalid_content_encoding")
                    decoded += len(data)
                    if decoded > max_bytes or decompressor.unconsumed_tail:
                        fail("decoded_response_too_large")
                    if decompressor.unused_data:
                        fail("invalid_content_encoding")
                else:
                    data = chunk
                    decoded += len(data)
                    if decoded > max_bytes:
                        fail("decoded_response_too_large")
                body.extend(data)
            if decompressor and not decompressor.eof:
                fail("invalid_content_encoding")
        finally:
            response.close()
    except FutuError:
        raise
    except Exception as exc:
        category = (
            "timeout" if isinstance(exc, requests.exceptions.Timeout) else "transport"
        )
        raise FutuError(
            category,
            request_id=request_id,
            started_at_utc=started,
            http_status=locals().get("status"),
            received_bytes=locals().get("decoded"),
            encoded_bytes=locals().get("encoded"),
            decoded_bytes=locals().get("decoded"),
        ) from None
    finally:
        session.close()

    def unique(pairs):
        obj = {}
        for key, value in pairs:
            if key in obj:
                raise ValueError
            obj[key] = value
        return obj

    try:
        obj = json.loads(body, object_pairs_hook=unique)

        def sensitive(value):
            if isinstance(value, dict):
                return any(
                    k.lower()
                    in {
                        "authorization",
                        "access_token",
                        "refresh_token",
                        "client_id",
                        "client_secret",
                        "password",
                        "secret",
                        "credential",
                        "credential_id",
                        "signature",
                        "private_key",
                        "api_key",
                        "token",
                        "cookie",
                        "set-cookie",
                    }
                    or sensitive(k)
                    or sensitive(v)
                    for k, v in value.items()
                )
            if isinstance(value, list):
                return any(sensitive(v) for v in value)
            return isinstance(value, str) and (
                bool(re.search(r"(?i)\bbearer\s+\S+", value))
                or any(s and s in value for s in (access_token, *secrets))
            )

        if sensitive(obj):
            raise FutuError(
                "sensitive_response",
                request_id=request_id,
                started_at_utc=started,
                http_status=status,
                received_bytes=len(body),
                encoded_bytes=encoded,
                decoded_bytes=decoded,
            )
    except FutuError:
        raise
    except Exception:
        raise FutuError(
            "uninspectable_response",
            request_id=request_id,
            started_at_utc=started,
            http_status=status,
            received_bytes=len(body),
            encoded_bytes=encoded,
            decoded_bytes=decoded,
        ) from None
    return SimpleNamespace(
        body=bytes(body),
        status=status,
        request_id=request_id,
        started_at_utc=started,
        parsed=obj,
        content_encoding=encoding,
    )


def raw_snapshot(
    symbols, access_token, *, owner=None, timeout=10, max_bytes=1024 * 1024, secrets=()
):
    """Capture one response while holding owner and consuming the shared budget."""
    if (
        not access_token
        or not symbols
        or len(symbols) > 400
        or len(set(symbols)) != len(symbols)
    ):
        raise FutuError("invalid_request")
    if not 0 < timeout <= 60 or not 1 <= max_bytes <= 1024 * 1024:
        raise FutuError("invalid_request")
    if http.client.HTTPConnection.debuglevel or http.client.HTTPSConnection.debuglevel:
        raise FutuError("unsafe_http_debug")
    directory = coordination_directory()
    try:
        with owner.borrow(directory) if owner else Owner(directory):
            if RequestBudget(directory).acquire() > 0:
                raise FutuError("local_budget_exhausted")
            return _raw_snapshot_reserved(
                symbols,
                access_token,
                timeout=timeout,
                max_bytes=max_bytes,
                secrets=secrets,
                request=(uuid.uuid4().hex, datetime.now(timezone.utc).isoformat()),
            )
    except FutuError:
        raise
    except Exception:
        raise FutuError("coordination") from None

"""Bounded native v2 queries through the configured OpenBB HTTP helper."""

import json
import re
from typing import Any

from openbb_core.app.model.abstract.error import OpenBBError
from openbb_core.provider.utils.helpers import make_request
from requests import RequestException

from openbb_dq_quant_data.client import DQAPIClient


class NativeQueryError(OpenBBError):
    """A native request failure with a public status and optional retry delay."""

    def __init__(self, status: int | None, retry_after: str | None = None) -> None:
        message = (
            f"DQ native API request failed (HTTP {status})."
            if status is not None
            else "DQ native API is unavailable."
        )
        super().__init__(message)
        self.upstream_status = status
        self.status_code = (
            status if status is not None and 400 <= status <= 599 else 502
        )
        self.retry_after = (
            retry_after
            if status == 429
            and retry_after
            and re.fullmatch(r"[0-9]{1,10}", retry_after)
            else None
        )


class NativeAPIClient(DQAPIClient):
    """Read native records with the existing DQ URL/key/CA settings."""

    def native_query(self, request: dict[str, Any]) -> dict:
        """Run one bounded v2 query without paging, retries or numeric projection."""
        if not isinstance(request, dict):
            raise OpenBBError("DQ native query must be a JSON object.")
        limit = request.get("limit", 1000)
        if (
            not isinstance(limit, int)
            or isinstance(limit, bool)
            or not 1 <= limit <= 10000
        ):
            raise OpenBBError(
                "DQ native query limit must be an integer from 1 to 10,000."
            )
        try:
            encoded = json.dumps(request, allow_nan=False).encode()
        except (TypeError, ValueError):
            raise OpenBBError("DQ native query must contain JSON values.") from None
        if len(encoded) > 64 * 1024:
            raise OpenBBError("DQ native query exceeds 64 KiB; narrow the request.")
        try:
            response = make_request(
                self.base_url + "/v2/query",
                method="POST",
                json=request,
                headers=self.headers.copy(),
                timeout=10,
                allow_redirects=False,
                **self.request_options,
            )
        except (RequestException, OSError):
            raise NativeQueryError(None) from None
        try:
            if response.status_code != 200:
                raise NativeQueryError(
                    response.status_code, response.headers.get("Retry-After")
                )
            if len(response.content) > 16 * 1024**2:
                raise OpenBBError(
                    "DQ native response exceeds 16 MiB; narrow the query."
                )
            try:
                payload = response.json()
            except ValueError:
                raise OpenBBError("DQ native API returned invalid JSON.") from None
            if (
                not isinstance(payload, dict)
                or not isinstance(payload.get("data"), list)
                or any(not isinstance(row, dict) for row in payload["data"])
                or not isinstance(payload.get("meta"), dict)
            ):
                raise OpenBBError("DQ native API returned an invalid data envelope.")
            rows, meta = payload["data"], payload["meta"]
            reported = meta.get("rows")
            if (
                len(rows) > limit
                or not isinstance(reported, int)
                or isinstance(reported, bool)
                or reported != len(rows)
            ):
                raise OpenBBError(
                    "DQ native API returned an invalid row count or exceeded the bound."
                )
            return payload
        finally:
            response.close()

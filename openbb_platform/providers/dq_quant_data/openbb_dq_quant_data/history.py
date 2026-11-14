"""Explicit frozen history, retaining the native JSON contract without fallback."""

import json
import os
import re
from datetime import date
from pathlib import Path
from typing import Literal

from openbb_core.app.model.abstract.error import OpenBBError
from openbb_core.provider.utils.helpers import make_request
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator
from requests import Session

from openbb_dq_quant_data.client import _credential
from openbb_dq_quant_data.history_transport import HistoryConnection, HistorySSHAdapter
from openbb_dq_quant_data.native import NativeQueryError


class ScopeRequest(BaseModel):
    """Only exact UTC calendar days; ranges, as-of and extra fields are rejected."""

    model_config = ConfigDict(extra="forbid", strict=True)
    day: str = Field(pattern=r"^\d{4}-\d{2}-\d{2}$")

    @field_validator("day")
    @classmethod
    def calendar_day(cls, value: str) -> str:
        """Reject invalid dates without normalizing timezone or session boundaries."""
        date.fromisoformat(value)
        return value


class PageRequest(ScopeRequest):
    """One instrument/day and one authenticated page."""

    instrument: str = Field(
        min_length=1, max_length=256, pattern=r"^[a-zA-Z0-9_.:\-]+$"
    )
    fields: list[str] = Field(min_length=1, max_length=64)
    limit: int = Field(default=512, ge=1, le=2048)
    cursor: str | None = Field(default=None, max_length=8192)


class DiscoveryRequest(ScopeRequest):
    """Bounded discovery within a release's discrete date manifest."""

    instrument_prefix: str = Field(
        default="", max_length=256, pattern=r"^[a-zA-Z0-9_.:\-]*$"
    )
    after_instrument: str | None = Field(default=None, max_length=256)
    limit: int = Field(default=16, ge=1, le=32)


class HistoryError(NativeQueryError):
    """Retain HTTP rejection and an allowlisted scope reason, never remote details."""

    def __init__(
        self, status: int, reason: str | None, retry_after: str | None
    ) -> None:
        super().__init__(status, retry_after)
        self.reason = (
            reason
            if isinstance(reason, str)
            and reason
            in {
                "history_scope_has_excluded_rows_use_legacy",
                "history_date_unavailable_use_legacy",
                "history_instrument_day_unavailable_use_legacy",
                "history_release_unavailable_use_legacy",
                "invalid_history_cursor",
            }
            else None
        )
        if self.reason:
            OpenBBError.__init__(
                self, f"{str(self)} {self.reason}; call the old interface separately."
            )


def connection() -> HistoryConnection:
    """Load explicit operator configuration; no default host, URL or release."""
    try:
        path = Path(os.environ["OPENBB_DQ_HISTORY_CONFIG"])
        if not path.is_absolute() or path.stat().st_size > 65536:
            raise ValueError
        config = HistoryConnection.model_validate_json(path.read_bytes())
        if any(
            not re.fullmatch(r"[0-9a-f]{64}", sha) for sha in config.releases.values()
        ):
            raise ValueError
        return config
    except (KeyError, OSError, ValueError):
        raise OpenBBError(
            "Configure OPENBB_DQ_HISTORY_CONFIG for explicit frozen history."
        ) from None


def read_history(
    credentials: dict | None,
    release_id: str,
    action: Literal["describe", "scopes", "coverage", "pages"],
    request: dict | None = None,
) -> dict:
    """One bounded request; every call, including every cursor, is authorized remotely."""
    config = connection()
    if (
        not re.fullmatch(r"[0-9a-f]{64}", release_id)
        or release_id not in config.releases.values()
    ):
        raise HistoryError(422, "history_release_unavailable_use_legacy", None)
    try:
        if action == "describe":
            if request is not None:
                raise ValueError
            body = None
        else:
            model = DiscoveryRequest if action == "scopes" else PageRequest
            body = model.model_validate(request).model_dump(exclude_none=True)
            if action == "coverage" and body.get("cursor"):
                raise ValueError
        encoded = json.dumps(body, allow_nan=False).encode()
        if len(encoded) > 16384:
            raise ValueError
    except (ValidationError, ValueError, TypeError):
        raise OpenBBError(
            "Invalid bounded history request; use an exact instrument and UTC day."
        ) from None
    if action not in {"describe", "scopes", "coverage", "pages"}:
        raise OpenBBError("Unsupported history action.")
    key = _credential(credentials, "dq_quant_data_api_key", "api_key") or ""
    path = f"/v2/history/{release_id}" + ("" if action == "describe" else f"/{action}")
    with Session() as session:
        session.trust_env = False
        adapter = HistorySSHAdapter(config, key)
        # Mount both schemes so no HTTP request can accidentally escape the bridge.
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        response = make_request(
            "http://dq-fixed-history.invalid" + path,
            method="GET" if body is None else "POST",
            json=body,
            session=session,
            allow_redirects=False,
            timeout=30,
        )
        try:
            if len(response.content) > 16 * 1024**2 or (key and key in response.text):
                raise NativeQueryError(None)
            if response.status_code != 200:
                try:
                    reason = response.json().get("detail")
                except (ValueError, AttributeError):
                    reason = None
                raise HistoryError(
                    response.status_code, reason, response.headers.get("Retry-After")
                )
            try:
                payload = response.json()
            except ValueError:
                raise OpenBBError("History returned invalid JSON.") from None
            _validate_response(payload, release_id, action, body)
            return payload
        finally:
            response.close()


def _validate_response(
    payload: dict, release_id: str, action: str, body: dict | None
) -> None:
    """Reject mismatched identities, malformed pages and false initial emptiness."""
    # Every condition rejects an unsafe native envelope.
    # pylint: disable=too-many-boolean-expressions
    if not isinstance(payload, dict):
        raise OpenBBError("Invalid history response.")
    meta = payload.get("meta") if action == "pages" else payload
    if not isinstance(meta, dict) or meta.get("release_id") != release_id:
        raise OpenBBError("History response release mismatch.")
    if action != "pages":
        return
    rows = payload.get("data")
    assert body is not None
    count = meta.get("rows")
    if (
        not isinstance(rows, list)
        or any(not isinstance(row, dict) for row in rows)
        or not isinstance(count, int)
        or isinstance(count, bool)
        or count != len(rows)
        or count > body["limit"]
        or (not rows and not body.get("cursor"))
        or meta.get("consistency") != "fixed_reconciled_batch_membership"
        or meta.get("permissions_checked_each_page") is not True
        or "next_cursor" not in meta
        or (
            meta["next_cursor"] is not None
            and (
                not isinstance(meta["next_cursor"], str)
                or not meta["next_cursor"]
                or meta["next_cursor"] == body.get("cursor")
            )
        )
        or any(set(row) != set(body["fields"]) for row in rows)
        or any(
            value is not None and not isinstance(value, str)
            for row in rows
            for field, value in row.items()
            if field
            in {
                "open",
                "high",
                "low",
                "close",
                "volume",
                "event_epoch_ns",
                "source_published_ns",
                "collector_observed_ns",
                "central_verified_ns",
                "original_authority_visible_ns",
            }
        )
    ):
        raise OpenBBError(
            "Invalid or empty initial history page; no fallback was performed."
        )

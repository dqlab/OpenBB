"""Bounded HTTP reads through OpenBB request helpers."""

from datetime import date
from typing import Any
from urllib.parse import urlsplit

from openbb_core.app.model.abstract.error import OpenBBError
from openbb_core.provider.utils.helpers import make_request


def _credential(credentials: dict | None, *names: str) -> str | None:
    values = credentials or {}
    value = next((values[name] for name in names if values.get(name)), None)
    return value.get_secret_value() if hasattr(value, "get_secret_value") else value


def clean_params(params: dict[str, Any]) -> dict[str, Any]:
    """Drop unset values and serialize dates without changing time zones."""
    return {
        key: value.isoformat() if isinstance(value, date) else value
        for key, value in params.items()
        if value is not None
    }


class DQAPIClient:
    """Read canonical API envelopes; reject incomplete bounded results."""

    def __init__(self, credentials: dict | None) -> None:
        self.base_url = (
            _credential(credentials, "dq_quant_data_api_url", "api_url") or "http://127.0.0.1:8000"
        ).rstrip("/")
        parsed = urlsplit(self.base_url)
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.hostname
            or parsed.username
            or parsed.password
            or parsed.query
            or parsed.fragment
        ):
            raise OpenBBError("DQ API URL must be an HTTP(S) base URL without credentials.")
        api_key = _credential(credentials, "dq_quant_data_api_key", "api_key")
        self.headers = {"X-API-Key": api_key} if api_key else {}

    def get_result(self, path: str, params: dict | None = None) -> dict:
        """Read at most 100 pages / 100,000 rows, preserving page provenance."""
        values = clean_params(params or {})
        rows, pages, seen = [], [], set()
        for _ in range(100):
            response = make_request(
                self.base_url + path,
                params=values,
                headers=self.headers.copy(),
                timeout=30,
                allow_redirects=False,
            )
            if response.status_code != 200:
                raise OpenBBError(f"DQ API request failed (HTTP {response.status_code}).")
            payload = response.json()
            if not isinstance(payload, dict) or "data" not in payload:
                raise OpenBBError("DQ API returned an invalid data envelope.")
            data = payload["data"]
            batch = data if isinstance(data, list) else ([] if data is None else [data])
            if any(not isinstance(row, dict) for row in batch):
                raise OpenBBError("DQ API returned invalid records.")
            rows.extend(batch)
            if len(rows) > 100000:
                raise OpenBBError("DQ query exceeds 100,000 rows; narrow the query.")
            meta = payload.get("meta") or {}
            if not isinstance(meta, dict):
                raise OpenBBError("DQ API returned invalid metadata.")
            pages.append(meta)
            snapshot = meta.get("snapshot_id")
            if len(pages) > 1 and snapshot != pages[0].get("snapshot_id"):
                raise OpenBBError("DQ snapshot changed during pagination; retry the query.")
            cursor = meta.get("next_cursor")
            if not cursor:
                return {"data": rows, "meta": {"pages": pages}}
            if not isinstance(cursor, str) or cursor in seen:
                raise OpenBBError("DQ API returned a repeated or invalid cursor.")
            seen.add(cursor)
            values["cursor"] = cursor
        raise OpenBBError("DQ query exceeds 100 pages; narrow the query.")

    def get(self, path: str, params: dict | None = None) -> list[dict]:
        """Return records from a complete bounded query."""
        return self.get_result(path, params)["data"]

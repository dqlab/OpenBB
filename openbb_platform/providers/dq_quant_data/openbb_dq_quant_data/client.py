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
        ca_file = _credential(credentials, "dq_quant_data_api_ca_file", "api_ca_file")
        self.request_options = {"verify": ca_file} if ca_file else {}

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
                **self.request_options,
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

    def get_market_result(self, method: str, params: dict | None = None) -> dict:
        """Read one bounded market-store response, retaining its exact provenance.

        History cursors and instrument-listing has_more remain visible to callers;
        a chain is a complete capture and cannot be silently paged or truncated.
        """
        paths = {
            "daily-bars": "/api/v1/market/daily-bars",
            "futures-curve": "/api/v1/market/futures/curve",
        }
        if method not in {
            "status",
            "instruments",
            "captures",
            "chain",
            "history",
            "basket",
            *paths,
        }:
            raise OpenBBError("Unsupported market-data read.")
        values = clean_params(params or {})
        if isinstance(values.get("instruments"), list):
            values["instruments"] = ",".join(values["instruments"])
        response = make_request(
            self.base_url + paths.get(method, f"/api/v1/market-store/{method}"),
            params=values,
            headers=self.headers.copy(),
            timeout=180,
            allow_redirects=False,
            **self.request_options,
        )
        if response.status_code != 200:
            raise OpenBBError(f"DQ market API request failed (HTTP {response.status_code}).")
        try:
            payload = response.json()
        except ValueError:
            raise OpenBBError("DQ market API returned invalid JSON.") from None
        if (
            not isinstance(payload, dict)
            or not isinstance(payload.get("data"), list)
            or any(not isinstance(row, dict) for row in payload["data"])
            or not isinstance(payload.get("meta"), dict)
        ):
            raise OpenBBError("DQ market API returned an invalid data envelope.")
        rows, meta = payload["data"], payload["meta"]
        maximum = values.get("max_rows", values.get("limit", 100000))
        if len(rows) > maximum:
            raise OpenBBError("DQ market API exceeded the requested row bound.")
        if method == "chain":
            if meta.get("next_cursor") or meta.get("rows") != len(rows):
                raise OpenBBError("DQ market API returned a partial chain.")
            if not values.get("allow_incomplete") and meta.get("response_complete") is not True:
                raise OpenBBError("DQ market API returned an incomplete capture.")
            if (
                not values.get("expiration")
                and not values.get("option_type")
                and meta.get("capture_rows") != len(rows)
            ):
                raise OpenBBError("DQ market API capture membership is incomplete.")
        if method in {"daily-bars", "futures-curve"}:
            if meta.get("rows") != len(rows) or meta.get("response_complete") is not True:
                raise OpenBBError("DQ market API returned a partial data product.")
            if method == "futures-curve":
                if len(rows) > values.get("max_contracts", 1000):
                    raise OpenBBError("DQ market API exceeded the contract bound.")
                if not values.get("allow_incomplete") and meta.get("complete") is not True:
                    raise OpenBBError("DQ market API returned an incomplete futures curve.")
        return payload

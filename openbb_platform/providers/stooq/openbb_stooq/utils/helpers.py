"""Helpers for the Stooq provider."""

import asyncio
import csv
from datetime import date, datetime, timedelta
from io import StringIO
from typing import Any, Literal, cast

from openbb_core.provider.utils.errors import EmptyDataError, UnauthorizedError
from openbb_core.provider.utils.helpers import amake_request

STOOQ_DOWNLOAD_URL = "https://stooq.com/q/d/l/"
INTERVALS = {
    "1d": "d",
    "1W": "w",
    "1M": "m",
    "1Q": "q",
    "1Y": "y",
}

Interval = Literal["1d", "1W", "1M", "1Q", "1Y"]


def with_default_dates(params: dict[str, Any]) -> dict[str, Any]:
    """Return query parameters with a one-year default date range."""
    transformed = params.copy()
    today = datetime.now().date()
    if transformed.get("start_date") is None:
        transformed["start_date"] = today - timedelta(days=365)
    if transformed.get("end_date") is None:
        transformed["end_date"] = today
    return transformed


def normalize_equity_symbol(symbol: str, country: str = "us") -> str:
    """Convert an equity symbol to Stooq symbology."""
    normalized = symbol.strip().lower()
    if not normalized:
        return normalized
    if "." in normalized:
        base, suffix = normalized.rsplit(".", maxsplit=1)
        return base if suffix == "pl" else normalized
    return normalized if country == "pl" else f"{normalized}.{country}"


def normalize_index_symbol(symbol: str) -> str:
    """Convert an index symbol to Stooq symbology."""
    normalized = symbol.strip().lower()
    return normalized if normalized.startswith("^") else f"^{normalized}"


def normalize_currency_symbol(symbol: str) -> str:
    """Convert a currency pair to Stooq symbology."""
    return symbol.strip().lower().replace("/", "").replace("-", "")


def _clean_value(value: str | None) -> str | None:
    """Normalize empty or unavailable CSV values."""
    if value is None:
        return None
    cleaned = value.strip()
    return None if cleaned.upper() in {"", "N/D", "N/A", "-"} else cleaned


def parse_csv_response(response_text: str, symbol: str) -> list[dict[str, Any]]:
    """Parse and validate a Stooq CSV response."""
    text = response_text.lstrip("\ufeff").strip()
    lowered = text.lower()

    credential_markers = (
        "get your apikey",
        "uzyskaj apikey",
        "requires javascript to verify your browser",
        "<!doctype html",
        "<html",
    )
    if not text or any(marker in lowered for marker in credential_markers):
        raise UnauthorizedError(
            "Stooq rejected the CSV download. Check that stooq_api_key is valid "
            "and refresh it from https://stooq.com/q/d/?s=aapl.us&get_apikey."
        )

    reader = csv.DictReader(StringIO(text))
    fieldnames = {name.strip().lower() for name in (reader.fieldnames or [])}
    required = {"date", "open", "high", "low", "close"}
    if not required.issubset(fieldnames):
        raise EmptyDataError(f"Unexpected Stooq response for '{symbol}'.")

    records: list[dict[str, Any]] = []
    for row in reader:
        record = {key.strip().lower(): _clean_value(value) for key, value in row.items() if key is not None}
        if any(record.get(field) is None for field in required):
            continue
        record["symbol"] = symbol.upper()
        records.append(record)

    if not records:
        raise EmptyDataError(f"No historical data found for '{symbol}'.")

    return records


def _get_api_key(credentials: dict[str, str] | None) -> str:
    """Read the Stooq API key from provider credentials."""
    api_key: Any = credentials.get("stooq_api_key") if credentials else None
    if hasattr(api_key, "get_secret_value"):
        api_key = api_key.get_secret_value()
    if not api_key:
        raise UnauthorizedError("Missing credentials: stooq_api_key")
    return str(api_key)


async def _response_callback(response, _):
    """Return a Stooq response as text."""
    response.raise_for_status()
    return await response.text()


async def fetch_historical_data(
    symbols: list[str],
    start_date: date | None,
    end_date: date | None,
    interval: Interval,
    credentials: dict[str, str] | None,
    *,
    preferences: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Download and combine historical CSV data for one or more symbols."""
    if not start_date or not end_date:
        raise ValueError("start_date and end_date are required.")

    api_key = _get_api_key(credentials)

    async def get_one(symbol: str) -> list[dict[str, Any]]:
        response = await amake_request(
            STOOQ_DOWNLOAD_URL,
            params={
                "s": symbol,
                "d1": start_date.strftime("%Y%m%d"),
                "d2": end_date.strftime("%Y%m%d"),
                "i": INTERVALS[interval],
                "apikey": api_key,
            },
            preferences=preferences or {},
            response_callback=_response_callback,
        )
        return parse_csv_response(cast(str, response), symbol)

    results = await asyncio.gather(*(get_one(symbol) for symbol in symbols))
    return [record for symbol_records in results for record in symbol_records]

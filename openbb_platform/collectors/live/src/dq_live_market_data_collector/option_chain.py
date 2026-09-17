"""Explicit OCC option-chain identity and per-contract quote reconciliation."""

from __future__ import annotations

import math
import re
from datetime import date
from typing import Any

from .config import Instrument, Source
from .quality import _normalize_quote

OCC_SYMBOL = re.compile(
    r"(?P<root>[A-Z0-9.]{1,10}?)(?P<day>[0-9]{6})(?P<right>[CP])(?P<strike>[0-9]{8})"
)
CHAIN_NUMERIC = {
    "strike",
    "underlying_price",
    "theoretical_price",
    "implied_volatility",
    "delta",
    "gamma",
    "theta",
    "vega",
    "rho",
    "dte",
}
SIGNED_FIELDS = {"delta", "gamma", "theta", "vega", "rho", "dte"}


def normalize_option(
    row: dict[str, Any], *, source: Source, instrument: Instrument, source_id: str, **context: Any
) -> tuple[dict[str, Any] | None, list[str]]:
    def value(name: str) -> Any:
        return row.get(source.field_map.get(name, name))

    underlying = instrument.source_symbols.get(source_id, instrument.symbol)
    reasons = []
    if row.get("asset_type") and row["asset_type"] != "option":
        reasons.append("security_type_mismatch")
    source_underlying = str(value("underlying_symbol") or "").upper()
    mapped_underlying = source.underlying_symbol_map.get(source_underlying, source_underlying)
    if mapped_underlying.upper() != underlying.upper():
        reasons.append("underlying_mismatch")
    contract = value("contract_symbol")
    match = OCC_SYMBOL.fullmatch(contract) if isinstance(contract, str) else None
    if match is None or match["root"] not in source.option_chain_roots:
        reasons.append("invalid_option_contract")
    try:
        expiry = date.fromisoformat(str(value("expiration")))
        strike_value = value("strike")
        if isinstance(strike_value, bool):
            raise ValueError("Boolean strike")
        strike = float(strike_value)
        if not math.isfinite(strike) or strike <= 0:
            raise ValueError("Invalid strike")
        right = str(value("option_type")).lower()
        if right not in {"call", "put"}:
            raise ValueError("Unknown option right")
        if match:
            encoded = match["day"]
            occ_date = date(2000 + int(encoded[:2]), int(encoded[2:4]), int(encoded[4:]))
            if (
                expiry != occ_date
                or right[0].upper() != match["right"]
                or not math.isclose(strike, int(match["strike"]) / 1000, rel_tol=0, abs_tol=1e-8)
            ):
                reasons.append("option_contract_mismatch")
    except (TypeError, ValueError, OverflowError):
        reasons.append("invalid_option_definition")
    if reasons:
        return None, sorted(set(reasons))

    normalized_row = {
        **row,
        "symbol": contract,
        "asset_type": "option",
        source.field_map.get("underlying_symbol", "underlying_symbol"): instrument.symbol,
    }
    for name in CHAIN_NUMERIC & set(context["collection"].fields):
        raw_name = source.field_map.get(name, name)
        number = normalized_row.get(raw_name)
        if number is None:
            continue
        try:
            if isinstance(number, bool):
                raise ValueError("Boolean option value")
            number = float(number)
            if not math.isfinite(number) or (number < 0 and name not in SIGNED_FIELDS):
                raise ValueError("Invalid option value")
            normalized_row[raw_name] = number
        except (TypeError, ValueError, OverflowError):
            reasons.append(f"invalid_{name}")
    if reasons:
        return None, sorted(set(reasons))

    # The configured instrument identifies the requested underlying. The record's
    # symbol and mapping fingerprint identify its own qualified OCC contract.
    child = instrument.model_copy(
        update={"symbol": contract, "asset_type": "option", "con_id": None, "source_symbols": {}}
    )
    record, reasons = _normalize_quote(
        normalized_row, source=source, instrument=child, source_id=source_id, **context
    )
    if record:
        record["underlying_symbol"] = instrument.symbol
        record["underlying_source_symbol"] = source_underlying
    return record, reasons

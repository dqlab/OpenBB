"""Build a native dqlib equity volatility surface from an OpenBB SPX chain.

Install the Cboe and quantitative extensions, configure a licensed dqlib 3.0.2
runtime, and run this file from the repository root::

    python examples/dqlib_spx_volatility_surface.py

The option quotes and spot are fetched from Cboe through OpenBB. The flat
discount, repo, and dividend curves below are illustrative market assumptions;
replace them with curves appropriate for the chain's observation date.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
from openbb_quantitative.equity.models import BuildEqVolatilitySurfaceRequest
from pandas import DataFrame

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class SurfaceSelection:
    """Controls for reducing the full SPX chain to liquid calibration quotes."""

    minimum_days: int = 21
    maximum_days: int = 365
    maximum_expiries: int = 4
    strikes_per_expiry: int = 15
    minimum_strikes_per_expiry: int = 7
    minimum_moneyness: float = 0.80
    maximum_moneyness: float = 1.20


def _metadata(chain_response: Any) -> dict[str, Any]:
    """Return provider result metadata from an OpenBB response."""
    extra = getattr(chain_response, "extra", {})
    if not isinstance(extra, dict):
        return {}
    metadata = extra.get("results_metadata", {})
    return metadata if isinstance(metadata, dict) else {}


def infer_as_of_date(chain_response: Any, chain: DataFrame) -> date:
    """Infer the market observation date from standardized chain data."""
    if "eod_date" in chain:
        dates = pd.to_datetime(chain["eod_date"], errors="coerce").dropna()
        if not dates.empty:
            return dates.max().date()

    timestamp = _metadata(chain_response).get("last_trade_timestamp")
    if timestamp:
        parsed = pd.to_datetime(timestamp, errors="coerce")
        if not pd.isna(parsed):
            return parsed.date()
    return datetime.now(ZoneInfo("America/New_York")).date()


def extract_underlying_price(chain_response: Any, chain: DataFrame) -> float:
    """Read the positive SPX level from standardized data or Cboe metadata."""
    if "underlying_price" in chain:
        prices = pd.to_numeric(chain["underlying_price"], errors="coerce").dropna()
        prices = prices[prices > 0]
        if not prices.empty:
            return float(prices.median())

    current_price = _metadata(chain_response).get("current_price")
    try:
        price = float(current_price)
    except (TypeError, ValueError):
        price = 0.0
    if price > 0:
        return price
    raise ValueError("The SPX option-chain response did not contain an underlying price.")


def _normalize_quotes(
    chain: DataFrame,
    as_of_date: date,
    underlying_price: float,
    selection: SurfaceSelection,
) -> DataFrame:
    """Normalize and filter standardized OpenBB option-chain fields."""
    required = {"expiration", "strike", "option_type", "bid", "ask"}
    missing = sorted(required.difference(chain.columns))
    if missing:
        raise ValueError(f"The option chain is missing required columns: {', '.join(missing)}")

    quotes = chain.copy()
    quotes["expiration"] = pd.to_datetime(quotes["expiration"], errors="coerce").dt.normalize()
    for column in ("strike", "bid", "ask"):
        quotes[column] = pd.to_numeric(quotes[column], errors="coerce")
    if "open_interest" in quotes:
        quotes["open_interest"] = pd.to_numeric(quotes["open_interest"], errors="coerce").fillna(0)
    else:
        quotes["open_interest"] = 0
    quotes["option_type"] = quotes["option_type"].astype(str).str.lower()
    quotes["days_to_expiry"] = (quotes["expiration"] - pd.Timestamp(as_of_date)).dt.days
    quotes["moneyness"] = quotes["strike"] / underlying_price
    quotes["distance_to_spot"] = (quotes["moneyness"] - 1.0).abs()

    valid = (
        quotes["expiration"].notna()
        & quotes["strike"].gt(0)
        & quotes["bid"].gt(0)
        & quotes["ask"].ge(quotes["bid"])
        & quotes["option_type"].isin(("call", "put"))
        & quotes["days_to_expiry"].between(selection.minimum_days, selection.maximum_days)
        & quotes["moneyness"].between(selection.minimum_moneyness, selection.maximum_moneyness)
    )
    out_of_the_money = ((quotes["strike"] < underlying_price) & quotes["option_type"].eq("put")) | (
        (quotes["strike"] >= underlying_price) & quotes["option_type"].eq("call")
    )
    return quotes[valid & out_of_the_money]


def select_calibration_quotes(
    chain: DataFrame,
    as_of_date: date,
    underlying_price: float,
    selection: SurfaceSelection | None = None,
) -> DataFrame:
    """Select near-spot OTM quotes for a stable native surface calibration."""
    settings = selection or SurfaceSelection()
    quotes = _normalize_quotes(chain, as_of_date, underlying_price, settings)
    selected: list[DataFrame] = []

    for _, expiry_quotes in quotes.groupby("expiration", sort=True):
        ranked_quotes = expiry_quotes.sort_values(
            ["distance_to_spot", "open_interest"],
            ascending=[True, False],
        ).drop_duplicates("strike")
        ranked_quotes = ranked_quotes.head(settings.strikes_per_expiry)
        if ranked_quotes["strike"].nunique() >= settings.minimum_strikes_per_expiry:
            selected.append(ranked_quotes.sort_values("strike"))
        if len(selected) == settings.maximum_expiries:
            break

    if not selected:
        raise ValueError(
            "No SPX expiries had enough positive, non-crossed OTM quotes in the requested maturity and moneyness ranges."
        )
    return pd.concat(selected, ignore_index=True)


def build_spx_surface_request(
    chain_response: Any,
    chain: DataFrame,
    selection: SurfaceSelection | None = None,
) -> BuildEqVolatilitySurfaceRequest:
    """Translate an OpenBB SPX chain into the typed dqlib surface request."""
    as_of_date = infer_as_of_date(chain_response, chain)
    underlying_price = extract_underlying_price(chain_response, chain)
    quotes = select_calibration_quotes(chain, as_of_date, underlying_price, selection)
    strikes = sorted(float(value) for value in quotes["strike"].unique())

    return BuildEqVolatilitySurfaceRequest(
        as_of_date=as_of_date,
        underlying="SPX",
        underlying_price=underlying_price,
        option_chain=[
            {
                "expiry_date": row.expiration.date(),
                "strike": float(row.strike),
                "option_type": row.option_type.upper(),
                "bid": float(row.bid),
                "ask": float(row.ask),
            }
            for row in quotes.itertuples(index=False)
        ],
        # Illustrative assumptions only. Replace these with observed curves.
        discount_curve={"flat_rate": 0.045},
        repo_curve={"flat_rate": 0.045},
        dividend_curve={"flat_rate": 0.012},
        build_settings={
            "smile_method": "LINEAR_SMILE_METHOD",
            "wing_strike_type": "ABSOLUTE_STRIKE",
            "lower": min(strikes) * 0.90,
            "upper": max(strikes) * 1.10,
            "exercise_type": "EUROPEAN",
            "pricing_method": "ANALYTICAL",
        },
        evaluation_strikes=strikes,
    )


def main() -> None:
    """Fetch the chain, run the native builder, and display the vol grid."""
    from openbb import obb

    chain_response = obb.derivatives.options.chains(symbol="SPX", provider="cboe")
    chain = chain_response.to_df(index=None)
    request = build_spx_surface_request(chain_response, chain)
    response = obb.quantitative.dqlib.equity.build_volatility_surface(
        request=request.model_dump(mode="json")
    )

    points = DataFrame(point.model_dump(mode="json") for point in response.results.points)
    grid = points.pivot(index="strike", columns="expiry_date", values="volatility").sort_index()
    LOGGER.info(
        "Built the SPX surface from %d quotes across %d expiries (spot %.2f, as of %s).\n%s",
        len(request.option_chain),
        len(request.expiry_dates),
        request.underlying_price,
        request.as_of_date,
        grid.to_string(float_format=lambda value: f"{value:.4%}"),
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    main()

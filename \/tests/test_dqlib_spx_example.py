"""Tests for the OpenBB SPX-to-dqlib volatility-surface example."""

import sys
from datetime import date
from importlib.util import module_from_spec, spec_from_file_location
from math import exp, log, sqrt
from os import getenv
from pathlib import Path
from statistics import NormalDist
from types import ModuleType, SimpleNamespace

import pandas as pd
import pytest
from openbb_quantitative.equity import analytics as eqanalytics


def _load_example() -> ModuleType:
    """Load the standalone example without executing its main function."""
    path = Path(__file__).resolve().parents[4] / "examples" / "dqlib_spx_volatility_surface.py"
    spec = spec_from_file_location("dqlib_spx_volatility_surface_example", path)
    assert spec is not None and spec.loader is not None
    module = module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_spx_chain_is_translated_to_typed_surface_request():
    """The example selects valid OTM quotes and creates typed dqlib inputs."""
    example = _load_example()
    response = SimpleNamespace(
        extra={
            "results_metadata": {
                "current_price": 100.0,
                "last_trade_timestamp": "2026-01-02 16:00:00",
            }
        }
    )
    records = []
    for expiration in ("2026-03-20", "2026-06-19"):
        for strike in (80, 85, 90, 95, 100, 105, 110, 115, 120):
            option_type = "put" if strike < 100 else "call"
            records.append(
                {
                    "expiration": expiration,
                    "strike": strike,
                    "option_type": option_type,
                    "bid": 1.0 + abs(strike - 100) / 20,
                    "ask": 1.2 + abs(strike - 100) / 20,
                    "open_interest": 1000 - abs(strike - 100),
                    "underlying_price": 100.0,
                }
            )
    records.extend(
        [
            {
                "expiration": "2026-03-20",
                "strike": 100,
                "option_type": "put",
                "bid": 2.0,
                "ask": 2.2,
                "open_interest": 2000,
            },
            {
                "expiration": "2026-03-20",
                "strike": 125,
                "option_type": "call",
                "bid": 0.0,
                "ask": 0.2,
                "open_interest": 2000,
            },
        ]
    )

    request = example.build_spx_surface_request(
        response,
        pd.DataFrame.from_records(records),
        example.SurfaceSelection(
            maximum_expiries=2,
            strikes_per_expiry=7,
            minimum_strikes_per_expiry=7,
        ),
    )

    assert request.as_of_date == date(2026, 1, 2)
    assert request.underlying_price == 100.0
    assert len(request.option_chain) == 14
    assert request.expiry_dates == [date(2026, 3, 20), date(2026, 6, 19)]
    assert {quote.option_type for quote in request.option_chain} == {"CALL", "PUT"}
    assert all(quote.bid > 0 and quote.ask >= quote.bid for quote in request.option_chain)
    assert request.build_settings.lower <= min(request.output_strikes)
    assert request.build_settings.upper >= max(request.output_strikes)
    assert request.discount_curve.flat_rate == 0.045
    assert request.repo_curve.flat_rate == 0.045
    assert request.dividend_curve.flat_rate == 0.012


@pytest.mark.skipif(
    getenv("OPENBB_RUN_DQLIB_NATIVE_TESTS") != "1",
    reason="set OPENBB_RUN_DQLIB_NATIVE_TESTS=1 to use the licensed runtime",
)
def test_spx_example_request_builds_with_native_dqlib():
    """The example's selected OTM chain calibrates in the real native builder."""
    example = _load_example()
    as_of_date = date(2026, 1, 2)
    spot = 100.0
    discount_rate = 0.045
    dividend_rate = 0.012
    volatility = 0.20
    normal = NormalDist()
    records = []

    for expiration in (date(2026, 4, 2), date(2026, 7, 2)):
        term = (expiration - as_of_date).days / 365
        for strike in (85.0, 90.0, 95.0, 100.0, 105.0, 110.0, 115.0):
            d1 = (
                log(spot / strike)
                + (discount_rate - dividend_rate + volatility**2 / 2) * term
            ) / (volatility * sqrt(term))
            d2 = d1 - volatility * sqrt(term)
            call = spot * exp(-dividend_rate * term) * normal.cdf(d1) - strike * exp(
                -discount_rate * term
            ) * normal.cdf(d2)
            put = strike * exp(-discount_rate * term) * normal.cdf(-d2) - spot * exp(
                -dividend_rate * term
            ) * normal.cdf(-d1)
            price = put if strike < spot else call
            spread = min(price * 0.01, 0.01)
            records.append(
                {
                    "expiration": expiration,
                    "strike": strike,
                    "option_type": "put" if strike < spot else "call",
                    "bid": price - spread,
                    "ask": price + spread,
                    "open_interest": 1000,
                    "underlying_price": spot,
                    "eod_date": as_of_date,
                }
            )

    response = SimpleNamespace(extra={})
    request = example.build_spx_surface_request(
        response,
        pd.DataFrame.from_records(records),
        example.SurfaceSelection(
            maximum_expiries=2,
            strikes_per_expiry=7,
            minimum_strikes_per_expiry=7,
        ),
    )
    result = eqanalytics.build_volatility_surface(request).results

    assert len(result.points) == 14
    assert [point.volatility for point in result.points] == pytest.approx(
        [volatility] * 14,
        abs=1e-8,
    )

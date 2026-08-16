"""Opt-in tests against the installed native dqlib 3.0.2 Linux runtime."""

import inspect
import os
from datetime import date
from math import exp, log, sqrt
from statistics import NormalDist

import pytest
from openbb_quantitative import datetime as datetime_analytics
from openbb_quantitative.commodity import analytics as cmanalytics
from openbb_quantitative.common import analytics as common_analytics
from openbb_quantitative.credit import analytics as cranalytics
from openbb_quantitative.dqlib_models import (
    BuildEqVolatilitySurfaceRequest,
    CommodityEuropeanOptionRequest,
    CreditCurveAnalyticsRequest,
    EquityEuropeanOptionRequest,
    FixedCouponBondYtmRequest,
    FxAtmStrikeRequest,
    IrCurveAnalyticsRequest,
    IrSingleCurrencyCurveBuildRequest,
    TailRiskRequest,
)
from openbb_quantitative.equity import analytics as eqanalytics
from openbb_quantitative.fixed_income import analytics as fianalytics
from openbb_quantitative.foreign_exchange import analytics as fxanalytics
from openbb_quantitative.interest_rate import analytics as iranalytics
from openbb_quantitative.risk import analytics as mktrisk

DOMAIN_MODULES = {
    "analytics": common_analytics,
    "cmanalytics": cmanalytics,
    "cranalytics": cranalytics,
    "eqanalytics": eqanalytics,
    "fianalytics": fianalytics,
    "fxanalytics": fxanalytics,
    "iranalytics": iranalytics,
    "mktrisk": mktrisk,
}

pytestmark = pytest.mark.skipif(
    os.getenv("OPENBB_RUN_DQLIB_NATIVE_TESTS") != "1",
    reason="set OPENBB_RUN_DQLIB_NATIVE_TESTS=1 to use the licensed runtime",
)


@pytest.mark.parametrize(("domain", "bridge"), DOMAIN_MODULES.items())
def test_native_public_analytics_surface_is_fully_mapped(domain, bridge):
    """Every public function defined by installed dqlib has an explicit binding."""
    native = __import__(f"dqlib.{domain}", fromlist=[domain])
    expected = {
        name
        for name, value in vars(native).items()
        if inspect.isfunction(value) and value.__module__ == native.__name__
    }

    assert set(bridge.PUBLIC_FUNCTIONS) == expected


def test_native_simple_year_fraction():
    """The existing typed date command still reaches the real native runtime."""
    result = datetime_analytics.simple_year_fraction(
        date(2022, 3, 7), date(2023, 6, 7)
    ).results

    assert result.value == pytest.approx(1.252054794520548)


def test_native_value_at_risk_and_expected_shortfall():
    """VaR and ES execute through the dqlib 3.0.2 protobuf compatibility path."""
    samples = [-12.0, -8.0, -5.0, -3.0, -1.0, 0.0, 1.0, 2.0, 4.0, 7.0]

    value_at_risk = mktrisk.value_at_risk(
        TailRiskRequest(
            profit_loss_samples=samples,
            probability=0.95,
            antithetic=True,
        )
    ).results
    expected_shortfall = mktrisk.expected_shortfall(
        TailRiskRequest(
            profit_loss_samples=samples,
            probability=0.95,
            antithetic=True,
        )
    ).results

    assert value_at_risk.value_at_risk == pytest.approx(5.65)
    assert value_at_risk.value_at_risk_mirrored == pytest.approx(-10.2)
    assert expected_shortfall.expected_shortfall == pytest.approx(-1.5)
    assert expected_shortfall.expected_shortfall_mirrored == pytest.approx(
        -1 / 3
    )


def test_native_ir_curve_analytics():
    """A typed IR request builds and queries a real native yield curve."""
    request = IrCurveAnalyticsRequest(
        as_of_date=date(2026, 1, 2),
        currency="USD",
        pillars=[
            {"date": date(2026, 7, 2), "zero_rate": 0.018, "name": "6M"},
            {"date": date(2027, 1, 2), "zero_rate": 0.020, "name": "1Y"},
            {"date": date(2028, 1, 2), "zero_rate": 0.023, "name": "2Y"},
            {"date": date(2031, 1, 2), "zero_rate": 0.028, "name": "5Y"},
        ],
        query_dates=[date(2026, 4, 2), date(2027, 7, 2), date(2030, 1, 2)],
        curve_name="USD_TEST",
    )

    result = iranalytics.curve_analytics(request).results

    assert [point.zero_rate for point in result.points] == pytest.approx(
        [0.018, 0.02148767123287671, 0.02633485401459854]
    )
    assert [point.discount_factor for point in result.points] == pytest.approx(
        [0.9955714787826248, 0.9683679005580244, 0.8999540555557722]
    )


def test_native_ir_single_currency_curve_builder():
    """Deposits and swaps bootstrap through the real single-currency builder."""
    request = IrSingleCurrencyCurveBuildRequest(
        as_of_date=date(2026, 1, 2),
        currency="USD",
        ibor_indices=[
            {
                "index_name": "OPENBB_USD_LIBOR_3M_NATIVE",
                "tenor": "3M",
                "calendars": ["USNY"],
                "start_delay": 2,
            }
        ],
        instrument_templates=[
            {
                "instrument_type": "DEPOSIT",
                "instrument_name": "OPENBB_USD_DEP_NATIVE",
                "calendar": "USNY",
                "start_delay": 2,
            },
            {
                "instrument_type": "IR_VANILLA_SWAP",
                "instrument_name": "OPENBB_USD_SWAP_NATIVE",
                "calendar": "USNY",
                "start_delay": 2,
                "reference_index": "OPENBB_USD_LIBOR_3M_NATIVE",
                "fixing_calendars": ["USNY"],
                "fixed_leg": {
                    "frequency": "SEMIANNUAL",
                    "day_count": "THIRTY_360",
                },
                "floating_leg": {
                    "frequency": "QUARTERLY",
                    "fixing_frequency": "QUARTERLY",
                    "day_count": "ACT_360",
                },
            },
        ],
        targets=[
            {
                "curve_name": "OPENBB_USD_SINGLE_NATIVE",
                "forward_curves": {
                    "OPENBB_USD_LIBOR_3M_NATIVE": "OPENBB_USD_SINGLE_NATIVE"
                },
                "quotes": [
                    {
                        "instrument_type": "DEPOSIT",
                        "instrument_name": "OPENBB_USD_DEP_NATIVE",
                        "term": "1M",
                        "quote": 0.040,
                    },
                    {
                        "instrument_type": "DEPOSIT",
                        "instrument_name": "OPENBB_USD_DEP_NATIVE",
                        "term": "3M",
                        "quote": 0.041,
                    },
                    {
                        "instrument_type": "DEPOSIT",
                        "instrument_name": "OPENBB_USD_DEP_NATIVE",
                        "term": "6M",
                        "quote": 0.042,
                    },
                    {
                        "instrument_type": "IR_VANILLA_SWAP",
                        "instrument_name": "OPENBB_USD_SWAP_NATIVE",
                        "term": "1Y",
                        "quote": 0.043,
                    },
                    {
                        "instrument_type": "IR_VANILLA_SWAP",
                        "instrument_name": "OPENBB_USD_SWAP_NATIVE",
                        "term": "2Y",
                        "quote": 0.044,
                    },
                    {
                        "instrument_type": "IR_VANILLA_SWAP",
                        "instrument_name": "OPENBB_USD_SWAP_NATIVE",
                        "term": "3Y",
                        "quote": 0.045,
                    },
                    {
                        "instrument_type": "IR_VANILLA_SWAP",
                        "instrument_name": "OPENBB_USD_SWAP_NATIVE",
                        "term": "5Y",
                        "quote": 0.047,
                    },
                ],
            }
        ],
        query_dates=[
            date(2026, 2, 2),
            date(2026, 4, 2),
            date(2026, 7, 2),
            date(2027, 1, 2),
            date(2028, 1, 2),
            date(2031, 1, 2),
        ],
        calculate_jacobian=True,
    )

    result = iranalytics.single_currency_curve(request).results

    assert len(result.curves) == 1
    assert result.curves[0].curve_name == "OPENBB_USD_SINGLE_NATIVE"
    assert [pillar.name for pillar in result.curves[0].pillars] == [
        "1M",
        "3M",
        "6M",
        "1Y",
        "2Y",
        "3Y",
        "5Y",
    ]
    assert [point.zero_rate for point in result.curves[0].points] == pytest.approx(
        [
            0.040485869800075826,
            0.04126413896176373,
            0.042069753765041205,
            0.042520685552040666,
            0.043525876331184876,
            0.04660907627306925,
        ]
    )
    assert [
        point.discount_factor for point in result.curves[0].points
    ] == pytest.approx(
        [
            0.9965673790319954,
            0.9898768681488099,
            0.9793541183692214,
            0.9583706408901396,
            0.9166296563804843,
            0.7920164814562519,
        ]
    )
    assert result.curves[0].jacobians == []


def test_native_fixed_coupon_bond_yield_to_maturity():
    """A typed FI request builds a real bond and calculates its native yield."""
    request = FixedCouponBondYtmRequest(
        calculation_date=date(2026, 1, 2),
        issue_date=date(2025, 1, 2),
        maturity="6Y",
        coupon_rate=0.05,
        price=102.0,
        curve_rate=0.04,
        calendar="USNY",
    )

    result = fianalytics.fixed_coupon_bond_ytm(request).results

    assert result.yield_to_maturity == pytest.approx(0.044438035673887766)


def test_native_equity_european_option():
    """A typed equity request returns a real native European option price."""
    request = EquityEuropeanOptionRequest(
        valuation_date=date(2026, 1, 2),
        expiry_date=date(2027, 1, 2),
        strike=100.0,
        spot=100.0,
        volatility=0.20,
        discount_rate=0.02,
        carry_rate=0.01,
        underlying="SPX",
    )

    result = eqanalytics.european_option(request).results

    assert result.present_value == pytest.approx(8.349405767096755)
    assert result.currency == "USD"


def test_native_build_equity_volatility_surface():
    """A typed option chain calibrates through the real dqlib EQ builder."""
    as_of_date = date(2026, 1, 2)
    expiries = [date(2026, 4, 2), date(2026, 7, 2), date(2027, 1, 2)]
    strikes = [80.0, 90.0, 100.0, 110.0, 120.0]
    spot = 100.0
    discount_rate = 0.02
    repo_rate = 0.015
    dividend_rate = 0.01
    effective_dividend_rate = discount_rate - repo_rate + dividend_rate
    expected_volatility = 0.20
    normal = NormalDist()
    option_chain = []

    for expiry in expiries:
        term = (expiry - as_of_date).days / 365
        for strike in strikes:
            d1 = (
                log(spot / strike)
                + (
                    repo_rate
                    - dividend_rate
                    + expected_volatility**2 / 2
                )
                * term
            ) / (expected_volatility * sqrt(term))
            d2 = d1 - expected_volatility * sqrt(term)
            call_price = (
                spot * exp(-effective_dividend_rate * term) * normal.cdf(d1)
                - strike
                * exp(-discount_rate * term)
                * normal.cdf(d2)
            )
            put_price = (
                strike
                * exp(-discount_rate * term)
                * normal.cdf(-d2)
                - spot
                * exp(-effective_dividend_rate * term)
                * normal.cdf(-d1)
            )
            option_chain.extend(
                [
                    {
                        "expiry_date": expiry,
                        "strike": strike,
                        "option_type": "CALL",
                        "price": call_price,
                    },
                    {
                        "expiry_date": expiry,
                        "strike": strike,
                        "option_type": "PUT",
                        "price": put_price,
                    },
                ]
            )

    request = BuildEqVolatilitySurfaceRequest(
        as_of_date=as_of_date,
        option_chain=option_chain,
        underlying_price=spot,
        discount_curve={
            "pillars": [
                {"date": date(2026, 2, 2), "rate": discount_rate},
                {"date": date(2028, 1, 2), "rate": discount_rate},
            ]
        },
        repo_curve={
            "pillars": [
                {"date": date(2026, 2, 2), "rate": repo_rate},
                {"date": date(2028, 1, 2), "rate": repo_rate},
            ]
        },
        dividend_curve={
            "pillars": [
                {"date": date(2026, 2, 2), "rate": dividend_rate},
                {"date": date(2028, 1, 2), "rate": dividend_rate},
            ]
        },
        build_settings={
            "smile_method": "LINEAR_SMILE_METHOD",
            "wing_strike_type": "ABSOLUTE_STRIKE",
            "lower": 50.0,
            "upper": 150.0,
        },
        underlying="SPX",
        currency="USD",
        evaluation_strikes=[90.0, 100.0, 110.0],
    )

    result = eqanalytics.build_volatility_surface(request).results

    assert len(result.points) == 9
    assert [point.volatility for point in result.points] == pytest.approx(
        [expected_volatility] * 9,
        abs=1e-8,
    )


def test_native_fx_atm_strike():
    """A typed FX request returns a real native ATM forward strike."""
    request = FxAtmStrikeRequest(
        valuation_date=date(2026, 1, 2),
        expiry_date=date(2027, 1, 2),
        currency_pair="EURUSD",
        spot=1.10,
        domestic_rate=0.04,
        foreign_rate=0.02,
        volatility=0.12,
    )

    result = fxanalytics.atm_strike(request).results

    assert result.strike == pytest.approx(1.1222214740294314)


def test_native_credit_curve_analytics():
    """A typed credit request builds and queries a real native hazard curve."""
    request = CreditCurveAnalyticsRequest(
        as_of_date=date(2026, 1, 2),
        pillars=[
            {"date": date(2027, 1, 2), "hazard_rate": 0.01, "name": "1Y"},
            {"date": date(2028, 1, 2), "hazard_rate": 0.015, "name": "2Y"},
            {"date": date(2031, 1, 2), "hazard_rate": 0.02, "name": "5Y"},
        ],
        query_dates=[date(2027, 1, 2), date(2028, 1, 2), date(2031, 1, 2)],
        curve_name="ACME",
    )

    result = cranalytics.curve_analytics(request).results

    assert [point.credit_spread for point in result.points] == pytest.approx(
        [0.01, 0.015, 0.02]
    )
    assert [point.survival_probability for point in result.points] == pytest.approx(
        [0.9900498337491681, 0.9704455335485082, 0.9047878392617994]
    )


def test_native_commodity_european_option():
    """A typed commodity request returns a real native European option price."""
    request = CommodityEuropeanOptionRequest(
        valuation_date=date(2026, 1, 2),
        expiry_date=date(2027, 1, 2),
        strike=80.0,
        spot=75.0,
        volatility=0.25,
        discount_rate=0.03,
        carry_rate=0.02,
        underlying="WTI",
    )

    result = cmanalytics.european_option(request).results

    assert result.present_value == pytest.approx(5.634795853371306)
    assert result.currency == "USD"

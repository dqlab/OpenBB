"""Unit tests for typed dqlib domain integrations."""

from datetime import date, datetime
from types import SimpleNamespace
from typing import Any

import pytest
from fastapi import FastAPI
from openbb_quantitative import (
    cmanalytics,
    cranalytics,
    eqanalytics,
    fianalytics,
    fxanalytics,
    iranalytics,
    mktrisk,
)
from openbb_quantitative.dqlib_models import (
    CommodityEuropeanOptionRequest,
    CreditCurveAnalyticsRequest,
    EquityEuropeanOptionRequest,
    FixedCouponBondYtmRequest,
    FxAtmStrikeRequest,
    IrCurveAnalyticsRequest,
    TailRiskRequest,
)
from openbb_quantitative.dqlib_router import router as dqlib_router
from pydantic import ValidationError


def _vector(values: list[float]) -> Any:
    """Return a protobuf-vector-shaped test value."""
    return SimpleNamespace(data=values)


def _pricing_response(value: float, currency: str = "USD") -> Any:
    """Return a protobuf-pricing-shaped test value."""

    class Results:
        present_value = value
        cash_value = 0.0

        def __init__(self):
            self.currency = currency

        @staticmethod
        def ListFields():
            return [(SimpleNamespace(name="present_value"), value)]

    return SimpleNamespace(success=True, results=Results())


def test_ir_curve_analytics_converts_native_vectors(monkeypatch):
    """The IR command retains a native curve and returns typed curve points."""
    curve = object()
    captured: list[tuple[str, str, list[Any]]] = []

    def fake_execute(domain, function, args=None, kwargs=None):
        captured.append((domain, function, args or []))
        return {
            "create_ir_yield_curve": curve,
            "get_zero_rate": _vector([0.02]),
            "get_discount_factor": _vector([0.98]),
            "get_fwd_rate": _vector([0.021]),
        }[function]

    monkeypatch.setattr(iranalytics, "execute_function", fake_execute)
    request = IrCurveAnalyticsRequest(
        as_of_date=date(2026, 1, 2),
        pillars=[
            {"date": date(2027, 1, 2), "zero_rate": 0.02},
            {"date": date(2028, 1, 2), "zero_rate": 0.025},
        ],
        query_dates=[date(2027, 7, 2)],
    )

    result = iranalytics.curve_analytics(request).results

    assert result.points[0].model_dump() == {
        "date": date(2027, 7, 2),
        "zero_rate": 0.02,
        "discount_factor": 0.98,
        "forward_rate": 0.021,
    }
    assert captured[0][0:2] == ("analytics", "create_ir_yield_curve")
    assert captured[1][2][0] is curve
    assert captured[0][2][0] == datetime(2026, 1, 2)


def test_ir_curve_request_rejects_unordered_pillars():
    """Typed IR inputs reject ambiguous curve ordering before native execution."""
    with pytest.raises(ValidationError, match="unique and increasing"):
        IrCurveAnalyticsRequest(
            as_of_date=date(2026, 1, 2),
            pillars=[
                {"date": date(2028, 1, 2), "zero_rate": 0.025},
                {"date": date(2027, 1, 2), "zero_rate": 0.02},
            ],
            query_dates=[date(2027, 7, 2)],
        )


def test_fixed_coupon_bond_ytm_runs_native_object_pipeline(monkeypatch):
    """The FI command builds a template, bond, and curve before calculating YTM."""
    native = {
        "create_fixed_cpn_bond_template": object(),
        "build_fixed_cpn_bond": object(),
        "create_flat_ir_yield_curve": object(),
    }
    captured: list[tuple[str, str, list[Any]]] = []

    def fake_execute(domain, function, args=None, kwargs=None):
        captured.append((domain, function, args or []))
        if function == "yield_to_maturity_calculator":
            return 0.044
        return native[function]

    monkeypatch.setattr(fianalytics, "execute_function", fake_execute)
    request = FixedCouponBondYtmRequest(
        calculation_date=date(2026, 1, 2),
        issue_date=date(2025, 1, 2),
        maturity="6Y",
        coupon_rate=0.05,
        price=102.0,
    )

    result = fianalytics.fixed_coupon_bond_ytm(request).results

    assert result.yield_to_maturity == 0.044
    assert [item[1] for item in captured] == [
        "create_fixed_cpn_bond_template",
        "build_fixed_cpn_bond",
        "create_flat_ir_yield_curve",
        "yield_to_maturity_calculator",
    ]
    assert captured[-1][2][2] is native["build_fixed_cpn_bond"]


@pytest.mark.parametrize(
    ("module", "option_request", "market_function", "pricing_function"),
    [
        (
            eqanalytics,
            EquityEuropeanOptionRequest(
                valuation_date=date(2026, 1, 2),
                expiry_date=date(2027, 1, 2),
                strike=100.0,
                spot=100.0,
                volatility=0.2,
                underlying="SPX",
            ),
            "create_eq_mkt_data_set",
            "eq_european_option_pricer",
        ),
        (
            cmanalytics,
            CommodityEuropeanOptionRequest(
                valuation_date=date(2026, 1, 2),
                expiry_date=date(2027, 1, 2),
                strike=80.0,
                spot=75.0,
                volatility=0.25,
                underlying="WTI",
            ),
            "create_cm_mkt_data_set",
            "cm_european_option_pricer",
        ),
    ],
)
def test_european_option_commands_translate_pricing_response(
    monkeypatch, module, option_request, market_function, pricing_function
):
    """Equity and commodity commands translate native pricing outputs."""
    captured: list[str] = []

    def fake_execute(domain, function, args=None, kwargs=None):
        captured.append(function)
        if function == pricing_function:
            return _pricing_response(8.25)
        return object()

    monkeypatch.setattr(module, "execute_function", fake_execute)
    monkeypatch.setattr(
        module, "build_option_settings", lambda domain, currency: (1, 2, 3)
    )

    result = module.european_option(option_request).results

    assert result.present_value == 8.25
    assert result.cash_value is None
    assert result.currency == "USD"
    assert market_function in captured
    assert pricing_function in captured


def test_fx_atm_strike_builds_native_market_inputs(monkeypatch):
    """The FX command sends typed flat-market inputs to the native calculator."""
    captured: list[tuple[str, str, list[Any]]] = []

    def fake_execute(domain, function, args=None, kwargs=None):
        captured.append((domain, function, args or []))
        if function == "fx_atm_strike_calculator":
            return 1.122
        return object()

    monkeypatch.setattr(fxanalytics, "execute_function", fake_execute)
    monkeypatch.setattr(
        fxanalytics, "_flat_fx_volatility_surface", lambda request, value: object()
    )
    request = FxAtmStrikeRequest(
        valuation_date=date(2026, 1, 2),
        expiry_date=date(2027, 1, 2),
        currency_pair="EURUSD",
        spot=1.1,
        domestic_rate=0.04,
        foreign_rate=0.02,
    )

    result = fxanalytics.atm_strike(request).results

    assert result.strike == 1.122
    assert captured[0][2][1] == "USD"
    assert captured[1][2][1] == "EUR"
    assert captured[-1][1] == "fx_atm_strike_calculator"


def test_credit_curve_analytics_converts_native_vectors(monkeypatch):
    """The credit command returns typed spread and survival curve points."""

    def fake_execute(domain, function, args=None, kwargs=None):
        return {
            "create_credit_curve": object(),
            "get_credit_spread": _vector([0.012]),
            "get_survival_probability": _vector([0.975]),
        }[function]

    monkeypatch.setattr(cranalytics, "execute_function", fake_execute)
    request = CreditCurveAnalyticsRequest(
        as_of_date=date(2026, 1, 2),
        pillars=[{"date": date(2027, 1, 2), "hazard_rate": 0.01}],
        query_dates=[date(2028, 1, 2)],
    )

    result = cranalytics.curve_analytics(request).results

    assert result.points[0].credit_spread == 0.012
    assert result.points[0].survival_probability == 0.975


def test_expected_shortfall_returns_typed_mirrored_result(monkeypatch):
    """Expected shortfall translates both native response values when requested."""
    response = SimpleNamespace(
        success=True,
        expected_shortfall=-1.5,
        expected_shortfall_mirrored=-0.25,
    )
    monkeypatch.setattr(mktrisk, "_tail_risk_request", lambda *args, **kwargs: response)

    result = mktrisk.expected_shortfall(
        TailRiskRequest(
            profit_loss_samples=[-4.0, 1.0, -2.0],
            probability=0.975,
            antithetic=True,
        )
    ).results

    assert result.model_dump() == {
        "probability": 0.975,
        "expected_shortfall": -1.5,
        "expected_shortfall_mirrored": -0.25,
    }


def test_tail_risk_rejects_invalid_probability(monkeypatch):
    """Tail-risk inputs fail validation before invoking the native runtime."""
    called = False

    def fake_request(*args, **kwargs):
        nonlocal called
        called = True

    monkeypatch.setattr(mktrisk, "_tail_risk_request", fake_request)

    with pytest.raises(ValidationError):
        mktrisk.value_at_risk(
            TailRiskRequest(profit_loss_samples=[-1.0, 1.0], probability=1.0)
        )

    assert called is False


def test_typed_routes_publish_request_and_response_models():
    """Every native vertical slice exposes typed request and response schemas."""
    app = FastAPI()
    app.include_router(dqlib_router.api_router)
    schema = app.openapi()
    expected = {
        "/dqlib/iranalytics/curve_analytics": (
            "IrCurveAnalyticsRequest",
            "OBBject_IrCurveAnalyticsResult_",
        ),
        "/dqlib/fianalytics/fixed_coupon_bond_ytm": (
            "FixedCouponBondYtmRequest",
            "OBBject_FixedCouponBondYtmResult_",
        ),
        "/dqlib/eqanalytics/european_option": (
            "EquityEuropeanOptionRequest",
            "OBBject_EuropeanOptionResult_",
        ),
        "/dqlib/fxanalytics/atm_strike": (
            "FxAtmStrikeRequest",
            "OBBject_FxAtmStrikeResult_",
        ),
        "/dqlib/cranalytics/curve_analytics": (
            "CreditCurveAnalyticsRequest",
            "OBBject_CreditCurveAnalyticsResult_",
        ),
        "/dqlib/cmanalytics/european_option": (
            "CommodityEuropeanOptionRequest",
            "OBBject_EuropeanOptionResult_",
        ),
        "/dqlib/mktrisk/value_at_risk": (
            "TailRiskRequest",
            "OBBject_ValueAtRiskResult_",
        ),
        "/dqlib/mktrisk/expected_shortfall": (
            "TailRiskRequest",
            "OBBject_ExpectedShortfallResult_",
        ),
    }

    for path, (request_model, response_model) in expected.items():
        operation = schema["paths"][path]["post"]
        request_schema = operation["requestBody"]["content"][
            "application/json"
        ]["schema"]
        response_schema = operation["responses"]["200"]["content"][
            "application/json"
        ]["schema"]
        assert request_schema["$ref"].endswith(f"/{request_model}")
        assert response_schema["$ref"].endswith(f"/{response_model}")

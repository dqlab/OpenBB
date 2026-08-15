"""Tests for executable dqlib OpenBB commands."""

from dataclasses import dataclass
from datetime import date, datetime
from importlib import import_module
from types import ModuleType
from typing import Any

import pytest
from openbb_quantitative import (
    _dqlib,
    datetime as datetime_bridge,
)
from openbb_quantitative.dqlib_models import TailRiskRequest
from openbb_quantitative.dqlib_router import router as dqlib_router
from openbb_quantitative.risk import analytics as mktrisk


@dataclass
class NativePayload:
    """Synthetic stand-in for a native dqlib/protobuf object."""

    value: float
    as_of: date


def test_function_discovery_is_allowlisted(monkeypatch):
    """Discovery exposes analytics and constructors but not file operations."""
    fake_module = ModuleType("dqlib.iranalytics")
    fake_module.calculate_metric = lambda value: value
    fake_module.dqCreateProtoVector = lambda values: values
    fake_module.to_period = lambda value: value
    fake_module.load_curve_from_file = lambda path: path
    fake_module.unrelated_callable = lambda: None

    monkeypatch.setattr(_dqlib, "load_module", lambda domain=None: fake_module)

    names = {
        item["name"] for item in _dqlib.list_functions("iranalytics")
    }

    assert names == {
        "calculate_metric",
        "dqCreateProtoVector",
        "to_period",
    }


def test_execute_function_calls_real_domain_callable(monkeypatch):
    """The execution adapter invokes an installed dqlib domain callable."""
    fake_module = ModuleType("dqlib.eqanalytics")
    fake_module.calculate_metric = lambda value, scale=1: value * scale
    monkeypatch.setattr(_dqlib, "load_module", lambda domain=None: fake_module)

    result = _dqlib.execute_function(
        "eqanalytics",
        "calculate_metric",
        [3],
        {"scale": 4},
    )

    assert result == 12


def test_execute_function_rejects_unsafe_callable(monkeypatch):
    """Filesystem-oriented plugin functions cannot run through the gateway."""
    fake_module = ModuleType("dqlib.iranalytics")
    fake_module.load_curve_from_file = lambda path: path
    monkeypatch.setattr(_dqlib, "load_module", lambda domain=None: fake_module)

    with pytest.raises(_dqlib.DQLibExecutionError, match="unavailable"):
        _dqlib.execute_function(
            "iranalytics",
            "load_curve_from_file",
            ["private.json"],
        )


def test_pipeline_retains_native_objects_and_resolves_typed_values(monkeypatch):
    """Native results pass between steps before selected outputs are serialized."""
    fake_module = ModuleType("dqlib.analytics")

    def create_payload(value: float, as_of: date) -> NativePayload:
        return NativePayload(value=value, as_of=as_of)

    def calculate_payload(
        payload: NativePayload,
        increment: float,
    ) -> dict[str, Any]:
        return {
            "value": payload.value + increment,
            "as_of": payload.as_of,
        }

    fake_module.create_payload = create_payload
    fake_module.calculate_payload = calculate_payload
    monkeypatch.setattr(_dqlib, "load_module", lambda domain=None: fake_module)

    result = _dqlib.execute_pipeline(
        [
            {
                "id": "payload",
                "domain": "analytics",
                "function": "create_payload",
                "args": [4.0, {"$date": "2026-08-12"}],
            },
            {
                "id": "calculation",
                "domain": "analytics",
                "function": "calculate_payload",
                "args": [{"$ref": "payload"}],
                "kwargs": {"increment": 1.5},
            },
        ],
        outputs=["payload.value", "calculation"],
    )

    assert isinstance(result["calculation"]["as_of"], date)
    assert _dqlib.to_jsonable(result) == {
        "payload.value": 4.0,
        "calculation": {
            "value": 5.5,
            "as_of": "2026-08-12",
        },
    }


def test_pipeline_rejects_unknown_reference(monkeypatch):
    """A bad native-object reference fails before invoking dqlib."""
    fake_module = ModuleType("dqlib.analytics")
    fake_module.calculate_metric = lambda value: value
    monkeypatch.setattr(_dqlib, "load_module", lambda domain=None: fake_module)

    with pytest.raises(_dqlib.DQLibExecutionError, match="Unknown"):
        _dqlib.execute_pipeline(
            [
                {
                    "id": "metric",
                    "domain": "analytics",
                    "function": "calculate_metric",
                    "args": [{"$ref": "missing"}],
                }
            ]
        )


def test_json_serializer_handles_native_shapes():
    """Final results support dataclasses, dates, tuples, and binary payloads."""
    result = _dqlib.to_jsonable(
        {
            "payload": NativePayload(2.0, date(2026, 8, 12)),
            "timestamp": datetime(2026, 8, 12, 9, 30),
            "values": (1, 2),
            "raw": b"dq",
        }
    )

    assert result == {
        "payload": {"value": 2.0, "as_of": "2026-08-12"},
        "timestamp": "2026-08-12T09:30:00",
        "values": [1, 2],
        "raw": {"encoding": "base64", "data": "ZHE="},
    }


@pytest.mark.parametrize("domain", sorted(_dqlib.DQLIB_DOMAINS))
def test_each_domain_exposes_a_call_router(domain):
    """Every released dqlib domain has an executable OpenBB call command."""
    route_prefixes = {
        "analytics": "common",
        "cmanalytics": "commodity",
        "cranalytics": "credit",
        "eqanalytics": "equity",
        "fianalytics": "fixed_income",
        "fxanalytics": "foreign_exchange",
        "iranalytics": "interest_rate",
        "mktrisk": "risk",
    }
    bridge = import_module(f"openbb_quantitative.{domain}")
    paths = {route.path for route in bridge.router.api_router.routes}
    assert f"/{route_prefixes.get(domain, domain)}/call" in paths


def test_datetime_command_executes_dqlib(monkeypatch):
    """The typed date command delegates to dqlib's native calculator."""
    captured: dict[str, Any] = {}

    def fake_execute(domain, function, args, kwargs=None):
        captured.update(
            {
                "domain": domain,
                "function": function,
                "args": args,
            }
        )
        return 1.25

    monkeypatch.setattr(datetime_bridge, "execute_function", fake_execute)

    result = datetime_bridge.simple_year_fraction(
        date(2022, 3, 7),
        date(2023, 6, 7),
    )

    assert result.results.value == 1.25
    assert captured["domain"] == "datetime"
    assert captured["function"] == "simple_year_frac_calculator"
    assert captured["args"][0] == datetime(2022, 3, 7)


def test_market_risk_value_at_risk_uses_native_request(monkeypatch):
    """The typed VaR command bypasses broken dqlib 3.0.2 wrapper names."""
    native_vector = object()
    captured: dict[str, Any] = {}

    class FakeInput:
        def __init__(self):
            self.profit_loss_samples = self
            self.probability = 0.0
            self.calc_var_mirrored = False

        def CopyFrom(self, value):
            captured["vector"] = value

        def SerializeToString(self):
            return b"var-input"

    class FakeOutput:
        def __init__(self):
            self.success = True
            self.err_msg = ""
            self.value_at_risk = 8.5

        def ParseFromString(self, response):
            captured["response"] = response

    class FakeDQProto:
        CalculateValueAtRiskInput = FakeInput
        CalculateValueAtRiskOutput = FakeOutput

    def fake_request(name, payload):
        captured["request"] = (name, payload)
        return b"var-output"

    monkeypatch.setattr(
        mktrisk,
        "_tail_risk_runtime",
        lambda: (FakeDQProto, fake_request),
    )
    monkeypatch.setattr(
        mktrisk,
        "execute_function",
        lambda domain, function, args, kwargs=None: native_vector,
    )

    result = mktrisk.value_at_risk(
        TailRiskRequest(
            profit_loss_samples=[-4.0, 1.0, -2.0], probability=0.975
        )
    )

    assert result.results.value_at_risk == 8.5
    assert result.results.probability == 0.975
    assert captured == {
        "vector": native_vector,
        "request": ("CALCULATE_VALUE_AT_RISK", b"var-input"),
        "response": b"var-output",
    }


def test_generic_market_risk_call_uses_compatibility_path(monkeypatch):
    """Generic calls and pipelines avoid the broken dqlib 3.0.2 wrapper."""
    native_vector = object()
    captured: dict[str, Any] = {}

    def fake_tail_risk(samples, probability, antithetic, **metadata):
        captured.update(
            {
                "samples": samples,
                "probability": probability,
                "antithetic": antithetic,
                **metadata,
            }
        )
        return {"value_at_risk": 9.0}

    monkeypatch.setattr(mktrisk, "_tail_risk_request", fake_tail_risk)

    result = _dqlib.execute_function(
        "mktrisk",
        "calculate_value_at_risk",
        [native_vector, 0.975, False],
    )

    assert result == {"value_at_risk": 9.0}
    assert captured["samples"] is native_vector
    assert captured["request_name"] == "CALCULATE_VALUE_AT_RISK"


def test_root_router_builds_without_dqlib():
    """OpenBB exposes the full command tree without importing proprietary code."""
    routes = dqlib_router.api_router.routes
    paths = {route.path for route in routes}
    operation_ids = [
        route.operation_id
        for route in routes
        if getattr(route, "operation_id", None)
    ]

    assert {
        "/dqlib/status",
        "/dqlib/functions",
        "/dqlib/call",
        "/dqlib/pipeline",
        "/dqlib/interest_rate/call",
        "/dqlib/interest_rate/curve_analytics",
        "/dqlib/fixed_income/fixed_coupon_bond_ytm",
        "/dqlib/equity/european_option",
        "/dqlib/foreign_exchange/atm_strike",
        "/dqlib/credit/curve_analytics",
        "/dqlib/commodity/european_option",
        "/dqlib/datetime/simple_year_fraction",
        "/dqlib/risk/value_at_risk",
        "/dqlib/risk/expected_shortfall",
    } <= paths
    assert len(operation_ids) == len(set(operation_ids))

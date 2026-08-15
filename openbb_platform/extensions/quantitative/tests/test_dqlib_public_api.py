"""Tests for explicit public dqlib analytics bindings."""

from types import ModuleType

import pytest
from openbb_quantitative import _dqlib_api
from openbb_quantitative._dqlib import DQLibExecutionError
from openbb_quantitative.commodity import analytics as commodity
from openbb_quantitative.common import analytics as common
from openbb_quantitative.credit import analytics as credit
from openbb_quantitative.equity import analytics as equity
from openbb_quantitative.fixed_income import analytics as fixed_income
from openbb_quantitative.foreign_exchange import analytics as foreign_exchange
from openbb_quantitative.interest_rate import analytics as interest_rate
from openbb_quantitative.risk import analytics as risk

DOMAIN_MODULES = {
    "analytics": common,
    "cmanalytics": commodity,
    "cranalytics": credit,
    "eqanalytics": equity,
    "fianalytics": fixed_income,
    "fxanalytics": foreign_exchange,
    "iranalytics": interest_rate,
    "mktrisk": risk,
}


@pytest.mark.parametrize(("domain", "module"), DOMAIN_MODULES.items())
def test_public_function_bindings_are_explicit_and_callable(domain, module):
    """Each audited name is an actual callable in its OpenBB domain module."""
    assert module.PUBLIC_FUNCTIONS
    assert len(module.PUBLIC_FUNCTIONS) == len(set(module.PUBLIC_FUNCTIONS))
    assert all(name.isidentifier() for name in module.PUBLIC_FUNCTIONS)
    assert all(callable(getattr(module, name)) for name in module.PUBLIC_FUNCTIONS)
    assert all(
        getattr(module, name).__module__ == module.__name__
        for name in module.PUBLIC_FUNCTIONS
    )


def test_all_installed_analytics_families_have_declared_bindings():
    """The audited dqlib 3.0.2 analytics surface has no unowned functions."""
    assert sum(len(module.PUBLIC_FUNCTIONS) for module in DOMAIN_MODULES.values()) == 188


def test_public_binding_calls_the_declared_native_function(monkeypatch):
    """An explicit Python binding delegates to its exact installed function."""
    native_module = ModuleType("dqlib.iranalytics")
    native_module.ir_single_ccy_curve_builder = lambda curve, *, bump=0: (
        curve,
        bump,
    )
    monkeypatch.setattr(_dqlib_api, "load_module", lambda domain: native_module)

    result = interest_rate.ir_single_ccy_curve_builder("USD", bump=2)

    assert result == ("USD", 2)


def test_public_binding_sanitizes_native_errors(monkeypatch):
    """Bindings expose stable domain context without leaking native details."""
    native_module = ModuleType("dqlib.iranalytics")

    def failing_builder(*args, **kwargs):
        raise RuntimeError("private native detail")

    native_module.ir_single_ccy_curve_builder = failing_builder
    monkeypatch.setattr(_dqlib_api, "load_module", lambda domain: native_module)

    with pytest.raises(
        DQLibExecutionError,
        match=r"^dqlib operation failed: iranalytics\.ir_single_ccy_curve_builder$",
    ):
        interest_rate.ir_single_ccy_curve_builder("USD")

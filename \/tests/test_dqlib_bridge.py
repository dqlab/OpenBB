"""Tests for the optional dqlib bridge."""

from importlib import import_module
from types import ModuleType

import pytest
from openbb_quantitative import _dqlib


@pytest.mark.parametrize("domain", sorted(_dqlib.DQLIB_DOMAINS))
def test_domain_bridges_import_without_dqlib(domain):
    """Domain bridges must not import the proprietary runtime eagerly."""
    module = import_module(f"openbb_quantitative.{domain}")
    assert module.__doc__


def test_load_module_reports_actionable_error(monkeypatch):
    """Missing plugin errors should not expose loader or license internals."""

    def raise_missing(_name):
        raise ModuleNotFoundError("No module named 'dqlib'")

    monkeypatch.setattr(_dqlib, "import_module", raise_missing)

    assert _dqlib.is_available() is False
    with pytest.raises(_dqlib.DQLibUnavailableError, match="dqlib is unavailable"):
        _dqlib.load_module("iranalytics")


def test_load_module_rejects_unknown_domain():
    """Only documented release domains may be proxied."""
    with pytest.raises(ValueError, match="Unknown dqlib domain"):
        _dqlib.load_module("not_a_domain")


def test_domain_bridge_delegates_attributes(monkeypatch):
    """A bridge should preserve the released dqlib function API."""
    fake_module = ModuleType("dqlib.iranalytics")
    fake_module.curve_builder = lambda: "curve"

    monkeypatch.setattr(
        _dqlib,
        "load_module",
        lambda domain=None: fake_module,
    )

    bridge = import_module("openbb_quantitative.iranalytics")
    assert bridge.curve_builder() == "curve"
    assert "curve_builder" in dir(bridge)


def test_status_for_unavailable_supported_runtime(monkeypatch):
    """Status should stay useful when CI does not install proprietary dqlib."""
    monkeypatch.setattr(_dqlib, "is_available", lambda: False)
    monkeypatch.setattr(_dqlib, "installed_version", lambda: None)
    monkeypatch.setattr(_dqlib, "is_supported_runtime", lambda: True)

    status = _dqlib.get_status()

    assert status["available"] is False
    assert status["release_version"] == "3.0.2"
    assert status["installed_version"] is None
    assert "runtime license" in str(status["message"])

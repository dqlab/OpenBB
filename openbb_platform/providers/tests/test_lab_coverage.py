"""Guard registry inventory changes and missing provider test suites."""

from types import SimpleNamespace

import pytest

from providers.tests.utils import lab_coverage


def test_new_fetcher_needs_inventory_update(tmp_path, monkeypatch):
    """A newly registered model cannot silently inherit an existing declaration."""
    monkeypatch.setattr(lab_coverage, "ROOT", tmp_path)
    provider = SimpleNamespace(name="fixture", fetcher_dict={"Old": object(), "New": object()})
    with pytest.raises(AssertionError, match="update model inventory"):
        lab_coverage.validate_declaration(provider, {"models": ["Old"], "suites": ["tests"]})


def test_missing_or_empty_suite_fails(tmp_path, monkeypatch):
    """A directory alone is insufficient to satisfy test ownership."""
    monkeypatch.setattr(lab_coverage, "ROOT", tmp_path)
    provider = SimpleNamespace(name="fixture", fetcher_dict={})
    spec = {"models": [], "suites": ["tests"]}
    with pytest.raises(AssertionError, match="missing or empty"):
        lab_coverage.validate_declaration(provider, spec)
    (tmp_path / "tests").mkdir()
    with pytest.raises(AssertionError, match="missing or empty"):
        lab_coverage.validate_declaration(provider, spec)
    (tmp_path / "tests/test_source.py").write_text("def test_source(): assert True\n")
    lab_coverage.validate_declaration(provider, spec)

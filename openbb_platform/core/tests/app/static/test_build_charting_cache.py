"""Chart discovery is cached only within an individual package build."""

from types import SimpleNamespace

import pytest
from openbb_core.app.static import package_builder


def test_chart_discovery_is_scoped_to_build(monkeypatch):
    """Keep dynamic discovery outside a build and refresh it on the next build."""
    names = ["equity_price"]
    calls = []

    def functions():
        calls.append(1)
        return list(names)

    monkeypatch.setattr(package_builder, "CHARTING_INSTALLED", True)
    monkeypatch.setattr(
        package_builder, "Charting", SimpleNamespace(functions=functions), raising=False
    )
    assert package_builder._has_charting_function("/equity/price")
    assert package_builder._has_charting_function("/equity/price")
    assert len(calls) == 2

    with package_builder._build_charting_functions():
        assert package_builder._has_charting_function("/equity/price")
        names[:] = ["crypto_price"]
        assert package_builder._has_charting_function("/equity/price")
        assert not package_builder._has_charting_function("/crypto/price")
        with package_builder._build_charting_functions():
            assert package_builder._has_charting_function("/equity/price")
    assert len(calls) == 3

    with pytest.raises(RuntimeError), package_builder._build_charting_functions():
        assert package_builder._has_charting_function("/crypto/price")
        raise RuntimeError("abort build")
    names[:] = ["equity_price"]
    assert package_builder._has_charting_function("/equity/price")
    assert len(calls) == 5

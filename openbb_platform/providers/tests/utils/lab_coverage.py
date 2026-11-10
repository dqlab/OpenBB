"""Explicit model inventory and suite ownership for parameterized lab providers."""

from importlib import import_module
from pathlib import Path

from tomlkit import load

ROOT = Path(__file__).resolve().parents[4]


def declaration(provider_name: str) -> dict | None:
    """Find an audited declaration without importing optional native SDKs."""
    with (ROOT / ".github/lab-ci.toml").open(encoding="utf-8") as file:
        metadata = load(file)
    declarations = dict(metadata["provider_tests"])
    declarations.update(metadata["tick"]["provider_tests"])
    return declarations.get(provider_name)


def validate_declaration(provider, spec: dict) -> None:
    """Require declared model ownership and suites containing real tests."""
    expected = set(spec["models"])
    for family in spec.get("families", []):
        module, name = family.split(":")
        expected.update(getattr(import_module(module), name))
    assert (
        set(provider.fetcher_dict) == expected
    ), f"{provider.name}: update model inventory for the changed registry"
    assert spec["suites"], f"{provider.name}: missing test suite declaration"
    for suite in spec["suites"]:
        path = ROOT / suite
        assert path.is_dir() and any(
            path.rglob("test_*.py")
        ), f"{provider.name}: declared suite missing or empty: {suite}"

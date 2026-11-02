"""Exercise public Python/API schemas and exact offline command ownership."""

import pytest
from openbb_core.app.router import CommandMap

from .utils.lab_inventory import (
    installed_cases,
    manifest,
    route_owners,
    validate_routes,
)


def test_internal_public_routes_have_python_and_api_schemas():
    """Keep every declared command visible in the actual generated interfaces."""
    from openbb import obb
    from openbb_core.api.rest_api import app
    from openbb_core.app.service.system_service import SystemService

    routes = list(CommandMap().map)
    validate_routes(routes, manifest()["router_tests"])
    schemas = app.openapi()["paths"]
    api_prefix = SystemService().system_settings.api_settings.prefix
    owned = route_owners()
    for path in routes:
        if path not in owned:
            continue
        command = obb
        for part in path.strip("/").split("/"):
            command = getattr(command, part)
        assert callable(command), path
        api_path = api_prefix + path
        assert api_path in schemas and any(
            method in schemas[api_path] for method in ("get", "post")
        ), path


def test_new_public_route_cannot_inherit_suite_ownership():
    """A prefix is insufficient: adding or deleting a command changes inventory."""
    spec = {
        "fixture": {"prefix": "/fixture", "commands": ["old"], "suites": ["real/tests"]}
    }
    validate_routes(["/fixture/old"], spec)
    with pytest.raises(AssertionError, match="inventory differs"):
        validate_routes(["/fixture/old", "/fixture/new"], spec)
    with pytest.raises(AssertionError, match="inventory differs"):
        validate_routes([], spec)
    spec["fixture"]["commands"] = ["live/export"]
    with pytest.raises(AssertionError, match="inventory differs"):
        validate_routes(["/fixture/live_export"], spec)


def test_fixture_scope_preserves_installed_cases_and_default_cases():
    """Only provider-specific fixtures for unavailable distributions are inapplicable."""
    cases = [
        {"provider": "fmp", "symbol": "AAPL"},
        {"provider": "absent", "foreign": True},
        {"symbol": "AAPL"},
    ]
    assert installed_cases(cases, {"fmp": {}}) == [cases[0], cases[2]]


def test_installed_but_unsupported_provider_is_an_error():
    """An installed provider must never disappear because this model lacks it."""
    from .utils.integration_tests_testers import check_wrong_params

    cases = installed_cases([{"provider": "fmp"}], {"fmp": {}})
    errors = check_wrong_params({"openbb": {}}, cases, "fixture")
    assert errors == ["Provider fmp does not support function fixture"]


def test_api_example_for_installed_unsupported_provider_fails(monkeypatch):
    """Unavailable providers are inapplicable; installed model misuse is invalid."""
    from types import SimpleNamespace

    from .utils import router_testers

    monkeypatch.setattr(router_testers, "installed_providers", lambda: {"fmp": {}})
    monkeypatch.setattr(router_testers, "get_required_fields", lambda _: [])
    monkeypatch.setattr(router_testers, "get_all_fields", lambda _: ["symbol"])
    monkeypatch.setattr(
        router_testers,
        "ProviderInterface",
        lambda: SimpleNamespace(map={"Example": {"openbb": {}}}),
    )
    function = SimpleNamespace(__name__="fixture")
    errors = router_testers.check_api(
        "[APIEx(parameters={'provider': 'fmp', 'symbol': 'AAPL'})]",
        "fixture",
        "'Example'",
        function,
    )
    assert errors and "installed provider fmp does not support" in errors[0]
    assert not router_testers.check_api(
        "[APIEx(parameters={'provider': 'absent'})]", "fixture", "'Example'", function
    )


def test_unavailable_route_requires_unchanged_source_and_missing_model(
    tmp_path, monkeypatch
):
    """The explicit legacy route scope cannot hide deletion or an available model."""
    import hashlib

    from .utils import lab_inventory

    source = tmp_path / "router.py"
    source.write_text("@router.command(model='Unavailable')\ndef example(): pass\n")
    spec = {"model": "Unavailable", "source": "router.py", "function": "example"}
    spec["source_sha256"] = hashlib.sha256(source.read_bytes()).hexdigest()
    monkeypatch.setattr(lab_inventory, "ROOT", tmp_path)
    monkeypatch.setattr(
        lab_inventory, "manifest", lambda: {"unavailable_routes": {"fixture": spec}}
    )
    assert lab_inventory.unavailable_model_route("fixture", {})
    assert not lab_inventory.unavailable_model_route("fixture", {"Unavailable": {}})
    assert not lab_inventory.unavailable_model_route("other", {})
    source.write_text("@router.command(model='Changed')\ndef example(): pass\n")
    with pytest.raises(AssertionError, match="source inventory changed"):
        lab_inventory.unavailable_model_route("fixture", {})
    spec["source_sha256"] = hashlib.sha256(source.read_bytes()).hexdigest()
    with pytest.raises(AssertionError, match="source model inventory changed"):
        lab_inventory.unavailable_model_route("fixture", {})
    source.write_text("def removed(): pass\n")
    spec["source_sha256"] = hashlib.sha256(source.read_bytes()).hexdigest()
    with pytest.raises(AssertionError, match="no longer exists"):
        lab_inventory.unavailable_model_route("fixture", {})

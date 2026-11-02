"""Audit offline suite ownership without importing uninstalled upstream packages."""

import ast
import hashlib
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path

from openbb_core.provider.registry import RegistryLoader
from tomlkit import load

ROOT = Path(__file__).resolve().parents[4]


def manifest() -> dict:
    """Read the shared lab contract used by the actual CI runner."""
    with (ROOT / ".github/lab-ci.toml").open(encoding="utf-8") as file:
        return load(file)


def installed_extension(directory: Path) -> bool:
    """Only inspect integration fixtures belonging to installed distributions."""
    project = directory / "pyproject.toml"
    if not project.is_file():
        return False
    with project.open(encoding="utf-8") as file:
        data = load(file)
    name = data.get("project", {}).get("name") or data.get("tool", {}).get(
        "poetry", {}
    ).get("name")
    if not name:
        return False
    try:
        version(name)
    except PackageNotFoundError:
        return False
    return True


def installed_cases(cases: list[dict], providers: dict) -> list[dict]:
    """Validate provider-specific fixture parameters against installed providers."""
    return [
        case
        for case in cases
        if "provider" not in case or case["provider"] in providers
    ]


def installed_providers() -> dict:
    """Use the global installed registry, including providers with no fetchers."""
    return RegistryLoader.from_extensions().providers


def route_owners() -> dict[str, list[str]]:
    """Expand the explicit command inventory, with no prefix wildcard exemptions."""
    return {
        spec["prefix"] + "/" + command: spec["suites"]
        for prefix, spec in manifest()["router_tests"].items()
        for command in spec["commands"]
    }


def validate_routes(routes: list[str], declarations: dict) -> None:
    """Require an inventory update when an internal public command changes."""
    available = set(routes)
    for prefix, spec in declarations.items():
        expected = {spec["prefix"] + "/" + command for command in spec["commands"]}
        actual = {
            route for route in available if route.startswith(spec["prefix"] + "/")
        }
        assert (
            actual == expected
        ), f"{prefix}: public route inventory differs: {actual ^ expected}"
        assert spec["suites"], f"{prefix}: missing offline suite ownership"


def unavailable_model_route(route: str, available_models: dict) -> bool:
    """Recognize an explicit source command hidden by an uninstalled model."""
    spec = manifest().get("unavailable_routes", {}).get(route)
    if not spec or spec["model"] in available_models:
        return False
    source = (ROOT / spec["source"]).read_bytes()
    assert (
        hashlib.sha256(source).hexdigest() == spec["source_sha256"]
    ), f"{route}: source inventory changed"
    tree = ast.parse(source)
    for node in tree.body:
        if (
            not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            or node.name != spec["function"]
        ):
            continue
        for decorator in node.decorator_list:
            if (
                isinstance(decorator, ast.Call)
                and ast.unparse(decorator.func) == "router.command"
            ):
                models = [
                    kw.value.value
                    for kw in decorator.keywords
                    if kw.arg == "model" and isinstance(kw.value, ast.Constant)
                ]
                assert models == [
                    spec["model"]
                ], f"{route}: source model inventory changed"
                return True
    raise AssertionError(
        f"{route}: declared unavailable source command no longer exists"
    )

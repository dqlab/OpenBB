"""Regression tests for reference metadata generation."""

import json
from copy import deepcopy
from types import SimpleNamespace
from typing import Literal

from openbb_core.app.static.package_builder import PackageBuilder, ReferenceGenerator
from pydantic import BaseModel, Field


def test_reference_json_preserves_text(tmp_path):
    """Writing JSON must preserve descriptions, choices, and keys verbatim."""
    value = {
        "ListKey": "List typing.List Listing",
        "description": "List of typing.List and Listing",
        "default": "typing.List",
        "choices": ["List", "typing.List"],
        "type": "list[str]",
    }
    builder = PackageBuilder.__new__(PackageBuilder)
    builder.directory = tmp_path
    builder.console = SimpleNamespace(log=lambda *args, **kwargs: None)
    builder._write(json.dumps(value), "reference", "json", "assets")
    assert json.loads((tmp_path / "assets/reference.json").read_text()) == value
    builder._write("x: typing.List[str] = []", "example")
    assert (tmp_path / "package/example.py").read_text() == "x: list[str] = []"


def test_provider_metadata_repeatable(monkeypatch):
    """Reference generation must not consume nested provider metadata."""

    class Model(BaseModel):
        symbols: list[Literal["List", "typing.List"]] = Field(
            default=[],
            description="List typing.List Listing",
            json_schema_extra={
                "demo": {
                    "choices": ["List", "typing.List"],
                    "multiple_items_allowed": True,
                    "note": "typing.List",
                }
            },
        )

    field = Model.model_fields["symbols"]
    before = deepcopy(field.json_schema_extra)
    mapping = {"Probe": {"demo": {"QueryParams": {"class": Model, "fields": {"symbols": field}}}}}
    monkeypatch.setattr(ReferenceGenerator, "pi", SimpleNamespace(map=mapping))
    first = ReferenceGenerator._get_provider_field_params("Probe", "QueryParams", "demo")
    second = ReferenceGenerator._get_provider_field_params("Probe", "QueryParams", "demo")
    assert first == second
    assert field.json_schema_extra == before
    assert first[0]["choices"] == ["List", "typing.List"]
    assert first[0]["multiple_items_allowed"]
    assert first[0]["json_schema_extra"]["note"] == "typing.List"
    assert "List typing.List Listing" in first[0]["description"]
    assert "list[" in first[0]["type"].lower()

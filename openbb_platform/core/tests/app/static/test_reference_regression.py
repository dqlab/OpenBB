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


def test_python_writer_preserves_values_and_comments(tmp_path):
    """Check python writer preserves values and comments."""
    builder = PackageBuilder.__new__(PackageBuilder)
    builder.directory = tmp_path
    builder.console = SimpleNamespace(log=lambda *args, **kwargs: None)
    source = "x: typing.List[Literal['List', 'typing.List']] = ['List']\n# Listing typing.List\n"
    builder._write(source, "probe")
    assert (tmp_path / "package/probe.py").read_text() == source.replace("typing.List[Literal", "list[Literal")


def test_provider_type_preserves_literal_values_and_no_nested_list(monkeypatch):
    """Check provider type preserves literal values and no nested list."""

    class Model(BaseModel):
        x: list[Literal["List", "typing.List"]] = Field(
            default=[], json_schema_extra={"demo": {"choices": ["List", "typing.List"], "multiple_items_allowed": True}}
        )

    field = Model.model_fields["x"]
    mapping = {"Probe": {"demo": {"QueryParams": {"class": Model, "fields": {"x": field}}}}}
    monkeypatch.setattr(ReferenceGenerator, "pi", SimpleNamespace(map=mapping))
    result = ReferenceGenerator._get_provider_field_params("Probe", "QueryParams", "demo")[0]
    assert "'List'" in result["type"]
    assert "'typing.List'" in result["type"]
    assert "list[list[" not in result["type"].replace(" ", "")
    assert "list[None" not in result["type"]


def test_route_literal_and_default_preserved():
    """Check route literal and default preserved."""
    from typing import Annotated

    from fastapi import Query

    def endpoint(
        x: Annotated[
            Literal["List", "typing.List"],
            Query(
                description="List typing.List",
                json_schema_extra={"choices": ["List", "typing.List"], "multiple_items_allowed": True},
            ),
        ] = "List",
    ):
        pass

    first = ReferenceGenerator._get_function_signature_info(endpoint)[0]
    assert "'typing.List'" in first["type"]
    assert first["default"] == "List"
    assert first["choices"] == ["List", "typing.List"]


def test_signature_generation_preserves_literal_default_and_description():
    """Check signature generation preserves literal default and description."""
    from collections import OrderedDict
    from inspect import Parameter
    from typing import Annotated

    from openbb_core.app.model.field import OpenBBField
    from openbb_core.app.static.package_builder import MethodDefinition

    param = Parameter(
        "x",
        Parameter.KEYWORD_ONLY,
        default="typing.List",
        annotation=Annotated[Literal["List", "typing.List"], OpenBBField(description="List typing.List")],
    )
    result = MethodDefinition.build_func_params(OrderedDict(x=param))
    assert "'typing.List'" in result
    assert "'List'" in result
    assert "List typing.List" in result


def test_route_annotated_query_keeps_signature_default():
    """Check route annotated query keeps signature default."""
    from typing import Annotated

    from fastapi import Query

    def endpoint(x: Annotated[int, Query(description="value")] = 42):
        pass

    result = ReferenceGenerator._get_function_signature_info(endpoint)[0]
    assert result["default"] == 42


def test_generated_python_executes_with_exact_annotations_and_data(tmp_path):
    """Check generated python executes with exact annotations and data."""
    from collections import OrderedDict
    from inspect import Parameter, signature
    from typing import Annotated, get_args

    from openbb_core.app.model.field import OpenBBField
    from openbb_core.app.static.package_builder import MethodDefinition

    description = "List typing.List  with  exact whitespace\nand a long description " * 2
    annotation = Annotated[Literal["List", "typing.List"] | int, OpenBBField(description=description)]
    param = Parameter("x", Parameter.KEYWORD_ONLY, default="typing.List", annotation=annotation)
    params = MethodDefinition.build_func_params(OrderedDict(x=param))
    source = (
        "import typing\nfrom typing import Annotated, Literal, Union\n"
        "from openbb_core.app.model.field import OpenBBField\n"
        + f"def probe({params}):\n    return x\n"
    )
    builder = PackageBuilder.__new__(PackageBuilder)
    builder.directory = tmp_path
    builder.console = SimpleNamespace(log=lambda *args, **kwargs: None)
    builder._write(source, "probe")
    namespace = {}
    exec(compile((tmp_path / "package/probe.py").read_text(), "probe.py", "exec"), namespace)  # noqa: S102
    function = namespace["probe"]
    result = signature(function).parameters["x"]
    assert function() == "typing.List"
    assert function(x="List") == "List"
    assert result.default == "typing.List"
    typ, metadata = get_args(result.annotation)
    assert typ == (Literal["List", "typing.List"] | int)
    assert get_args(typ) == (Literal["List", "typing.List"], int)
    assert metadata.description == description
    assert all(len(line) <= 122 for line in source.splitlines())


def test_python_writer_preserves_shadowed_typing_and_metadata(tmp_path):
    """Check python writer preserves shadowed typing and metadata."""
    builder = PackageBuilder.__new__(PackageBuilder)
    builder.directory = tmp_path
    builder.console = SimpleNamespace(log=lambda *args, **kwargs: None)
    source = "def probe(typing):\n    x: typing.List[str] = []\n    return x\n"
    builder._write(source, "probe")
    assert (tmp_path / "package/probe.py").read_text() == source
    source = "x: Annotated[typing.List[str], field(typing.List)] = []\n"
    builder._write(source, "probe")
    assert (tmp_path / "package/probe.py").read_text() == "x: Annotated[list[str], field(typing.List)] = []\n"


def test_multiple_items_metadata_matches_provider_validation(monkeypatch):
    """Check multiple items metadata matches provider validation."""
    import pytest
    from pydantic import ValidationError

    class Model(BaseModel):
        flexible: str | list[str] | None = Field(
            default=None, json_schema_extra={"multiple_items_allowed": True, "choices": []}
        )
        scalar: str = Field(json_schema_extra={"multiple_items_allowed": True, "choices": None})

    mapping = {"Probe": {"demo": {"QueryParams": {"class": Model, "fields": Model.model_fields}}}}
    monkeypatch.setattr(ReferenceGenerator, "pi", SimpleNamespace(map=mapping))
    params = ReferenceGenerator._get_provider_field_params("Probe", "QueryParams", "demo")
    by_name = {p["name"]: p for p in json.loads(json.dumps(params, allow_nan=False))}
    assert by_name["flexible"]["type"] == "str | list[str] | None"
    assert by_name["scalar"]["type"] == "str"
    assert by_name["flexible"]["choices"] == []
    assert by_name["scalar"]["choices"] is None
    assert Model(flexible=["A", "B"], scalar="A,B").flexible == ["A", "B"]
    with pytest.raises(ValidationError):
        Model(scalar=["A", "B"])


def test_reference_loader_preserves_provider_choices(tmp_path):
    """The public reference consumer retains distinct null and empty choices."""
    from openbb_core.app.static.reference_loader import ReferenceLoader

    reference = {
        "paths": {
            "/economy/calendar": {
                "parameters": {
                    "nasdaq": [
                        {"name": "country", "choices": None},
                        {"name": "other", "choices": []},
                    ]
                }
            }
        }
    }
    assets = tmp_path / "assets"
    assets.mkdir()
    (assets / "reference.json").write_text(json.dumps(reference), encoding="utf-8")
    instances = ReferenceLoader._instances
    ReferenceLoader._instances = {}
    try:
        loaded = ReferenceLoader(tmp_path).reference
        assert loaded == reference
        params = loaded["paths"]["/economy/calendar"]["parameters"]["nasdaq"]
        assert params[0]["choices"] is None
        assert params[1]["choices"] == []
    finally:
        ReferenceLoader._instances = instances


def test_format_annotation_preserves_forward_references_and_unicode():
    """Check format annotation preserves forward references and unicode."""
    from typing import ForwardRef, Union

    from openbb_core.app.static.package_builder import DocstringGenerator

    assert DocstringGenerator.get_field_type(ForwardRef("typing.List[Thing]"), True) == "typing.List[Thing]"
    result = DocstringGenerator.get_field_type(
        Union[Literal["文字 typing.List", "NoneType"], list[str], None], True, "website"  # noqa: UP007
    )
    assert "文字 typing.List" in result and "'NoneType'" in result
    assert "list[str]" in result and "None" in result

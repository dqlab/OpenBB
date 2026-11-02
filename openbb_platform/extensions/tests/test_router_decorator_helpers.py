"""Check exact router decorator discovery, including unrelated helpers."""

from .utils import helpers


def test_find_decorator_uses_exact_function_and_multiline_ast(tmp_path):
    """A preceding command must not be attributed to an undecorated helper."""
    path = tmp_path / "router.py"
    path.write_text(
        "@router.command(\n    model='Example',\n    examples=[1],\n)\n"
        "@another_decorator\n"
        "def command():\n    pass\n\n"
        "def helper():\n    pass\n"
    )
    assert (
        helpers.find_decorator(str(path), "command")
        == "@router.command(model='Example', examples=[1])"
    )
    assert helpers.find_decorator(str(path), "helper") == ""
    assert helpers.find_decorator(str(path), "missing") == ""


def test_get_decorator_details_selects_command_and_handles_no_decorators(monkeypatch):
    """Return the command decorator even when another decorator follows it."""
    source = "@router.command(model='Example')\n@other\ndef command():\n    pass\n"
    monkeypatch.setattr(helpers.inspect, "getsource", lambda _: source)
    result = helpers.get_decorator_details(object())
    assert result.name == "router.command"
    assert result.kwargs == {"model": "'Example'"}
    monkeypatch.setattr(
        helpers.inspect, "getsource", lambda _: "def helper():\n    pass\n"
    )
    assert helpers.get_decorator_details(object()) is None
    source_result = helpers.get_decorator_details(
        object(), "@router.command(model='Example')"
    )
    assert source_result.name == "router.command"
    assert source_result.kwargs == {"model": "'Example'"}

"""The development installer understands the collectors' PEP 621 metadata."""

import importlib.util
import subprocess
from pathlib import Path

import pytest
from tomlkit import loads


def test_pep621_dependencies_retain_extras_markers_and_urls(tmp_path, monkeypatch):
    path = Path(__file__).resolve().parents[3] / "dev_install.py"
    spec = importlib.util.spec_from_file_location("collector_dev_install", path)
    installer = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(installer)
    package = tmp_path / "sample"
    package.mkdir()
    (package / "pyproject.toml").write_text(
        "[project]\ndependencies = ['pydantic>=2.7,<3']\n"
        "[project.optional-dependencies]\n"
        "dev = [\"pytest[extra]>=8; python_version >= '3.11'\", "
        "'sample @ https://example.invalid/sample.whl']\n"
    )
    monkeypatch.setattr(installer, "PLATFORM_PATH", tmp_path)
    result = installer.extract_dependencies("sample", dev=True)
    assert result["pytest"]["extras"] == ["extra"]
    assert result["pytest"]["markers"] == 'python_version >= "3.11"'
    assert result["sample"] == {"url": "https://example.invalid/sample.whl"}
    assert installer.extract_dependencies("sample")["pydantic"]["version"] == "<3,>=2.7"
    dependencies = loads(installer.LOCAL_DEPS)["tool"]["poetry"]["dependencies"]
    for name in (
        "openbb-collector-core",
        "dq-historical-market-data-collector",
        "dq-live-market-data-collector",
        "openbb-collection",
    ):
        assert dependencies[name]["markers"] == "python_version >= '3.11'"


@pytest.mark.parametrize("target", ["platform", "cli"])
@pytest.mark.parametrize("failure", ["lock", "install", "interrupt"])
def test_poetry_failure_restores_files_and_propagates(tmp_path, monkeypatch, target, failure):
    """Restore both metadata files while preserving failure and cancellation."""
    path = Path(__file__).resolve().parents[3] / "dev_install.py"
    spec = importlib.util.spec_from_file_location("failed_dev_install", path)
    installer = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(installer)
    project = tmp_path / "pyproject.toml"
    lock = tmp_path / "poetry.lock"
    original = '[tool.poetry.dependencies]\nopenbb="^4.7.3"\n'
    project.write_text(original)
    lock.write_text("original lock\n")
    for name, value in {
        "PLATFORM_PATH": tmp_path,
        "PYPROJECT": project,
        "LOCK": lock,
        "CLI_PATH": tmp_path,
        "CLI_PYPROJECT": project,
        "CLI_LOCK": lock,
    }.items():
        monkeypatch.setattr(installer, name, value)
    calls = []

    def execute(command, **kwargs):
        assert kwargs["check"] is True
        calls.append(command)
        lock.write_text("generated lock\n")
        if failure == "interrupt":
            raise KeyboardInterrupt
        if command[3] == failure:
            raise subprocess.CalledProcessError(19, command)

    monkeypatch.setattr(installer.subprocess, "run", execute)
    expected = KeyboardInterrupt if failure == "interrupt" else subprocess.CalledProcessError
    with pytest.raises(expected):
        if target == "platform":
            installer.install_platform_local()
        else:
            installer.install_platform_cli()
    assert project.read_text() == original
    assert lock.read_text() == "original lock\n"
    assert len(calls) == (2 if failure == "install" else 1)

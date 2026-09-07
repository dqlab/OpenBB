"""The development installer understands the collectors' PEP 621 metadata."""

import importlib.util
from pathlib import Path

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

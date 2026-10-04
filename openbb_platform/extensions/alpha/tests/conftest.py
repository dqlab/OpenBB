"""Isolated offline fixtures."""

import pytest


@pytest.fixture(autouse=True)
def private_store(tmp_path, monkeypatch):
    root = tmp_path / "artifacts"
    root.mkdir(mode=0o700)
    monkeypatch.setenv("OPENBB_ALPHA_ROOT", str(root))
    return root

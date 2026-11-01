"""Avoid importing the provider tests on unsupported Python versions."""

import sys

import pytest


@pytest.fixture(autouse=True)
def short_window_for_synthetic_rest_tests(request, monkeypatch, tmp_path):
    """Isolate legacy synthetic REST cases and avoid production cooldown waits."""
    if request.module.__name__.split(".")[-1] in {
        "test_provider", "test_sampled_snapshot", "test_review_regressions",
        "test_review_blockers", "test_review_transport", "test_recovery_compat_alert",
        "test_snapshot_independent_review",
    }:
        from openbb_futu import coordination

        monkeypatch.setattr(coordination, "BUDGET_WINDOW", 0.02)
        monkeypatch.setenv("FUTU_COORDINATION_DIR", str(tmp_path / "coord"))
        # This injection is confined to test subprocesses; production defaults
        # remain unchanged.  It also reaches the sampler's spawned worker.
        import os
        injection = tmp_path / "test_python"
        injection.mkdir()
        (injection / "sitecustomize.py").write_text(
            "import openbb_futu.coordination as c\nc.BUDGET_WINDOW = 0.02\n"
        )
        monkeypatch.setenv("PYTHONPATH", str(injection) + os.pathsep + os.environ.get("PYTHONPATH", ""))


def pytest_ignore_collect(collection_path):
    """Exclude test modules outside the provider's supported Python range."""
    if collection_path.name.startswith("test_") and (
        sys.version_info < (3, 11) or sys.version_info >= (3, 14)
    ):
        return True
    return None

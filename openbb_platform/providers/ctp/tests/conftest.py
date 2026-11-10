"""Avoid importing the provider tests on unsupported Python versions."""

import sys


def pytest_ignore_collect(collection_path):
    """Exclude test modules outside the provider's supported Python range."""
    return collection_path.name.startswith("test_") and (
        sys.version_info < (3, 11) or sys.version_info >= (3, 14)
    )

"""Record the exact integration-marked tests excluded by the offline command."""

import json
import os
from pathlib import Path

DESELECTED: list[str] = []


def pytest_deselected(items):
    """Keep excluded node IDs in the same attempt as the JUnit results."""
    DESELECTED.extend(item.nodeid for item in items)


def pytest_runtest_logreport(report):
    """Show failures immediately during long subprocess acceptance suites."""
    if report.failed:
        print(  # noqa: T201
            f"\nFAILED {report.nodeid} ({report.when})\n{report.longrepr}", flush=True
        )


def pytest_sessionfinish():
    """Write even an empty exclusion list so report presence is verifiable."""
    Path(os.environ["LAB_DESELECTED_REPORT"]).write_text(
        json.dumps(DESELECTED, indent=2) + "\n"
    )

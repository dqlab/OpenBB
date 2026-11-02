"""Nox sessions."""

from pathlib import Path

import nox

ROOT_DIR = Path(__file__).parent.parent.parent
PLATFORM_DIR = ROOT_DIR / "openbb_platform"
LAB_CI = ROOT_DIR / ".github" / "scripts" / "lab_ci.py"
CLI_DIR = ROOT_DIR / "cli"
CLI_TESTS = CLI_DIR / "tests"


@nox.session(python="3.12.13")
def unit_test_platform(session):
    """Run the explicit lab inventory in the same offline mode as Actions."""
    session.run("python", str(LAB_CI), "install")
    session.run("python", str(ROOT_DIR / ".github/scripts/run_lab_offline.py"))


@nox.session(python=["3.10", "3.11", "3.12", "3.13", "3.14"])
def unit_test_cli(session):
    """Run the test suite."""
    session.install("poetry")
    session.run(
        "python",
        str(PLATFORM_DIR / "dev_install.py"),
        "-e",
        "--cli",
        external=True,
    )
    session.install("pytest")
    session.install("pytest-cov")
    session.run("pytest", CLI_TESTS, f"--cov={CLI_DIR}")

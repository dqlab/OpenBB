"""Install and test the internal lab against one explicit package inventory."""

# ruff: noqa: S603, T201

import argparse
import json
import os
import socket
import subprocess
import sys
import tempfile
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
MANIFEST = ROOT / ".github/lab-ci.toml"


def inventory(root: Path = ROOT) -> dict:
    """Require the baseline and include the complete PR6 pair when present."""
    data = tomllib.loads((root / ".github/lab-ci.toml").read_text())
    tick = data.pop("tick")
    present = [(root / path.split("[", 1)[0]).is_dir() for path in tick["packages"]]
    if any(present) and not all(present):
        raise RuntimeError("Incomplete PR6 tree: Futu and CTP must both be present")
    data["tick_present"] = all(present)
    if all(present):
        for key in ("packages", "tests", "required_imports"):
            data[key].extend(tick[key])
        data.setdefault("provider_tests", {}).update(tick.get("provider_tests", {}))
    data["tests"].extend(data.get("offline_integration_tests", []))
    for path in data["packages"] + data["tests"]:
        if not (root / path.split("::", 1)[0].split("[", 1)[0]).exists():
            raise FileNotFoundError(f"Required lab path missing: {path}")
    return data


def run(args: list[str], **kwargs) -> subprocess.CompletedProcess:
    """Run with the selected interpreter; a failed prerequisite stops execution."""
    print("RUN", *args, flush=True)
    return subprocess.run(args, cwd=ROOT, check=True, **kwargs)


def install(data: dict, reports: Path) -> None:
    """Resolve local packages together and propagate every installation error."""
    args = [
        sys.executable,
        "-m",
        "pip",
        "install",
        "-r",
        str(ROOT / ".github/requirements-lab-ci.txt"),
    ]
    for package in data["packages"]:
        args.extend(["-e", str(ROOT / package)])
    run(args)
    run([sys.executable, "-m", "pip", "check"])
    result = run(
        [sys.executable, "-m", "pip", "list", "--format=json"],
        capture_output=True,
        text=True,
    )
    (reports / "environment.json").write_text(result.stdout)


def test_environment(reports: Path, tick: bool) -> dict[str, str]:
    """Keep settings private and require a namespace with only loopback."""
    if {name for _, name in socket.if_nameindex()} != {"lo"}:
        raise RuntimeError(
            "Run tests in a network namespace containing only loopback (see CI README)"
        )
    env = os.environ.copy()
    if env.get("PYTEST_ADDOPTS"):
        raise RuntimeError(
            "Unset PYTEST_ADDOPTS so the lab test selection is reproducible"
        )
    env["OPENBB_ALPHA_TEST_SETTINGS"] = str(reports / "settings")
    env["PYTHONPATH"] = str(ROOT / "scripts/alpha_research")
    env["PYTHONPATH"] += os.pathsep + str(ROOT / ".github/scripts")
    env["OPENBB_DUCKDB_INTERFACE_TEST"] = "1"
    # Avoid oversubscribing small CI runners during real research-engine tests.
    env["POLARS_MAX_THREADS"] = "2"
    env["OMP_NUM_THREADS"] = "2"
    env["OPENBLAS_NUM_THREADS"] = "2"
    if tick:
        locale_dir = reports / "locale"
        locale_dir.mkdir(exist_ok=True)
        run(
            [
                "localedef",
                "--no-archive",
                "-i",
                "zh_CN",
                "-f",
                "GB18030",
                str(locale_dir / "zh_CN.GB18030"),
            ]
        )
        env["LOCPATH"] = str(locale_dir)
        env["CTP_REQUIRE_SDK"] = "1"
    return env


def run_suite(
    group: str, junit: Path, env: dict[str, str], marker: str = "not integration"
) -> dict:
    """Preserve collection errors, empty selections, skip reasons and exclusions."""
    # Installation must bootstrap in a venv containing only pip and the stdlib.
    from defusedxml.ElementTree import parse  # pylint: disable=import-outside-toplevel

    env = {**env, "LAB_DESELECTED_REPORT": str(junit.with_suffix(".json"))}
    command = [
        sys.executable,
        "-m",
        "pytest",
        group,
        "-q",
        "-ra",
        "-m",
        marker,
        "-p",
        "lab_pytest_report",
        f"--junitxml={junit}",
    ]
    print("TEST", group, flush=True)
    result = subprocess.run(command, cwd=ROOT, env=env, check=False)
    suites = parse(junit).getroot() if junit.is_file() else []
    counts = {
        key: sum(int(suite.get(key, 0)) for suite in suites)
        for key in ("tests", "failures", "errors", "skipped")
    }
    passed = counts["tests"] - counts["failures"] - counts["errors"] - counts["skipped"]
    skips = [
        {
            "test": case.get("classname", "") + "." + case.get("name", ""),
            "reason": skip.get("message", ""),
        }
        for suite in suites
        for case in suite.findall("testcase")
        for skip in case.findall("skipped")
    ]
    deselected_path = junit.with_suffix(".json")
    deselected = json.loads(deselected_path.read_text())
    return {
        "path": group,
        "marker_expr": marker,
        "exit": result.returncode,
        **counts,
        "passed": passed,
        "skip_reasons": skips,
        "deselected_integration": deselected,
    }


def suite_failed(row: dict) -> bool:
    """All-skipped, empty and errored suites cannot produce a passing check."""
    return bool(row["exit"] or row["failures"] or row["errors"] or row["passed"] <= 0)


def missing_provider_suites(data: dict, results: list[dict]) -> list[str]:
    """Require each declared provider suite to have passed in this attempt."""
    outcomes = {row["path"]: row for row in results}
    return [
        f"{kind}.{provider}: {suite}"
        for kind in ("provider_tests", "router_tests")
        for provider, spec in data.get(kind, {}).items()
        for suite in spec["suites"]
        if suite not in outcomes or suite_failed(outcomes[suite])
    ]


def test_groups(data: dict, reports: Path) -> int:
    """Test clean assets first, build, then run every selected suite separately."""
    env = test_environment(reports, data["tick_present"])
    run(
        [
            sys.executable,
            "-c",
            "import importlib,sys; [importlib.import_module(n) for n in sys.argv[1:]]",
            *data["required_imports"],
        ],
        env=env,
    )
    results = []
    built = False
    for index, group in enumerate(data["tests"]):
        if group == "openbb_platform/core/tests":
            run([sys.executable, "-m", "openbb_core.build"], env=env)
            built = True
        if group.startswith("openbb_platform/core/") and not built:
            raise RuntimeError("Core tests require a successful build")
        junit = reports / f"suite-{index:02d}.xml"
        marker = (
            "integration"
            if group in data.get("offline_integration_tests", [])
            else "not integration"
        )
        if marker == "integration" and not built:
            raise RuntimeError("Offline integration smoke requires a successful build")
        results.append(run_suite(group, junit, env, marker))
        (reports / "tests.json").write_text(json.dumps(results, indent=2) + "\n")
    # Installed CLI check uses synthetic 503/200 responses and a private directory.
    run(
        [sys.executable, "scripts/cffex-cutover/integration_installed_offline.py"],
        env=env,
    )
    missing = missing_provider_suites(data, results)
    if missing:
        print("FAILED provider suite declarations:", *missing, sep="\n", flush=True)
    print(json.dumps(results, indent=2), flush=True)
    return int(bool(missing) or any(suite_failed(row) for row in results))


def main() -> int:
    """Run the requested stage and leave machine-readable evidence."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("stage", choices=("inventory", "install", "test"))
    parser.add_argument("--reports", type=Path, default=ROOT / "lab-ci-results")
    args = parser.parse_args()
    data = inventory()
    if tuple(map(int, data["python"].split("."))) != sys.version_info[:3]:
        raise RuntimeError(
            f"Lab CI requires Python {data['python']}; got {sys.version}"
        )
    reports = args.reports.resolve()
    reports.mkdir(parents=True, exist_ok=True)
    (reports / "inventory.json").write_text(json.dumps(data, indent=2) + "\n")
    print(f"Python {sys.version}; PR6 present: {data['tick_present']}", flush=True)
    if not data["tick_present"]:
        print(
            "ABSENT on this checkout: Futu/CTP and tick collector tests (unmerged PR6)",
            flush=True,
        )
    if args.stage == "install":
        install(data, reports)
    elif args.stage == "test":
        # Each attempt gets a fresh directory: retain earlier evidence and never
        # accept an old JUnit file if pytest fails before writing a new one.
        attempt = Path(tempfile.mkdtemp(prefix="attempt-", dir=reports))
        return test_groups(data, attempt)
    return 0


if __name__ == "__main__":
    sys.exit(main())

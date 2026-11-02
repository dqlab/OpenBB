"""Check changed Python files with every required linter and preserve failures."""

# ruff: noqa: S603, T201

import argparse
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def changed_files(base: str) -> list[str]:
    """Use Git's merge base, NUL delimiters and existing paths, including tests."""
    git = shutil.which("git")
    if git is None:
        raise FileNotFoundError("git is required for the lint comparison")
    result = subprocess.run(
        [git, "diff", "--name-only", "--diff-filter=ACMR", "-z", f"{base}...HEAD"],
        cwd=ROOT,
        check=True,
        capture_output=True,
    )
    return [
        path
        for path in result.stdout.decode().split("\0")
        if path.endswith(".py")
        and path.startswith(
            ("openbb_platform/", "cli/", "scripts/", ".github/scripts/")
        )
        and not path.startswith("openbb_platform/core/openbb/package/")
        and (ROOT / path).is_file()
    ]


def commands(files: list[str]) -> list[list[str]]:
    """Check tests with Ruff; run all four production checks without autofixes."""
    production = [
        path
        for path in files
        if "tests" not in Path(path).parts and not Path(path).name.startswith("test_")
    ]
    checks = []
    if production:
        checks.extend(
            [
                ["black", "--diff", "--check", *production],
                [
                    "mypy",
                    "--explicit-package-bases",
                    "--ignore-missing-imports",
                    "--scripts-are-modules",
                    "--check-untyped-defs",
                    *production,
                ],
                ["pylint", *production],
            ]
        )
    if files:
        checks.append(["ruff", "check", "--no-fix", *files])
    return [[sys.executable, "-m", *command] for command in checks]


def lint_environment() -> dict[str, str]:
    """Anchor editable packages at their real source roots for mypy."""
    roots = {ROOT}
    roots.update(
        path
        for value in sys.path
        if (path := Path(value).resolve()).is_relative_to(ROOT)
        and path.is_dir()
        and not {"site-packages", "dist-packages"}.intersection(path.parts)
    )
    return {**os.environ, "MYPYPATH": os.pathsep.join(sorted(map(str, roots)))}


def main() -> int:
    """Report every linter's exit code, even when an earlier check fails."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", required=True)
    parser.add_argument("--reports", type=Path, default=ROOT / "lab-lint-results")
    args = parser.parse_args()
    files = changed_files(args.base)
    args.reports.mkdir(parents=True, exist_ok=True)
    results = []
    env = lint_environment()
    print("Changed Python files:", json.dumps(files, indent=2), flush=True)
    for index, command in enumerate(commands(files)):
        print("CHECK", *command, flush=True)
        result = subprocess.run(
            command,
            cwd=ROOT,
            env=env,
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        print(result.stdout, flush=True)
        (args.reports / f"lint-{index}.log").write_text(result.stdout)
        results.append({"command": command, "exit": result.returncode})
    (args.reports / "results.json").write_text(
        json.dumps(
            {
                "files": files,
                "mypy_source_roots": env["MYPYPATH"].split(os.pathsep),
                "results": results,
            },
            indent=2,
        )
        + "\n"
    )
    return int(any(result["exit"] for result in results))


if __name__ == "__main__":
    sys.exit(main())

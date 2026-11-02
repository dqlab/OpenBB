"""Regression checks for test selection, installation and failure propagation."""

import importlib.util
import os
import socket
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

SCRIPTS = Path(__file__).resolve().parents[1]


def test_runtime_preserves_real_ownership_and_permissions():
    """Verify the sandbox without mocking trust checks or using root privilege."""
    assert os.geteuid() != 0
    assert Path("/").stat().st_uid == os.geteuid()
    assert {name for _, name in socket.if_nameindex()} == {"lo"}
    status = Path("/proc/self/status").read_text()
    effective = next(
        line.split()[1] for line in status.splitlines() if line.startswith("CapEff:")
    )
    assert int(effective, 16) == 0


def load(name):
    """Load a CI entry point without installing it as an OpenBB extension."""
    spec = importlib.util.spec_from_file_location(name, SCRIPTS / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def sample_tree(tmp_path):
    """Make the smallest explicit lab inventory with an optional PR6 pair."""
    (tmp_path / ".github").mkdir()
    (tmp_path / ".github/lab-ci.toml").write_text(
        'packages=["core"]\ntests=["core/tests"]\nrequired_imports=[]\n'
        '[tick]\npackages=["futu", "ctp[native]"]\ntests=["futu/tests", "ctp/tests"]\n'
        'required_imports=["openbb_futu", "openbb_ctp"]\n'
    )
    (tmp_path / "core/tests").mkdir(parents=True)
    return tmp_path


def test_develop_absence_and_pr6_pair_are_distinct(tmp_path):
    """A partial PR6 checkout fails; a complete pair gains both test suites."""
    ci = load("lab_ci")
    root = sample_tree(tmp_path)
    assert ci.inventory(root)["tick_present"] is False
    (root / "futu/tests").mkdir(parents=True)
    with pytest.raises(RuntimeError, match="Incomplete PR6"):
        ci.inventory(root)
    (root / "ctp/tests").mkdir(parents=True)
    result = ci.inventory(root)
    assert result["tick_present"] is True
    assert result["packages"] == ["core", "futu", "ctp[native]"]
    assert result["tests"] == ["core/tests", "futu/tests", "ctp/tests"]


def test_missing_required_suite_fails(tmp_path):
    """Deleting an internal suite is a broken contract, never a silent skip."""
    ci = load("lab_ci")
    root = sample_tree(tmp_path)
    (root / "core/tests").rmdir()
    with pytest.raises(FileNotFoundError, match="core/tests"):
        ci.inventory(root)


def test_failed_install_prevents_dependency_check(tmp_path, monkeypatch):
    """A nonzero pip install propagates before another stage can hide it."""
    ci = load("lab_ci")
    calls = []

    def fail(command, **_kwargs):
        calls.append(command)
        raise subprocess.CalledProcessError(17, command)

    monkeypatch.setattr(ci, "run", fail)
    with pytest.raises(subprocess.CalledProcessError) as exc:
        ci.install({"packages": ["openbb_platform/core"]}, tmp_path)
    assert exc.value.returncode == 17
    assert len(calls) == 1
    assert calls[0][:4] == [sys.executable, "-m", "pip", "install"]


@pytest.mark.parametrize(
    "source,failed,skipped,excluded",
    [
        ("def test_ok(): assert True\n", False, 0, 0),
        ("def test_bad(): assert False\n", True, 0, 0),
        ("raise ImportError('broken collection')\n", True, 0, 0),
        ("value = 1\n", True, 0, 0),
        (
            "import pytest\n@pytest.mark.skip(reason='fixture reason')\ndef test_skip(): pass\n",
            True,
            1,
            0,
        ),
        (
            "import pytest\n@pytest.mark.integration\ndef test_live(): pass\n",
            True,
            0,
            1,
        ),
    ],
)
def test_real_pytest_outcomes_cannot_turn_green(
    tmp_path, monkeypatch, source, failed, skipped, excluded
):
    """Run real mini suites to exercise exit codes, collection and JUnit parsing."""
    ci = load("lab_ci")
    (tmp_path / "test_sample.py").write_text(source)
    monkeypatch.setattr(ci, "ROOT", tmp_path)
    env = {
        **os.environ,
        "PYTHONPATH": str(SCRIPTS),
        "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "1",
    }
    row = ci.run_suite("test_sample.py", tmp_path / "result.xml", env)
    assert ci.suite_failed(row) is failed
    assert row["skipped"] == skipped
    assert len(row["deselected_integration"]) == excluded
    if skipped:
        assert row["skip_reasons"][0]["reason"] == "fixture reason"


def test_every_linter_runs_and_early_failure_propagates(tmp_path, monkeypatch):
    """Black failure followed by successful Ruff must still fail the CI stage."""
    lint = load("lint_ci")
    calls = []

    def result(command, **_kwargs):
        calls.append(command)
        return SimpleNamespace(
            returncode=1 if command[2] == "black" else 0, stdout="diagnostic"
        )

    monkeypatch.setattr(
        lint, "changed_files", lambda _base: ["openbb_platform/sample.py"]
    )
    monkeypatch.setattr(lint.subprocess, "run", result)
    monkeypatch.setattr(
        sys, "argv", ["lint_ci.py", "--base", "base", "--reports", str(tmp_path)]
    )
    assert lint.main() == 1
    assert [command[2] for command in calls] == ["black", "mypy", "pylint", "ruff"]
    assert "--no-fix" in calls[-1]


def test_explicit_offline_integration_is_executed(tmp_path, monkeypatch):
    """An offline reload smoke keeps its legacy marker and must actually pass."""
    ci = load("lab_ci")
    (tmp_path / "test_offline.py").write_text(
        "import pytest\n@pytest.mark.integration\ndef test_reload(): assert True\n"
    )
    monkeypatch.setattr(ci, "ROOT", tmp_path)
    row = ci.run_suite(
        "test_offline.py",
        tmp_path / "result.xml",
        {**os.environ, "PYTHONPATH": str(SCRIPTS), "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "1"},
        "integration",
    )
    assert not ci.suite_failed(row)
    assert row["passed"] == 1
    assert row["deselected_integration"] == []


def test_provider_declaration_requires_current_successful_suite():
    """Inventory ownership cannot pass with missing, failed or all-skipped tests."""
    ci = load("lab_ci")
    data = {"provider_tests": {"ctp": {"suites": ["ctp/tests"]}}}
    assert ci.missing_provider_suites(data, []) == ["provider_tests.ctp: ctp/tests"]
    row = {"path": "ctp/tests", "exit": 0, "passed": 1, "failures": 0, "errors": 0}
    assert ci.missing_provider_suites(data, [row]) == []
    for change in ({"exit": 1}, {"errors": 1}, {"failures": 1}, {"passed": 0}):
        assert ci.missing_provider_suites(data, [{**row, **change}]) == [
            "provider_tests.ctp: ctp/tests"
        ]


def test_bootstrap_inventory_needs_no_installed_packages(tmp_path):
    """A new venv must reach installation before third-party imports are needed."""
    result = subprocess.run(  # noqa: S603
        [
            sys.executable,
            "-S",
            str(SCRIPTS / "lab_ci.py"),
            "inventory",
            "--reports",
            str(tmp_path),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr


def test_changed_files_use_merge_base_and_preserve_spaces(tmp_path, monkeypatch):
    """Base-only commits and deleted files must not enter the PR lint file list."""
    import shutil

    lint = load("lint_ci")
    git = shutil.which("git")

    def execute(*args):
        return subprocess.run(  # noqa: S603
            [git, *args],
            cwd=tmp_path,
            check=True,
            capture_output=True,
        )

    execute("init", "-b", "base")
    execute("config", "user.email", "ci-fixture@example.invalid")
    execute("config", "user.name", "CI fixture")
    source = tmp_path / "openbb_platform"
    source.mkdir()
    (source / "deleted.py").write_text("old = 1\n")
    execute("add", ".")
    execute("commit", "-m", "base")
    execute("checkout", "-b", "feature")
    (source / "file with spaces.py").write_text("changed = 1\n")
    (source / "tests").mkdir()
    (source / "tests/test_new.py").write_text("def test_new(): assert True\n")
    (source / "deleted.py").unlink()
    execute("add", "-A")
    execute("commit", "-m", "feature")
    execute("checkout", "base")
    (source / "base_only.py").write_text("base = 2\n")
    execute("add", ".")
    execute("commit", "-m", "advance base")
    execute("checkout", "feature")
    monkeypatch.setattr(lint, "ROOT", tmp_path)
    assert lint.changed_files("base") == [
        "openbb_platform/file with spaces.py",
        "openbb_platform/tests/test_new.py",
    ]


def test_provider_and_router_ownership_cannot_override_each_other():
    """Equal declaration names still require both independently owned suites."""
    ci = load("lab_ci")
    data = {
        "provider_tests": {"same": {"suites": ["provider/tests"]}},
        "router_tests": {"same": {"suites": ["router/tests"]}},
    }
    row = {"path": "provider/tests", "exit": 0, "passed": 1, "failures": 0, "errors": 0}
    assert ci.missing_provider_suites(data, [row]) == [
        "router_tests.same: router/tests"
    ]
    row["path"] = "router/tests"
    assert ci.missing_provider_suites(data, [row]) == [
        "provider_tests.same: provider/tests"
    ]


@pytest.mark.parametrize("directory_alias", [False, True])
def test_interpreter_alias_does_not_bind_host_local_tree(
    tmp_path, monkeypatch, directory_alias
):
    """Expose only a private alias and the selected runtime, not its host parent."""
    offline = load("run_lab_offline")
    runtime = tmp_path / "runtime"
    (runtime / "bin").mkdir(parents=True)
    binary = runtime / "bin/python3.12"
    binary.write_text("runtime fixture")
    venv = tmp_path / "venv"
    (venv / "bin").mkdir(parents=True)
    host_local = tmp_path / "host-home/.local"
    (host_local / "bin").mkdir(parents=True)
    if directory_alias:
        alias = host_local / "python-version"
        alias.symlink_to(runtime, target_is_directory=True)
        target = alias / "bin/python3.12"
    else:
        alias = host_local / "bin/python3.12"
        alias.symlink_to(binary)
        target = alias
    (venv / "bin/python").symlink_to(target)
    monkeypatch.setattr(
        offline,
        "sys",
        SimpleNamespace(
            prefix=str(venv),
            base_prefix=str(runtime),
            executable=str(venv / "bin/python"),
        ),
    )
    command = offline.sandbox_command(tmp_path / "checkout", tmp_path / "reports")
    assert str(host_local) not in command
    # A directory alias exposes only the actual runtime; a file alias only its binary.
    index = command.index("--symlink")
    assert command[index + 1 : index + 3] == [
        str(runtime if directory_alias else binary),
        str(alias),
    ]


def test_apparmor_profile_is_explicit_and_missing_launcher_fails(tmp_path, monkeypatch):
    """Select the job-only profile for bwrap without a shell or silent fallback."""
    offline = load("run_lab_offline")
    monkeypatch.setattr(offline.shutil, "which", lambda name: f"/usr/bin/{name}")
    command = offline.sandbox_command(tmp_path, tmp_path / "reports", "ci-profile")
    assert command[:5] == [
        "/usr/bin/aa-exec", "-p", "ci-profile", "--", "/usr/bin/bwrap"
    ]
    monkeypatch.setattr(
        offline.shutil, "which", lambda name: "/usr/bin/bwrap" if name == "bwrap" else None
    )
    with pytest.raises(FileNotFoundError, match="apparmor"):
        offline.sandbox_command(tmp_path, tmp_path / "reports", "ci-profile")


def test_mypy_uses_editable_source_roots_and_retains_type_errors(tmp_path, monkeypatch):
    """Resolve one package name across distributions and still reject bad types."""
    lint = load("lint_ci")
    distribution = tmp_path / "distribution"
    package = distribution / "lab_pkg"
    package.mkdir(parents=True)
    (distribution / "__init__.py").write_text("")
    (package / "__init__.py").write_text("")
    (package / "first.py").write_text("def value() -> str: return 'typed'\n")
    consumer = tmp_path / "consumer.py"
    consumer.write_text("from lab_pkg.first import value\nanswer: str = value()\n")
    site = tmp_path / ".venv/lib/python3.12/site-packages"
    dist = tmp_path / ".nox/session/dist-packages"
    site.mkdir(parents=True)
    dist.mkdir(parents=True)
    monkeypatch.setattr(lint, "ROOT", tmp_path)
    monkeypatch.setattr(
        lint.sys, "path", [str(distribution), str(site), str(dist), str(tmp_path.parent)]
    )
    monkeypatch.setenv("MYPYPATH", "/unrelated/inherited/source")
    env = lint.lint_environment()
    assert set(env["MYPYPATH"].split(os.pathsep)) == {str(tmp_path), str(distribution)}
    command = next(
        command
        for command in lint.commands([str(package / "first.py"), str(consumer)])
        if command[2] == "mypy"
    )
    result = subprocess.run(  # noqa: S603
        [part for part in command if part != "--explicit-package-bases"],
        cwd=tmp_path, env=env, capture_output=True, text=True, check=False
    )
    assert result.returncode == 2
    assert "Source file found twice" in result.stdout
    result = subprocess.run(  # noqa: S603
        command, cwd=tmp_path, env=env, capture_output=True, text=True, check=False
    )
    assert result.returncode == 0, result.stdout + result.stderr
    consumer.write_text("from lab_pkg.first import value\nanswer: int = value()\n")
    result = subprocess.run(  # noqa: S603
        command, cwd=tmp_path, env=env, capture_output=True, text=True, check=False
    )
    assert result.returncode == 1
    assert "Incompatible types" in result.stdout

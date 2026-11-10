"""Run lab tests in an isolated filesystem and a loopback-only network."""

# ruff: noqa: S603

import argparse
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def sandbox_command(
    root: Path, reports: Path, apparmor_profile: str | None = None
) -> list[str]:
    """Keep real ownership checks intact with a private user-owned root."""
    if sys.prefix == sys.base_prefix:
        raise RuntimeError("Activate an isolated Python venv before running lab tests")
    bwrap = shutil.which("bwrap")
    if bwrap is None:
        raise FileNotFoundError("Install bubblewrap before running lab tests")
    command = [bwrap, "--unshare-user", "--unshare-net", "--die-with-parent"]
    for name in ("/usr", "/etc", "/bin", "/sbin", "/lib", "/lib64", "/sys"):
        if Path(name).exists():
            command.extend(["--ro-bind", name, name])
    command.extend(["--proc", "/proc", "--dev", "/dev", "--dir", "/tmp"])  # noqa: S108
    # Do not bind the host home. SDK subprocess bootstraps may replace
    # PYTHONPATH/sitecustomize; they still see only this private home.
    command.extend(["--dir", str(Path.home())])
    mounts = {
        Path(sys.base_prefix): "--ro-bind",
        Path(sys.prefix): "--bind",
        root: "--bind",
        reports: "--bind",
    }
    system_roots = [
        Path(name)
        for name in ("/usr", "/etc", "/bin", "/sbin", "/lib", "/lib64", "/sys")
    ]
    roots = [*system_roots, *mounts]
    resolved = Path(sys.executable).resolve()
    if not any(resolved.is_relative_to(path) for path in roots):
        mounts[resolved] = "--ro-bind"
    # Recreate interpreter-manager aliases privately. Binding an alias's parent
    # could expose an unrelated ~/.local tree when only one executable is needed.
    aliases: dict[Path, Path] = {}
    executable = Path(sys.executable)
    while executable.is_symlink():
        target = executable.readlink()
        executable = target if target.is_absolute() else executable.parent / target
        part = Path(executable.anchor)
        for name in executable.parts[1:]:
            part /= name
            if part.is_symlink() and not any(
                part.is_relative_to(path) for path in [*roots, *aliases]
            ):
                aliases[part] = part.resolve()
    for path, mode in sorted(mounts.items(), key=lambda item: len(item[0].parts)):
        command.extend([mode, str(path), str(path)])
    for alias, target in aliases.items():
        command.extend(["--symlink", str(target), str(alias)])
    command.extend(["--chdir", str(root), "--"])
    if apparmor_profile:
        aa_exec = shutil.which("aa-exec")
        if aa_exec is None:
            raise FileNotFoundError("Install apparmor to select an AppArmor profile")
        command = [aa_exec, "-p", apparmor_profile, "--", *command]
    return command


def main() -> int:
    """Preserve the test process exit code through the sandbox boundary."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reports", type=Path, default=ROOT / "lab-ci-results")
    parser.add_argument("--apparmor-profile")
    args = parser.parse_args()
    reports = args.reports.resolve()
    reports.mkdir(parents=True, exist_ok=True)
    command = sandbox_command(ROOT, reports, args.apparmor_profile)
    command.extend(
        [
            sys.executable,
            str(ROOT / ".github/scripts/lab_ci.py"),
            "test",
            "--reports",
            str(reports),
        ]
    )
    return subprocess.run(command, check=False).returncode


if __name__ == "__main__":
    sys.exit(main())

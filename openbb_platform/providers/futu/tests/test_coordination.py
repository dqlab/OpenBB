"""Private owner locking and owner-scoped quota integration."""

import os
import subprocess
import sys
import time

import pytest
from openbb_futu import coordination
from openbb_futu.coordination import Owner


def test_owner_is_released_when_process_exits(tmp_path):
    """Test owner is released when process exits."""
    script = (
        "from pathlib import Path; from openbb_futu.coordination import Owner; "
        "import sys; owner=Owner(Path(sys.argv[1])); print('locked', flush=True); "
        "sys.stdin.read()"
    )
    process = subprocess.Popen(  # noqa: S603 -- fixed local test command
        [sys.executable, "-c", script, str(tmp_path)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
        env=os.environ,
    )
    try:
        assert process.stdout.readline().strip() == "locked"
        with pytest.raises(RuntimeError, match="already owned"):
            Owner(tmp_path)
    finally:
        process.kill()
        process.wait(timeout=5)
        process.stdin.close()
        process.stdout.close()
    with Owner(tmp_path):
        pass


def test_replaced_parent_rejected_and_evidence_retained(tmp_path):
    """Detect replacement without deleting the original lock."""
    directory = tmp_path / "locks"
    with Owner(directory) as owner:
        directory.rename(tmp_path / "old")
        directory.mkdir(mode=0o700)
        with pytest.raises(Exception, match="changed|replaced"), owner.borrow(directory):
            pass
    assert (tmp_path / "old" / "upstream.lock").exists()
    assert not (directory / "upstream.lock").exists()

def test_budget_shared_and_configuration_fixed(tmp_path, monkeypatch):
    """All budget handles on one owner share the same quota."""
    monkeypatch.setattr(coordination, "BUDGET_LIMIT", 2)
    monkeypatch.setattr(coordination, "BUDGET_WINDOW", 0.03)
    with Owner(tmp_path) as owner:
        first, second = owner.budget(), owner.budget()
        assert first.acquire() > 0
        time.sleep(0.04)
        assert first.acquire() == 0
        assert second.acquire() == 0
        assert first.acquire() > 0
        with pytest.raises(ValueError, match="configuration mismatch"):
            owner.budget(3, 0.03)
    assert not (tmp_path / "requests.sqlite3").exists()


def test_rest_obeys_shared_budget_and_owner(tmp_path, monkeypatch):
    """A full owner quota and a competing owner both prevent REST sends."""
    from openbb_futu import client

    monkeypatch.setenv("FUTU_COORDINATION_DIR", str(tmp_path))
    monkeypatch.setattr(coordination, "BUDGET_LIMIT", 1)
    monkeypatch.setattr(coordination, "BUDGET_WINDOW", 0.03)
    monkeypatch.setattr(client, "make_request", lambda *a, **k: pytest.fail("request sent"))
    with Owner(tmp_path) as owner:
        time.sleep(0.04)
        assert owner.budget().acquire() == 0
        with pytest.raises(client.FutuError, match="local_budget_exhausted"):
            client._post("/test", owner=owner)
        with pytest.raises(client.FutuError, match="coordination"):
            client._post("/test")


def test_rejects_unsafe_files_without_touching_targets(tmp_path):
    """The owner lock rejects symlinks, hardlinks and FIFOs."""
    target = tmp_path / "target"
    target.write_bytes(b"evidence")
    target.chmod(0o600)
    directory = tmp_path / "locks"
    directory.mkdir(mode=0o700)
    entry = directory / "upstream.lock"
    entry.symlink_to(target)
    with pytest.raises(OSError):
        Owner(directory)
    entry.unlink()
    os.mkfifo(entry, 0o600)
    with pytest.raises(PermissionError):
        Owner(directory)
    entry.unlink()
    os.link(target, entry)
    with pytest.raises(PermissionError):
        Owner(directory)
    assert target.read_bytes() == b"evidence"


def test_rejects_directory_symlink_and_insecure_ancestor(tmp_path):
    """The lock directory cannot traverse links or writable ancestors."""
    target = tmp_path / "target"
    target.mkdir(mode=0o700)
    (tmp_path / "link").symlink_to(target, target_is_directory=True)
    with pytest.raises(OSError):
        Owner(tmp_path / "link")
    parent = tmp_path / "public"
    parent.mkdir(mode=0o777)
    parent.chmod(0o777)
    with pytest.raises(PermissionError):
        Owner(parent / "private")
    assert not (parent / "private").exists()

"""Owner-scoped quota and lock lifetime regression tests."""
import fcntl
import multiprocessing
import os
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest
from openbb_futu import coordination
from openbb_futu.coordination import Owner


def test_production_window_default():
    """The public quota keeps the production sixty-second window."""
    assert coordination.BUDGET_WINDOW == 60.0


def _contend(path, queue):
    try:
        with Owner(Path(path)):
            queue.put("acquired")
    except RuntimeError:
        queue.put("blocked")


def test_shared_threads_config_and_restart(tmp_path, monkeypatch):
    """Threads share quota and a restarted owner waits a full window."""
    window = 0.3
    monkeypatch.setattr(coordination, "BUDGET_LIMIT", 4)
    monkeypatch.setattr(coordination, "BUDGET_WINDOW", window)
    with Owner(tmp_path) as owner:
        budget = owner.budget(4, window)
        assert budget.acquire() > 0
        time.sleep(window + 0.03)
        with ThreadPoolExecutor(max_workers=16) as pool:
            results = list(pool.map(lambda _: owner.budget(4, window).acquire(), range(24)))
        assert results.count(0) == 4
        assert all(value > 0 for value in results if value != 0)
        with pytest.raises(ValueError):
            owner.budget(5, window)
        with pytest.raises(ValueError):
            owner.budget(4, float("nan"))
        assert not (tmp_path / "requests.sqlite3").exists()
    with Owner(tmp_path) as owner:
        started = time.monotonic()
        budget = owner.budget(4, window)
        assert budget.acquire() > 0
        time.sleep(window + 0.03)
        assert budget.acquire() == 0
        assert time.monotonic() - started >= window


def test_other_process_contends_and_release(tmp_path):
    """Separate processes contend on the same kernel lock."""
    context = multiprocessing.get_context("spawn")
    queue = context.Queue()
    with Owner(tmp_path):
        process = context.Process(target=_contend, args=(str(tmp_path), queue))
        process.start()
        assert queue.get(timeout=10) == "blocked"
        process.join(10)
        assert process.exitcode == 0
    process = context.Process(target=_contend, args=(str(tmp_path), queue))
    process.start()
    assert queue.get(timeout=10) == "acquired"
    process.join(10)
    assert process.exitcode == 0
    queue.close()


def test_lock_loss_and_close_waits_for_borrow(tmp_path, monkeypatch):
    """Close waits for borrowers and unlocked owners cannot reserve."""
    monkeypatch.setattr(coordination, "BUDGET_LIMIT", 1)
    monkeypatch.setattr(coordination, "BUDGET_WINDOW", 0.01)
    owner = Owner(tmp_path)
    budget = owner.budget(1, 0.01)
    entered, release, closed = threading.Event(), threading.Event(), threading.Event()
    def hold():
        with owner.borrow(tmp_path):
            entered.set()
            assert release.wait(5)
    def close():
        owner.close()
        closed.set()
    thread = threading.Thread(target=hold)
    thread.start()
    assert entered.wait(5)
    closer = threading.Thread(target=close)
    closer.start()
    assert not closed.wait(0.05)
    release.set()
    thread.join(5)
    closer.join(5)
    assert closed.is_set()
    with pytest.raises(RuntimeError):
        budget.acquire()
    with Owner(tmp_path) as owner:
        budget = owner.budget(1, 0.01)
        fcntl.flock(owner._fd, fcntl.LOCK_UN)
        with pytest.raises(RuntimeError, match="lock lost"):
            budget.acquire()


def test_other_process_lock_cannot_impersonate_owner(tmp_path, monkeypatch):
    """A different process holding the lock cannot validate the old owner."""
    monkeypatch.setattr(coordination, "BUDGET_WINDOW", 0.001)
    with Owner(tmp_path) as owner:
        budget = owner.budget()
        fcntl.flock(owner._fd, fcntl.LOCK_UN)
        process = subprocess.Popen(  # noqa: S603 -- fixed interpreter and local test script
            [sys.executable, "-c", "import fcntl,sys; f=open(sys.argv[1], 'r+'); "
             "fcntl.flock(f, fcntl.LOCK_EX); print('locked', flush=True); sys.stdin.read()",
             str(tmp_path / "upstream.lock")],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True,
        )
        try:
            assert process.stdout.readline().strip() == "locked"
            with pytest.raises(RuntimeError, match="lock lost"):
                budget.acquire()
        finally:
            process.stdin.close()
            assert process.wait(timeout=10) == 0
            process.stdout.close()


def test_fdinfo_unavailable_fails_closed(tmp_path, monkeypatch):
    """Reject ownership when kernel lock evidence cannot be read."""
    import builtins

    with Owner(tmp_path) as owner:
        original = builtins.open
        def no_fdinfo(path, *args, **kwargs):
            if os.fspath(path).startswith("/proc/self/fdinfo/"):
                raise OSError("unavailable")
            return original(path, *args, **kwargs)
        monkeypatch.setattr(builtins, "open", no_fdinfo)
        with pytest.raises(RuntimeError, match="lock unavailable"):
            owner.budget()


def test_inherited_owner_rejected_after_fork(tmp_path):
    """A child cannot use its parent's inherited owner."""
    if not hasattr(os, "fork"):
        pytest.skip("requires fork")
    with Owner(tmp_path) as owner:
        read_fd, write_fd = os.pipe()
        child = os.fork()
        if child == 0:
            os.close(read_fd)
            try:
                owner.budget().acquire()
            except RuntimeError:
                os.write(write_fd, b"rejected")
                os._exit(0)
            os._exit(1)
        os.close(write_fd)
        try:
            assert os.read(read_fd, 32) == b"rejected"
            _, status = os.waitpid(child, 0)
            assert os.waitstatus_to_exitcode(status) == 0
        finally:
            os.close(read_fd)


def test_public_collection_obeys_owner_cooldown(tmp_path, monkeypatch):
    """Public collection cannot send before the owner cooldown expires."""
    from openbb_futu import client, discovery, sampled_snapshot

    def forbidden(*_args, **_kwargs):
        pytest.fail("unexpected network request")

    monkeypatch.setattr(client, "make_request", forbidden)
    monkeypatch.setattr(discovery, "make_request", forbidden)
    monkeypatch.setenv("FUTU_COORDINATION_DIR", str(tmp_path / "coord"))
    with Owner(tmp_path / "coord") as owner:
        for call in (
            lambda: client.snapshot(["US.A"], "token", owner=owner),
            lambda: client.raw_snapshot(["US.A"], "token", owner=owner),
        ):
            with pytest.raises(client.FutuError, match="local_budget_exhausted"):
                call()
        assert discovery.discover([], "token", tmp_path / "raw", owner=owner)["requests"] == []
        stop = threading.Event()
        stop.set()
        with pytest.raises(client.FutuError, match="stopped"):
            sampled_snapshot._controlled_snapshot(
                ["US.A"], "token", 1, (), stop=stop, archive=None, owner=owner,
                journal=None, run_id="test", entries=[{"asset": "equity", "symbol": "US.A"}],
            )
        assert not owner._requests

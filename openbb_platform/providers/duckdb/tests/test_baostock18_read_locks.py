"""Real flock and DuckDB regression tests for readers, auditors and publishers."""

import fcntl
import json
import select
import subprocess
import sys
import time
from contextlib import nullcontext
from datetime import datetime, timedelta

import pytest
from openbb_duckdb.baostock18 import (
    full_audit as audit,
    import_candidate,
    set_visible,
)
from openbb_duckdb.baostock18.contract import AdmissionError
from openbb_duckdb.baostock18.manifest import sha256
from openbb_duckdb.baostock18.production import CATALOG_NAME, market_lock, market_read_lock, write_json
from openbb_duckdb.baostock18.reader import _existing_read_lock
from openbb_duckdb.baostock18.visibility import publisher_lock

from .test_baostock18_admission import make_candidate, query, read

READER_CODE = """
import json,sys,time
from contextlib import contextmanager
from datetime import date
from openbb_duckdb.baostock18 import reader
if sys.argv[2] == 'pause':
    real_connect = reader.duckdb.connect
    @contextmanager
    def paused_connect(*args, **kwargs):
        with real_connect(*args, **kwargs) as connection:
            print('ready', flush=True)
            import select
            if not select.select([sys.stdin], [], [], 10)[0]:
                raise RuntimeError('parent did not release reader')
            sys.stdin.readline()
            yield connection
    reader.duckdb.connect = paused_connect
started = time.monotonic()
try:
    rows = reader.read_bars(sys.argv[1], symbols=['SH.600000'], start_date=date(2000,1,1),
                            end_date=date(2000,1,2), basis='unadjusted', limit=2)
    print(json.dumps(dict(rows=len(rows), seconds=time.monotonic()-started)), flush=True)
except Exception as exc:
    print(json.dumps(dict(error=str(exc), seconds=time.monotonic()-started)), flush=True)
    sys.exit(1)
"""


@pytest.fixture
def visible(tmp_path):
    """Admit a tiny synthetic catalog before giving it the production-shaped name."""
    manifest = make_candidate(tmp_path / "candidate.duckdb", rows=2, quarantined=1)
    scratch = tmp_path / "scratch.duckdb"
    receipt = import_candidate(manifest, scratch)
    event = set_visible(scratch, receipt["snapshot_id"], basis="unadjusted", expected_clean_rows=2)
    target = scratch.rename(tmp_path / CATALOG_NAME)
    target.with_suffix(".market.lock").touch()
    target.with_suffix(".publisher.lock").touch()
    return target, event


def run_reader(target):
    """Use a separate process to isolate DuckDB's connection configuration."""
    return subprocess.run([sys.executable, "-B", "-c", READER_CODE, str(target), "read"],  # noqa: S603
                          capture_output=True, text=True, timeout=5, check=False)


def test_two_readers_have_overlapping_locks_and_connections(visible):
    """Both readers open real read-only connections before either is released."""
    target, _ = visible
    processes = []
    try:
        for _ in range(2):
            processes.append(subprocess.Popen(  # noqa: S603
                [sys.executable, "-B", "-c", READER_CODE, str(target), "pause"],
                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True))
        for process in processes:
            assert select.select([process.stdout], [], [], 5)[0], "reader blocked acquiring a shared lock"
            assert process.stdout.readline().strip() == "ready"
        for process in processes:
            stdout, stderr = process.communicate("continue\n", timeout=10)
            assert process.returncode == 0, stderr + stdout
            assert json.loads(stdout)["rows"] == 2
    finally:
        for process in processes:
            if process.poll() is None:
                process.communicate("continue\n", timeout=15)


def test_exclusive_writer_rejects_reader_immediately(visible):
    """A real external reader returns a busy error instead of waiting for EX."""
    target, _ = visible
    with market_lock(target):
        result = run_reader(target)
    assert result.returncode == 1
    measured = json.loads(result.stdout)
    assert "Market catalog busy; reader did not wait or create a lock" in measured["error"]
    assert measured["seconds"] < 0.5


@pytest.mark.parametrize("writer,suffix", [(market_lock, ".market.lock"), (publisher_lock, ".publisher.lock")])
def test_writer_and_publisher_still_require_exclusive_nonblocking_locks(visible, writer, suffix):
    """SH excludes a writer, and a writer excludes both SH and another EX."""
    target, _ = visible
    with target.with_suffix(suffix).open("rb") as stream:
        fcntl.flock(stream, fcntl.LOCK_SH | fcntl.LOCK_NB)
        started = time.monotonic()
        with pytest.raises(AdmissionError, match="busy"), writer(target):
            pytest.fail("writer admitted under a shared lock")
        assert time.monotonic() - started < 0.5
        fcntl.flock(stream, fcntl.LOCK_UN)
        with writer(target):
            for mode in (fcntl.LOCK_SH, fcntl.LOCK_EX):
                with pytest.raises(BlockingIOError):
                    fcntl.flock(stream, mode | fcntl.LOCK_NB)


def test_reader_never_creates_a_missing_lock(visible):
    """An optional missing lock retains the reader's previous no-create behavior."""
    target, _ = visible
    lock = target.with_suffix(".market.lock")
    lock.unlink()  # Only this synthetic fixture, never a production lock.
    assert len(read(target, basis="unadjusted")) == 2
    assert not lock.exists()


def test_reader_lock_mode_is_shared_nonblocking_and_descriptor_is_readonly(visible, monkeypatch):
    """Pin SH|NB, O_RDONLY and unlock; waiting and writable opens are regressions."""
    target, _ = visible
    original = fcntl.flock
    operations = []

    def record(fd, mode):
        operations.append(mode)
        assert fcntl.fcntl(fd, fcntl.F_GETFL) & 3 == 0
        return original(fd, mode)

    monkeypatch.setattr(fcntl, "flock", record)
    with _existing_read_lock(target):
        pass
    assert operations == [fcntl.LOCK_SH | fcntl.LOCK_NB, fcntl.LOCK_UN]


def audit_task(tmp_path, target):
    """Save only the small, already-approved task inputs read by the audit."""
    task = tmp_path / "task"
    (task / "verification").mkdir(parents=True)
    write_json(task / "verification-config.json", dict(target=str(target), reference_root=str(tmp_path),
                                                      admission_root=str(tmp_path), json_root=[str(tmp_path)]))
    write_json(task / "verification/receipt.json", dict(reference_verification={}))
    write_json(task / "RUN_RECEIPT.json", dict(status="PASS"))
    return task


def test_full_audit_holds_only_shared_lock_and_reader_succeeds(visible, tmp_path, monkeypatch):
    """The actual audit entrypoint shares its projection lock, then closes it before hashing."""
    target, _ = visible
    task = audit_task(tmp_path, target)
    payload = tmp_path / "payload.json"
    payload.write_text('{"immutable":true}')
    before = sha256(target)
    original_flock, original_hash = fcntl.flock, audit.stable_hash
    operations = []

    def record(fd, mode):
        operations.append(mode)
        assert fcntl.fcntl(fd, fcntl.F_GETFL) & 3 == 0
        return original_flock(fd, mode)

    def projection(connection, contract, ledger, observed, deadline, *, record):
        assert connection.execute("SELECT 42").fetchone() == (42,)
        with target.with_suffix(".market.lock").open("rb") as stream, pytest.raises(BlockingIOError):
            original_flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        result = run_reader(target)
        assert result.returncode == 0, result.stderr + result.stdout
        assert json.loads(result.stdout)["rows"] == 2
        record(payload, sha256(payload), payload.stat().st_size, tmp_path)
        return [dict(rows=1)]

    def payload_hash(*args):
        with target.with_suffix(".market.lock").open("rb") as stream:
            original_flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
            original_flock(stream, fcntl.LOCK_UN)
        return original_hash(*args)

    monkeypatch.setattr(fcntl, "flock", record)
    monkeypatch.setattr(audit, "reference_contract", lambda *args, **kwargs: {})
    monkeypatch.setattr(audit, "enumerate_references", projection)
    monkeypatch.setattr(audit, "stable_hash", payload_hash)
    result = audit.full_audit(task, tmp_path / "audit", max_seconds=30)
    assert result["status"] == "PASS", result
    assert result["catalog_connection_closed_before_payload_hashing"]
    assert result["catalog_lock_mode"] == "LOCK_SH|LOCK_NB"
    assert result["files"] == result["processed_files"] == 1
    assert result["bytes"] == payload.stat().st_size
    assert operations == [fcntl.LOCK_SH | fcntl.LOCK_NB, fcntl.LOCK_UN]
    assert sha256(target) == before


@pytest.mark.parametrize("missing", [False, True])
def test_audit_refuses_busy_or_missing_lock_without_waiting_or_creating(visible, tmp_path, missing):
    """The read-only audit cannot fall back to EX or run unlocked."""
    target, _ = visible
    task = audit_task(tmp_path, target)
    before = sha256(target)
    if missing:
        target.with_suffix(".market.lock").unlink()  # Isolated fixture only.
    with nullcontext() if missing else market_lock(target):
        result = audit.full_audit(task, tmp_path / "audit", max_seconds=30)
    assert result["status"] == "FAIL"
    assert ("required" if missing else "busy") in result["error"]
    assert result["seconds"] < 0.5
    assert target.with_suffix(".market.lock").exists() != missing
    assert sha256(target) == before


def test_shared_audit_retains_asof_and_overflow_semantics(visible):
    """SH changes no visibility boundary, quarantine filtering or overflow behavior."""
    target, event = visible
    known = datetime.fromisoformat(event["known_at"])
    with market_read_lock(target):
        assert read(target, basis="unadjusted", as_of=known - timedelta(microseconds=1)) == []
        assert len(read(target, basis="unadjusted", as_of=known, limit=2)) == 2
        with pytest.raises(AdmissionError, match="exceeds limit"):
            read(target, basis="unadjusted", as_of=known, limit=1)
        for limit in (0, 100_001):
            with pytest.raises(AdmissionError, match="1..100,000"):
                read(target, basis="unadjusted", limit=limit)
    assert len(query(target, "SELECT * FROM baostock18_admission.visibility_event")) == 1

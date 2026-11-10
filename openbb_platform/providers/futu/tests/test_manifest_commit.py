"""Manifest failures do not imply anything about original commit durability."""

import errno
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
from openbb_futu.archive import Archive
from openbb_futu.coverage import prepare, report
from openbb_futu.offline import verify


def make_plan(root, run_id):
    """Create a small plan for reconstruction."""
    plan, _ = prepare(
        {"dimensions": [{"asset": "equity", "symbol": "US.A", "channel": "quote"}], "subscription_budget": 1}
    )
    (root / f"futu-plan-{run_id}.json").write_text(json.dumps(plan))


@pytest.mark.parametrize("stage", ["manifest_file_fsync", "manifest_directory_fsync"])
def test_fsync_failure_is_preserved(tmp_path, monkeypatch, stage):
    """A failed sync leaves evidence and never claims a successful commit."""
    run_id = "a" * 32
    make_plan(tmp_path, run_id)
    (tmp_path / "futu-manifests").mkdir()
    archive = Archive(tmp_path, run_id=run_id)
    assert archive.accept('{"type":"QUOTE","symbol":"US.A","data":{}}', "c")
    archive.stop.set()
    archive.thread.join()
    original = os.fsync
    triggered = False

    def fail(fd):
        nonlocal triggered
        target = os.readlink(f"/proc/self/fd/{fd}")
        match = target.endswith(".tmp") if stage == "manifest_file_fsync" else target.endswith("futu-manifests")
        if match and not triggered:
            triggered = True
            raise OSError(errno.EIO, "synthetic")
        return original(fd)

    monkeypatch.setattr(os, "fsync", fail)
    with pytest.raises(OSError) as exc:
        archive.close()
    assert exc.value.errno == errno.EIO and triggered
    events = [json.loads(line) for line in (tmp_path / f"futu-events-{run_id}.jsonl").read_text().splitlines()]
    assert events[0]["stage"] == stage
    assert list((tmp_path / "futu-manifests").glob("*.tmp"))
    manifests = list((tmp_path / "futu-manifests").glob("*.json"))
    result = report(tmp_path, run_id, publish=False)
    assert result["original_commit_status"] == ("failed" if manifests else "unknown")
    assert result["run_status"] == ("failed" if manifests else "unknown")
    if not manifests:
        assert result["unattributed_commit_failure"]
    if manifests:
        assert verify(manifests[0])["status"] == "verified"
        result = report(tmp_path, run_id, publish=False)
        assert result["integrity_status"] == "verified"
        assert result["original_commit_status"] == "failed"
        assert result["run_status"] == "failed"
        assert result["dimensions"][0]["synced_rows"] == 1


def test_legacy_manifest_and_failed_run(tmp_path):
    """Old manifests retain counts and explicit failure takes precedence."""
    run_id = "b" * 32
    make_plan(tmp_path, run_id)
    archive = Archive(tmp_path, run_id=run_id)
    assert archive.accept('{"type":"QUOTE","symbol":"US.A","data":{}}', "c")
    path = Path(archive.close()["manifest"])
    old = json.loads(path.read_text())
    old.pop("uncertain_loss")
    path.write_text(json.dumps(old))
    checked = verify(path)
    assert checked["integrity_status"] == "verified"
    assert checked["original_commit_status"] == "unknown"
    (tmp_path / f"futu-events-{run_id}.jsonl").write_text(
        '{"event":"stopped","status":"failed"}\n{"event":"stopped","status":"stopped"}\n'
    )
    result = report(tmp_path, run_id, publish=False)
    assert result["evidence_status"] == "verified"
    assert result["run_status"] == "failed"
    assert result["original_commit_status"] == "unknown"
    assert result["dimensions"][0]["synced_rows"] == 1


@pytest.mark.parametrize("failure_first", [True, False])
def test_other_session_failure_same_run(tmp_path, failure_first):
    """A run failure is not scoped to whichever manifest happens to exist."""
    run_id = "c" * 32
    make_plan(tmp_path, run_id)
    event = {"event": "manifest_commit_failed", "run_id": run_id, "session_id": "d" * 32}
    event_path = tmp_path / f"futu-events-{run_id}.jsonl"
    if failure_first:
        event_path.write_text(json.dumps(event) + "\n")
    Archive(tmp_path, run_id=run_id).close()
    if not failure_first:
        event_path.write_text(json.dumps(event) + "\n")
    result = report(tmp_path, run_id, publish=False)
    assert result["original_commit_status"] == "unknown"
    assert result["run_status"] == "unknown"


@pytest.mark.parametrize("event_run,session", [("e" * 32, "d" * 32), ("c" * 32, None)])
def test_unattributed_failure_is_unknown(tmp_path, event_run, session):
    """Unattributed failures cannot establish a run failure."""
    run_id = "c" * 32
    make_plan(tmp_path, run_id)
    Archive(tmp_path, run_id=run_id).close()
    event = {"event": "manifest_commit_failed", "run_id": event_run, "session_id": session}
    (tmp_path / f"futu-events-{run_id}.jsonl").write_text(json.dumps(event) + "\n")
    result = report(tmp_path, run_id, publish=False)
    assert result["unattributed_commit_failure"]
    assert result["evidence_status"] == "unknown"
    assert result["original_commit_status"] == "unknown"
    assert result["run_status"] == "unknown"


@pytest.mark.parametrize("tail", [b'{"partial":\xff', b'{"event":"stopped"}'])
def test_corrupt_tail_then_real_failure(tmp_path, monkeypatch, tail):
    """A corrupt tail does not hide a later publish failure."""
    run_id = "c" * 32
    make_plan(tmp_path, run_id)
    Archive(tmp_path, run_id=run_id).close()
    event_path = tmp_path / f"futu-events-{run_id}.jsonl"
    event_path.write_bytes(tail)
    archive = Archive(tmp_path, run_id=run_id)
    archive.stop.set()
    archive.thread.join()
    original = os.link

    def fail(*args, **kwargs):
        raise PermissionError(errno.EACCES, "synthetic")

    monkeypatch.setattr(os, "link", fail)
    with pytest.raises(PermissionError):
        archive.close()
    monkeypatch.setattr(os, "link", original)
    assert json.loads(event_path.read_bytes().splitlines()[-1])["event"] == "manifest_commit_failed"
    result = report(tmp_path, run_id, publish=False)
    assert result["event_log_status"] == "corrupt"
    assert result["original_commit_status"] == "unknown"
    assert result["evidence_status"] == "unknown"
    assert result["run_status"] == "unknown"


def test_unreadable_event_log_is_unknown(tmp_path, monkeypatch):
    """Unreadable evidence remains unknown."""
    run_id = "c" * 32
    make_plan(tmp_path, run_id)
    Archive(tmp_path, run_id=run_id).close()
    event_name = f"futu-events-{run_id}.jsonl"
    original = os.open

    def fail(path, *args, **kwargs):
        if path == event_name:
            raise PermissionError(errno.EACCES, "synthetic")
        return original(path, *args, **kwargs)

    monkeypatch.setattr(os, "open", fail)
    result = report(tmp_path, run_id, publish=False)
    assert result["event_log_status"] == "unreadable"
    assert result["evidence_status"] == "unknown"
    assert result["run_status"] == "unknown"


def test_failure_event_write_denied_preserves_original_exception(tmp_path, monkeypatch):
    """Preserve the original error when recording is denied."""
    run_id = "c" * 32
    make_plan(tmp_path, run_id)
    Archive(tmp_path, run_id=run_id).close()
    archive = Archive(tmp_path, run_id=run_id)
    archive.stop.set()
    archive.thread.join()
    original_open = os.open

    def denied(path, *args, **kwargs):
        if str(path).startswith("futu-events-"):
            raise PermissionError(errno.EACCES, "event log denied")
        return original_open(path, *args, **kwargs)

    def fail(*args, **kwargs):
        raise OSError(errno.EIO, "manifest publish failed")

    monkeypatch.setattr(os, "open", denied)
    monkeypatch.setattr(os, "link", fail)
    with pytest.raises(OSError) as exc:
        archive.close()
    assert exc.value.errno == errno.EIO
    monkeypatch.setattr(os, "open", original_open)
    result = report(tmp_path, run_id, publish=False)
    assert result["original_commit_status"] == "unknown"
    assert result["event_log_status"] == "missing"
    assert result["evidence_status"] == "unknown"


def test_real_manifest_directory_permission_failure(tmp_path):
    """An actual filesystem permission failure is recorded and propagated."""
    if os.geteuid() == 0:
        pytest.skip("root bypasses directory permissions")
    run_id = "f" * 32
    make_plan(tmp_path, run_id)
    directory = tmp_path / "futu-manifests"
    directory.mkdir()
    archive = Archive(tmp_path, run_id=run_id)
    archive.stop.set()
    archive.thread.join()
    directory.chmod(0o500)
    try:
        with pytest.raises(PermissionError):
            archive.close()
    finally:
        directory.chmod(0o700)
    result = report(tmp_path, run_id, publish=False)
    assert result["original_commit_status"] == "unknown"
    assert result["run_status"] == "unknown"
    assert result["unattributed_commit_failure"]


def test_cli_fails_for_corrupt_event_log(tmp_path):
    """The CLI cannot report success for uncertain event evidence."""
    run_id = "f" * 32
    make_plan(tmp_path, run_id)
    Archive(tmp_path, run_id=run_id).close()
    (tmp_path / f"futu-events-{run_id}.jsonl").write_text("{broken")
    result = subprocess.run(  # noqa: S603
        [sys.executable, "-m", "openbb_futu.evidence_cli", "coverage", "--output", str(tmp_path), "--run-id", run_id],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode != 0
    assert json.loads(result.stdout)["evidence_status"] == "unknown"


def test_repeat_close_and_preexisting_manifest_are_not_original_failure(tmp_path):
    """An existing manifest survives repeated and distinct publication attempts."""
    run_id = "a" * 32
    make_plan(tmp_path, run_id)
    archive = Archive(tmp_path, run_id=run_id)
    manifest = Path(archive.close()["manifest"])
    before = manifest.read_bytes()
    with pytest.raises(FileExistsError):
        archive.close()
    event = json.loads((tmp_path / f"futu-events-{run_id}.jsonl").read_text())
    assert event["publish_attempt"] == 2
    assert event["original_commit_failure"] is False
    assert manifest.read_bytes() == before
    assert report(tmp_path, run_id, publish=False)["original_commit_status"] == "unknown"
    other = Archive(tmp_path, run_id=run_id)
    other.stop.set()
    other.thread.join()
    other.writer.session_id = archive.writer.session_id
    with pytest.raises(FileExistsError):
        other.close()
    event = json.loads((tmp_path / f"futu-events-{run_id}.jsonl").read_text().splitlines()[-1])
    assert event["publish_attempt"] == 1 and event["original_commit_failure"] is False
    assert manifest.read_bytes() == before
    result = report(tmp_path, run_id, publish=False)
    assert result["original_commit_status"] == result["evidence_status"] == "unknown"


def test_cleanup_failure_is_not_directory_fsync_failure(tmp_path, monkeypatch):
    """Failure after a successful sync is attributed to cleanup."""
    run_id = "b" * 32
    make_plan(tmp_path, run_id)
    archive = Archive(tmp_path, run_id=run_id)
    original = os.unlink

    def fail(path, *args, **kwargs):
        if str(path).endswith(".tmp"):
            raise OSError(errno.EIO, "cleanup")
        return original(path, *args, **kwargs)

    monkeypatch.setattr(os, "unlink", fail)
    with pytest.raises(OSError):
        archive.close()
    event = json.loads((tmp_path / f"futu-events-{run_id}.jsonl").read_text())
    assert event["stage"] == "manifest_temp_cleanup"
    assert event["original_commit_failure"] is False
    result = report(tmp_path, run_id, publish=False)
    assert result["integrity_status"] == "verified"
    assert result["original_commit_status"] == "unknown"


@pytest.mark.parametrize("event", [
    {"event": "stopped"},
    {"event": "manifest_commit_failed", "run_id": "a" * 32, "session_id": "b" * 32,
     "publish_attempt": True, "original_commit_failure": True},
])
def test_malformed_events_are_unknown(tmp_path, event):
    """Malformed event data cannot prove a failure or crash reconstruction."""
    run_id = "a" * 32
    make_plan(tmp_path, run_id)
    (tmp_path / f"futu-events-{run_id}.jsonl").write_text(json.dumps(event) + "\n")
    result = report(tmp_path, run_id, publish=False)
    assert result["event_log_status"] == "corrupt"
    assert result["run_status"] == "unknown"
    assert result["original_commit_status"] == "unknown"


def test_replaced_and_restored_during_verification_fails(tmp_path):
    """An ABA write during parsing is detected even when original bytes return."""
    archive = Archive(tmp_path, run_id="a" * 32)
    assert archive.accept('{"type":"QUOTE","symbol":"US.A","data":{}}', "c")
    manifest = archive.close()
    segment = Path(manifest["files"][0]["file"])
    original = segment.read_bytes()
    original_stat = segment.stat()
    records = []

    def replace(record):
        import os
        records.append(record)
        segment.write_bytes(b"adversarial replacement\n")
        segment.write_bytes(original)
        # tmpfs may assign both writes the same timestamp as the initial file.
        # Make the metadata-change premise explicit and deterministic.
        os.utime(segment, ns=(original_stat.st_atime_ns, original_stat.st_mtime_ns + 1_000_000_000))

    assert verify(Path(manifest["manifest"]), on_record=replace)["status"] == "failed"
    assert records == [json.loads(original)]
    assert segment.read_bytes() == original

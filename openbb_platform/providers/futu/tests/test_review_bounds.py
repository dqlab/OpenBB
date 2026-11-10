"""Regression tests for event attribution and bounded archive reads."""

import json
import threading
from pathlib import Path

import pytest
from openbb_futu.archive import MAX_LINE_BYTES, Archive
from openbb_futu.coverage import prepare, report
from openbb_futu.offline import verify


def evidence(root):
    """Create one sealed synthetic record."""
    run = "a" * 32
    plan, _ = prepare({"dimensions": [{"asset": "equity", "symbol": "US.A", "channel": "quote"}],
                       "subscription_budget": 1})
    (root / f"futu-plan-{run}.json").write_text(json.dumps(plan))
    archive = Archive(root, run_id=run)
    assert archive.accept('{"type":"QUOTE","symbol":"US.A","data":{}}', "c")
    return run, archive.close()


@pytest.mark.parametrize("field,value", [
    ("stage", None), ("stage", []), ("stage", "bogus"), ("stage", True),
    ("publish_attempt", True), ("publish_attempt", None), ("original_commit_failure", 1),
    ("run_id", None), ("session_id", None), ("time", True),
    ("stage", ...), ("publish_attempt", ...), ("original_commit_failure", ...),
])
def test_invalid_commit_cannot_be_attributed(tmp_path, field, value):
    """Malformed events cannot erase or create attributed history."""
    run, manifest = evidence(tmp_path)
    valid = {"event": "manifest_commit_failed", "run_id": run, "session_id": manifest["session_id"],
             "publish_attempt": 1, "original_commit_failure": True, "stage": "manifest_write"}
    invalid = dict(valid)
    if value is ...:
        del invalid[field]
    else:
        invalid[field] = value
    path = tmp_path / f"futu-events-{run}.jsonl"
    path.write_text(json.dumps(invalid) + "\n")
    result = report(tmp_path, run, publish=False)
    assert result["event_log_status"] == "corrupt"
    assert result["unattributed_commit_failure"]
    assert result["original_commit_status"] == "unknown"
    path.write_text(json.dumps(valid) + "\n" + json.dumps(invalid) + "\n")
    result = report(tmp_path, run, publish=False)
    assert result["original_commit_status"] == "failed"
    assert result["original_commit_failure_stage"] == "manifest_write"


def test_append_stops_at_declared_boundary(tmp_path):
    """An append during the callback cannot extend parsing."""
    _, manifest = evidence(tmp_path)
    segment = Path(manifest["files"][0]["file"])
    calls = []
    ready = threading.Event()
    written = threading.Event()

    def writer():
        assert ready.wait(5)
        with segment.open("ab") as stream:
            stream.write(b"x" * 100_000)
        written.set()

    thread = threading.Thread(target=writer)
    thread.start()

    def append(record):
        calls.append(record)
        ready.set()
        assert written.wait(5)

    try:
        result = verify(Path(manifest["manifest"]), on_record=append)
    finally:
        thread.join(5)
    assert not thread.is_alive()
    assert result["status"] == "failed"
    assert len(calls) == 1


def test_boundary_limit_and_damage(tmp_path):
    """Exact limits succeed and damaged segments fail."""
    _, manifest = evidence(tmp_path)
    path = Path(manifest["manifest"])
    size = manifest["files"][0]["bytes"]
    assert verify(path, max_segment_bytes=size)["status"] == "verified"
    assert verify(path, max_segment_bytes=size - 1)["status"] == "failed"
    with pytest.raises(ValueError):
        verify(path, max_segment_bytes=True)
    segment = Path(manifest["files"][0]["file"])
    segment.write_bytes(b"x" * size)
    assert verify(path)["status"] == "failed"


def test_long_line_rejected_before_decode(tmp_path):
    """Reject oversized non-UTF8 lines without decoding them."""
    _, manifest = evidence(tmp_path)
    segment = Path(manifest["files"][0]["file"])
    segment.write_bytes(b"\xff" * (MAX_LINE_BYTES + 1))
    manifest["files"][0]["bytes"] = MAX_LINE_BYTES + 1
    Path(manifest["manifest"]).write_text(json.dumps(manifest))
    assert verify(Path(manifest["manifest"]))["status"] == "failed"


def test_empty_archive(tmp_path):
    """Empty legacy-compatible archives remain valid."""
    manifest = Archive(tmp_path, run_id="a" * 32).close()
    assert verify(Path(manifest["manifest"]))["status"] == "verified"

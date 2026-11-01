"""Manifest failures do not imply anything about original commit durability."""

import errno
import json
import os
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
    assert result["original_commit_status"] == "failed"
    assert result["run_status"] == "failed"
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

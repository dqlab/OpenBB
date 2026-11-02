"""Regression tests for report publication and recovery failures."""
import asyncio
import errno
import json
import time
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from openbb_futu.archive import Archive, recover_open_files
from openbb_futu.collector import Collector
from openbb_futu.coverage import report, write_json
from openbb_futu.read_budget import Budget

CONFIG = {"dimensions": [{"asset": "equity", "symbol": "US.A", "channel": "quote"}], "subscription_budget": 1}


def run(root, **patches):
    """Verify the report and evidence contract."""
    class ReadyOwner:
        """A ready owner double for tests that stop before any network send."""

        acquired_at = time.monotonic() - 1

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def budget(self):
            return SimpleNamespace(window=0.01)

    collector = Collector._isolated_for_test(dict(CONFIG), None, root)
    collector.stop.set()
    context = patch.multiple("openbb_futu.collector", **patches) if patches else nullcontext()
    with patch("openbb_futu.collector.Owner", return_value=ReadyOwner()), context:
        rc = asyncio.run(collector.run())
    events = [json.loads(line) for line in (root / f"futu-events-{collector.run_id}.jsonl").read_text().splitlines()]
    return rc, collector.run_id, events


def test_repeat_and_final_budget_failure(tmp_path):
    """Verify the report and evidence contract."""
    rc, first, _ = run(tmp_path)
    assert rc == 0
    assert run(tmp_path)[0] == 0
    assert report(tmp_path, first)["run_status"] == "stopped"
    assert report(tmp_path, first, budget=Budget(bytes_limit=1))["budget_or_input_error"]
    assert json.loads((tmp_path / f"futu-coverage-{first}.json").read_text())["run_status"] == "stopped"
    other = tmp_path / "other"
    other.mkdir()
    rc, run_id, events = run(other, Budget=lambda: Budget(bytes_limit=1))
    assert rc == 1 and events[-2]["event"] == "coverage_report_failure" and events[-1]["status"] == "failed"
    assert report(other, run_id)["run_status"] == "failed"


def test_replacement_failure_preserves_old_and_evidence(tmp_path):
    """Verify the report and evidence contract."""
    target = tmp_path / "report.json"
    target.write_bytes(b"old")
    with patch("os.replace", side_effect=OSError(errno.EIO, "injected")), pytest.raises(OSError):
        write_json(target, {"new": True}, replace=True)
    assert target.read_bytes() == b"old"
    assert not list(tmp_path.glob(".tmp-*"))
    with pytest.raises(FileExistsError):
        write_json(target, {}, replace=False)
    assert target.read_bytes() == b"old"
    write_json(target, {"new": True}, replace=True)
    assert json.loads(target.read_text()) == {"new": True}


@pytest.mark.parametrize("damage", ["tail", "digest"])
def test_corrupt_sealed_segment_is_rejected_without_mutation(tmp_path, damage):
    """Verify the report and evidence contract."""
    archive = Archive._isolated_for_test(tmp_path, run_id="a" * 32)
    assert archive.accept('{"type":"QUOTE","symbol":"US.A","data":{}}', "c")
    manifest = archive.close()
    segment = Path(manifest["files"][0]["file"])
    if damage == "tail":
        with segment.open("ab") as f:
            f.write(b"bad-tail")
    else:
        manifest_path = Path(manifest["manifest"])
        value = json.loads(manifest_path.read_text())
        value["files"][0]["sha256"] = "0" * 64
        manifest_path.write_text(json.dumps(value))
    before = {p: p.read_bytes() for p in tmp_path.rglob("*") if p.is_file()}
    with pytest.raises(ValueError, match="sealed segment"):
        recover_open_files(tmp_path)
    assert {p: p.read_bytes() for p in tmp_path.rglob("*") if p.is_file()} == before


def test_final_publication_failure_is_readable(tmp_path):
    """Verify the report and evidence contract."""
    with patch("openbb_futu.root_handle.os.replace", side_effect=OSError(errno.EIO, "injected")):
        rc, run_id, events = run(tmp_path)
    assert rc == 1 and events[-1]["status"] == "failed"
    assert report(tmp_path, run_id)["run_status"] == "failed"


def test_historical_preflight_and_partial_publish(tmp_path):
    """Verify the report and evidence contract."""
    from openbb_futu import collector as module
    from openbb_futu.read_budget import BudgetExceeded

    _, first, _ = run(tmp_path)
    _, second, _ = run(tmp_path)
    reports = sorted(tmp_path.glob("futu-coverage-*.json"))
    before = {p: p.read_bytes() for p in reports}
    raw = {p: p.read_bytes() for p in tmp_path.glob("futu-plan-*.json")}
    original_report = module._report
    calls = []
    def exhaust(*args, **kwargs):
        calls.append(args[1])
        if len(calls) == 2:
            raise BudgetExceeded("injected shared budget exhaustion")
        return original_report(*args, **kwargs)
    rc, _, events = run(tmp_path, _report=exhaust)
    assert rc == 1 and len(calls) == 2
    assert not any(e["event"] == "historical_reports_publish_started" for e in events)
    assert {p: p.read_bytes() for p in reports} == before
    assert {p: p.read_bytes() for p in raw} == raw

    original_write = module.write_json
    writes = []
    def fail_second(path, obj, **kwargs):
        if path.name.startswith("futu-coverage-"):
            writes.append(path)
            if len(writes) == 2:
                raise OSError(errno.EIO, "injected")
        return original_write(path, obj, **kwargs)
    rc, _, events = run(tmp_path, write_json=fail_second)
    assert rc == 1 and len(writes) == 2
    assert [e["event"] for e in events].count("historical_report_published") == 1
    assert writes[1].read_bytes() == before[writes[1]]
    assert json.loads(writes[0].read_text())["run_id"] in (first, second)
    assert {p: p.read_bytes() for p in raw} == raw


def test_real_shared_budget_exhausts_across_plans(tmp_path):
    """Verify the report and evidence contract."""
    from openbb_futu.coverage import prepare

    plan, _ = prepare(CONFIG)
    for run_id in ("a" * 32, "b" * 32):
        (tmp_path / f"futu-plan-{run_id}.json").write_text(json.dumps(plan))
        (tmp_path / f"futu-events-{run_id}.jsonl").write_text('{"event":"mode_change"}\n')
        (tmp_path / f"futu-coverage-{run_id}.json").write_bytes(b"old report")
    before = {p: p.read_bytes() for p in tmp_path.iterdir()}
    rc, _, events = run(tmp_path, Budget=lambda: Budget(lines_limit=1))
    assert rc == 1
    assert not any(e["event"] == "historical_reports_publish_started" for e in events)
    assert {p: p.read_bytes() for p in before} == before


def test_file_and_directory_fsync_failures(tmp_path):
    """Distinguish a failed temp sync from a failed post-replace directory sync."""
    import os
    import stat

    from openbb_futu.root_handle import PublicationDurabilityUnknown

    target = tmp_path / "report.json"
    original = os.fsync
    for fail_directory in (False, True):
        target.write_bytes(b"old")
        def fail(fd):
            if stat.S_ISDIR(os.fstat(fd).st_mode) == fail_directory:
                raise OSError(errno.EIO, "injected")
            return original(fd)
        expected = PublicationDurabilityUnknown if fail_directory else OSError
        with patch("openbb_futu.root_handle.os.fsync", side_effect=fail), pytest.raises(expected):
            write_json(target, {"new": True}, replace=True)
        assert (json.loads(target.read_text()) == {"new": True}) if fail_directory else target.read_bytes() == b"old"
        assert not list(tmp_path.glob(".tmp-*"))


def test_historical_directory_fsync_event_and_failed_channel(tmp_path):
    """Report visible publication and keep a failure exit even if logging fails."""
    import os
    import stat
    _, first, _ = run(tmp_path)
    original = os.fsync
    original_replace = os.replace
    replaced = False
    def track_replace(*args, **kwargs):
        nonlocal replaced
        result = original_replace(*args, **kwargs)
        replaced = True
        return result
    def fail_dir(fd):
        if replaced and stat.S_ISDIR(os.fstat(fd).st_mode):
            raise OSError(errno.EIO, "injected")
        return original(fd)
    with patch("openbb_futu.root_handle.os.replace", side_effect=track_replace), patch(
        "openbb_futu.root_handle.os.fsync", side_effect=fail_dir
    ):
        rc, _, events = run(tmp_path)
    assert rc == 1
    failure = next(e for e in events if e["event"] == "historical_report_publish_failure")
    assert failure["run_id"] == first and failure["publication_status"] == "visible_durability_unknown"
    assert json.loads((tmp_path / f"futu-coverage-{first}.json").read_text())["run_id"] == first
    original_event = Collector.event
    def bad_channel(self, kind, **fields):
        if kind in ("historical_report_publish_failure", "fatal", "stopped"):
            raise OSError(errno.EIO, "event channel")
        return original_event(self, kind, **fields)
    with patch.object(Collector, "event", bad_channel), patch("openbb_futu.root_handle.os.fsync", side_effect=fail_dir):
        assert run(tmp_path)[0] == 1


def test_collector_sealed_failure_identifies_preserved_segment(tmp_path):
    """Collector reports a safe locator without altering corrupt evidence."""
    archive = Archive._isolated_for_test(tmp_path, run_id="a" * 32)
    assert archive.accept('{"type":"QUOTE","symbol":"US.A","data":{}}', "c")
    result = archive.close()
    segment = Path(result["files"][0]["file"])
    with segment.open("ab") as stream:
        stream.write(b"broken")
    before = {p: p.read_bytes() for p in tmp_path.rglob("*") if p.is_file()}
    rc, _, events = run(tmp_path)
    fatal = next(e for e in events if e["event"] == "fatal")
    assert rc == 1 and fatal["category"] == "sealed_segment_invalid"
    assert fatal["reason"] == "corrupt_or_incomplete"
    assert fatal["relative_path"] == segment.relative_to(tmp_path).as_posix()
    assert {p: p.read_bytes() for p in before} == before


def test_final_report_directory_sync_failure(tmp_path):
    """Final report is visible but its directory sync failure makes the run fail."""
    import os
    import stat
    original = os.fsync
    def fail_report_dir(fd):
        # Plan publication must succeed; fail only when the final report exists.
        if stat.S_ISDIR(os.fstat(fd).st_mode) and list(tmp_path.glob("futu-coverage-*.json")):
            raise OSError(errno.EIO, "injected")
        return original(fd)
    with patch("openbb_futu.root_handle.os.fsync", side_effect=fail_report_dir):
        rc, run_id, events = run(tmp_path)
    failure = next(e for e in events if e["event"] == "coverage_report_failure")
    assert rc == 1 and failure["publication_status"] == "visible_durability_unknown"
    assert json.loads((tmp_path / f"futu-coverage-{run_id}.json").read_text())["run_id"] == run_id
    assert events[-1]["status"] == "failed"

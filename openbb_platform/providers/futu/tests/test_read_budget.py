"""Offline limits for coverage evidence reads."""

import os
from pathlib import Path
from unittest.mock import patch

import pytest
from openbb_futu.coverage import prepare, report
from openbb_futu.read_budget import Budget, BudgetExceeded


def test_regular_only(tmp_path):
    """Reject special files and symlinks."""
    fifo = tmp_path / "fifo"
    os.mkfifo(fifo)
    link = tmp_path / "link"
    link.symlink_to(fifo)
    for path in (fifo, link, Path("/dev/null")):
        with pytest.raises((OSError, ValueError)):
            Budget().read(path, max_bytes=10)


def test_limits_and_existing_data(tmp_path):
    """Respect exact boundaries and ordinary files."""
    path = tmp_path / "data"
    path.write_bytes(b"a\nb\n")
    budget = Budget(bytes_limit=4, lines_limit=2, chunk_size=1, line_limit=2)
    assert list(budget.lines(path)) == [b"a\n", b"b\n"]
    with pytest.raises(BudgetExceeded):
        list(Budget(lines_limit=1).lines(path))
    path.write_bytes(b"x" * 10000)
    with pytest.raises(BudgetExceeded):
        list(Budget(chunk_size=7, line_limit=10).lines(path))
    with pytest.raises(ValueError):
        Budget(chunk_size=0)


def test_unending_append_is_bounded(tmp_path):
    """Simulate a writer that never reaches EOF."""
    path = tmp_path / "data"
    path.touch()
    with patch("openbb_futu.read_budget.os.read", return_value=b"a\n"), pytest.raises(BudgetExceeded):
        list(Budget(bytes_limit=10, lines_limit=100).lines(path))


def test_plan_and_event_limits_never_publish(tmp_path):
    """Fail closed when a plan or event log exceeds limits."""
    run = "a" * 32
    plan = tmp_path / f"futu-plan-{run}.json"
    plan.write_text('{"selected": [], "not_selected": [], "by_asset_channel": {}}')
    events = tmp_path / f"futu-events-{run}.jsonl"
    events.write_text('{"event":"mode_change"}\n' * 2)
    for budget in (Budget(bytes_limit=5), Budget(lines_limit=1)):
        result = report(tmp_path, run, budget=budget)
        assert result["run_status"] == result["evidence_status"] == "unknown"
        assert not (tmp_path / f"futu-coverage-{run}.json").exists()
    with pytest.raises(ValueError):
        prepare({"dimensions": [{}] * 10001, "subscription_budget": 1})


def test_directory_entries_count_all_names(tmp_path):
    """Unmatched names are charged, including at the exact boundary."""
    (tmp_path / "one").touch()
    (tmp_path / "two").touch()
    assert len(list(Budget(entries_limit=2).entries(tmp_path))) == 2
    with pytest.raises(BudgetExceeded):
        list(Budget(entries_limit=1).entries(tmp_path))


def test_exhausted_report_preserves_existing_snapshot(tmp_path):
    """A failed rebuild cannot overwrite an earlier published report."""
    run = "a" * 32
    (tmp_path / f"futu-plan-{run}.json").write_text('{"selected": [], "not_selected": [], "by_asset_channel": {}}')
    snapshot = tmp_path / f"futu-coverage-{run}.json"
    snapshot.write_bytes(b"previous evidence\n")
    manifests = tmp_path / "futu-manifests"
    manifests.mkdir()
    (manifests / "unmatched").touch()
    (manifests / "also-unmatched").touch()
    result = report(tmp_path, run, budget=Budget(entries_limit=1))
    assert result["evidence_status"] == result["integrity_status"] == "unknown"
    assert snapshot.read_bytes() == b"previous evidence\n"


def test_parent_symlink_rejected(tmp_path):
    """Do not traverse symlinks in evidence directory paths."""
    target = tmp_path / "target"
    target.mkdir()
    (target / "data").write_bytes(b"ok")
    (tmp_path / "alias").symlink_to(target, target_is_directory=True)
    with pytest.raises(OSError):
        Budget().read(tmp_path / "alias" / "data", max_bytes=10)
    with pytest.raises(OSError):
        list(Budget().entries(tmp_path / "alias"))


def test_shared_budget_covers_all_segments(tmp_path):
    """The second segment cannot evade a budget already used by the first."""
    import json

    from openbb_futu.archive import Archive
    from openbb_futu.offline import verify

    run = "b" * 32
    archive = Archive(tmp_path, run_id=run, max_bytes=1)
    for _ in range(3):
        assert archive.accept('{"type":"QUOTE","symbol":"US.A","data":{}}', "c")
    manifest = archive.close()
    assert len(manifest["files"]) >= 2
    path = Path(manifest["manifest"])
    assert verify(path)["status"] == "verified"
    with pytest.raises(BudgetExceeded):
        verify(path, budget=Budget(lines_limit=1))
    plan, _ = prepare({"dimensions": [{"asset": "equity", "symbol": "US.A", "channel": "quote"}],
                       "subscription_budget": 1})
    (tmp_path / f"futu-plan-{run}.json").write_text(json.dumps(plan))
    snapshot = tmp_path / f"futu-coverage-{run}.json"
    snapshot.write_bytes(b"saved\n")
    result = report(tmp_path, run, budget=Budget(lines_limit=1))
    assert result["integrity_status"] == "unknown"
    assert snapshot.read_bytes() == b"saved\n"


@pytest.mark.parametrize("replace_plan", [False, True])
def test_replaced_inputs_do_not_publish(tmp_path, monkeypatch, replace_plan):
    """Replacements between enumeration and verification fail closed."""
    import json

    from openbb_futu import coverage
    from openbb_futu.archive import Archive

    run = "c" * 32
    plan, _ = prepare({"dimensions": [{"asset": "equity", "symbol": "US.A", "channel": "quote"}],
                       "subscription_budget": 1})
    plan_path = tmp_path / f"futu-plan-{run}.json"
    plan_path.write_text(json.dumps(plan))
    manifest = Archive(tmp_path, run_id=run).close()
    snapshot = tmp_path / f"futu-coverage-{run}.json"
    snapshot.write_bytes(b"saved\n")
    original = coverage.verify

    def replace_then_verify(path, **kwargs):
        target = plan_path if replace_plan else Path(path)
        replacement = target.with_suffix(".replacement")
        replacement.write_bytes(target.read_bytes())
        replacement.replace(target)
        return original(path, **kwargs)

    monkeypatch.setattr(coverage, "verify", replace_then_verify)
    result = report(tmp_path, run)
    assert result["integrity_status"] != "verified"
    assert snapshot.read_bytes() == b"saved\n"
    assert manifest["run_id"] == run


def test_directory_replacement_between_reads_fails(tmp_path):
    """A budget remembers the directory identities used by earlier reads."""
    directory = tmp_path / "data"
    directory.mkdir()
    (directory / "file").write_bytes(b"first")
    budget = Budget()
    assert budget.read(directory / "file", max_bytes=10) == b"first"
    directory.rename(tmp_path / "old")
    directory.mkdir()
    (directory / "file").write_bytes(b"other")
    with pytest.raises(ValueError, match="directory replaced"):
        budget.read(directory / "file", max_bytes=10)

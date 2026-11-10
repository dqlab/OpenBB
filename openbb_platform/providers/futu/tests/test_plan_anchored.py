"""Real filesystem checks for the held-root plan reconstruction path."""
import asyncio
import json
import os

import pytest
from openbb_futu.collector import Collector
from openbb_futu.coverage import _report, prepare, report
from openbb_futu.read_budget import Budget, BudgetExceeded, EvidenceChanged
from openbb_futu.root_handle import RootHandle

CONFIG = {"dimensions": [{"asset": "equity", "symbol": "US.A", "channel": "quote"}],
          "subscription_budget": 1}
RUN = "a" * 32


def make_plan(root, run=RUN):
    """Create a historical plan using the real compiler."""
    plan, _ = prepare(CONFIG)
    path = root / f"futu-plan-{run}.json"
    path.write_text(json.dumps(plan))
    return path


def test_historical_plan_and_replacement(tmp_path):
    """Exercise the held plan boundary with real files."""
    path = make_plan(tmp_path)
    report_path = tmp_path / f"futu-coverage-{RUN}.json"
    report_path.write_bytes(b"existing")
    with RootHandle(tmp_path) as handle:
        budget = Budget()
        assert path.name in list(budget.entries_at(handle))
        snapshot = budget.read_identity_at(handle, path.name, max_bytes=8_388_608)
        assert _report(tmp_path, RUN, publish=False, budget=budget, root_handle=handle,
                       plan_snapshot=snapshot)["run_id"] == RUN
        replacement = tmp_path / "replacement"
        replacement.write_bytes(path.read_bytes())
        os.replace(replacement, path)
        with pytest.raises(EvidenceChanged):
            _report(tmp_path, RUN, publish=True, budget=budget, root_handle=handle,
                    plan_snapshot=snapshot)
    assert report_path.read_bytes() == b"existing"


def test_root_replacement_rejected(tmp_path):
    """Exercise the held plan boundary with real files."""
    root = tmp_path / "root"
    root.mkdir()
    make_plan(root)
    with RootHandle(root) as handle:
        root.rename(tmp_path / "original")
        root.mkdir()
        make_plan(root)
        result = report(root, RUN, root_handle=handle)
        assert result["budget_or_input_error"]
        assert not list(root.glob("futu-coverage-*"))
        assert not list((tmp_path / "original").glob("futu-coverage-*"))


@pytest.mark.parametrize("kind", ["symlink", "fifo"])
def test_special_plan_fails_without_overwriting(tmp_path, kind):
    """Exercise the held plan boundary with real files."""
    path = tmp_path / f"futu-plan-{RUN}.json"
    if kind == "fifo":
        os.mkfifo(path)
    else:
        target = tmp_path / "target"
        target.write_text("{}")
        path.symlink_to(target)
    out = tmp_path / f"futu-coverage-{RUN}.json"
    out.write_bytes(b"existing")
    assert report(tmp_path, RUN)["budget_or_input_error"]
    assert out.read_bytes() == b"existing"


def test_growth_during_read_and_limits(tmp_path, monkeypatch):
    """Exercise the held plan boundary with real files."""
    path = make_plan(tmp_path)
    original = os.read
    grown = False
    def read(fd, size):
        nonlocal grown
        data = original(fd, size)
        if data and not grown:
            grown = True
            with path.open("ab") as stream:
                stream.write(b" ")
        return data
    with RootHandle(tmp_path) as handle:
        monkeypatch.setattr(os, "read", read)
        with pytest.raises(EvidenceChanged):
            Budget(chunk_size=10).read_identity_at(handle, path.name, max_bytes=8_388_608)
    monkeypatch.setattr(os, "read", original)
    with RootHandle(tmp_path) as handle:
        with pytest.raises(BudgetExceeded):
            Budget(bytes_limit=1).read_identity_at(handle, path.name, max_bytes=8_388_608)
        (tmp_path / "another").write_text("x")
        with pytest.raises(BudgetExceeded):
            list(Budget(entries_limit=1).entries_at(handle))


def test_collector_shared_budget_preserves_all_existing_reports(tmp_path, monkeypatch):
    """Exercise the held plan boundary with real files."""
    import openbb_futu.collector as module
    for run in (RUN, "b" * 32):
        make_plan(tmp_path, run)
        (tmp_path / f"futu-coverage-{run}.json").write_bytes(b"existing")
    before = {p: p.read_bytes() for p in tmp_path.iterdir()}
    monkeypatch.setattr(module, "coordination_directory", lambda: tmp_path / "locks")
    monkeypatch.setattr(module, "Budget", lambda: Budget(bytes_limit=600))
    collector = Collector._isolated_for_test(dict(CONFIG), None, tmp_path)
    collector.stop.set()
    assert asyncio.run(collector.run()) == 1
    assert all(p.read_bytes() == content for p, content in before.items())
    events = (tmp_path / f"futu-events-{collector.run_id}.jsonl").read_text()
    assert "historical_reports_publish_started" not in events


def test_discovery_to_read_replacement_rejected(tmp_path):
    """Exercise the held plan boundary with real files."""
    path = make_plan(tmp_path)
    with RootHandle(tmp_path) as handle:
        budget = Budget()
        assert path.name in list(budget.entries_at(handle))
        replacement = tmp_path / "new"
        replacement.write_bytes(path.read_bytes())
        os.replace(replacement, path)
        with pytest.raises(EvidenceChanged):
            budget.read_identity_at(handle, path.name, max_bytes=8_388_608)

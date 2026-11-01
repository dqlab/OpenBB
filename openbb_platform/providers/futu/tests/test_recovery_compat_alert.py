"""Recovery alert publication at the actual no-replace rename boundary."""

import json
from pathlib import Path

import pytest
from openbb_futu import archive


def make_source(root):
    """Create an interrupted raw source."""
    path = root / "source_id=futu/recv_date=2025-01-01/session=abc/part-000001.jsonl.open"
    path.parent.mkdir(parents=True)
    path.write_bytes(b'{"local_seq":1}\npartial')
    return path


def test_legacy_alert_and_v2_snapshot(tmp_path):
    """Publish a conservative v2 alert without changing the source."""
    source = make_source(tmp_path)
    original = source.read_bytes()
    report = archive.recover_open_files(tmp_path)[0]
    directory = Path(report["evidence_dir"])
    v2 = json.loads((directory / "recovery-unknown-v2.json").read_bytes())
    assert not (directory / "recovery.json").exists()
    assert v2["error"] and v2["records"] is None
    assert all(v2[k] is None for k in ("first_seq", "last_seq", "valid_bytes", "unread_bytes"))
    assert v2["snapshot"]["records"] == 1
    assert source.read_bytes() == original


@pytest.mark.parametrize("failure", [PermissionError, KeyboardInterrupt])
def test_report_rename_failure_preserves_source_and_temp(tmp_path, monkeypatch, failure):
    """An interrupted report publication retains original and temporary evidence."""
    source = make_source(tmp_path)
    original = source.read_bytes()
    rename = archive._rename_noreplace
    def inject(fd, src, dst):
        if dst == "recovery-unknown-v2.json":
            raise failure()
        rename(fd, src, dst)
    monkeypatch.setattr(archive, "_rename_noreplace", inject)
    with pytest.raises(failure):
        archive.recover_open_files(tmp_path)
    assert source.read_bytes() == original
    assert list(tmp_path.glob("futu-recovery/*/.tmp-*"))
    assert not list(tmp_path.glob("futu-recovery/*/recovery.json"))
    assert not list(tmp_path.glob("futu-recovery/*/recovery-unknown-v2.json"))


def test_isolated_collector_restart_reports_alert_and_fails(tmp_path, monkeypatch):
    """A restart reports uncertain recovery and does not claim success."""
    import asyncio
    import time

    from openbb_futu.collector import Collector

    monkeypatch.setenv("FUTU_COORDINATION_DIR", str(tmp_path / "coord"))
    root = tmp_path / "out"
    source = make_source(root)
    original = source.read_bytes()
    class Store:
        def read(self):
            return {"access_token": "synthetic", "refresh_token": "refresh",
                    "client_id": "client", "expires_at": time.time() + 3600}
    collector = Collector._isolated_for_test({"channels": {"quote": ["US.A"]}}, Store(), root)
    async def session(_owner):
        collector.stop.set()
    monkeypatch.setattr(collector, "session", session)
    assert asyncio.run(collector.run()) == 1
    events = [json.loads(line) for line in (root / f"futu-events-{collector.run_id}.jsonl").read_text().splitlines()]
    assert any(e["event"] == "interrupted_segment" and e["error"] == "source_stability_unknown" for e in events)
    assert any(e["event"] == "stopped" and e["status"] == "failed" for e in events)
    assert source.read_bytes() == original


def test_collector_publication_failure_keeps_unresolved_and_originals(tmp_path, monkeypatch, capsys):
    """A real publication fault cannot clear the fail-closed recovery state."""
    import asyncio
    import hashlib

    from openbb_futu.collector import Collector

    monkeypatch.setenv("FUTU_COORDINATION_DIR", str(tmp_path / "coord"))
    root = tmp_path / "out"
    source = make_source(root)
    old = root / "futu-recovery/legacy/recovery.json"
    old.parent.mkdir(parents=True)
    old.write_bytes(b'{"legacy":true}\n')
    before = {p: hashlib.sha256(p.read_bytes()).digest() for p in (source, old)}
    def fail_publish(*args, **kwargs):
        raise OSError(28, "private details must not be logged")
    monkeypatch.setattr(archive, "_rename_noreplace", fail_publish)
    collector = Collector._isolated_for_test({"channels": {"quote": ["US.A"]}}, object(), root)
    assert asyncio.run(collector.run()) == 1
    assert collector.archive is None
    assert "recovery_unresolved" in capsys.readouterr().err
    assert all(hashlib.sha256(p.read_bytes()).digest() == digest for p, digest in before.items())
    events = (root / f"futu-events-{collector.run_id}.jsonl").read_text()
    assert '"recovery_unresolved"' in events
    assert "private details" not in events
    assert not list(root.glob("futu-manifests/*.json"))


def test_empty_session_does_not_signal_unresolved(tmp_path):
    """A valid empty session is not an interrupted segment."""
    session = tmp_path / ("source_id=futu/recv_date=2025-01-01/session=" + "a" * 32)
    session.mkdir(parents=True)
    alerts = []
    assert archive.recover_open_files(tmp_path, on_unresolved=lambda: alerts.append(True)) == []
    assert not alerts


def test_discovery_alert_precedes_directory_budget_failure(tmp_path):
    """The alert fires as the open entry is seen, before entry-budget failure."""
    from openbb_futu.read_budget import Budget, BudgetExceeded

    source = make_source(tmp_path)
    before = source.read_bytes()
    alerts = []
    with pytest.raises(BudgetExceeded):
        archive.recover_open_files(tmp_path, budget=Budget(entries_limit=2),
                                   on_unresolved=lambda: alerts.append(True))
    assert alerts == [True]
    assert source.read_bytes() == before

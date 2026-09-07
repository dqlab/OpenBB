import sqlite3
from datetime import UTC, datetime
from pathlib import Path

import pytest
from pydantic import ValidationError

from openbb_collector_core import delivery as d


@pytest.fixture
def clock(monkeypatch):
    now = [datetime(2026, 1, 31, 12, tzinfo=UTC).timestamp()]
    monkeypatch.setattr(d.time, "time", lambda: now[0])
    return now


def make_store(tmp_path, policy="1d"):
    config = d.DeliveryConfig(
        collector_id="edge",
        transport="local",
        destination_root=str(tmp_path / "master"),
        local_retention=policy,
    )
    source = tmp_path / "collector"
    raw = source / "bronze/aa/response.json"
    raw.parent.mkdir(parents=True)
    raw.write_bytes(b'{"price":100}\n')
    store = d.DeliveryStore(source, config)
    store.stage(
        "live",
        "session",
        [{"path": "bronze/aa/response.json", "role": "raw", "remove": True}],
        {"revision": "v1"},
    )
    return store, raw


@pytest.mark.parametrize(
    "policy,seconds", [("1h", 3600), ("1d", 86400), ("1w", 604800), ("7d", 604800)]
)
def test_fixed_durations(policy, seconds):
    config = d.DeliveryConfig(
        collector_id="edge", transport="local", destination_root="/data", local_retention=policy
    )
    start = datetime(2026, 3, 8, 6, 30, tzinfo=UTC).timestamp()
    assert config.cleanup_after(start) == start + seconds


@pytest.mark.parametrize(
    "start,policy,expected",
    [
        ("2026-01-31T12:00:00+00:00", "1mo", "2026-02-28T12:00:00+00:00"),
        ("2024-01-31T12:00:00+00:00", "1mo", "2024-02-29T12:00:00+00:00"),
        ("2026-01-31T12:00:00+00:00", "2mo", "2026-03-31T12:00:00+00:00"),
        ("2026-12-31T12:00:00+00:00", "1mo", "2027-01-31T12:00:00+00:00"),
    ],
)
def test_calendar_month_boundaries(start, policy, expected):
    config = d.DeliveryConfig(
        collector_id="edge", transport="local", destination_root="/data", local_retention=policy
    )
    assert (
        config.cleanup_after(datetime.fromisoformat(start).timestamp())
        == datetime.fromisoformat(expected).timestamp()
    )


def test_retention_survives_restart_and_force_cannot_expire_early(tmp_path, clock, monkeypatch):
    store, raw = make_store(tmp_path)
    first = clock[0]
    event = store.send_pending()[0]
    assert event["status"] == "delivered" and event["local_removed"] is False
    assert raw.exists()
    job = store.status()["jobs"][0]
    assert job["local_state"] == "retained"
    assert datetime.fromisoformat(job["cleanup_after"]).timestamp() == first + 86400
    root, config = store.root, store.config
    store.close()
    store = d.DeliveryStore(root, config)
    backend = d.destination

    def fail(*args):
        raise AssertionError("Unexpired copies must not trigger a network retry")

    monkeypatch.setattr(d, "destination", fail)
    try:
        clock[0] += 86399
        assert store.send_pending(force=True) == []
        assert raw.exists()
        assert store.status()["jobs"][0]["first_verified_at"] == job["first_verified_at"]
        monkeypatch.setattr(d, "destination", backend)
        clock[0] += 1
        assert d.read_delivery_status(root, config)["jobs"][0]["local_state"] == "cleanup_due"
        assert store.send_pending(force=True)[0]["local_removed"] is True
        assert not raw.exists()
        assert store.status()["jobs"][0]["local_state"] == "removed"
    finally:
        store.close()


def test_expiry_still_requires_current_central_checksum(tmp_path, clock):
    store, raw = make_store(tmp_path, "1w")
    original = raw.read_bytes()
    try:
        store.send_pending()
        central = next(
            Path(store.config.destination_root).glob("remote_collectors/edge/objects/sha256/*/*")
        )
        central.write_bytes(b"corruption")
        clock[0] += 604800
        assert store.send_pending()[0]["error"] == "central_checksum_conflict"
        assert raw.exists()
        central.write_bytes(original)
        assert store.send_pending(force=True)[0]["local_removed"] is True
        assert not raw.exists()
    finally:
        store.close()


def test_failed_transfer_does_not_start_the_retention_clock(tmp_path, clock, monkeypatch):
    store, raw = make_store(tmp_path)
    backend = d.destination

    def fail(*args):
        raise OSError("master offline")

    monkeypatch.setattr(d, "destination", fail)
    try:
        assert store.send_pending()[0]["status"] == "pending"
        assert store.status()["jobs"][0]["first_verified_at"] is None
        clock[0] += 60 * 86400
        monkeypatch.setattr(d, "destination", backend)
        assert store.send_pending(force=True)[0]["status"] == "delivered"
        assert raw.exists()
        expiry = store.status()["jobs"][0]["cleanup_after"]
        assert datetime.fromisoformat(expiry).timestamp() == clock[0] + 86400
    finally:
        store.close()


def test_receipt_crash_retry_does_not_restart_retention_clock(tmp_path, clock, monkeypatch):
    store, raw = make_store(tmp_path)
    cleanup = store._cleanup

    def fail(*args):
        raise OSError("crash after acknowledgment")

    monkeypatch.setattr(store, "_cleanup", fail)
    try:
        assert store.send_pending()[0]["status"] == "pending"
        first = store.status()["jobs"][0]["first_verified_at"]
        clock[0] += 7200
        monkeypatch.setattr(store, "_cleanup", cleanup)
        assert store.send_pending(force=True)[0]["status"] == "delivered"
        assert raw.exists()
        assert store.status()["jobs"][0]["first_verified_at"] == first
    finally:
        store.close()


def test_keep_forever_then_change_to_calendar_month(tmp_path, clock):
    store, raw = make_store(tmp_path, "forever")
    first = clock[0]
    try:
        store.send_pending()
        clock[0] += 100 * 86400
        assert store.send_pending(force=True) == [] and raw.exists()
        assert store.status()["jobs"][0]["cleanup_after"] is None
        store.config.local_retention = "1mo"
        assert store.config.cleanup_after(first) < clock[0]
        assert store.send_pending(force=True)[0]["local_removed"] is True
        assert not raw.exists()
    finally:
        store.close()


def test_after_verification_and_legacy_boolean_policy(tmp_path, clock):
    store, raw = make_store(tmp_path, "after_verification")
    try:
        assert store.send_pending()[0]["local_removed"] is True and not raw.exists()
        for flag, expected in [(False, "forever"), (True, "after_verification")]:
            config = store.config.model_copy(
                update={"local_retention": None, "remove_local_after_verification": flag}
            )
            assert config.retention_policy() == expected
    finally:
        store.close()


def test_old_outbox_read_only_status_and_additive_migration(tmp_path, clock):
    store, raw = make_store(tmp_path, "forever")
    store.send_pending()
    root, config = store.root, store.config
    store.close()
    path = root / "delivery/state.sqlite3"
    with sqlite3.connect(path) as db:
        db.execute("ALTER TABLE jobs DROP COLUMN first_verified_at")
    assert d.read_delivery_status(root, config)["jobs"][0]["first_verified_at"] is None
    with sqlite3.connect(path) as db:
        assert "first_verified_at" not in {row[1] for row in db.execute("PRAGMA table_info(jobs)")}
    config.local_retention = "1d"
    clock[0] += 30 * 86400
    store = d.DeliveryStore(root, config)
    try:
        assert store.send_pending()[0]["local_removed"] is False
        assert raw.exists()
        assert (
            datetime.fromisoformat(store.status()["jobs"][0]["first_verified_at"]).timestamp()
            == clock[0]
        )
    finally:
        store.close()


@pytest.mark.parametrize("value", ["0d", "-1d", "1.5d", "1m", "month", "1000000d", "", True])
def test_invalid_retention_rejected(value):
    with pytest.raises(ValidationError):
        d.DeliveryConfig(
            collector_id="edge", transport="local", destination_root="/data", local_retention=value
        )


def test_conflicting_retention_settings_rejected():
    with pytest.raises(ValidationError, match="Use local_retention"):
        d.DeliveryConfig(
            collector_id="edge",
            transport="local",
            destination_root="/data",
            local_retention="1w",
            remove_local_after_verification=True,
        )

import hashlib
import json
import time
from pathlib import Path

import pytest
from pydantic import ValidationError

from openbb_collector_core import delivery as d


def setup_store(tmp_path, **settings):
    source = tmp_path / "collector"
    source.mkdir(exist_ok=True)
    raw = source / "bronze/aa/example.json"
    raw.parent.mkdir(parents=True, exist_ok=True)
    raw.write_bytes(b'{"price":100}\n')
    journal = source / "journal.sqlite3"
    journal.write_bytes(b"operational-state")
    config = d.DeliveryConfig(
        collector_id="edge-01",
        transport="local",
        destination_root=str(tmp_path / "master"),
        **settings,
    )
    store = d.DeliveryStore(source, config)
    return store, raw, journal


def stage(store, session="session1"):
    return store.stage(
        "live",
        session,
        [{"path": "bronze/aa/example.json", "role": "raw", "remove": True}],
        {"revision": "v1"},
    )


def test_verified_move_and_restart_are_idempotent(tmp_path):
    store, raw, journal = setup_store(tmp_path, remove_local_after_verification=True)
    payload = raw.read_bytes()
    identity = stage(store)
    assert store.send_pending()[0]["status"] == "delivered"
    assert not raw.exists()
    assert journal.read_bytes() == b"operational-state"
    receipt = store.root / f"delivery/receipts/{identity}.json"
    assert receipt.exists()
    root, config = store.root, store.config
    central = Path(config.destination_root)
    objects = list(central.glob("remote_collectors/*/objects/sha256/*/*"))
    assert len(objects) == 1 and objects[0].read_bytes() == payload
    manifest_path = next(central.rglob("*.manifest.json"))
    manifest = json.loads(manifest_path.read_bytes())
    assert hashlib.sha256(manifest_path.read_bytes()).hexdigest() == identity
    assert manifest["files"][0]["sha256"] == hashlib.sha256(payload).hexdigest()
    store.close()
    store = d.DeliveryStore(root, config)
    try:
        assert stage(store) == identity
        assert store.send_pending(force=True) == []
        assert store.status()["counts"] == {"delivered": 1}
        # A later session may reference an already moved immutable object.
        stage(store, "session2")
        assert store.send_pending(force=True)[0]["status"] == "delivered"
        assert len(list(central.rglob("*.received.json"))) == 2
    finally:
        store.close()


def test_failed_transfer_keeps_data_and_retries_after_reopen(tmp_path, monkeypatch):
    store, raw, _ = setup_store(tmp_path, remove_local_after_verification=True)
    stage(store)
    original = d.put_verified

    def fail_manifest(backend, name, *args):
        if name.endswith(".manifest.json"):
            raise OSError("sensitive endpoint details")
        return original(backend, name, *args)

    monkeypatch.setattr(d, "put_verified", fail_manifest)
    try:
        event = store.send_pending()[0]
        assert event["error"] == "delivery_transport_error"
        assert raw.exists()
        assert not list(Path(store.config.destination_root).rglob("*.received.json"))
        assert store.send_pending() == []  # Retry backoff.
        monkeypatch.setattr(d, "put_verified", original)
        assert store.send_pending(force=True)[0]["status"] == "delivered"
        assert not raw.exists()
    finally:
        store.close()


def test_corrupt_central_object_is_not_overwritten_or_acknowledged(tmp_path):
    store, raw, _ = setup_store(tmp_path, remove_local_after_verification=True)
    sha = hashlib.sha256(raw.read_bytes()).hexdigest()
    target = (
        Path(store.config.destination_root)
        / f"remote_collectors/edge-01/objects/sha256/{sha[:2]}/{sha}"
    )
    target.parent.mkdir(parents=True)
    target.write_bytes(b"corruption")
    try:
        stage(store)
        assert store.send_pending()[0]["error"] == "central_checksum_conflict"
        assert target.read_bytes() == b"corruption" and raw.exists()
        assert not list(target.parents[4].rglob("*.received.json"))
    finally:
        store.close()


def test_receipt_saved_before_cleanup_and_crash_retry(tmp_path, monkeypatch):
    store, raw, _ = setup_store(tmp_path, remove_local_after_verification=True)
    identity = stage(store)
    cleanup = store._cleanup

    def crash(job):
        assert (store.root / f"delivery/receipts/{identity}.json").exists()
        raise OSError("simulated interruption")

    monkeypatch.setattr(store, "_cleanup", crash)
    try:
        assert store.send_pending()[0]["status"] == "pending" and raw.exists()
        monkeypatch.setattr(store, "_cleanup", cleanup)
        assert store.send_pending(force=True)[0]["status"] == "delivered"
        assert not raw.exists()
    finally:
        store.close()


def test_copy_then_enable_move(tmp_path):
    store, raw, _ = setup_store(tmp_path)
    try:
        stage(store)
        store.send_pending()
        assert raw.exists()
        store.config.remove_local_after_verification = True
        assert store.send_pending(force=True)[0]["status"] == "delivered"
        assert not raw.exists()
    finally:
        store.close()


@pytest.mark.parametrize(
    "name", ["../journal.sqlite3", "/etc/passwd", "bronze/../../file", "C:/x", "bronze\\x"]
)
def test_unsafe_paths_rejected(tmp_path, name):
    store, _, _ = setup_store(tmp_path)
    try:
        with pytest.raises(d.DeliveryError, match="unsafe_artifact_path"):
            store.stage("live", "s", [{"path": name, "role": "raw"}], {"revision": "v1"})
    finally:
        store.close()


def test_journal_cannot_be_removal_artifact_and_symlinks_rejected(tmp_path):
    store, raw, journal = setup_store(tmp_path)
    try:
        with pytest.raises(d.DeliveryError, match="protected_local_artifact"):
            store.stage(
                "live",
                "s",
                [{"path": "journal.sqlite3", "role": "raw", "remove": True}],
                {"revision": "v1"},
            )
        link = raw.parent / "link.json"
        link.symlink_to(journal)
        with pytest.raises(d.DeliveryError, match="symlink_artifact"):
            store.stage(
                "live", "s", [{"path": "bronze/aa/link.json", "role": "raw"}], {"revision": "v1"}
            )
        store.config.destination_root = str(store.root / "inside")
        with pytest.raises(d.DeliveryError, match="destination_overlaps_collector"):
            d.destination(store.config, store.root)
    finally:
        store.close()


def test_byte_limits_and_changed_source_never_remove_data(tmp_path):
    store, raw, _ = setup_store(tmp_path, max_bytes=2, remove_local_after_verification=True)
    try:
        with pytest.raises(d.DeliveryError, match="artifact_size_limit"):
            stage(store)
        store.config.max_bytes = 1000
        stage(store)
        raw.write_bytes(b"changed")
        assert store.send_pending()[0]["error"] == "local_artifact_changed"
        assert raw.exists()
        with pytest.raises(d.DeliveryError, match="immutable_local_artifact_changed"):
            stage(store)
    finally:
        store.close()


def test_changed_destination_cannot_use_missing_local_receipt(tmp_path):
    store, raw, _ = setup_store(tmp_path, remove_local_after_verification=True)
    stage(store)
    store.send_pending()
    store.close()
    config = store.config.model_copy(update={"destination_root": str(tmp_path / "new-master")})
    store = d.DeliveryStore(store.root, config)
    try:
        with pytest.raises(d.DeliveryError, match="missing_local_artifact"):
            stage(store)
    finally:
        store.close()


def test_snapshot_limits_and_read_only_status(tmp_path):
    store, _, _ = setup_store(tmp_path, max_bytes=10)
    try:
        with pytest.raises(d.DeliveryError, match="snapshot_byte_limit"):
            store.snapshot("live", "s", "v1", "observations", [{"large": "x" * 20}])
        assert not list(store.root.rglob("*.tmp"))
        assert d.read_delivery_status(store.root, store.config)["counts"] == {}
    finally:
        store.close()


def test_transport_timeout_does_not_publish_partial_file(tmp_path):
    store, raw, _ = setup_store(tmp_path)
    backend = d.destination(store.config, store.root)
    try:
        with raw.open("rb") as stream, pytest.raises(d.DeliveryError, match="delivery_timeout"):
            d.put_verified(
                backend, "raw/object", stream, "a" * 64, raw.stat().st_size, time.monotonic() - 1
            )
        assert not list(Path(store.config.destination_root).rglob("*.incomplete"))
        assert not backend.exists("raw/object")
    finally:
        backend.close()
        store.close()


@pytest.mark.parametrize(
    "settings",
    [
        {"transport": "sftp", "destination_root": "/data"},
        {"collector_id": "../unsafe"},
        {"max_attempt_seconds": float("inf")},
    ],
)
def test_invalid_delivery_config(settings):
    with pytest.raises(ValidationError):
        d.DeliveryConfig.model_validate(
            {"collector_id": "edge", "transport": "local", "destination_root": "/data", **settings}
        )


def test_master_reader_requires_commit_marker_and_verifies_every_object(tmp_path):
    store, raw, _ = setup_store(tmp_path)
    try:
        stage(store)
        store.send_pending()
        master = Path(store.config.destination_root)
        manifest = next(master.rglob("*.manifest.json"))
        relative = manifest.relative_to(master).as_posix()
        result = d.read_received_session(master, relative)
        assert (master / result["files"][0]["object_path"]).read_bytes() == raw.read_bytes()
        receipt = next(master.rglob("*.received.json"))
        saved = receipt.read_bytes()
        receipt.unlink()
        with pytest.raises(FileNotFoundError):
            d.read_received_session(master, relative)
        receipt.write_bytes(saved)
        (master / result["files"][0]["object_path"]).write_bytes(b"corruption")
        with pytest.raises(d.DeliveryError, match="central_checksum_conflict"):
            d.read_received_session(master, relative)
    finally:
        store.close()

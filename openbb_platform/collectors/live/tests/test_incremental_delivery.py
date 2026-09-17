import json
from pathlib import Path

from openbb_collector_core import delivery as core
from openbb_collector_core.delivery import DeliveryConfig, read_received_session

from dq_live_market_data_collector.delivery import deliver


def test_batch_retry_overlap_and_local_retention(rig, tmp_path, monkeypatch):
    config, clock, provider, journal, service = rig
    config.delivery = DeliveryConfig(
        collector_id="edge", transport="local", destination_root=str(tmp_path / "master"),
        local_retention="forever", incremental=True, incremental_max_attempts=1,
    )
    root = Path(config.delivery.destination_root)
    original = core.put_verified

    def fail_receipt(backend, name, *args):
        if name.endswith(".received.json"):
            raise OSError("simulated transfer failure")
        return original(backend, name, *args)

    monkeypatch.setattr(core, "put_verified", fail_receipt)
    service.step()
    assert not list(root.rglob("*.received.json"))
    assert list(journal.root.glob("bronze/*/*.json"))
    monkeypatch.setattr(core, "put_verified", original)
    deliver(config, journal, force=True)
    deliver(config, journal, force=True)
    batches = list(root.glob("remote_collectors/*/batches/*/*/*.manifest.json"))
    assert len(batches) == 2

    def records(path):
        manifest = json.loads(path.read_text())
        item = next(x for x in manifest["files"] if x["role"] == "observations")
        sha = item["sha256"]
        obj = root / "remote_collectors/edge/objects/sha256" / sha[:2] / sha
        return {json.loads(x)["record_id"] for x in obj.read_text().splitlines()}

    identities = set()
    for path in batches:
        read_received_session(root, path.relative_to(root).as_posix())
        assert json.loads(path.read_text())["schema_version"] == 2
        identities.update(records(path))
    assert len(identities) == 2
    deliver(config, journal, force=True)
    assert len(list(root.glob("remote_collectors/*/batches/*/*/*.manifest.json"))) == 2
    clock.advance(60)
    service.step()
    final = next(root.glob("remote_collectors/*/sessions/*/*/*.manifest.json"))
    assert records(final) == identities
    assert list(journal.root.glob("bronze/*/*.json"))
    heartbeat = json.loads((root / "remote_collectors/edge/status/heartbeat.json").read_text())
    assert heartbeat["local_files_retained"] is True
    assert heartbeat["collections"]["quotes"]["session_state"] == "closed"


def test_batches_wait_for_local_exports(rig, tmp_path):
    config, clock, provider, journal, service = rig
    service.step()
    config.delivery = DeliveryConfig(
        collector_id="edge", transport="local", destination_root=str(tmp_path / "master"),
        local_retention="forever", incremental=True,
    )
    journal.db.execute("UPDATE observations SET exported=0")
    journal.db.commit()
    deliver(config, journal, force=True)
    assert not list((tmp_path / "master").rglob("*.manifest.json"))
    journal.db.execute("UPDATE observations SET exported=1")
    journal.db.commit()
    deliver(config, journal, force=True)
    assert len(list((tmp_path / "master").rglob("*.manifest.json"))) == 1

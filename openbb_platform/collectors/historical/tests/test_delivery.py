import json

from openbb_collector_core.delivery import DeliveryConfig, read_delivery_status

from dq_historical_market_data_collector.config import load_config
from dq_historical_market_data_collector.delivery import deliver
from dq_historical_market_data_collector.runtime import Collector
from dq_historical_market_data_collector.storage import Journal


def test_completed_session_moves_and_resume_keeps_checkpoint(config, client, now, tmp_path):
    original_hash = config.fingerprint()
    config.delivery = DeliveryConfig(
        collector_id="historical-edge",
        transport="local",
        destination_root=str(tmp_path / "master"),
        remove_local_after_verification=True,
    )
    assert config.fingerprint() == original_hash
    with Journal(config.storage) as journal:
        collector = Collector(config, journal, client=client, clock=lambda: now)
        report = collector.collect()
        assert report["status"] == "complete"
        assert read_delivery_status(journal.root, config.delivery)["counts"] == {"delivered": 1}
        assert not list(journal.root.glob("bronze/*/*.json"))
        assert not list(journal.root.glob("records/*"))
        assert journal.db.execute("SELECT COUNT(*) FROM observations").fetchone()[0] == 3
        collector.collect()
        assert len(client.calls) == 1
        assert read_delivery_status(journal.root, config.delivery)["counts"] == {"delivered": 2}
    manifests = [json.loads(p.read_bytes()) for p in (tmp_path / "master").rglob("*.manifest.json")]
    assert all(m["metadata"]["session_status"] == "complete" for m in manifests)
    assert any(
        {f["role"] for f in m["files"]} == {"raw", "observations", "attempts", "report", "export"}
        for m in manifests
    )


def test_quarantine_evidence_is_transferred(config, client, now, tmp_path):
    config.delivery = DeliveryConfig(
        collector_id="edge",
        transport="local",
        destination_root=str(tmp_path / "master"),
        remove_local_after_verification=True,
    )
    client.rows[0]["high"] = -100
    with Journal(config.storage) as journal:
        Collector(config, journal, client=client, clock=lambda: now).collect()
        assert journal.db.execute("SELECT SUM(rejected) FROM attempts").fetchone()[0] > 0
        assert not list(journal.root.glob("quarantine/*.json"))
    manifest = json.loads(next((tmp_path / "master").rglob("*.manifest.json")).read_bytes())
    assert any(item["role"] == "quarantine" for item in manifest["files"])


def test_transfer_failure_does_not_change_collection_success(config, client, now, tmp_path):
    config.delivery = DeliveryConfig(
        collector_id="edge",
        transport="local",
        destination_root=str(config.storage.root / "overlap"),
        remove_local_after_verification=True,
    )
    with Journal(config.storage) as journal:
        report = Collector(config, journal, client=client, clock=lambda: now).collect()
        assert report["status"] == "complete"
        assert read_delivery_status(journal.root, config.delivery)["counts"] == {"pending": 1}
        assert list(journal.root.glob("bronze/*/*.json"))
        assert deliver(config, journal, force=True)["events"][0]["status"] == "pending"


def test_config_delivery_paths_relative_to_yaml(config_dict, tmp_path):
    import yaml

    config_dict["delivery"] = {
        "collector_id": "edge",
        "transport": "local",
        "destination_root": "../master",
    }
    path = tmp_path / "config/config.yaml"
    path.parent.mkdir()
    path.write_text(yaml.safe_dump(config_dict))
    assert load_config(path).delivery.destination_root == str(tmp_path / "master")


def test_archived_response_delivered_even_if_normalization_crashes(
    config, client, now, tmp_path, monkeypatch
):
    from dq_historical_market_data_collector import runtime

    config.delivery = DeliveryConfig(
        collector_id="edge",
        transport="local",
        destination_root=str(tmp_path / "master"),
        remove_local_after_verification=True,
    )

    def fail(*args):
        raise ValueError("normalization failed")

    monkeypatch.setattr(runtime, "normalize", fail)
    with Journal(config.storage) as journal:
        result = Collector(config, journal, client=client, clock=lambda: now).collect()
        assert result["status"] == "storage_or_runtime_error"
        assert journal.db.execute("SELECT COUNT(*) FROM attempts").fetchone()[0] == 0
        assert not list(journal.root.glob("bronze/*/*.json"))
    manifest = json.loads(next((tmp_path / "master").rglob("*.manifest.json")).read_bytes())
    assert any(item["role"] == "raw" for item in manifest["files"])

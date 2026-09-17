import json

from openbb_collector_core.delivery import DeliveryConfig

from dq_historical_market_data_collector.runtime import Collector
from dq_historical_market_data_collector.storage import Journal


def test_historical_attempt_batch_precedes_closed_session(config, client, now, tmp_path):
    config.delivery = DeliveryConfig(
        collector_id="edge", transport="local", destination_root=str(tmp_path / "master"),
        local_retention="forever", incremental=True,
    )
    with Journal(config.storage) as journal:
        result = Collector(config, journal, client=client, clock=lambda: now).collect()
        assert result["status"] == "complete"
        assert journal.db.execute("SELECT count(*) FROM observations").fetchone()[0] == 3
        assert list(journal.root.glob("bronze/*/*.json"))
    manifests = list((tmp_path / "master").rglob("*.manifest.json"))
    batch = next(json.loads(p.read_text()) for p in manifests if "/batches/" in str(p))
    final = next(json.loads(p.read_text()) for p in manifests if "/sessions/" in str(p))
    assert batch["metadata"]["session_status"] == "running"
    assert batch["metadata"]["observation_count"] == 3
    assert batch["schema_version"] == 2 and final["schema_version"] == 1

    def records(manifest):
        item = next(x for x in manifest["files"] if x["role"] == "observations")
        sha = item["sha256"]
        obj = tmp_path / "master/remote_collectors/edge/objects/sha256" / sha[:2] / sha
        return {json.loads(x)["record_id"] for x in obj.read_text().splitlines()}

    assert records(batch) == records(final)

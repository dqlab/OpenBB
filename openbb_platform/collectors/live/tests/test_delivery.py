import json

from openbb_collector_core.delivery import DeliveryConfig, read_delivery_status

from dq_live_market_data_collector.service import Collector


def enable(config, tmp_path):
    previous = config.fingerprint()
    config.delivery = DeliveryConfig(
        collector_id="live-edge",
        transport="local",
        destination_root=str(tmp_path / "master"),
        remove_local_after_verification=True,
    )
    assert previous == config.fingerprint()


def test_only_closed_session_moves_and_journal_survives(rig, tmp_path):
    config, clock, provider, journal, service = rig
    enable(config, tmp_path)
    service.step()
    assert list(journal.root.glob("bronze/*/*.json"))
    assert not list((tmp_path / "master").rglob("*.manifest.json"))
    clock.advance(60)
    service.step()
    assert read_delivery_status(journal.root, config.delivery)["counts"] == {"delivered": 1}
    assert not list(journal.root.glob("bronze/*/*.json"))
    assert journal.db.execute("SELECT COUNT(*) FROM observations").fetchone()[0] == 2
    manifest = json.loads(next((tmp_path / "master").rglob("*.manifest.json")).read_bytes())
    assert manifest["metadata"]["session_status"] == "completed"
    assert {item["role"] for item in manifest["files"]} == {
        "raw",
        "observations",
        "attempts",
        "report",
    }
    service.deliver_pending()
    assert len(list((tmp_path / "master").rglob("*.manifest.json"))) == 1


def test_interrupted_session_moves_then_resumes_same_window(rig, tmp_path):
    config, clock, provider, journal, service = rig
    enable(config, tmp_path)
    service.step()
    service.stop.set()  # Timed run and SIGTERM still attempt delivery on shutdown.
    service.shutdown("bounded_run")
    assert not list(journal.root.glob("bronze/*/*.json"))
    assert read_delivery_status(journal.root, config.delivery)["counts"] == {"delivered": 1}
    clock.advance(10)
    resumed = Collector(config, journal, provider=provider, clock=clock)
    resumed.step()
    resumed.shutdown("bounded_run")
    assert read_delivery_status(journal.root, config.delivery)["counts"] == {"delivered": 2}
    assert journal.db.execute("SELECT COUNT(*) FROM observations").fetchone()[0] == 4
    assert not list(journal.root.glob("bronze/*/*.json"))
    assert len(list((tmp_path / "master").rglob("*.manifest.json"))) == 2


def test_closed_session_waits_for_export_recovery(raw_config, tmp_path, monkeypatch):
    from conftest import Clock, FakeProvider

    from dq_live_market_data_collector.config import CollectorConfig
    from dq_live_market_data_collector.storage import Journal

    raw_config["storage"]["format"] = "jsonl"
    config = CollectorConfig.model_validate(raw_config)
    enable(config, tmp_path)
    clock = Clock()
    with Journal(config.storage) as journal:
        service = Collector(config, journal, provider=FakeProvider(clock), clock=clock)
        original = journal.export_pending

        def fail(*args):
            raise OSError("unavailable storage")

        monkeypatch.setattr(journal, "export_pending", fail)
        service.step()
        clock.advance(60)
        service.step()
        assert not list((tmp_path / "master").rglob("*.manifest.json"))
        assert list(journal.root.glob("bronze/*/*.json"))
        monkeypatch.setattr(journal, "export_pending", original)
        service.step()
        assert read_delivery_status(journal.root, config.delivery)["counts"] == {"delivered": 1}
        assert not list(journal.root.glob("observations/*/*.jsonl"))
        assert journal.db.execute("SELECT SUM(exported) FROM observations").fetchone()[0] == 2


def test_transfer_commands_do_not_require_provider_or_gateway(rig, tmp_path, monkeypatch, capsys):
    import yaml

    from dq_live_market_data_collector import cli

    config, clock, provider, journal, service = rig
    enable(config, tmp_path)
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(config.model_dump(mode="json")))
    service.step()
    service.shutdown("bounded_run")

    def fail(*args):
        raise AssertionError("provider validation must not run")

    monkeypatch.setattr(cli, "validate_providers", fail)
    assert cli.main(["delivery-status", "--config", str(path)]) == 0
    assert json.loads(capsys.readouterr().out)["counts"] == {"delivered": 1}
    journal.close()
    assert cli.main(["deliver", "--config", str(path)]) == 0
    assert json.loads(capsys.readouterr().out)["counts"] == {"delivered": 1}


def test_local_week_retention_cleanup_on_later_collector_pass(rig, tmp_path, monkeypatch):
    from openbb_collector_core import delivery as core_delivery

    config, clock, provider, journal, service = rig
    config.delivery = DeliveryConfig(
        collector_id="retained-live",
        transport="local",
        destination_root=str(tmp_path / "master"),
        local_retention="1w",
    )
    monkeypatch.setattr(core_delivery.time, "time", lambda: clock().timestamp())
    service.step()
    clock.advance(60)
    service.step()
    assert list(journal.root.glob("bronze/*/*.json"))
    status = read_delivery_status(journal.root, config.delivery)
    assert status["counts"] == {"delivered": 1}
    assert status["jobs"][0]["local_state"] == "retained"
    clock.advance(604799)
    service.deliver_pending()
    assert list(journal.root.glob("bronze/*/*.json"))
    clock.advance(1)
    service.deliver_pending()
    assert not list(journal.root.glob("bronze/*/*.json"))
    assert journal.db.execute("SELECT COUNT(*) FROM observations").fetchone()[0] == 2
    assert len(provider.calls) == 2

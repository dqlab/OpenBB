"""Offline data-plane acceptance using the ordinary collector-core protocol."""

import json
import signal
from pathlib import Path
from threading import Event

import pytest
from openbb_collector_core.delivery import DeliveryError, DeliveryStore, read_received_session

from dq_historical_market_data_collector.etf_options import cli, scheduled
from dq_historical_market_data_collector.etf_options.config import load_config
from dq_historical_market_data_collector.etf_options.delivery import deliver
from dq_historical_market_data_collector.etf_options.storage import Journal

from .test_etf_options import DAY, FakeClient, config, contract, quote, run


def settings(tmp_path, **extra):
    return config(
        tmp_path,
        delivery={
            "collector_id": "openbb-etf-options",
            "transport": "local",
            "destination_root": str(tmp_path / "central"),
        },
        **extra,
    )


def received(cfg):
    paths = sorted(
        Path(cfg.delivery.destination_root).glob("remote_collectors/*/sessions/*/*/*.manifest.json")
    )
    return [
        read_received_session(
            Path(cfg.delivery.destination_root),
            p.relative_to(cfg.delivery.destination_root).as_posix(),
        )
        for p in paths
    ]


def rows(cfg, manifest, role):
    return [
        json.loads(line)
        for item in manifest["files"]
        if item["role"] == role
        for line in (Path(cfg.delivery.destination_root) / item["object_path"])
        .read_text()
        .splitlines()
    ]


def test_full_underlying_not_request_block_and_lossless_diagnostics(tmp_path):
    cfg = settings(tmp_path, block_size=1)
    master = [contract(), contract("10000002")]
    basket = [quote(), quote("10000002", source_time="2026-10-14T00:00:00+08:00", bid=0)]
    with Journal(cfg.root) as journal:
        assert (
            run(cfg, journal, FakeClient(master, [[basket[0]], [basket[1]]]))["status"]
            == "complete"
        )
        capture = dict(journal.db.execute("SELECT * FROM captures").fetchone())
        report = deliver(cfg, journal, day=DAY.isoformat())
    assert report["status"] == "complete" and report["counts"] == {"delivered": 1}
    (manifest,) = received(cfg)
    assert manifest["schema_version"] == 1 and manifest["engine"] == "live"
    assert manifest["metadata"]["observation_schema"] == "live_v1"
    records = rows(cfg, manifest, "observations")
    assert len(records) == 2
    assert {r["available_at"] for r in records} == {capture["observed_at"]}
    assert len({r["poll_id"] for r in records}) == 1
    assert len({r["raw_hash"] for r in records}) == 1
    assert records[0]["values"]["contract_unit"] is None
    assert "unknown_contract_unit" in records[0]["quality_flags"]
    assert records[1]["event_time"] is None and not records[1]["pricing_eligible"]
    assert records[1]["values"]["bid"] == 0
    assert records[0]["instrument_id"] == "SSE:10000001"
    assert records[0]["currency"] == "CNY" and records[0]["provider"] == "akshare"
    assert records[0]["collection_id"] == "sse_etf_chain"
    assert records[0]["source_revision"] == records[0]["content_hash"]
    (raw_item,) = [f for f in manifest["files"] if f["role"] == "raw"]
    raw = json.loads((Path(cfg.delivery.destination_root) / raw_item["object_path"]).read_text())
    assert raw["source"] == "akshare" and len(raw["rows"]) == 2
    assert raw["envelope_kind"] == "collector_constructed_underlying_snapshot"
    assert raw["observed_at"] == capture["observed_at"]
    assert raw["rows"][1]["quote"]["source_time"] == basket[1]["source_time"]
    (attempt,) = rows(cfg, manifest, "attempts")
    assert attempt["attempts"][0]["input_rows"] == 2
    assert attempt["attempts"][0]["status"] == "accepted"
    assert attempt["attempts"][0]["rejected"] == 0
    assert len([f for f in manifest["files"] if f["role"] == "export"]) == 3
    assert all("sqlite" not in f["path"] and f["remove"] is False for f in manifest["files"])


def test_reopen_retry_does_not_duplicate_session_or_rebuild_artifacts(tmp_path, monkeypatch):
    cfg = settings(tmp_path)
    with Journal(cfg.root) as journal:
        run(cfg, journal, FakeClient())
        assert deliver(cfg, journal, send=False)["status"] == "pending"
    with Journal(cfg.root) as journal:
        monkeypatch.setattr(
            DeliveryStore, "snapshot", lambda *a, **kw: pytest.fail("already staged")
        )
        assert deliver(cfg, journal, force=True)["status"] == "complete"
        assert deliver(cfg, journal)["counts"] == {"delivered": 1}
    assert len(received(cfg)) == 1


def test_exclude_failed_capture_preserve_good_snapshot(tmp_path):
    cfg = settings(tmp_path)
    with Journal(cfg.root) as journal:
        run(cfg, journal, FakeClient())
        run(cfg, journal, FakeClient(baskets=[[quote(source_time="2026-10-13T15:00:00+08:00")]]))
        assert deliver(cfg, journal)["counts"] == {"delivered": 1}
        assert journal.db.execute("SELECT count(*) FROM current_quotes").fetchone()[0] == 1
    assert len(received(cfg)) == 1


def test_membership_mismatch_does_not_stage(tmp_path):
    cfg = settings(tmp_path)
    with Journal(cfg.root) as journal:
        run(cfg, journal, FakeClient())
        journal.db.execute("UPDATE captures SET quotes_json='[]'")
        report = deliver(cfg, journal)
    assert report["status"] == "failed"
    assert report["counts"] == {} and received(cfg) == []


def test_destination_failure_retains_pending_outbox_then_retry(tmp_path):
    cfg = settings(tmp_path)
    target = Path(cfg.delivery.destination_root)
    target.write_text("not a directory")
    with Journal(cfg.root) as journal:
        run(cfg, journal, FakeClient())
        assert deliver(cfg, journal)["status"] == "failed"
    target.unlink()
    with Journal(cfg.root) as journal:
        assert deliver(cfg, journal, force=True)["status"] == "complete"
    assert len(received(cfg)) == 1


def test_config_delivery_optional_and_not_acquisition_identity(tmp_path):
    plain, cfg = config(tmp_path), settings(tmp_path)
    assert plain.delivery is None and plain.identity() == cfg.identity()
    path = tmp_path / "config.yaml"
    path.write_text(
        "root: state\ncalendar_file: calendar.json\ndelivery:\n"
        "  collector_id: openbb-etf-options\n  transport: local\n"
        "  destination_root: central\n"
    )
    assert load_config(path).delivery.destination_root == str(tmp_path / "central")


def test_cli_delivers_old_date_without_calendar_or_provider(tmp_path, monkeypatch, capsys):
    cfg = settings(tmp_path)
    with Journal(cfg.root) as journal:
        run(cfg, journal, FakeClient())
    path = tmp_path / "config.yaml"
    value = cfg.model_dump(mode="json")
    value.update(not_before="15:10", quote_not_before="14:57")
    path.write_text(json.dumps(value))
    monkeypatch.setattr(cli, "Collector", lambda *a, **kw: pytest.fail("network collector"))
    assert cli.main(["deliver", "--config", str(path), "--date", DAY.isoformat()]) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["command"] == "deliver" and report["status"] == "complete"
    assert report["central_publication_status"] == "not_requested"
    assert cli.main(["delivery-status", "--config", str(path)]) == 0
    assert json.loads(capsys.readouterr().out)["counts"] == {"delivered": 1}


def test_scheduler_delivery_failure_does_not_erase_acquisition_success(tmp_path, monkeypatch):
    calls = []

    def invoke(command, config_path, extra):
        calls.append((command, extra))
        return (
            (1, {"command": command, "status": "failed"})
            if command == "deliver"
            else (0, {"command": command, "status": "complete"})
        )

    monkeypatch.setattr(scheduled, "invoke", invoke)
    code, report = scheduled.one_pass(
        tmp_path / "cfg.yaml", ["--date", DAY.isoformat(), "--no-resume"]
    )
    assert code == 1 and report["status"] == "failed"
    assert report["collection"]["status"] == "complete"
    assert report["delivery"]["status"] == "failed"
    assert calls[-1] == ("deliver", ["--date", DAY.isoformat()])


def test_resumed_rows_preserve_prior_run_source_evidence(tmp_path):
    cfg = settings(tmp_path, block_size=1)
    master = [contract(), contract("10000002")]
    with Journal(cfg.root) as journal:
        assert run(cfg, journal, FakeClient(master, [[quote()], []]))["status"] == "failed"
        original = journal.db.execute(
            "SELECT raw_path FROM attempts WHERE model='AkshareEtfOptionQuotes' ORDER BY rowid"
        ).fetchone()[0]
        assert run(cfg, journal, FakeClient(master, [[quote("10000002")]]))["resumed_rows"] == 1
        assert deliver(cfg, journal)["status"] == "complete"
    (manifest,) = received(cfg)
    record = rows(cfg, manifest, "observations")[0]
    assert original in {x["original_path"] for x in record["values"]["source_evidence"]}


def test_platform_archive_attempts_and_real_publisher(tmp_path):
    archive = pytest.importorskip("dq_quant_invest_data.data.market.archive")
    platform_config = pytest.importorskip("dq_quant_invest_data.data.market.config")
    publisher = pytest.importorskip("dq_quant_invest_data.data.market.publisher")
    attempt_module = pytest.importorskip("dq_quant_invest_data.data.market.attempts")
    cfg = settings(tmp_path, block_size=1)
    with Journal(cfg.root) as journal:
        run(
            cfg,
            journal,
            FakeClient([contract(), contract("10000002")], [[quote()], [quote("10000002")]]),
        )
        assert deliver(cfg, journal)["status"] == "complete"
    central = platform_config.MarketConfig(
        data_root=Path(cfg.delivery.destination_root),
        archive_relative_path="remote_collectors",
        datasets=[
            dict(
                dataset_id="sse_etf_chain",
                collector_id="openbb-etf-options",
                collection_id="sse_etf_chain",
                provider="akshare",
                kind="chain",
            )
        ],
    )
    (manifest,) = archive.discover(central)
    delivery = archive.verify_delivery(central, manifest)
    attempts, _ = attempt_module.attempts(delivery, 100)
    assert len(attempts) == 1 and attempts[0]["accepted"] == 2
    actual = publisher.MarketPublisher(central)
    report = actual.sync()
    assert report["failed"] == 0 and report["processed"] == 1, report
    with actual.catalog.connection() as con:
        assert con.execute("SELECT count(*) FROM market_data.source_record").fetchone()[0] == 2
        assert (
            con.execute(
                "SELECT count(*) FROM market_data.source_record WHERE gold_eligible"
            ).fetchone()[0]
            == 0
        )
        assert con.execute(
            "SELECT response_complete,input_rows FROM market_data.capture"
        ).fetchone() == (True, 2)
    assert actual.sync()["processed"] == 0


def test_underlying_membership_and_partial_exchange_status(tmp_path):
    cfg = settings(tmp_path, exchanges={"SSE": {"source": "sina"}, "SZSE": {"source": "official"}})
    master = [contract(), contract("10000002", underlying="510300")]
    with Journal(cfg.root) as journal:
        report = run(
            cfg, journal, FakeClient(master, [[quote(), quote("10000002", underlying="510300")]])
        )
        assert report["status"] == "failed" and report["row_count"] == 2
        assert deliver(cfg, journal)["status"] == "complete"
    (manifest,) = received(cfg)
    assert manifest["metadata"]["acquisition_status"] == "failed"
    records = rows(cfg, manifest, "observations")
    assert len({r["poll_id"] for r in records}) == 2
    assert len({r["raw_hash"] for r in records}) == 2
    (report,) = rows(cfg, manifest, "report")
    assert report["acquisition_report"]["response_complete"] is False
    assert report["response_complete"] is True and report["exchange"] == "SSE"


def test_szse_route_without_occ_or_conid(tmp_path):
    cfg = settings(tmp_path, exchanges={"SZSE": {"source": "official"}})
    meta = contract(
        "90000001",
        exchange="SZSE",
        underlying="159919",
        metadata_source="official",
        source_date=DAY.isoformat(),
        contract_unit=10000,
    )
    with Journal(cfg.root) as journal:
        run(cfg, journal, FakeClient([meta], [[quote("90000001", underlying="159919")]]))
        assert deliver(cfg, journal)["status"] == "complete"
    (manifest,) = received(cfg)
    (record,) = rows(cfg, manifest, "observations")
    assert record["instrument_id"] == "SZSE:90000001"
    assert record["collection_id"] == "szse_etf_chain" and record.get("con_id") is None
    assert record["values"]["contract_unit"] == 10000
    assert record["pricing_eligible"] is True


def test_content_revision_excludes_acquisition_timestamps(tmp_path):
    cfg = settings(tmp_path)
    with Journal(cfg.root) as journal:
        for fetched in ("2026-10-14T07:19:00Z", "2026-10-14T07:20:00Z"):
            run(cfg, journal, FakeClient(baskets=[[quote(fetched_at=fetched)]]))
        run(cfg, journal, FakeClient(baskets=[[quote(last=0.17)]]))
        assert deliver(cfg, journal)["counts"] == {"delivered": 3}
    records = [rows(cfg, m, "observations")[0] for m in received(cfg)]
    original = [r for r in records if r["values"]["last_price"] == 0.15]
    changed = [r for r in records if r["values"]["last_price"] == 0.17][0]
    assert original[0]["content_hash"] == original[1]["content_hash"]
    assert original[0]["source_revision"] != changed["source_revision"]


def test_crash_before_claim_reuses_files_and_preserves_acquisition(tmp_path, monkeypatch):
    cfg = settings(tmp_path)
    stage = DeliveryStore.stage
    monkeypatch.setattr(
        DeliveryStore,
        "stage",
        lambda *a, **kw: (_ for _ in ()).throw(DeliveryError("injected_seal_failure")),
    )
    with Journal(cfg.root) as journal:
        run(cfg, journal, FakeClient())
        assert deliver(cfg, journal)["status"] == "failed"
        assert journal.db.execute("SELECT count(*) FROM current_quotes").fetchone()[0] == 1
    before = {p: p.read_bytes() for p in cfg.root.glob("delivery/snapshots/**/*.jsonl")}
    monkeypatch.setattr(DeliveryStore, "stage", stage)
    with Journal(cfg.root) as journal:
        assert deliver(cfg, journal)["counts"] == {"delivered": 1}
    assert all(p.read_bytes() == payload for p, payload in before.items())
    assert len(received(cfg)) == 1


def test_cancel_after_claim_leaves_pending_for_reopen(tmp_path, monkeypatch):
    cfg, stop = settings(tmp_path), Event()
    stage = DeliveryStore.stage

    def claim(*args, **kwargs):
        result = stage(*args, **kwargs)
        stop.set()
        return result

    monkeypatch.setattr(DeliveryStore, "stage", claim)
    with Journal(cfg.root) as journal:
        run(cfg, journal, FakeClient())
        with pytest.raises(InterruptedError):
            deliver(cfg, journal, stop=stop)
    assert received(cfg) == []
    monkeypatch.setattr(DeliveryStore, "stage", stage)
    with Journal(cfg.root) as journal:
        assert deliver(cfg, journal, pending_only=True)["counts"] == {"delivered": 1}


def test_pending_only_does_not_stage_new_acquisitions(tmp_path):
    cfg = settings(tmp_path)
    with Journal(cfg.root) as journal:
        run(cfg, journal, FakeClient())
        assert deliver(cfg, journal, pending_only=True)["counts"] == {}
        assert deliver(cfg, journal)["counts"] == {"delivered": 1}


def test_scheduler_pending_flush_keeps_collection_failure(tmp_path, monkeypatch):
    cfg = settings(tmp_path)
    monkeypatch.setattr(scheduled, "load_config", lambda _: cfg)
    calls = []

    def invoke(command, config_path, extra):
        calls.append((command, extra))
        return (
            (1, {"status": "failed", "command": command})
            if command == "collect"
            else (0, {"status": "complete", "command": command})
        )

    monkeypatch.setattr(scheduled, "invoke", invoke)
    code, report = scheduled.one_pass(tmp_path / "config.yaml")
    assert code == 1 and report["status"] == "failed"
    assert report["delivery"]["status"] == "complete"
    assert calls[-1] == ("deliver", ["--pending-only"])


def test_scheduler_signal_does_not_accept_successful_child(tmp_path, monkeypatch):
    handlers, sent = {}, []

    def handler(signum, value):
        handlers[signum] = value

    class Child:
        returncode = 0

        def poll(self):
            return None

        def send_signal(self, sig):
            sent.append(sig)

        def communicate(self):
            handlers[signal.SIGTERM](signal.SIGTERM, None)
            return '{"command":"deliver","status":"complete"}', ""

    monkeypatch.setattr(scheduled.signal, "signal", handler)
    monkeypatch.setattr(scheduled.subprocess, "Popen", lambda *a, **kw: Child())
    code, report = scheduled.invoke("deliver", tmp_path / "config.yaml", [])
    assert code == 4 and report["status"] == "interrupted"
    assert sent == [signal.SIGTERM]

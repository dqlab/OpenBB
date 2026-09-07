import json
import multiprocessing as mp
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

import pytest
import yaml
from openbb_collector_core import Response
from openbb_collector_core import process as providers

from dq_live_market_data_collector.cli import main
from dq_live_market_data_collector.config import AssetType, Instrument, Source
from dq_live_market_data_collector.providers import (
    OpenBBClient,
    ProviderFailure,
    request_parameters,
)
from dq_live_market_data_collector.quality import normalize


def sleeping_worker(connection, payload):
    connection.recv()
    time.sleep(30)


def failed_worker(connection, payload):
    connection.recv()
    connection.close()


def fake_dispatcher(rows, warning_count=0):
    class Dispatcher:
        def __init__(self, source):
            pass

        def fetch(self, params):
            return Response(rows(params), warning_count)

        def close(self):
            pass

    return Dispatcher


def fork_client():
    if "fork" not in mp.get_all_start_methods():
        pytest.skip("Offline worker injection uses WSL/POSIX fork")
    client = OpenBBClient()
    client._context = mp.get_context("fork")
    return client


def test_worker_timeout_is_killable_and_next_request_recovers(monkeypatch):
    client = fork_client()
    monkeypatch.setattr(providers, "_worker", sleeping_worker)
    source = Source(model="EquityQuote", provider="yfinance", timeout_seconds=0.1)
    instrument = Instrument(symbol="AAPL", asset_type="stock", currency="USD")
    started = time.monotonic()
    with pytest.raises(ProviderFailure, match="timeout"):
        client.fetch("primary", source, instrument)
    assert time.monotonic() - started < 5
    assert not client._workers
    monkeypatch.setattr(providers, "_worker", failed_worker)
    with pytest.raises(ProviderFailure, match="provider_process_failed"):
        client.fetch("primary", source, instrument)
    client.close()


def test_real_worker_dispatches_standard_openbb_results_and_enforces_bound(monkeypatch):
    monkeypatch.setattr(
        providers,
        "ProviderDispatcher",
        fake_dispatcher(lambda params: [{"symbol": params["symbol"], "last_price": 12.5}], 2),
    )
    client = fork_client()
    source = Source(model="EquityQuote", provider="yfinance", timeout_seconds=5)
    instrument = Instrument(symbol="AAPL", asset_type="stock", currency="USD")
    try:
        response = client.fetch("primary", source, instrument)
        assert response.rows == [{"symbol": "AAPL", "last_price": 12.5}]
        assert response.warning_count == 2
        pid = client._workers["primary"].process.pid
        client.fetch("primary", source, instrument)
        assert client._workers["primary"].process.pid == pid
    finally:
        client.close()
    monkeypatch.setattr(
        providers, "ProviderDispatcher", fake_dispatcher(lambda _: [{"symbol": "AAPL"}] * 3)
    )
    client = fork_client()
    source.max_rows = 2
    try:
        with pytest.raises(ProviderFailure, match="response_row_limit"):
            client.fetch("primary", source, instrument)
    finally:
        client.close()


@pytest.mark.parametrize("asset_type", AssetType.__args__)
def test_configured_identity_contract(raw_config, asset_type):
    from dq_live_market_data_collector.config import Collection

    source = Source(
        provider="configured_feed",
        model="CustomQuote",
        timestamp_semantics="provider_receive",
        field_map={"last_price": "last"},
        timestamp_field="timestamp",
    )
    instrument = Instrument(symbol="CONTRACT", asset_type=asset_type, currency="USD", con_id=123456)
    now = datetime(2026, 9, 7, 13, 30, tzinfo=UTC)
    row = {
        "symbol": "CONTRACT",
        "asset_type": asset_type,
        "con_id": 123456,
        "currency": "USD",
        "last": 100,
        "bid": 99,
        "ask": 101,
        "timestamp": now.isoformat(),
    }
    arguments = dict(
        source_id="ib",
        source=source,
        instrument_id="contract",
        instrument=instrument,
        collection_id="quotes",
        collection=Collection.model_validate(raw_config["collections"]["quotes"]),
        session_id="session",
        poll_id="poll",
        received_at=now,
        raw_hash="raw",
    )
    record, reasons = normalize(row, **arguments)
    assert reasons == []
    assert record["values"]["last_price"] == 100
    assert record["event_time"] is None
    assert record["actual_feed_type"] == "unknown"
    row["con_id"] = 654321
    assert "contract_mismatch" in normalize(row, **arguments)[1]


def test_standard_source_symbol_mapping():
    source = Source(model="EquityQuote", provider="fmp")
    instrument = Instrument(
        symbol="BRK.B", asset_type="stock", currency="USD", source_symbols={"fmp": "BRK-B"}
    )
    assert request_parameters("fmp", source, instrument) == {"symbol": "BRK-B"}


def test_cli_validate_plan_and_status_are_offline(raw_config, tmp_path, capsys):
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(raw_config))
    assert main(["validate-config", "--config", str(path)]) == 0
    assert json.loads(capsys.readouterr().out)["valid"]
    assert main(["plan", "--config", str(path), "--date", "2026-09-07"]) == 0
    plan = json.loads(capsys.readouterr().out)
    assert plan["collections"][0]["start_utc"] == "2026-09-07T13:30:00+00:00"
    assert not Path(raw_config["storage"]["root"]).exists()
    assert main(["status", "--config", str(path)]) == 0
    status = json.loads(capsys.readouterr().out)
    assert status["sessions"] == []


def test_cli_validation_does_not_print_inline_secret(raw_config, tmp_path, capsys):
    raw_config["sources"]["primary"]["parameters"] = {"api_key": "never-print-this-value"}
    path = tmp_path / "bad.yaml"
    path.write_text(yaml.safe_dump(raw_config))
    assert main(["validate-config", "--config", str(path)]) == 2
    assert "never-print-this-value" not in capsys.readouterr().out


def test_stop_interrupts_provider_pacing_without_waiting_for_the_interval(monkeypatch):
    import threading

    client = OpenBBClient()
    source = Source(model="EquityQuote", provider="yfinance", min_interval_seconds=3600)
    instrument = Instrument(symbol="AAPL", asset_type="stock", currency="USD")
    client._last_call["primary"] = time.monotonic()
    timer = threading.Timer(0.05, client.stop.set)
    timer.start()
    started = time.monotonic()
    try:
        with pytest.raises(ProviderFailure, match="cancelled"):
            client.fetch("primary", source, instrument)
        assert time.monotonic() - started < 2
        assert not client._workers
    finally:
        timer.cancel()
        client.close()


def test_worker_response_bytes_are_bounded(monkeypatch):
    monkeypatch.setattr(
        providers,
        "ProviderDispatcher",
        fake_dispatcher(lambda _: [{"symbol": "AAPL", "name": "x" * 2000}]),
    )
    client = fork_client()
    source = Source(
        model="EquityQuote",
        provider="yfinance",
        max_response_bytes=1024,
        timeout_seconds=5,
    )
    instrument = Instrument(symbol="AAPL", asset_type="stock", currency="USD")
    try:
        with pytest.raises(ProviderFailure, match="response_byte_limit"):
            client.fetch("primary", source, instrument)
    finally:
        client.close()


def test_init_config_resource_is_valid_and_never_overwrites(tmp_path, capsys):
    from dq_live_market_data_collector.config import load_config

    path = tmp_path / "collector.yaml"
    assert main(["init-config", "--output", str(path)]) == 0
    config = load_config(path)
    assert config.instruments["spx"].asset_type == "index"
    before = path.read_bytes()
    assert main(["init-config", "--output", str(path)]) == 2
    assert path.read_bytes() == before


def test_imports_preserve_optional_dependency_boundary():
    import os
    import subprocess

    script = """
import importlib.util
import sys
import dq_live_market_data_collector.cli
if importlib.util.find_spec("dq_quant_invest_data"):
    import dq_quant_invest_data
for name in ("openbb", "openbb_ibkr", "ib_async", "duckdb", "pyarrow"):
    assert name not in sys.modules, name
"""
    package = Path(__file__).resolve().parents[1]
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=package,
        capture_output=True,
        text=True,
        check=False,
        env={**os.environ, "PYTHONPATH": str(package / "src")},
    )
    assert result.returncode == 0, result.stderr


def test_smoke_cli_fails_without_new_samples(raw_config, tmp_path, capsys, monkeypatch):
    from dq_live_market_data_collector.service import Collector

    monkeypatch.setattr(Collector, "run", lambda self, seconds: None)
    path = tmp_path / "smoke.yaml"
    path.write_text(yaml.safe_dump(raw_config))
    assert main(["smoke", "--config", str(path), "--require-source", "primary"]) == 1
    report = json.loads(capsys.readouterr().out)
    assert report["event"] == "live_acceptance"
    assert not report["passed"]
    assert report["new_records"] == 0
    assert main(["smoke", "--config", str(path), "--require-source", "typo"]) == 2

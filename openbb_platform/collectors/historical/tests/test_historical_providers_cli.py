from __future__ import annotations

import subprocess
import sys
import threading
import time
from datetime import date

import pytest
import yaml
from openbb_collector_core import process as providers

from dq_historical_market_data_collector.cli import main
from dq_historical_market_data_collector.planner import Chunk
from dq_historical_market_data_collector.providers import (
    OpenBBClient,
    ProviderFailure,
    request_parameters,
)


def hang_worker(connection, payload):
    time.sleep(30)


def oversized_worker(connection, payload):
    connection.recv()
    connection.send_bytes(b"x" * 2000)
    connection.close()


@pytest.mark.parametrize("cancel", [False, True])
def test_worker_timeout_and_cancellation_are_bounded(config, monkeypatch, cancel):
    monkeypatch.setattr(providers, "_worker", hang_worker)
    stop = threading.Event()
    client = OpenBBClient(stop)
    source = config.sources["yahoo"]
    source.timeout_seconds = 0.3 if not cancel else 30
    timer = threading.Timer(0.1, stop.set) if cancel else None
    if timer:
        timer.start()
    started = time.monotonic()
    try:
        with pytest.raises(ProviderFailure, match="cancelled" if cancel else "timeout"):
            client.fetch(
                "yahoo",
                source,
                config.instruments["aapl"],
                config.collections["daily"],
                Chunk("daily", "aapl", date(2026, 9, 1), date(2026, 9, 4)),
            )
        assert time.monotonic() - started < 4
        assert not client._workers
    finally:
        client.close()
        if timer:
            timer.cancel()


def test_worker_response_bytes_are_bounded(config, monkeypatch):
    monkeypatch.setattr(providers, "_worker", oversized_worker)
    client = OpenBBClient()
    config.sources["yahoo"].max_response_bytes = 1024
    try:
        with pytest.raises(ProviderFailure, match="provider_process_failed"):
            client.fetch(
                "yahoo",
                config.sources["yahoo"],
                config.instruments["aapl"],
                config.collections["daily"],
                Chunk("daily", "aapl", date(2026, 9, 1), date(2026, 9, 4)),
            )
    finally:
        client.close()


def test_source_mapping_and_frequency(config):
    instrument = config.instruments["aapl"]
    instrument.source_symbols["yahoo"] = "BRK-B"
    collection = config.collections["daily"]
    collection.frequency = "5m"
    request = request_parameters(
        "yahoo",
        config.sources["yahoo"],
        instrument,
        collection,
        Chunk("daily", "aapl", date(2026, 9, 1), date(2026, 9, 2)),
    )
    assert request["symbol"] == "BRK-B" and request["interval"] == "5m"
    assert request["adjustment"] == "splits_only"


def test_cli_config_plan_status_and_safe_errors(config_dict, tmp_path, capsys):
    path = tmp_path / "collector.yaml"
    path.write_text(yaml.safe_dump(config_dict))
    assert main(["validate-config", "--config", str(path)]) == 0
    assert main(["plan", "--config", str(path), "--limit", "1", "--as-of", "2026-09-07"]) == 0
    assert main(["status", "--config", str(path)]) == 0
    output = capsys.readouterr().out
    assert str(tmp_path) not in output
    assert main(["status", "--config", str(path), "--limit", "1001"]) == 2
    config_dict["sources"]["ibkr"]["parameters"] = {"password": "DO-NOT-PRINT"}
    path.write_text(yaml.safe_dump(config_dict))
    assert main(["validate-config", "--config", str(path)]) == 2
    assert "DO-NOT-PRINT" not in capsys.readouterr().out


def test_base_imports_never_import_optional_providers():
    code = """
import sys
import dq_historical_market_data_collector.cli
assert not {'openbb', 'openbb_ibkr', 'pyarrow', 'duckdb'} & set(sys.modules)
"""
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


def test_init_config_packaged_resource(tmp_path, capsys):
    path = tmp_path / "new.yaml"
    assert main(["init-config", "--output", str(path)]) == 0
    assert "collections:" in path.read_text()
    assert main(["init-config", "--output", str(path)]) == 2


def test_shutdown_signal_releases_lock(config_dict, tmp_path):
    # No provider calls: schedule is deliberately in the future today.
    config_dict["schedule"] = {
        "timezone": "UTC",
        "at": "23:59:59",
        "catch_up_days": 1,
        "heartbeat_seconds": 0.1,
    }
    path = tmp_path / "service.yaml"
    path.write_text(yaml.safe_dump(config_dict))
    process = subprocess.Popen(
        [sys.executable, "-m", "dq_historical_market_data_collector", "run", "--config", str(path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        deadline = time.monotonic() + 5
        while not (tmp_path / "data" / "journal.sqlite3").exists() and time.monotonic() < deadline:
            time.sleep(0.03)
        started = time.monotonic()
        process.terminate()
        process.communicate(timeout=5)
        assert time.monotonic() - started < 5
        assert process.returncode == 0
    finally:
        if process.poll() is None:
            process.kill()
            process.communicate()

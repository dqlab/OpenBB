"""Check the installed ETF collector interface without acquiring market data."""

import importlib.metadata
import importlib.resources
import subprocess
import sys
from pathlib import Path

from dq_historical_market_data_collector.etf_options.cli import main
from dq_historical_market_data_collector.etf_options.config import load_config


def test_installed_cli_entrypoint_and_help():
    entries = list(
        importlib.metadata.entry_points(
            group="console_scripts", name="dq-etf-option-chain-collector"
        )
    )
    assert len(entries) == 1
    assert entries[0].load() is main
    result = subprocess.run(
        [sys.executable, "-m", "dq_historical_market_data_collector.etf_options.cli", "--help"],
        capture_output=True,
        text=True,
        timeout=20,
        check=True,
    )
    for command in ("collect", "calendar-refresh", "status", "export", "deliver"):
        assert command in result.stdout


def test_packaged_documentation_and_example_config():
    readme = importlib.resources.files("dq_historical_market_data_collector.etf_options").joinpath(
        "README.md"
    )
    assert "source-displayed quotes" in readme.read_text()
    example = Path(__file__).resolve().parents[1] / "configs/etf_options_daily.yaml"
    config = load_config(example)
    assert set(config.exchanges) == {"SSE", "SZSE"}
    assert config.exchanges["SSE"].source == "sina"
    assert config.exchanges["SZSE"].source == "official"
    assert config.block_size == 50

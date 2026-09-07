from __future__ import annotations

import os
import sys
from copy import deepcopy
from datetime import UTC, datetime

import pytest

from dq_historical_market_data_collector.config import CollectorConfig
from dq_historical_market_data_collector.providers import ProviderFailure, Response


@pytest.fixture
def config_dict(tmp_path):
    return {
        "instruments": {"aapl": {"symbol": "AAPL", "asset_type": "stock", "currency": "USD"}},
        "sources": {
            "yahoo": {
                "model": "EquityHistorical",
                "parameter_map": {
                    "symbol": "symbol",
                    "start_date": "start_date",
                    "end_date": "end_date",
                    "interval": "frequency",
                    "adjustment": "adjustment",
                },
                "provider": "yfinance",
                "adjustment": "splits_only",
                "retries": 0,
                "retry_delay_seconds": 0,
            },
            "ibkr": {
                "provider": "ibkr",
                "model": "MarketHistorical",
                "parameter_map": {
                    "symbol": "symbol",
                    "start_date": "start_date",
                    "end_date": "end_date",
                    "interval": "frequency",
                    "asset_type": "asset_type",
                    "currency": "currency",
                    "exchange": "exchange",
                    "primary_exchange": "primary_exchange",
                    "con_id": "con_id",
                    "end_datetime": "end_datetime",
                    "what_to_show": "what_to_show",
                    "use_rth": "use_rth",
                    "timeout": "timeout",
                },
                "retries": 0,
                "retry_delay_seconds": 0,
            },
        },
        "collections": {
            "daily": {
                "instrument_ids": ["aapl"],
                "primary": "yahoo",
                "secondary": ["ibkr"],
                "start_date": "2026-09-01",
                "end_date": "2026-09-04",
                "chunk_days": 3,
            },
        },
        "storage": {"root": str(tmp_path / "data"), "format": "jsonl", "export_batch_size": 2},
        "schedule": {"catch_up_days": 1, "heartbeat_seconds": 0.1},
    }


@pytest.fixture
def config(config_dict):
    return CollectorConfig.model_validate(config_dict)


@pytest.fixture
def now():
    return datetime(2026, 9, 7, 12, tzinfo=UTC)


@pytest.fixture
def rows():
    return [
        {"date": f"2026-09-0{day}", "open": 100, "high": 105, "low": 98, "close": 103, "volume": 50}
        for day in (1, 2, 3)
    ]


class FakeClient:
    def __init__(self, rows, failures=()):
        self.rows = rows
        self.failures = failures
        self.calls = []
        self.resets = 0
        self.closed = False

    def fetch(self, source_id, source, instrument, collection, chunk):
        self.calls.append((source_id, instrument.symbol, chunk))
        if source_id in self.failures or instrument.symbol in self.failures:
            raise ProviderFailure("test_provider_failed")
        return Response(deepcopy(self.rows))

    def reset(self):
        self.resets += 1

    def close(self):
        self.closed = True


@pytest.fixture
def client(rows):
    return FakeClient(rows)


@pytest.fixture(autouse=True)
def subprocess_pythonpath(monkeypatch):
    monkeypatch.setenv("PYTHONPATH", os.pathsep.join(sys.path))

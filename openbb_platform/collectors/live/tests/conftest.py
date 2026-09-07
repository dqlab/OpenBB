from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from dq_live_market_data_collector.config import CollectorConfig
from dq_live_market_data_collector.providers import Response
from dq_live_market_data_collector.service import Collector
from dq_live_market_data_collector.storage import Journal


class Clock:
    def __init__(self, value=None):
        self.value = value or datetime(2026, 9, 7, 13, 30, tzinfo=UTC)

    def __call__(self):
        return self.value

    def advance(self, seconds):
        self.value += timedelta(seconds=seconds)


class FakeProvider:
    def __init__(self, clock):
        self.clock = clock
        self.calls = []
        self.resets = 0
        self.closed = False
        self.handler = None

    def fetch(self, source_id, source, instrument):
        self.calls.append((source_id, instrument.symbol))
        if self.handler:
            return self.handler(source_id, source, instrument)
        return Response([quote(instrument.symbol, self.clock())])

    def reset(self):
        self.resets += 1

    def close(self):
        self.closed = True


def quote(symbol, timestamp, **fields):
    return {
        "symbol": symbol,
        "currency": "USD",
        "last_price": 100,
        "bid": 99,
        "ask": 101,
        "last_timestamp": timestamp.isoformat(),
        **fields,
    }


@pytest.fixture
def raw_config(tmp_path):
    return {
        "sources": {
            "primary": {
                "model": "EquityQuote",
                "provider": "yfinance",
                "retries": 0,
                "timestamp_semantics": "last_trade",
            },
            "secondary": {
                "model": "EquityQuote",
                "provider": "fmp",
                "retries": 0,
                "timestamp_semantics": "last_trade",
            },
        },
        "instruments": {
            "apple": {"symbol": "AAPL", "asset_type": "stock", "currency": "USD"},
            "spy": {"symbol": "SPY", "asset_type": "etf", "currency": "USD"},
        },
        "collections": {
            "quotes": {
                "instrument_ids": ["apple", "spy"],
                "primary": "primary",
                "secondary": ["secondary"],
                "frequency_seconds": 10,
                "schedule": {
                    "timezone": "America/New_York",
                    "start": "09:30",
                    "end": "09:31",
                },
            },
        },
        "storage": {"root": str(tmp_path / "data"), "format": "sqlite"},
    }


@pytest.fixture
def rig(raw_config):
    config = CollectorConfig.model_validate(raw_config)
    clock = Clock()
    provider = FakeProvider(clock)
    with Journal(config.storage) as journal:
        service = Collector(config, journal, provider=provider, clock=clock)
        yield config, clock, provider, journal, service
        provider.close()

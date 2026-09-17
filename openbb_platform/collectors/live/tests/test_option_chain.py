"""A full-chain poll has independent OCC identities and per-row quarantine."""

import json
from copy import deepcopy

import pytest
from conftest import Clock, FakeProvider

from dq_live_market_data_collector.config import CollectorConfig
from dq_live_market_data_collector.providers import Response
from dq_live_market_data_collector.service import Collector
from dq_live_market_data_collector.storage import Journal


def chain_config(raw_config):
    raw_config["sources"] = {
        "chain": {
            "provider": "cboe",
            "model": "OptionsChains",
            "record_type": "option_chain",
            "option_chain_roots": ["SPX", "SPXW"],
            "underlying_symbol_map": {"SPXW": "SPX"},
            "max_rows": 100000,
            "max_response_bytes": 45000000,
            "retries": 0,
        }
    }
    raw_config["instruments"] = {"spx": {"symbol": "SPX", "asset_type": "index", "currency": "USD"}}
    collection = raw_config["collections"]["quotes"]
    collection.update(
        instrument_ids=["spx"],
        primary="chain",
        secondary=[],
        fields=[
            "contract_symbol",
            "underlying_symbol",
            "expiration",
            "strike",
            "option_type",
            "bid",
            "ask",
            "theta",
        ],
        required_fields=[
            "contract_symbol",
            "underlying_symbol",
            "expiration",
            "strike",
            "option_type",
            "bid",
            "ask",
        ],
    )
    return CollectorConfig.model_validate(raw_config)


def row(**updates):
    return {
        "contract_symbol": "SPXW260918C05000000",
        "underlying_symbol": "SPXW",
        "expiration": "2026-09-18",
        "strike": 5000,
        "option_type": "call",
        "bid": 10,
        "ask": 11,
        "theta": -0.5,
        **updates,
    }


def test_chain_identity_reconciliation_and_poll_replay(raw_config):
    config = chain_config(raw_config)
    clock = Clock()
    provider = FakeProvider(clock)
    data = [
        row(),
        row(contract_symbol="SPXW260918P05000000", option_type="put"),
        row(bid=12),
        row(strike=5001),
        row(),
    ]
    original = deepcopy(data)
    provider.handler = lambda *_: Response(deepcopy(data))
    with Journal(config.storage) as journal:
        service = Collector(config, journal, provider=provider, clock=clock)
        service.step()
        totals = journal.report(journal.running_sessions()[0]["id"], clock())["totals"]
        assert {
            name: totals[name] for name in ["input_rows", "accepted", "duplicates", "rejected"]
        } == {
            "input_rows": 5,
            "accepted": 2,
            "duplicates": 1,
            "rejected": 2,
        }
        saved = [
            json.loads(item[0]) for item in journal.db.execute("SELECT payload FROM observations")
        ]
        assert {item["symbol"] for item in saved} == {
            data[0]["contract_symbol"],
            data[1]["contract_symbol"],
        }
        assert len({item["observation_key"] for item in saved}) == 2
        assert all(
            item["instrument_id"] == "spx" and item["asset_type"] == "option" for item in saved
        )
        assert all(
            item["underlying_symbol"] == "SPX" and item["values"]["theta"] == -0.5 for item in saved
        )
        service.step()
        assert len(provider.calls) == 1
        assert data == original
        archive = json.loads(next((journal.root / "bronze").rglob("*.json")).read_text())
        assert archive["rows"] == original


@pytest.mark.parametrize(
    "bad",
    [
        {"underlying_symbol": "NDX"},
        {"contract_symbol": "BAD"},
        {"expiration": "2026-09-19"},
        {"option_type": "put"},
        {"strike": True},
        {"theta": "nan"},
        {"contract_symbol": "NDX260918C05000000"},
    ],
)
def test_wrong_or_ambiguous_contracts_are_quarantined(raw_config, bad):
    config = chain_config(raw_config)
    clock = Clock()
    provider = FakeProvider(clock)
    provider.handler = lambda *_: Response([row(**bad)])
    with Journal(config.storage) as journal:
        Collector(config, journal, provider=provider, clock=clock).step()
        report = journal.report(journal.running_sessions()[0]["id"], clock())
        assert report["totals"]["accepted"] == 0
        assert report["totals"]["rejected"] == 1


def test_chain_preflight_requires_identity_and_explicit_roots(raw_config):
    config = chain_config(raw_config).model_dump(mode="json")
    config["sources"]["chain"]["option_chain_roots"] = []
    with pytest.raises(ValueError, match="explicit allowed OCC roots"):
        CollectorConfig.model_validate(config)
    config = chain_config(raw_config).model_dump(mode="json")
    config["collections"]["quotes"]["required_fields"].remove("strike")
    with pytest.raises(ValueError, match="identity fields"):
        CollectorConfig.model_validate(config)

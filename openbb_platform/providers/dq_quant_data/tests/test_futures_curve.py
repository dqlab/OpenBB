"""Offline contracts for central-store futures term structures."""

import json
from copy import deepcopy
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import Mock

import pytest
from openbb_core.app.model.abstract.error import OpenBBError
from pydantic import ValidationError

from openbb_dq_quant_data import futures_curve as module

Fetcher = module.DQFuturesCurveFetcher
CUTOFF = "2026-09-10T14:15:00+00:00"


def contract(uid, symbol, expiration, month, **overrides):
    return {
        "instrument_uid": uid,
        "provider": "ibkr",
        "asset_class": "future",
        "version_id": "reference-" + uid,
        "terms_json": json.dumps(
            {
                "local_symbol": symbol,
                "expiration": expiration,
                "contract_month": month,
                "currency": "USD",
            }
        ),
        **overrides,
    }


def quote(uid, **overrides):
    return {
        "instrument_uid": uid,
        "bid": Decimal("95.50"),
        "ask": Decimal("95.52"),
        "last_trade_price": Decimal("95.49"),
        "currency": "USD",
        "event_time": None,
        "collector_observed_at": "2026-09-10T14:14:00+00:00",
        "gold_eligible": True,
        "quality_flags_json": "[]",
        "actual_feed_type": "delayed",
        "content_hash": "hash-" + uid,
        "observation_uid": "observation-" + uid,
        **overrides,
    }


@pytest.fixture
def store(monkeypatch):
    refs = [
        contract("later", "SR3U6", "2026-12-15", "202609"),
        contract("front", "SR3M6", "2026-09-15", "202606"),
    ]
    records = [quote("later"), quote("front")]
    payload = {
        "data": records,
        "meta": {
            "publication_id": "publication",
            "availability_basis": "central",
            "layer": "gold",
            "next_cursor": None,
        },
    }
    references = {
        "data": refs,
        "meta": {"has_more": False, "identity_listing": "current_catalog_with_as_of_terms"},
    }
    status = {"data": [{"kind": "quotes", "provider": "ibkr"}]}

    def read(method, config, **kwargs):
        return {"instruments": references, "status": status, "basket": payload, "history": payload}[
            method
        ]

    mock = Mock(side_effect=read)
    monkeypatch.setattr(module, "_query", mock)
    return references, payload, status, mock


def run(**params):
    query = Fetcher.transform_query({"symbol": "sofr3", "as_of": CUTOFF, **params})
    raw = Fetcher.extract_data(query, None)
    return Fetcher.transform_data(query, raw)


def test_latest_curve_uses_basket_with_pinning_and_maturity_sort(store):
    result = run(publication_id="publication", config_path="/tmp/market.yaml")
    assert [row.contract_symbol for row in result.result] == ["SR3M6", "SR3U6"]
    row = result.result[0]
    assert row.price == 95.51 and row.price_type == "midpoint"
    assert row.expiration == "2026-09-15" and row.contract_month == "202606"
    assert row.currency == "USD" and row.event_time is None
    assert row.age_seconds == 60 and row.instrument_uid == "front"
    assert result.metadata["complete"] and result.metadata["eligible"]
    assert result.metadata["publication_id"] == "publication"
    assert result.metadata["reference_versions"] == {
        "front": "reference-front",
        "later": "reference-later",
    }
    assert result.metadata["simultaneous_exchange_snapshot"] is False
    args, kwargs = store[3].call_args
    assert args == ("basket", "/tmp/market.yaml")
    assert kwargs["max_age_seconds"] == 900 and kwargs["max_skew_seconds"] == 300
    assert kwargs["publication_id"] == "publication" and kwargs["layer"] == "gold"
    assert kwargs["as_of"] == datetime(2026, 9, 10, 14, 15, tzinfo=UTC)
    assert set(kwargs["instruments"]) == {"front", "later"}


@pytest.mark.parametrize(
    "price_type,expected", [("midpoint", 95.51), ("bid", 95.5), ("ask", 95.52), ("last", 95.49)]
)
def test_price_basis_is_explicit(store, price_type, expected):
    assert run(price_type=price_type).result[0].price == expected


def test_silver_flags_and_observation_skew_are_not_promoted(store):
    for row in store[1]["data"]:
        row.update(
            gold_eligible=False,
            quality_flags_json='["volume_unit_unresolved","event_time_unknown"]',
        )
    store[1]["data"][0]["collector_observed_at"] = "2026-09-10T14:04:00+00:00"
    result = run(layer="silver")
    assert result.metadata["complete"] and not result.metadata["eligible"]
    assert result.metadata["observed_time_skew_seconds"] == 600
    assert result.metadata["quality_blocked_instruments"] == ["front", "later"]
    assert "volume_unit_unresolved" in result.result[0].quality_flags
    assert store[3].call_args.kwargs["layer"] == "silver"


@pytest.mark.parametrize(
    "updates",
    [
        {"ask": None},
        {"bid": 96, "ask": 95},
        {"bid": "NaN"},
        {"bid": -1, "quality_flags_json": '["negative_quote"]'},
    ],
)
def test_midpoint_does_not_fall_back_to_last_or_invalid_bid(store, updates):
    store[1]["data"][0].update(updates)
    with pytest.raises(OpenBBError, match="invalid selected prices"):
        run()
    result = run(allow_incomplete=True)
    assert result.result[1].price is None
    assert result.result[1].last_trade_price == 95.49
    assert not result.metadata["complete"] and not result.metadata["eligible"]
    assert result.metadata["invalid_price_instruments"] == ["later"]


def test_missing_contract_remains_visible_when_explicitly_allowed(store):
    store[1]["data"].pop()
    with pytest.raises(OpenBBError, match="1 missing"):
        run()
    result = run(allow_incomplete=True)
    row = result.result[0]
    assert row.contract_symbol == "SR3M6" and row.price is None
    assert row.date is None and row.collector_observed_at is None
    assert row.quality_flags == ["missing_quote"]
    assert result.metadata["missing_instruments"] == ["front"]


def test_zero_quote_is_preserved(store):
    store[1]["data"][0].update(bid=0, ask=0)
    assert run().result[1].price == 0


def test_historical_date_is_separate_from_knowledge_cutoff(store):
    for row in store[1]["data"]:
        row["collector_observed_at"] = "2026-09-09T21:00:00+00:00"
    result = run(date="2026-09-09")
    args, kwargs = store[3].call_args
    assert args[0] == "history"
    assert kwargs["start"] == datetime(2026, 9, 9, tzinfo=UTC)
    assert kwargs["end"] == datetime(2026, 9, 10, tzinfo=UTC)
    assert kwargs["as_of"] == datetime(2026, 9, 10, 14, 15, tzinfo=UTC)
    assert kwargs["revisions"] == "all" and kwargs["limit"] == 100000
    assert result.metadata["max_age_seconds"] == 86400
    assert result.result[0].date.isoformat() == "2026-09-09"


def test_date_window_respects_explicit_age(store):
    run(date="2026-09-09", max_age_seconds=3600)
    assert store[3].call_args.kwargs["start"] == datetime(2026, 9, 9, 23, tzinfo=UTC)


def test_future_date_is_rejected_without_reading(store):
    with pytest.raises(OpenBBError, match="after the as_of"):
        run(date="2026-09-11")
    store[3].assert_not_called()


@pytest.mark.parametrize(
    "values",
    [
        {"date": "2026-09-09,2026-09-10"},
        {"date": ["2026-09-09"]},
        {"as_of": "2026-09-10T14:15:00"},
        {"max_age_seconds": 0},
        {"max_contracts": 1001},
        {"price_type": "settlement"},
        {"max_age_seconds": 60, "max_skew_seconds": 300},
    ],
)
def test_invalid_query_inputs(values):
    with pytest.raises(ValidationError):
        Fetcher.transform_query({"symbol": "SOFR3", **values})


def test_naive_source_clock_fails(store):
    store[1]["data"][0]["collector_observed_at"] = "2026-09-10T14:14:00"
    with pytest.raises(ValueError, match="UTC offset"):
        run()


def test_reference_and_candidate_limits_fail_explicitly(store):
    store[0]["meta"]["has_more"] = True
    with pytest.raises(OpenBBError, match="truncated"):
        run()
    store[0]["meta"]["has_more"] = False
    with pytest.raises(OpenBBError, match="contract limit"):
        run(max_contracts=1)
    store[1]["meta"]["next_cursor"] = "more"
    with pytest.raises(OpenBBError, match="candidate limit"):
        run()


def test_wrong_dataset_and_missing_reference_terms_fail(store):
    store[2]["data"][0]["kind"] = "bars"
    with pytest.raises(OpenBBError, match="quote dataset"):
        run()
    store[2]["data"][0]["kind"] = "quotes"
    store[0]["data"][0]["terms_json"] = "{}"
    with pytest.raises(OpenBBError, match="reference terms"):
        run()


def test_latest_selection_and_ambiguous_tie(store):
    older = deepcopy(store[1]["data"][0])
    older.update(
        collector_observed_at="2026-09-10T14:13:00+00:00", bid=90, ask=91, content_hash="old"
    )
    store[1]["data"].append(older)
    assert run().result[1].price == 95.51
    older["collector_observed_at"] = "2026-09-10T14:14:00+00:00"
    with pytest.raises(OpenBBError, match="ambiguous"):
        run()


def test_reference_currency_conflict_fails(store):
    store[1]["data"][0]["currency"] = "EUR"
    with pytest.raises(OpenBBError, match="currency conflicts"):
        run()


def test_expired_and_other_provider_references_are_excluded(store):
    store[0]["data"].extend(
        [
            contract("expired", "OLD", "2026-09-08", "202603"),
            contract("other", "OTHER", "2026-12-15", "202609", provider="other"),
        ]
    )
    assert len(run().result) == 2
    assert set(store[3].call_args.kwargs["instruments"]) == {"front", "later"}


def test_snapshot_id_is_repeatable_for_pinned_inputs(store):
    assert run().metadata["curve_snapshot_id"] == run().metadata["curve_snapshot_id"]


def test_last_price_is_independent_of_missing_bid(store):
    store[1]["data"][0].update(bid=-1, gold_eligible=False, quality_flags_json='["negative_quote"]')
    result = run(price_type="last", layer="silver")
    assert result.result[1].price == 95.49
    assert not result.metadata["eligible"]
    assert "negative_quote" in result.result[1].quality_flags

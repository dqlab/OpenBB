"""REST boundary for the standard OpenBB futures term-structure command."""

from unittest.mock import Mock

import pytest
from openbb_core.app.model.abstract.error import OpenBBError
from pydantic import ValidationError

from openbb_dq_quant_data import client, futures_curve

Fetcher = futures_curve.DQFuturesCurveFetcher


def test_curve_uses_one_rest_request_and_retains_server_provenance(monkeypatch):
    payload = {
        "data": [
            {
                "symbol": "SOFR3",
                "contract_symbol": "SR3U6",
                "expiration": "2026-12-15",
                "contract_month": "202609",
                "instrument_uid": "ibkr-one",
                "price": "95.51",
                "price_type": "midpoint",
                "currency": "USD",
                "event_time": None,
                "quality_flags": ["volume_unit_unresolved"],
                "gold_eligible": False,
            }
        ],
        "meta": {
            "rows": 1,
            "response_complete": True,
            "complete": True,
            "eligible": False,
            "publication_id": "pub",
            "curve_snapshot_id": "curve",
        },
    }
    request = Mock(return_value=Mock(status_code=200, json=lambda: payload))
    monkeypatch.setattr(client, "make_request", request)
    query = Fetcher.transform_query({"symbol": "sofr3", "date": "2026-10-12", "layer": "silver"})
    result = Fetcher.transform_data(query, Fetcher.extract_data(query, {"api_url": "https://nas"}))
    assert result.result[0].price == 95.51
    assert result.result[0].event_time is None
    assert result.result[0].contract_month == "202609"
    assert result.metadata == payload["meta"] and not result.metadata["eligible"]
    request.assert_called_once()
    assert request.call_args.args == ("https://nas/api/v1/market/futures/curve",)
    assert request.call_args.kwargs["params"]["date"] == "2026-10-12"
    assert "config_path" not in request.call_args.kwargs["params"]


@pytest.mark.parametrize(
    "bad",
    [
        {"date": "2026-10-10,2026-10-11"},
        {"as_of": "2026-10-10T12:00:00"},
        {"price_type": "settlement"},
        {"max_contracts": 1001},
        {"max_age_seconds": 60},
    ],
)
def test_invalid_curve_query_is_rejected(bad):
    with pytest.raises(ValidationError):
        Fetcher.transform_query({"symbol": "SOFR3", **bad})


@pytest.mark.parametrize(
    "metadata,params",
    [
        ({"rows": 2, "response_complete": True, "complete": True}, {}),
        ({"rows": 0, "response_complete": False, "complete": True}, {}),
        ({"rows": 0, "response_complete": True, "complete": False}, {}),
    ],
)
def test_partial_or_unrequested_incomplete_curve_is_rejected(monkeypatch, metadata, params):
    monkeypatch.setattr(
        client,
        "make_request",
        Mock(return_value=Mock(status_code=200, json=lambda: {"data": [], "meta": metadata})),
    )
    with pytest.raises(OpenBBError):
        Fetcher.extract_data(Fetcher.transform_query({"symbol": "SOFR3", **params}), None)

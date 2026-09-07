"""Offline contracts for provider-owned bounded acquisition."""

import json
from datetime import date
from types import SimpleNamespace

import pytest

from openbb_ibkr.models.bounded_market_data import IbkrMarketHistoricalQueryParams
from openbb_ibkr.utils.bounded import bounded_history as ibkr_history
from openbb_ibkr.utils.bounded import bounded_quote, historical_error_category


class Event:
    def __init__(self):
        self.callbacks = []

    def __iadd__(self, callback):
        self.callbacks.append(callback)
        return self

    def __isub__(self, callback):
        self.callbacks.remove(callback)
        return self


def test_ibkr_explicit_end_contract_and_diagnostic_cleanup():
    query = IbkrMarketHistoricalQueryParams(
        symbol="AAPL",
        asset_type="stock",
        currency="USD",
        start_date="2026-09-01",
        end_date="2026-09-04",
        end_datetime="2026-09-04T04:00:00+00:00",
    )
    request = query.request_parameters()
    assert request["end"] == "20260904-04:00:00" and request["duration"] == "4 D"
    event = Event()
    captured = {}
    contract = SimpleNamespace(symbol="AAPL", secType="STK", currency="USD", conId=265598)

    def get_bars(contract, **kwargs):
        captured.update(kwargs)
        event.callbacks[0](1, 162, "Historical service: session from different IP: SECRET-HOST")
        return [
            SimpleNamespace(
                date=date(2026, 9, 1),
                open=1,
                high=2,
                low=1,
                close=2,
                volume=3,
                average=1.5,
                barCount=4,
            )
        ]

    ib = SimpleNamespace(errorEvent=event, reqHistoricalData=get_bars)
    client = SimpleNamespace(
        _run=lambda callback: callback(),
        _ensure_connected=lambda: ib,
        build_contract=lambda **kwargs: contract,
        _qualify_contract=lambda ib, contract: contract,
    )
    response = ibkr_history(client, request)
    assert captured["endDateTime"] == request["end"] and captured["formatDate"] == 2
    assert captured["keepUpToDate"] is False
    assert response.metadata["gateway_error_categories"] == ["competing_session"]
    assert "SECRET" not in json.dumps(response.metadata)
    assert response.rows[0]["con_id"] == 265598 and not event.callbacks
    contract.currency = "EUR"
    with pytest.raises(ValueError, match="contract_identity_mismatch"):
        ibkr_history(client, request)
    assert not event.callbacks


@pytest.mark.parametrize(
    "text,category",
    [
        ("No market data permissions", "missing_entitlement"),
        ("HMDS query returned no data", "no_data"),
        ("pacing violation", "pacing"),
        ("no security definition", "invalid_contract"),
        ("unclassified private path", "historical_service_error"),
    ],
)
def test_safe_error_classification(text, category):
    assert historical_error_category(text) == category


class ErrorEvent:
    def __init__(self):
        self.handlers = []

    def __iadd__(self, handler):
        self.handlers.append(handler)
        return self

    def __isub__(self, handler):
        self.handlers.remove(handler)
        return self

    def emit(self, request_id, code, message):
        for handler in self.handlers:
            handler(request_id, code, message, None)


class FakeIB:
    def __init__(self):
        self.errorEvent = ErrorEvent()
        self.cancelled = []
        self.waits = []
        self.requested_type = None
        self.ticker = SimpleNamespace(last=None, marketDataType=1, bid=-1, ask=-1, bidSize=0, askSize=0)
        self.delivery_type = 3

    def reqMarketDataType(self, kind):
        self.requested_type = kind

    def reqMktData(self, contract, *args):
        return self.ticker

    def sleep(self, seconds):
        self.waits.append(seconds)
        self.ticker.last = 100
        if self.delivery_type is not None:
            self.ticker.marketDataType = self.delivery_type
        self.errorEvent.emit(-1, 2104, "private account diagnostic")
        self.errorEvent.emit(42, 10167, "private account diagnostic")

    def cancelMktData(self, contract):
        self.cancelled.append(contract)


def fake_ibkr_client():
    class IbkrClient:
        settings = {"delayed": True}
        ib = FakeIB()

        @staticmethod
        def _run(callback):
            return callback()

        @classmethod
        def _ensure_connected(cls):
            return cls.ib

        @classmethod
        def configure(cls, **kwargs):
            cls.settings.update(kwargs)

        @staticmethod
        def build_contract(**kwargs):
            return kwargs

        @staticmethod
        def _qualify_contract(ib, contract):
            return contract

        @classmethod
        def _normalise_quote(cls, contract, ticker, delayed):
            assert cls.settings["read_only"] is True
            return {**contract, "last": ticker.last, "bid": ticker.bid, "ask": ticker.ask, "delayed": delayed}

    return IbkrClient


def test_ibkr_waits_for_data_and_cancels_and_detaches_on_success_or_failure():
    client = fake_ibkr_client()
    client.configure(read_only=True)
    request = {"symbol": "AAPL", "delayed": True}
    result = bounded_quote(client, request, 4)
    row = result.rows[0]
    assert row["symbol"] == "AAPL" and row["last"] == 100 and row["delayed"] is True
    assert row["market_data_type"] == 3 and row["feed_type"] == "delayed"
    assert row["bid"] is None and row["ask"] is None
    assert row["raw_quote"]["bid"] == -1 and row["raw_quote"]["ask"] == -1
    assert row["unavailable_fields"] == ["bid", "ask"]
    assert result.metadata["feed_type_basis"] == "gateway_callback"
    assert result.metadata["gateway_error_codes"] == [10167]
    assert client.ib.waits == [4]
    assert client.ib.requested_type == 3
    assert client.ib.cancelled == [{"symbol": "AAPL"}]
    assert client.ib.errorEvent.handlers == []
    client._normalise_quote = lambda *_: int("invalid")
    with pytest.raises(ValueError):
        bounded_quote(client, request, 4)
    assert len(client.ib.cancelled) == 2
    assert client.ib.errorEvent.handlers == []


@pytest.mark.parametrize("asset_type", ["future", "option", "future_option"])
def test_derivatives_require_a_qualified_contract(asset_type):
    from pydantic import ValidationError
    from openbb_ibkr.models.bounded_market_data import IbkrMarketQuoteQueryParams

    with pytest.raises(ValidationError, match="qualified con_id"):
        IbkrMarketQuoteQueryParams(symbol="ES", asset_type=asset_type, currency="USD")
    assert IbkrMarketQuoteQueryParams(symbol="ES", asset_type=asset_type, currency="USD", con_id=123).con_id == 123


@pytest.mark.parametrize(
    "changes",
    [
        {"end_date": "2026-09-01"},
        {"end_date": "2026-09-20"},
        {"interval": "1m", "end_date": "2026-09-04"},
        {"asset_type": "option", "con_id": 123, "interval": "1d"},
        {"what_to_show": "ADJUSTED_LAST"},
        {"end_datetime": "2026-09-02T00:00:00"},
        {"port": -1},
        {"client_id": True},
        {"read_only": False},
    ],
)
def test_bounded_history_rejects_unsupported_contracts(changes):
    from pydantic import ValidationError

    values = dict(
        symbol="AAPL",
        asset_type="stock",
        currency="USD",
        start_date="2026-09-01",
        end_date="2026-09-02",
        end_datetime="2026-09-02T04:00:00+00:00",
    )
    with pytest.raises(ValidationError):
        IbkrMarketHistoricalQueryParams(**(values | changes))


def test_quote_fetcher_preserves_wait_readonly_identity_and_metadata(monkeypatch):
    from openbb_ibkr.models import bounded_market_data as models

    client = fake_ibkr_client()
    monkeypatch.setattr(models, "IbkrClient", client)
    query = models.IbkrMarketQuoteQueryParams(
        symbol="ES",
        asset_type="future",
        currency="USD",
        exchange="CME",
        con_id=123,
        snapshot_wait_seconds=3,
        feed_type="live",
    )
    raw = models.IbkrMarketQuoteFetcher.extract_data(query, {"ibkr_port": "4002"})
    result = models.IbkrMarketQuoteFetcher.transform_data(query, raw)
    assert client.settings["read_only"] is True and client.settings["delayed"] is False
    assert client.settings["port"] == "4002"
    assert client.ib.waits == [3] and client.ib.requested_type == 1
    assert client.ib.errorEvent.handlers == [] and len(client.ib.cancelled) == 1
    row = result.result[0]
    assert row.symbol == "ES" and row.con_id == 123 and row.sec_type == "FUT"
    assert row.asset_type == "future"
    assert result.metadata["snapshot_wait_seconds"] == 3
    raw.rows[0]["currency"] = "EUR"
    with pytest.raises(ValueError, match="identity_mismatch"):
        models.IbkrMarketQuoteFetcher.transform_data(query, raw)


def test_historical_fetcher_passes_explicit_bounds_and_returns_annotated_rows(monkeypatch):
    from openbb_ibkr.models import bounded_market_data as models
    from openbb_ibkr.utils.bounded import Response

    calls = []
    client = SimpleNamespace(configure=lambda **kwargs: calls.append(kwargs))
    monkeypatch.setattr(models, "IbkrClient", client)

    def history(received_client, request):
        assert received_client is client
        calls.append(request)
        return Response(
            [
                dict(
                    symbol="AAPL",
                    sec_type="STK",
                    currency="USD",
                    con_id=123,
                    date="2026-09-01",
                    open=1,
                    high=2,
                    low=1,
                    close=2,
                )
            ],
            metadata={"historical_publication_time": "unknown"},
        )

    monkeypatch.setattr(models, "bounded_history", history)
    query = models.IbkrMarketHistoricalFetcher.transform_query(
        dict(
            symbol="AAPL",
            asset_type="stock",
            currency="USD",
            start_date="2026-09-01",
            end_date="2026-09-02",
            end_datetime="2026-09-02T04:00:00+00:00",
            interval="5m",
        )
    )
    raw = models.IbkrMarketHistoricalFetcher.extract_data(query, None)
    response = models.IbkrMarketHistoricalFetcher.transform_data(query, raw)
    assert calls[0]["read_only"] is True and calls[0]["delayed"] is False
    assert calls[1]["end"] == "20260902-04:00:00"
    assert calls[1]["duration"] == "2 D" and calls[1]["bar_size"] == "5 mins"
    assert response.result[0].asset_type == "stock"
    assert response.metadata["historical_publication_time"] == "unknown"


@pytest.mark.parametrize("delivery_type,expected", [(None, "unknown"), (1, "live"), (4, "delayed_frozen")])
def test_actual_feed_type_comes_from_callback_not_requested_mode_or_library_default(delivery_type, expected):
    client = fake_ibkr_client()
    client.configure(read_only=True)
    client.ib.delivery_type = delivery_type
    result = bounded_quote(client, {"symbol": "AAPL", "delayed": True}, 1)
    assert result.rows[0]["feed_type"] == expected
    assert result.rows[0]["market_data_type"] == delivery_type
    assert result.rows[0]["delayed"] is True  # Retained requested-mode field.
    assert result.metadata["feed_type_basis"] == ("unknown" if delivery_type is None else "gateway_callback")


def test_unavailable_bid_ask_preserves_raw_and_valid_negative_futures_quotes():
    from openbb_ibkr.utils.bounded import quote_record

    raw = {"sec_type": "FUT", "bid": -1, "ask": -0.5, "volume": 34054225730979}
    ticker = SimpleNamespace(bidSize=2, askSize=3, marketDataType=3)
    row = quote_record(raw, ticker)
    assert row["bid"] == -1 and row["ask"] == -0.5
    assert row["volume"] == row["raw_quote"]["volume"] == raw["volume"]
    assert raw == {"sec_type": "FUT", "bid": -1, "ask": -0.5, "volume": 34054225730979}
    ticker.bidSize = 0
    row = quote_record(raw, ticker)
    assert row["bid"] is None and row["raw_quote"]["bid"] == -1

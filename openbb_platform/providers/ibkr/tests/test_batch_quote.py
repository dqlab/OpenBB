"""Batch correctness under partial delivery, reconnects, and cutoff races."""

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from ib_insync import Contract, TickData, Ticker

from openbb_ibkr.utils import batch_quote
from openbb_ibkr.utils.client import IbkrClient


class Event:
    def __init__(self):
        self.callbacks = []

    def __iadd__(self, callback):
        self.callbacks.append(callback)
        return self

    def __isub__(self, callback):
        self.callbacks.remove(callback)
        return self

    def emit(self, *args):
        for callback in list(self.callbacks):
            callback(*args)


@pytest.fixture
def rig(monkeypatch):
    class IB:
        def __init__(self):
            self.now = 0.0
            self.connected = True
            self.errorEvent = Event()
            self.disconnectedEvent = Event()
            self.wrapper = SimpleNamespace(ticker2ReqId={"mktData": {}})
            self.active = []
            self.cancelled = []
            self.missing = set()
            self.disconnect_at = None
            self.overshoot = False
            self.delivery_type = 3

        def isConnected(self):
            return self.connected

        def qualifyContracts(self, *contracts):
            return list(contracts)

        def reqMarketDataType(self, value):
            self.requested_type = value

        def reqMktData(self, contract, *args):
            # Deliberately preload a stale quote to verify reset behavior.
            ticker = Ticker(contract=contract, bid=90, ask=91, marketDataType=1)
            self.active.append(ticker)
            self.wrapper.ticker2ReqId["mktData"][ticker] = 100 + contract.conId
            return ticker

        def sleep(self, seconds):
            self.now += seconds
            if self.overshoot and self.now >= 0.7:
                self.now += 1
            if self.disconnect_at and self.now >= self.disconnect_at:
                self.connected = False
                self.disconnectedEvent.emit()
            for ticker in self.active:
                if ticker.contract.conId in self.missing:
                    continue
                ticker.bid = 95 if self.now < 0.7 else 999
                ticker.ask = ticker.bid + 0.01
                ticker.bidSize = ticker.askSize = 5
                ticker.marketDataType = self.delivery_type
                ticker.time = epoch + timedelta(seconds=self.now)
                ticker.ticks = [TickData(ticker.time, 66, ticker.bid, 5), TickData(ticker.time, 67, ticker.ask, 5)]
                ticker.updateEvent.emit(ticker)
            self.errorEvent.emit(101, 10167, "PRIVATE_MESSAGE")
            self.errorEvent.emit(99999, 999, "UNRELATED")

        def cancelMktData(self, contract):
            self.cancelled.append(contract.conId)

    epoch = datetime(2026, 10, 13, tzinfo=timezone.utc)
    ib = IB()

    class DateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return epoch + timedelta(seconds=ib.now)

    monkeypatch.setattr(batch_quote.time, "monotonic", lambda: ib.now)
    monkeypatch.setattr(batch_quote, "datetime", DateTime)
    client = SimpleNamespace(
        _run=lambda fn: fn(),
        _ensure_connected=lambda: ib,
        build_contract=lambda **kw: Contract(
            symbol=kw["symbol"], secType=kw["sec_type"], currency=kw["currency"], conId=kw["con_id"], exchange="CME"
        ),
        _normalise_quote=IbkrClient._normalise_quote,
    )
    requests = [
        dict(symbol="SOFR3", sec_type="FUT", currency="USD", exchange="CME", con_id=i, delayed=True)
        for i in range(1, 4)
    ]
    return ib, client, requests


def test_common_cutoff_identity_and_scoped_errors(rig):
    ib, client, requests = rig
    results = batch_quote.bounded_quotes_batch(client, requests, 0.7)
    assert [r.rows[0]["con_id"] for r in results] == [1, 2, 3]
    assert len({r.metadata["batch_cutoff_at"] for r in results}) == 1
    assert len({r.metadata["batch_id"] for r in results}) == 1
    assert results[0].metadata["gateway_error_codes"] == [10167]
    assert all(r.metadata["gateway_error_codes"] == [] for r in results[1:])
    assert all(r.rows[0]["feed_type"] == "delayed" for r in results)
    assert all(r.metadata["event_time"] is None for r in results)
    assert ib.requested_type == 3 and sorted(ib.cancelled) == [1, 2, 3]
    assert not ib.errorEvent.callbacks and not ib.disconnectedEvent.callbacks
    assert ib.now < 1


def test_missing_contract_cannot_reuse_cached_quotes(rig):
    ib, client, requests = rig
    ib.missing = {2}
    results = batch_quote.bounded_quotes_batch(client, requests, 0.7)
    assert results[1].rows == []
    assert results[1].metadata["batch_status"] == "no_fresh_quote"
    assert len(results[0].rows) == len(results[2].rows) == 1


def test_disconnect_invalidates_whole_batch_and_cancels(rig):
    ib, client, requests = rig
    ib.disconnect_at = 0.3
    results = batch_quote.bounded_quotes_batch(client, requests, 0.7)
    assert all(r.rows == [] and r.metadata["batch_status"] == "disconnected" for r in results)
    assert sorted(ib.cancelled) == [1, 2, 3]


def test_callbacks_after_cutoff_cannot_overwrite_snapshot(rig):
    ib, client, requests = rig
    ib.overshoot = True
    results = batch_quote.bounded_quotes_batch(client, requests[:1], 0.7)
    assert results[0].rows[0]["bid"] == 95
    assert all(age >= 0 for age in results[0].metadata["field_age_at_cutoff_seconds"].values())


def test_actual_callback_feed_and_unknown_type(rig):
    ib, client, requests = rig
    ib.delivery_type = 1
    assert batch_quote.bounded_quotes_batch(client, requests[:1], 0.7)[0].rows[0]["feed_type"] == "live"
    ib.now = 0
    ib.active.clear()
    ib.delivery_type = 0
    assert batch_quote.bounded_quotes_batch(client, requests[:1], 0.7)[0].rows == []


def test_qualification_failure_is_isolated(rig):
    ib, client, requests = rig
    ib.qualifyContracts = lambda *contracts: [contracts[0], contracts[2]]
    results = batch_quote.bounded_quotes_batch(client, requests, 0.7)
    assert results[1].rows == []
    assert results[1].metadata["batch_status"] == "contract_qualification_failed"
    assert sorted(ib.cancelled) == [1, 3]

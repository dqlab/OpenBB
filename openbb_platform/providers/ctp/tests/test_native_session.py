"""Offline tests exercise the actual session against a controlled MdApi."""

# ruff: noqa: D101, D102, D103

import time
from types import SimpleNamespace

from openbb_ctp.native import NativeSession


class Depth:
    def __init__(self, instrument):
        self._instrument = instrument

    @property
    def InstrumentID(self):
        return self._instrument

    @property
    def LastPrice(self):
        return 12.5


class Api:
    def __init__(self):
        self.subscriptions = []
        self.released = False

    def RegisterSpi(self, spi):
        self.spi = spi

    def RegisterFront(self, front):
        self.front = front

    def Init(self):
        self.spi.OnFrontConnected()

    def ReqUserLogin(self, request, generation):
        self.spi.OnRspUserLogin(None, None, generation, True)
        return 0

    def SubscribeMarketData(self, instruments, count):
        self.subscriptions.append(instruments)
        return 0

    def UnSubscribeMarketData(self, instruments, count):
        return 0

    def Release(self):
        self.released = True


def wait_for(predicate):
    until = time.monotonic() + 2
    while not predicate() and time.monotonic() < until:
        time.sleep(0.005)
    assert predicate()


def test_subscribe_reconnect_and_writer(tmp_path):
    from dq_live_market_data_collector.tick_file import TickIngress, TickWriter

    api = Api()
    apis = [api]

    def create(_):
        if apis[0].released:
            apis.append(Api())
            return apis[-1]
        return api

    md = SimpleNamespace(
        CThostFtdcMdSpi=object,
        CThostFtdcReqUserLoginField=SimpleNamespace,
        CThostFtdcDepthMarketDataField=Depth,
        CThostFtdcMdApi=SimpleNamespace(CreateFtdcMdApi=create),
    )
    events = []
    ingress = TickIngress(
        TickWriter(tmp_path, "test"), profile="replay", environment="synthetic"
    )
    session = NativeSession(
        "fake",
        "b",
        "u",
        "p",
        str(tmp_path),
        ["a", "b"],
        ingress.callback,
        lambda kind, data: events.append((kind, data)),
        md=md,
        interval=0.01,
        ack_timeout=0.04,
        attempts=3,
    )
    session.start()
    wait_for(lambda: len(api.subscriptions) >= 1)
    api.spi.OnRspSubMarketData(SimpleNamespace(InstrumentID="a"), None, 0, True)
    wait_for(lambda: len(api.subscriptions) >= 2)
    api.spi.OnRspSubMarketData(SimpleNamespace(InstrumentID="b"), None, 0, True)
    tick = Depth("a")
    api.spi.OnRtnDepthMarketData(tick)
    tick._instrument = "mutated"
    old_spi = api.spi
    old_spi.OnFrontDisconnected(99)
    wait_for(lambda: len(apis) == 2 and len(apis[1].subscriptions) >= 1)
    old_spi.OnRspSubMarketData(SimpleNamespace(InstrumentID="a"), None, 0, True)
    assert session.status()["subscriptions"]["a"]["state"] == "requested"
    apis[1].spi.OnRspSubMarketData(SimpleNamespace(InstrumentID="a"), None, 0, True)
    wait_for(lambda: len(apis[1].subscriptions) >= 2)
    apis[1].spi.OnRspSubMarketData(SimpleNamespace(InstrumentID="b"), None, 0, True)
    assert session.status()["subscriptions"]["a"]["state"] == "acknowledged"
    assert session.close() and ingress.close()
    assert api.released
    assert '"InstrumentID":"a"' in next(tmp_path.rglob("*.jsonl")).read_text()
    assert any(kind == "disconnected" for kind, _ in events)


def test_timeout_uses_per_instrument_budget(tmp_path):
    api = Api()
    md = SimpleNamespace(
        CThostFtdcMdSpi=object,
        CThostFtdcReqUserLoginField=SimpleNamespace,
        CThostFtdcDepthMarketDataField=Depth,
        CThostFtdcMdApi=SimpleNamespace(CreateFtdcMdApi=lambda _: api),
    )
    session = NativeSession(
        "fake",
        "b",
        "u",
        "p",
        str(tmp_path),
        ["a"],
        lambda *_, **__: True,
        lambda *_: None,
        md=md,
        interval=0.01,
        ack_timeout=0.01,
        attempts=2,
    )
    session.start()
    wait_for(session.failed.is_set)
    assert len(api.subscriptions) == 2
    assert session.status()["subscriptions"]["a"]["state"] == "exhausted"
    assert session.close()


def test_partial_rejected_duplicate_and_late_ack_fail_closed(tmp_path):
    api = Api()
    md = SimpleNamespace(
        CThostFtdcMdSpi=object,
        CThostFtdcReqUserLoginField=SimpleNamespace,
        CThostFtdcDepthMarketDataField=Depth,
        CThostFtdcMdApi=SimpleNamespace(CreateFtdcMdApi=lambda _: api),
    )
    events = []
    session = NativeSession(
        "fake",
        "b",
        "u",
        "p",
        str(tmp_path),
        ["a", "b"],
        lambda *_, **__: True,
        lambda *event: events.append(event),
        md=md,
        batch_size=2,
        interval=0.01,
        ack_timeout=0.05,
        attempts=2,
    )
    session.start()
    wait_for(lambda: len(api.subscriptions) == 1)
    api.spi.OnRspSubMarketData(SimpleNamespace(InstrumentID="a"), None, 0, False)
    api.spi.OnRspSubMarketData(SimpleNamespace(InstrumentID="a"), None, 0, False)
    assert session.status()["subscriptions"]["a"]["state"] == "acknowledged"
    wait_for(lambda: len(api.subscriptions) >= 2)
    api.spi.OnRspSubMarketData(SimpleNamespace(InstrumentID="b"), None, 0, True)
    assert session.status()["subscriptions"]["b"]["state"] == "requested"
    assert any(kind == "ack" and data["ambiguous"] for kind, data in events)
    wait_for(session.failed.is_set)
    assert session.close()

    api = Api()
    md.CThostFtdcMdApi = SimpleNamespace(CreateFtdcMdApi=lambda _: api)
    session = NativeSession(
        "fake",
        "b",
        "u",
        "p",
        str(tmp_path),
        ["bad"],
        lambda *_, **__: True,
        lambda *event: events.append(event),
        md=md,
        interval=0.01,
        ack_timeout=1,
    )
    session.start()
    wait_for(lambda: bool(api.subscriptions))
    api.spi.OnRspSubMarketData(
        SimpleNamespace(InstrumentID="bad"),
        SimpleNamespace(ErrorID=7, ErrorMsg="denied"),
        0,
        True,
    )
    assert session.status()["subscriptions"]["bad"]["state"] == "rejected"
    assert session.status()["subscriptions"]["bad"]["message"] == "denied"
    assert session.close()


def test_source_shutdown_timeout(tmp_path):
    import threading

    api = Api()
    gate = threading.Event()
    api.Release = lambda: gate.wait(2)
    md = SimpleNamespace(
        CThostFtdcMdSpi=object,
        CThostFtdcReqUserLoginField=SimpleNamespace,
        CThostFtdcDepthMarketDataField=Depth,
        CThostFtdcMdApi=SimpleNamespace(CreateFtdcMdApi=lambda _: api),
    )
    session = NativeSession(
        "fake",
        "b",
        "u",
        "p",
        str(tmp_path),
        ["a"],
        lambda *_, **__: True,
        lambda *_: None,
        md=md,
        interval=0.01,
    )
    session.start()
    assert not session.close(timeout=0.05)
    gate.set()


def test_rapid_disconnect_backoff_is_bounded_and_resets_after_stability(tmp_path):
    apis, events = [], []

    def create(_):
        api = Api()
        apis.append(api)
        return api

    md = SimpleNamespace(
        CThostFtdcMdSpi=object,
        CThostFtdcReqUserLoginField=SimpleNamespace,
        CThostFtdcDepthMarketDataField=Depth,
        CThostFtdcMdApi=SimpleNamespace(CreateFtdcMdApi=create),
    )
    session = NativeSession(
        "fake",
        "b",
        "u",
        "p",
        str(tmp_path),
        ["a"],
        lambda *_, **__: True,
        lambda *event: events.append(event),
        md=md,
        interval=0.01,
        reconnect_max_delay=0.04,
        reconnect_stable_seconds=0.12,
    )
    session.start()
    for expected in (2, 3, 4, 5):
        apis[-1].spi.OnFrontDisconnected(1)
        wait_for(lambda: len(apis) == expected)
    wait_for(lambda: len([x for x in events if x[0] == "reconnect_started"]) == 4)
    delays = [
        data["delay_seconds"] for kind, data in events if kind == "reconnect_started"
    ]
    assert delays == [0.01, 0.02, 0.04, 0.04]
    time.sleep(0.14)
    apis[-1].spi.OnFrontDisconnected(1)
    wait_for(lambda: len(apis) == 6)
    wait_for(lambda: len([x for x in events if x[0] == "reconnect_started"]) == 5)
    assert [
        data["delay_seconds"] for kind, data in events if kind == "reconnect_started"
    ][-1] == 0.01
    assert session.close()


def test_disconnect_retains_ledger_but_cannot_confirm_current_generation(tmp_path):
    api = Api()
    md = SimpleNamespace(
        CThostFtdcMdSpi=object,
        CThostFtdcReqUserLoginField=SimpleNamespace,
        CThostFtdcDepthMarketDataField=Depth,
        CThostFtdcMdApi=SimpleNamespace(CreateFtdcMdApi=lambda _: api),
    )
    session = NativeSession(
        "fake",
        "b",
        "u",
        "p",
        str(tmp_path),
        ["a"],
        lambda *_, **__: True,
        lambda *_: None,
        md=md,
        interval=0.01,
        ack_timeout=1,
    )
    session.start()
    wait_for(lambda: bool(api.subscriptions))
    api.spi.OnRspSubMarketData(SimpleNamespace(InstrumentID="a"), None, 0, True)
    old = session.status()
    assert old["subscriptions"]["a"]["generation"] == old["generation"]
    api.spi.OnFrontDisconnected(1)
    state = session.stop_and_status()
    # This is the former CLI predicate and demonstrates the false positive.
    assert all(x["state"] == "acknowledged" for x in state["subscriptions"].values())
    assert not state["connected"] and not state["logged_in"]
    assert state["subscriptions"]["a"]["generation"] != state["generation"]
    assert session.close()


def test_keyword_constructor_and_public_runtime_state(tmp_path):
    import inspect

    events = []
    session = NativeSession(
        front="fake",
        broker="b",
        user="u",
        password="p",  # noqa: S106 - synthetic test credential
        flow_path=str(tmp_path),
        instruments=("a",),
        on_tick=lambda *_, **__: True,
        on_event=lambda *event: events.append(event),
        interval=0.01,
    )
    assert list(inspect.signature(NativeSession).parameters) == [
        "front",
        "broker",
        "user",
        "password",
        "flow_path",
        "instruments",
        "on_tick",
        "on_event",
        "batch_size",
        "interval",
        "ack_timeout",
        "attempts",
        "md",
        "reconnect_stable_seconds",
        "reconnect_max_delay",
        "on_raw_tick",
    ]
    assert session.instruments == ["a"]
    assert not session.failed.is_set() and session.generation == 0
    session.emit_event("test", {"value": 1})
    assert events == [("test", {"value": 1})]
    assert session.close()


def test_callback_exception_and_release_exception_report_failure(tmp_path):
    api = Api()
    md = SimpleNamespace(
        CThostFtdcMdSpi=object,
        CThostFtdcReqUserLoginField=SimpleNamespace,
        CThostFtdcDepthMarketDataField=Depth,
        CThostFtdcMdApi=SimpleNamespace(CreateFtdcMdApi=lambda _: api),
    )
    events = []

    def fail_tick(*_, **__):
        raise RuntimeError("tick failed")

    session = NativeSession(
        "fake",
        "b",
        "u",
        "p",
        str(tmp_path),
        ["a"],
        fail_tick,
        lambda *event: events.append(event),
        md=md,
    )
    session.start()
    api.spi.OnRtnDepthMarketData(Depth("a"))
    assert session.failed.is_set()
    assert ("callback_failed", {"error": "tick failed"}) in events

    def fail_release():
        raise RuntimeError("release failed")

    api.Release = fail_release
    assert not session.close()
    assert any(
        kind == "source_close_failed" and "release failed" in data["errors"]
        for kind, data in events
    )


def test_native_import_defers_provider_registry():
    import os
    import subprocess
    import sys

    code = (
        "import sys; import openbb_ctp.native; "
        "assert 'openbb_core.provider.abstract.provider' not in sys.modules; "
        "from openbb_ctp import ctp_provider; assert ctp_provider.name == 'ctp'"
    )
    subprocess.run([sys.executable, "-c", code], env=os.environ.copy(), check=True)  # noqa: S603

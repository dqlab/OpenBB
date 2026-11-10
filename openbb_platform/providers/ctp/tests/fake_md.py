"""Deterministic synthetic MdApi for subprocess tests; never opens a socket."""

# ruff: noqa: D101, D102
import ctypes
import errno
import os
import threading
import time
from pathlib import Path
from types import SimpleNamespace

created = []

if os.environ.get("FAKE_IMPORT_DELAY"):
    time.sleep(float(os.environ["FAKE_IMPORT_DELAY"]))

if os.environ.get("FAKE_ENOSPC"):
    from dq_live_market_data_collector.tick_file import TickWriter

    failures = set(os.environ["FAKE_ENOSPC"].split(","))

    def no_space(kind):
        """Fail a simulated output path."""
        os.write(2, f"ENOSPC_HIT:{kind}\n".encode())
        raise OSError(errno.ENOSPC, f"synthetic {kind} full")

    original_open = Path.open

    def full_open(self, *args, **kwargs):
        """Fail event and status output opens."""
        kind = {"events.jsonl": "events", "status.json.tmp": "status"}.get(self.name)
        if kind in failures:
            if kind != "status" or not os.environ.get("FAKE_STATUS_FINAL"):
                no_space(kind)
            with original_open(self.parent / "events.jsonl", encoding="utf-8") as events:
                if '"session_end"' in events.read():
                    no_space(kind)
        return original_open(self, *args, **kwargs)

    Path.open = full_open
    original_tick_append = TickWriter.append

    def full_append(self, record):
        """Inject an append failure when selected."""
        if "tick" in failures:
            no_space("tick")
        return original_tick_append(self, record)

    TickWriter.append = full_append

if os.environ.get("FAKE_DISK_FAIL") or os.environ.get("FAKE_OVERFLOW"):
    from dq_live_market_data_collector.tick_file import TickWriter

    original_append = TickWriter.append

    def fault_append(self, record):
        """Inject deterministic writer failures in isolated test processes."""
        if os.environ.get("FAKE_DISK_FAIL"):
            raise OSError("synthetic disk full")
        time.sleep(0.2)
        return original_append(self, record)

    TickWriter.append = fault_append

CThostFtdcMdSpi = object
CThostFtdcReqUserLoginField = SimpleNamespace


class CThostFtdcDepthMarketDataField:
    @property
    def InstrumentID(self):
        return "test"

    @property
    def LastPrice(self):
        return 42.5


class Api:
    def RegisterSpi(self, spi):
        self.spi = spi

    def RegisterFront(self, front):
        pass

    def Init(self):
        if os.environ.get("FAKE_HANG_INIT"):
            ctypes.PyDLL(None).sleep(30)
        self.spi.OnFrontConnected()

    def ReqUserLogin(self, request, generation):
        info = SimpleNamespace(ErrorID=7, ErrorMsg="synthetic rejection") if os.environ.get(
            "FAKE_LOGIN_REJECT"
        ) else None
        self.spi.OnRspUserLogin(None, info, generation, True)
        return 0

    def SubscribeMarketData(self, instruments, count):
        for item in instruments:
            self.spi.OnRspSubMarketData(SimpleNamespace(InstrumentID=item.decode()), None, 0, True)
        self.spi.OnRtnDepthMarketData(CThostFtdcDepthMarketDataField())
        if os.environ.get("FAKE_OVERFLOW"):
            for _ in range(100):
                self.spi.OnRtnDepthMarketData(CThostFtdcDepthMarketDataField())
        if os.environ.get("FAKE_RECONNECT") and self.number == 1:
            old_spi = self.spi
            def disconnect():
                time.sleep(0.05)
                old_spi.OnFrontDisconnected(99)
                time.sleep(0.1)
                old_spi.OnRspSubMarketData(SimpleNamespace(InstrumentID="test"), None, 0, True)
                old_spi.OnRtnDepthMarketData(CThostFtdcDepthMarketDataField())
            threading.Thread(target=disconnect, daemon=True).start()
        return 0

    def UnSubscribeMarketData(self, instruments, count):
        return 0

    def Release(self):
        if os.environ.get("FAKE_GIL_RELEASE"):
            ctypes.PyDLL(None).sleep(30)
        if os.environ.get("FAKE_HANG_RELEASE"):
            threading.Event().wait(30)


class CThostFtdcMdApi:
    @staticmethod
    def CreateFtdcMdApi(path):
        created.append(1)
        api = Api()
        api.number = len(created)
        return api

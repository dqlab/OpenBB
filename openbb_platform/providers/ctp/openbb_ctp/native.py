"""Explicitly started OpenCTP futures market-data session.

Each disconnect retires the API and creates a fresh callback generation.
No network activity occurs on import.
"""

from __future__ import annotations

import importlib
import inspect
import threading
import time
from contextlib import contextmanager
from dataclasses import (
    dataclass,
    fields as dataclass_fields,
)
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any


class RetiredCallback(Exception):
    """A callback from a retired or stopped SPI, not a current snapshot."""

    rejection_reason = "retired_generation"


def copy_fields(obj, field_type):
    """Copy public SWIG properties while the callback owns the proxy."""
    return {
        name: getattr(obj, name)
        for name, value in vars(field_type).items()
        if isinstance(value, property) and name != "thisown"
    }


@contextmanager
def _try_lock(lock):
    """Attempt a nonblocking lock and release it only when acquired."""
    acquired = lock.acquire(blocking=False)
    try:
        yield acquired
    finally:
        if acquired:
            lock.release()


@dataclass
class _RequiredArguments:
    front: Any
    broker: Any
    user: Any
    password: Any
    flow_path: Any
    instruments: Any
    on_tick: Any
    on_event: Any


@dataclass(kw_only=True)
class _OptionalArguments:
    batch_size: Any = 1
    interval: Any = 1.0
    ack_timeout: Any = 10.0
    attempts: Any = 3
    md: Any = None
    reconnect_stable_seconds: Any = 60.0
    reconnect_max_delay: Any = 30.0
    on_raw_tick: Any = None


@dataclass
class _Arguments(_OptionalArguments, _RequiredArguments):
    """The original positional and keyword-only constructor contract."""


class NativeSession:
    """Futures MdApi with bounded, per-instrument subscription retries."""

    __signature__ = inspect.signature(_Arguments)
    _argument_names = frozenset(item.name for item in dataclass_fields(_Arguments))
    _runtime_names = frozenset(
        (
            "connected_at",
            "api",
            "spi",
            "failed",
            "stopping",
            "lock",
            "generation",
            "connected",
            "logged_in",
            "ledger",
            "worker",
            "reconnect",
            "md_loaded",
            "last_connection_duration",
            "supervisor",
        )
    )

    def __init__(self, *args, **kwargs):
        self._arguments = _Arguments(*args, **kwargs)
        self._arguments.instruments = list(self._arguments.instruments)
        if (
            not self._arguments.instruments
            or len(set(self._arguments.instruments)) != len(self._arguments.instruments)
            or any(
                not x or not x.isascii() or "\x00" in x
                for x in self._arguments.instruments
            )
        ):
            raise ValueError(
                "instruments must be unique nonempty ASCII strings without NUL"
            )
        if (
            self._arguments.batch_size < 1
            or self._arguments.interval <= 0
            or self._arguments.ack_timeout <= 0
            or self._arguments.attempts < 1
        ):
            raise ValueError("subscription limits must be positive")
        if (
            self._arguments.reconnect_stable_seconds <= 0
            or self._arguments.reconnect_max_delay <= 0
        ):
            raise ValueError("reconnect limits must be positive")
        self._runtime = SimpleNamespace(
            connected_at=None,
            api=None,
            spi=None,
            failed=threading.Event(),
            stopping=threading.Event(),
            lock=threading.RLock(),
            generation=0,
            connected=False,
            logged_in=False,
            ledger={},
            worker=None,
            reconnect=threading.Event(),
            md_loaded=None,
            last_connection_duration=0.0,
            supervisor=None,
        )

    def __getattr__(self, name):
        """Expose legacy session attributes from their internal groups."""
        if name in self._argument_names:
            return getattr(self._arguments, name)
        if name in self._runtime_names:
            return getattr(self._runtime, name)
        raise AttributeError(name)

    def __setattr__(self, name, value):
        """Keep assignments to legacy session attributes working."""
        if name in self._argument_names:
            setattr(self._arguments, name, value)
        elif name in self._runtime_names:
            setattr(self._runtime, name, value)
        else:
            object.__setattr__(self, name, value)

    def emit_event(self, kind, data):
        """Forward an event to the session observer."""
        self._arguments.on_event(kind, data)

    def _subscribe_loop(self):
        while not self._runtime.stopping.wait(self._arguments.interval):
            now = time.monotonic()
            with self._runtime.lock:
                if not self._runtime.logged_in:
                    continue
                candidates = []
                for instrument, state in self._runtime.ledger.items():
                    if state["state"] in ("acknowledged", "rejected", "exhausted"):
                        continue
                    if state["state"] == "requested" and now < state["deadline"]:
                        continue
                    if state["attempts"] >= self._arguments.attempts:
                        state["state"] = "exhausted"
                        self.emit_event(
                            "subscription_exhausted",
                            {
                                "instrument": instrument,
                                "generation": self._runtime.generation,
                                "attempts": state["attempts"],
                            },
                        )
                        self._runtime.failed.set()
                        continue
                    candidates.append(instrument)
                    if len(candidates) == self._arguments.batch_size:
                        break
                if not candidates:
                    continue
                generation = self._runtime.generation
                for instrument in candidates:
                    state = self._runtime.ledger[instrument]
                    state.update(
                        state="requested",
                        attempts=state["attempts"] + 1,
                        deadline=now + self._arguments.ack_timeout,
                    )
                    self.emit_event(
                        "requested",
                        {
                            "instrument": instrument,
                            "generation": generation,
                            "attempt": state["attempts"],
                        },
                    )
                result = self._runtime.api.SubscribeMarketData(
                    [x.encode("ascii") for x in candidates], len(candidates)
                )
                if result:
                    for instrument in candidates:
                        self._runtime.ledger[instrument]["deadline"] = now
                    self.emit_event(
                        "subscribe_request_failed",
                        {
                            "code": result,
                            "generation": generation,
                            "instruments": candidates,
                        },
                    )

    def _new_api(self):
        md = self._runtime.md_loaded
        parent = self
        runtime = self._runtime
        arguments = self._arguments
        with self._runtime.lock:
            self._runtime.generation += 1
            generation = self._runtime.generation
            self._runtime.connected = self._runtime.logged_in = False
            self._runtime.ledger = {
                x: {"state": "pending", "attempts": 0, "observed": False}
                for x in self._arguments.instruments
            }

        def current():
            return not runtime.stopping.is_set() and runtime.generation == generation

        spi_base: Any = md.CThostFtdcMdSpi

        class Spi(spi_base):
            def OnFrontConnected(self):
                with runtime.lock:
                    if not current() or runtime.connected:
                        return
                    runtime.connected, runtime.logged_in = True, False
                    runtime.connected_at = time.monotonic()
                    runtime.ledger = {
                        x: {"state": "pending", "attempts": 0, "observed": False}
                        for x in arguments.instruments
                    }
                    parent.emit_event("connected", {"generation": runtime.generation})
                    request = md.CThostFtdcReqUserLoginField()
                    request.BrokerID, request.UserID, request.Password = (
                        arguments.broker,
                        arguments.user,
                        arguments.password,
                    )
                    code = runtime.api.ReqUserLogin(request, runtime.generation)
                    if code:
                        parent.emit_event("login_request_failed", {"code": code})
                        runtime.failed.set()

            def OnFrontDisconnected(self, reason):
                with runtime.lock:
                    if not current():
                        return
                    runtime.connected = runtime.logged_in = False
                    runtime.last_connection_duration = (
                        time.monotonic() - runtime.connected_at
                        if runtime.connected_at is not None
                        else 0
                    )
                    parent.emit_event(
                        "disconnected", {"reason": reason, "generation": generation}
                    )
                    # Invalidate this SPI before any callback can be accepted.
                    runtime.generation += 1
                    runtime.reconnect.set()

            def OnRspUserLogin(self, login, info, request_id, last):
                del login
                with runtime.lock:
                    if (
                        not current()
                        or request_id != generation
                        or not runtime.connected
                    ):
                        return
                    code = info.ErrorID if info else 0
                    if code:
                        parent.emit_event(
                            "login_rejected", {"code": code, "message": info.ErrorMsg}
                        )
                        runtime.failed.set()
                        return
                    if last:
                        runtime.logged_in = True
                        parent.emit_event(
                            "login_ack", {"generation": runtime.generation}
                        )

            def OnRspSubMarketData(self, instrument, info, request_id, last):
                del request_id
                with runtime.lock:
                    if not current():
                        return
                    name = instrument.InstrumentID if instrument else None
                    code = info.ErrorID if info else 0
                    state = runtime.ledger.get(name)
                    # MdApi subscription callbacks have no usable generation or attempt
                    # token. After a reconnect or retry an old response is indistinguishable.
                    tracked = (
                        state is not None
                        and runtime.logged_in
                        and state["state"] == "requested"
                        and state["attempts"] == 1
                        and time.monotonic() <= state["deadline"]
                    )
                    if tracked and state is not None:
                        state["state"] = "rejected" if code else "acknowledged"
                        state["generation"] = generation
                        state["code"] = code
                        state["message"] = info.ErrorMsg if info else ""
                    parent.emit_event(
                        "rejected" if code else "ack",
                        {
                            "instrument": name,
                            "code": code,
                            "message": info.ErrorMsg if info else "",
                            "generation": runtime.generation,
                            "last": last,
                            "tracked": tracked,
                            "ambiguous": not tracked,
                        },
                    )

            def OnRtnDepthMarketData(self, data):
                received_at = (
                    datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
                )
                monotonic_ns = time.monotonic_ns()

                def snapshot():
                    if not current():
                        raise RetiredCallback(
                            "callback from stopped or retired generation"
                        )
                    fields = copy_fields(data, md.CThostFtdcDepthMarketDataField)
                    if not current():
                        raise RetiredCallback(
                            "callback from stopped or retired generation"
                        )
                    state = runtime.ledger.get(fields.get("InstrumentID"))
                    if state is not None:
                        state["observed"] = True
                    return fields

                try:
                    if arguments.on_raw_tick:
                        accepted = arguments.on_raw_tick(
                            snapshot, received_at=received_at, monotonic_ns=monotonic_ns
                        )
                    else:
                        accepted = arguments.on_tick(
                            snapshot(),
                            received_at=received_at,
                            monotonic_ns=monotonic_ns,
                        )
                    if not accepted:
                        runtime.failed.set()
                except Exception as exc:
                    parent.emit_event("callback_failed", {"error": str(exc)})
                    runtime.failed.set()

        self._runtime.api = md.CThostFtdcMdApi.CreateFtdcMdApi(
            self._arguments.flow_path
        )
        self._runtime.spi = Spi()
        self._runtime.api.RegisterSpi(self._runtime.spi)
        self._runtime.api.RegisterFront(self._arguments.front)
        self._runtime.api.Init()

    def _supervise(self):
        delay = min(self._arguments.interval, self._arguments.reconnect_max_delay)
        while not self._runtime.stopping.is_set():
            if not self._runtime.reconnect.wait(0.1):
                continue
            self._runtime.reconnect.clear()
            if (
                self._runtime.last_connection_duration
                >= self._arguments.reconnect_stable_seconds
            ):
                delay = min(
                    self._arguments.interval, self._arguments.reconnect_max_delay
                )
            if self._runtime.stopping.wait(delay):
                return
            old = self._runtime.api

            # Release may block in native code; never accept callbacks from its SPI.
            def retire(api):
                try:
                    api.RegisterSpi(None)
                    api.Release()
                except Exception as exc:
                    self.emit_event("retire_failed", {"error": str(exc)})
                    self._runtime.failed.set()

            retirement = threading.Thread(target=retire, args=(old,), daemon=True)
            retirement.start()
            retirement.join(timeout=5.0)
            if retirement.is_alive() or self._runtime.failed.is_set():
                self.emit_event(
                    "retire_timeout", {"generation": self._runtime.generation}
                )
                self._runtime.failed.set()
                return
            if self._runtime.stopping.is_set():
                return
            try:
                self._new_api()
                self.emit_event(
                    "reconnect_started",
                    {"generation": self._runtime.generation, "delay_seconds": delay},
                )
                delay = min(delay * 2, self._arguments.reconnect_max_delay)
            except Exception as exc:
                self.emit_event(
                    "reconnect_failed", {"error": str(exc), "delay_seconds": delay}
                )
                delay = min(delay * 2, self._arguments.reconnect_max_delay)
                self._runtime.reconnect.set()

    def start(self):
        """Start the explicit native connection."""
        self._runtime.md_loaded = self._arguments.md or importlib.import_module(
            "openctp_ctp.thostmduserapi"
        )
        self._runtime.worker = threading.Thread(
            target=self._subscribe_loop, daemon=True
        )
        self._runtime.worker.start()
        self._runtime.supervisor = threading.Thread(target=self._supervise, daemon=True)
        self._runtime.supervisor.start()
        self._new_api()

    def status(self):
        """Return a snapshot of the current generation and subscription ledger."""
        with _try_lock(self._runtime.lock) as acquired:
            if not acquired:
                return {
                    "connected": None,
                    "logged_in": None,
                    "generation": self._runtime.generation,
                    "subscriptions": {},
                    "error": "source lock unavailable",
                }
            return {
                "connected": self._runtime.connected,
                "logged_in": self._runtime.logged_in,
                "generation": self._runtime.generation,
                "subscriptions": {
                    name: dict(state) for name, state in self._runtime.ledger.items()
                },
            }

    def stop_and_status(self):
        """Atomically stop accepting callbacks and capture the last active generation."""
        with self._runtime.lock:
            self._runtime.stopping.set()
            return self.status()

    def close(self, timeout=5.0):
        """Stop retries, unsubscribe, and release the API."""
        self._runtime.stopping.set()
        self._runtime.reconnect.set()  # Wake an idle supervisor immediately for shutdown.
        deadline = time.monotonic() + timeout
        if self._runtime.worker:
            self._runtime.worker.join(timeout=max(0, deadline - time.monotonic()))
            if self._runtime.worker.is_alive():
                self._runtime.failed.set()
                return False
        if self._runtime.supervisor:
            self._runtime.supervisor.join(timeout=max(0, deadline - time.monotonic()))
            if self._runtime.supervisor.is_alive():
                self._runtime.failed.set()
                self.emit_event(
                    "source_close_failed",
                    {"timeout": True, "errors": ["supervisor still running"]},
                )
                return False
        errors = []

        def release():
            if not self._runtime.api:
                return
            try:
                if self._runtime.logged_in:
                    instruments = [
                        x.encode("ascii") for x in self._arguments.instruments
                    ]
                    self._runtime.api.UnSubscribeMarketData(
                        instruments, len(instruments)
                    )
                self._runtime.api.RegisterSpi(None)
                self._runtime.api.Release()
                self._runtime.api = None
            except Exception as exc:
                errors.append(str(exc))

        closer = threading.Thread(target=release, daemon=True)
        closer.start()
        closer.join(timeout=max(0, deadline - time.monotonic()))
        if closer.is_alive() or errors:
            self._runtime.failed.set()
            self.emit_event(
                "source_close_failed", {"timeout": closer.is_alive(), "errors": errors}
            )
            return False
        return True

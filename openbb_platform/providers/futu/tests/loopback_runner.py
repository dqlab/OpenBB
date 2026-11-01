"""Test-only subprocess adapter: route the official endpoint to a local server."""

import os
import sys
import threading

from openbb_futu import coordination
from openbb_futu.collector import Collector, main
from openbb_futu.ws import URL
from websockets.asyncio.client import connect

# This executable only runs loopback tests; production retains its 60-second window.
coordination.BUDGET_WINDOW = 0.05

# Opt-in diagnostics for synthetic, isolated loopback tests only.
if os.environ.get("FUTU_TEST_TRACE") == "1":
    import traceback

    def trace(frame, event, arg):
        """Record selected exceptions from the isolated test process."""
        if event == "exception" and frame.f_code.co_name in ("_run", "run") and (
            frame.f_code.co_filename.endswith("/archive.py")
            or frame.f_code.co_filename.endswith("/collector.py")
        ):
            kind, exc, tb = arg
            with open(os.environ["FUTU_TEST_TRACE_FILE"], "a", encoding="utf-8") as trace_file:
                traceback.print_exception(kind, exc, tb, file=trace_file)
        return trace

    sys.settrace(trace)
    threading.settrace(trace)

original_init = Collector.__init__


def local_connect(url, **kwargs):
    """Confine the injected connection to the test loopback server."""
    assert url == URL
    return connect(f"ws://127.0.0.1:{int(os.environ['FUTU_LOOPBACK_PORT'])}", **kwargs)


def init(self, config, store, root):
    """Inject a loopback-only factory in this test subprocess."""
    original_init(self, config, store, root, connection_factory=local_connect)
    if os.environ.get("FUTU_TEST_ISOLATED") == "1":
        self._test_isolated = True


Collector.__init__ = init
if os.environ.get("FUTU_TEST_ISOLATED") == "1":
    # Keep integration subprocesses fast; production retains the 60-second window.
    coordination.BUDGET_WINDOW = 0.05
    # Test subprocess may connect only to literal loopback addresses.
    import socket
    _connect = socket.socket.connect
    _connect_ex = socket.socket.connect_ex
    _sendto = socket.socket.sendto

    def check_address(sock, address):
        """Reject non-loopback internet addresses."""
        if sock.family in (socket.AF_INET, socket.AF_INET6) and (
            not isinstance(address, tuple) or address[0] not in ("127.0.0.1", "::1")
        ):
            raise OSError("test network restricted to loopback")

    def safe_connect(sock, address):
        """Connect only to loopback."""
        check_address(sock, address)
        return _connect(sock, address)

    def safe_connect_ex(sock, address):
        """Connect only to loopback, returning the socket error code."""
        check_address(sock, address)
        return _connect_ex(sock, address)

    def safe_sendto(sock, *args):
        """Send only to loopback."""
        check_address(sock, args[-1])
        return _sendto(sock, *args)

    socket.socket.connect = safe_connect
    socket.socket.connect_ex = safe_connect_ex
    socket.socket.sendto = safe_sendto
fault = os.environ.get("FUTU_TEST_ARCHIVE_FAULT")
if fault:
    from dq_live_market_data_collector.tick_file import TickWriter

    original_event = Collector.event
    original_append = TickWriter.append

    def fail_event(self, kind, **fields):
        """Fail event writes after the archive failure has actually occurred."""
        if self.archive and self.archive.error:
            raise OSError(5, "synthetic event write failure")
        return original_event(self, kind, **fields)

    def fail_append(self, item):
        """Inject an errno or hold the real writer while the real queue fills."""
        if fault == "enospc":
            raise OSError(28, "synthetic ENOSPC")
        if fault == "queue":
            threading.Event().wait(20)
        return original_append(self, item)

    Collector.event = fail_event
    TickWriter.append = fail_append
if os.environ.get("FUTU_TEST_SEAL_HANDSHAKE") == "1":
    real_unlink = os.unlink

    def seal_handshake(path, *args, **kwargs):
        """Forward the actual unlink before notifying the parent and blocking."""
        real_unlink(path, *args, **kwargs)
        if str(path).endswith(".jsonl.open"):
            print("sealed", flush=True)  # noqa: T201
            sys.stdin.readline()

    os.unlink = seal_handshake
main()

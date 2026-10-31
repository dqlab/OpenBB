"""Test-only subprocess adapter: route the official endpoint to a local server."""

import os
import sys
import threading

from openbb_futu.collector import Collector, main
from openbb_futu.ws import URL
from websockets.asyncio.client import connect

original_init = Collector.__init__


def local_connect(url, **kwargs):
    """Confine the injected connection to the test loopback server."""
    assert url == URL
    return connect(f"ws://127.0.0.1:{int(os.environ['FUTU_LOOPBACK_PORT'])}", **kwargs)


def init(self, config, store, root):
    """Inject a loopback-only factory in this test subprocess."""
    original_init(self, config, store, root, connection_factory=local_connect)


Collector.__init__ = init
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
    real_rename = os.rename

    def seal_handshake(source, target, *args, **kwargs):
        """Forward the actual rename before notifying the parent and blocking."""
        real_rename(source, target, *args, **kwargs)
        if str(source).endswith(".jsonl.open"):
            print("sealed", flush=True)  # noqa: T201
            sys.stdin.readline()

    os.rename = seal_handshake
main()

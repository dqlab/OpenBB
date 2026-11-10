"""Push source lifecycle and synthetic JSONL implementation.

Sources call the supplied callback once per observed record. A false return means
stop; close must finish callbacks before the consumer closes its ingress.
"""

from __future__ import annotations

import importlib.metadata
import json
import threading
from pathlib import Path
from typing import Protocol


class TickSource(Protocol):
    """Shared method surface; construction, driving and status are adapter-specific."""

    failed: threading.Event

    def start(self) -> None: ...
    def status(self) -> dict: ...
    def stop_and_status(self) -> dict: ...
    def close(self, timeout: float = 5.0) -> bool: ...


def load_source(name: str):
    """Resolve exactly one installed adapter, rejecting ambiguous registrations."""
    entries = list(
        importlib.metadata.entry_points(group="openbb_tick_source", name=name)
    )
    if len(entries) != 1:
        raise RuntimeError(
            f"expected exactly one installed {name} tick source entry point"
        )
    return entries[0].load()


class ReplaySource:
    """Read each JSONL object as one callback; EOF is an ordinary completion."""

    def __init__(self, fixture: Path, on_tick, on_event):
        self.fixture = Path(fixture)
        self.on_tick = on_tick
        self.on_event = on_event
        self.failed = threading.Event()
        self.done = threading.Event()
        self._stop = threading.Event()
        self._lock = threading.RLock()
        self._state = "new"
        self._error: str | None = None
        self._count = 0
        self._thread: threading.Thread | None = None

    def start(self):
        with self._lock:
            if self._state != "new":
                raise RuntimeError("source already started")
            self._state = "running"
            self._thread = threading.Thread(target=self._run, daemon=True)
            self._thread.start()

    def _run(self):
        try:
            self.on_event("source_started", {"source": "replay"})
            with self.fixture.open(encoding="utf-8") as stream:
                for line in stream:
                    payload = json.loads(line)
                    if not isinstance(payload, dict):
                        raise ValueError("replay record must be a JSON object")
                    with self._lock:
                        if self._stop.is_set():
                            break
                    accepted = self.on_tick(payload)
                    with self._lock:
                        self._count += 1
                    if not accepted:
                        raise RuntimeError("ingress rejected replay callback")
                else:
                    with self._lock:
                        if self._state == "running" and not self._stop.is_set():
                            self._state = "eof"
            with self._lock:
                if self._state in ("running", "stopping"):
                    self._state = "stopped"
            self.on_event("source_end", self.status())
        except Exception as exc:
            with self._lock:
                self._state = "failed"
                self._error = f"{type(exc).__name__}: {exc}"
                self.failed.set()
            self.on_event("source_failed", self.status())
        finally:
            self.done.set()

    def status(self):
        with self._lock:
            return {
                "state": self._state,
                "callbacks": self._count,
                "error": self._error,
            }

    def stop_and_status(self):
        self._stop.set()
        with self._lock:
            if self._state in ("new", "running"):
                self._state = "stopping" if self._thread else "stopped"
            return self.status()

    def close(self, timeout=5.0):
        self.stop_and_status()
        if self._thread:
            if self._thread is not threading.current_thread():
                self._thread.join(max(0, timeout))
            if self._thread.is_alive():
                with self._lock:
                    self._state = "failed"
                    self._error = "source close timed out"
                    self.failed.set()
                return False
        return True

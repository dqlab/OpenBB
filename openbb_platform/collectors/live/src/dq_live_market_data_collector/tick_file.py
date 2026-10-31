"""Raw push snapshot JSONL primitives, independent of the polling journal.

The file is the log, not a claim of upstream completeness. A crash can lose
queued and written-but-unsynced records. Never repair an interrupted file in place.
"""

from __future__ import annotations

import base64
import json
import math
import os
import queue
import sys
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from datetime import timezone as tz
from pathlib import Path
from typing import Any, BinaryIO
from uuid import uuid4


def encode_value(value: Any) -> Any:
    """Copy SDK values to JSON-compatible values without rounding or loss."""
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if math.isfinite(value):
            return value
        return {
            "__float64__": (
                "nan" if math.isnan(value) else ("+inf" if value > 0 else "-inf")
            )
        }
    if isinstance(value, bytes):
        return {"__bytes_base64__": base64.b64encode(value).decode("ascii")}
    if isinstance(value, (list, tuple)):
        return [encode_value(item) for item in value]
    if isinstance(value, dict):
        if not all(isinstance(key, str) for key in value):
            raise TypeError("raw field names must be strings")
        return {key: encode_value(item) for key, item in value.items()}
    raise TypeError(f"unsupported SDK field type: {type(value).__name__}")


def inspect_file(path: Path) -> dict[str, Any]:
    """Inspect the validated complete prefix without changing the evidence."""
    count = offset = 0
    error = None
    first_seq = last_seq = None
    with path.open("rb") as stream:
        while line := stream.readline():
            if not line.endswith(b"\n"):
                error = "incomplete_line"
                break
            try:
                record = json.loads(
                    line.decode("utf-8"),
                    parse_constant=lambda _: (_ for _ in ()).throw(ValueError()),
                )
            except (ValueError, UnicodeDecodeError):
                error = "invalid_json"
                break
            if not isinstance(record, dict) or "local_seq" not in record:
                error = "missing_sequence"
                break
            if isinstance(record, dict) and "local_seq" in record:
                seq = record["local_seq"]
                if not isinstance(seq, int) or isinstance(seq, bool) or seq < 1:
                    error = "invalid_sequence"
                    break
                if last_seq is not None and seq != last_seq + 1:
                    error = "noncontiguous_sequence"
                    break
                first_seq = seq if first_seq is None else first_seq
                last_seq = seq
            count += 1
            offset = stream.tell()
    size = path.stat().st_size
    return {
        "file": str(path),
        "records": count,
        "first_seq": first_seq,
        "last_seq": last_seq,
        "valid_bytes": offset,
        "unread_bytes": size - offset,
        "error": error,
    }


def _sync_directory(path: Path) -> None:
    """Persist directory entries, including newly created ancestor directories."""
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _make_durable_directory(path: Path) -> None:
    missing = []
    current = path
    while not current.exists():
        missing.append(current)
        current = current.parent
    for directory in reversed(missing):
        directory.mkdir(exist_ok=True)
        _sync_directory(directory.parent)


@dataclass(frozen=True)
class RotationLimits:
    """Limits governing when an open segment is sealed."""

    max_bytes: int
    max_seconds: float


@dataclass(frozen=True)
class SourceMetadata:
    """Source identity and SDK versions recorded with each snapshot."""

    profile: str
    environment: str
    wrapper_version: str
    native_api_version: str


@dataclass
class IngressCounts:
    """Callback accounting, updated while holding the ingress lock."""

    received: int = 0
    enqueued: int = 0
    rejected: int = 0
    rejection_reasons: dict[str, int] = field(default_factory=dict)


class TickWriter:
    """Single-owner exclusive append segments; callers serialize access."""

    def __init__(
        self,
        root: Path,
        source_id: str,
        max_bytes: int = 256 * 1024 * 1024,
        max_seconds: float = 3600,
    ):
        if not source_id or not all(
            c.isascii() and (c.isalnum() or c in "-_") for c in source_id
        ):
            raise ValueError(
                "source_id must contain only ASCII letters, digits, '-' or '_'"
            )
        if max_bytes <= 0 or max_seconds <= 0:
            raise ValueError("rotation limits must be positive")
        self.root, self.source_id = Path(root), source_id
        self.rotation = RotationLimits(max_bytes, max_seconds)
        self.session_id = uuid4().hex
        self.part = 0
        self.stream: BinaryIO | None = None
        self.path: Path | None = None
        self.date: str | None = None
        self.opened = 0.0
        self.written = self.durable = 0

    def _open(self, date: str) -> None:
        directory = (
            self.root
            / f"source_id={self.source_id}"
            / f"recv_date={date}"
            / f"session={self.session_id}"
        )
        _make_durable_directory(directory)
        self.part += 1
        self.path = directory / f"part-{self.part:06d}.jsonl.open"
        self.stream = self.path.open("xb")
        _sync_directory(directory)
        self.date, self.opened = date, time.monotonic()

    def append(self, record: dict[str, Any]) -> None:
        encoded = (
            json.dumps(
                record, ensure_ascii=False, allow_nan=False, separators=(",", ":")
            )
            + "\n"
        ).encode("utf-8")
        date = record["received_at_utc"][:10]
        if self.stream and (
            date != self.date
            or self.stream.tell() + len(encoded) > self.rotation.max_bytes
            or time.monotonic() - self.opened >= self.rotation.max_seconds
        ):
            self.seal()
        if self.stream is None:
            self._open(date)
        assert self.stream is not None
        self.stream.write(encoded)
        self.written = record["local_seq"]

    def sync(self) -> None:
        if self.stream:
            self.stream.flush()
            os.fsync(self.stream.fileno())
            self.durable = self.written

    def seal(self) -> None:
        if self.stream:
            self.sync()
            self.stream.close()
            self.stream = None
            assert self.path is not None
            target = self.path.with_suffix("")
            os.rename(self.path, target)
            _sync_directory(target.parent)
            self.path = target

    close = seal


class TickIngress:
    """Bounded callback-to-writer bridge. Stop the source when callback returns False.

    A single callback lock establishes local sequence and enqueue ordering. Source
    code must stop delivering before calling close; close has a bounded wait.
    """

    def __init__(
        self,
        writer: TickWriter,
        *,
        profile: str,
        environment: str,
        capacity: int = 10000,
        sync_seconds: float = 1.0,
        wrapper_version: str = "unknown",
        native_api_version: str = "unknown",
    ):
        if capacity <= 0 or sync_seconds <= 0:
            raise ValueError("capacity and sync interval must be positive")
        self.writer = writer
        self.source = SourceMetadata(
            profile, environment, wrapper_version, native_api_version
        )
        self.queue: queue.Queue[dict[str, Any]] = queue.Queue(maxsize=capacity)
        self.lock = threading.Lock()
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.sync_seconds = sync_seconds
        self.counts = IngressCounts()
        self.coverage_lost = False
        self.error: str | None = None
        self.thread.start()

    def callback(
        self, payload: dict[str, Any], *, received_at=None, monotonic_ns=None
    ) -> bool:

        utc = tz.utc  # noqa: UP017
        now = received_at or datetime.now(utc).isoformat().replace("+00:00", "Z")
        monotonic = monotonic_ns if monotonic_ns is not None else time.monotonic_ns()
        with self.lock:
            self.counts.received += 1
            if self.stop.is_set():
                self.counts.rejected += 1
                self.counts.rejection_reasons["stopped"] = (
                    self.counts.rejection_reasons.get("stopped", 0) + 1
                )
                return False
            try:
                copy = encode_value(payload() if callable(payload) else payload)
                record = {
                    "schema_version": "ctp_raw_snapshot_v1",
                    "record_type": "market_snapshot",
                    "source_id": self.writer.source_id,
                    "source_profile": self.source.profile,
                    "source_environment": self.source.environment,
                    "session_id": self.writer.session_id,
                    "local_seq": self.counts.received,
                    "received_at_utc": now,
                    "received_monotonic_ns": monotonic,
                    "monotonic_scope": "process",
                    "native_api_version": self.source.native_api_version,
                    "wrapper_version": self.source.wrapper_version,
                    "adapter_version": "unknown",
                    "field_encoding": (
                        "SDK-decoded values; finite binary64 JSON; tagged nonfinite and bytes v1"
                    ),
                    "clock": (
                        "UTC system clock and process monotonic_ns; monotonic is not "
                        "comparable across processes"
                    ),
                    "native_exchange_id": copy.get("ExchangeID"),
                    "native_instrument_id": copy.get("InstrumentID"),
                    "raw_payload": copy,
                }
                self.queue.put_nowait(record)
            except Exception as exc:
                self.counts.rejected += 1
                reason = getattr(exc, "rejection_reason", "ingress_failure")
                self.counts.rejection_reasons[reason] = (
                    self.counts.rejection_reasons.get(reason, 0) + 1
                )
                if reason == "retired_generation":
                    try:
                        self.queue.put_nowait(
                            {
                                "schema_version": "ctp_raw_snapshot_v1",
                                "record_type": "rejected_callback",
                                "source_id": self.writer.source_id,
                                "session_id": self.writer.session_id,
                                "local_seq": self.counts.received,
                                "received_at_utc": now,
                                "received_monotonic_ns": monotonic,
                                "reason": reason,
                            }
                        )
                    except Exception as queue_exc:
                        self.error = (
                            f"rejection evidence failure: {type(queue_exc).__name__}: "
                            f"{queue_exc}"
                        )
                        self.stop.set()
                        return False
                    return True
                self.error = f"ingress failure: {type(exc).__name__}: {exc}"
                self.stop.set()
                return False
            self.counts.enqueued += 1
            return True

    def _run(self) -> None:
        next_sync = time.monotonic() + self.sync_seconds
        try:
            while not self.stop.is_set() or not self.queue.empty():
                try:
                    record = self.queue.get(timeout=min(self.sync_seconds, 0.1))
                except queue.Empty:
                    record = None
                if record is not None:
                    self.writer.append(record)
                    self.queue.task_done()
                if time.monotonic() >= next_sync:
                    self.writer.sync()
                    next_sync = time.monotonic() + self.sync_seconds
            self.writer.close()
        except Exception as exc:  # disk errors must reach stderr even when disk is full
            self.error = f"writer failure: {type(exc).__name__}: {exc}"
            print(self.error, file=sys.stderr, flush=True)
            self.stop.set()

    def close(self, timeout: float = 10.0) -> bool:
        self.stop.set()
        self.thread.join(timeout)
        if self.thread.is_alive():
            self.error = (
                "writer shutdown timed out; queued or unsynced records may be lost"
            )
        return not self.thread.is_alive() and self.error is None

    def status(self) -> dict[str, Any]:
        return {
            "received": self.counts.received,
            "enqueued": self.counts.enqueued,
            "rejected": self.counts.rejected,
            "rejection_reasons": dict(self.counts.rejection_reasons),
            "coverage_complete": (
                not self.coverage_lost
                and self.counts.rejected == 0
                and self.error is None
            ),
            "written_seq": self.writer.written,
            "durable_seq": self.writer.durable,
            "queue_depth": self.queue.qsize(),
            "current_file": str(self.writer.path) if self.writer.path else None,
            "error": self.error,
        }

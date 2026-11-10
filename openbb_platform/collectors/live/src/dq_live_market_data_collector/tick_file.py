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
import re
import stat
import sys
import threading
import time
from dataclasses import dataclass, field, replace
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


@dataclass
class SegmentState:
    """Path, timing and held identity for the writer's current segment."""

    path: Path | None = None
    date: str | None = None
    opened: float = 0.0
    chain: Any = None
    identity: tuple[int, int] | None = None


class TickWriter:
    """Single-owner exclusive append segments; callers serialize all access.

    Limit changes take effect at the next append, including for an open segment.
    An optional directory_chain_factory(parts, create=True) supplies a held
    chain with fd, check() and close(). Its root must outlive the writer.
    Identity checks detect replacement but cannot make moves atomic with I/O.
    """

    def __init__(
        self,
        root: Path,
        source_id: str,
        max_bytes: int = 256 * 1024 * 1024,
        max_seconds: float = 3600,
        *,
        directory_chain_factory=None,
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
        self._segment = SegmentState()
        self.written = self.durable = 0
        self._chain_factory = directory_chain_factory

    @property
    def path(self) -> Path | None:
        """Return the current segment's path."""
        return self._segment.path

    @path.setter
    def path(self, value: Path | None) -> None:
        self._segment.path = value

    @property
    def date(self) -> str | None:
        """Return the current segment's receive date."""
        return self._segment.date

    @date.setter
    def date(self, value: str | None) -> None:
        self._segment.date = value

    @property
    def opened(self) -> float:
        """Return the monotonic time at which the segment opened."""
        return self._segment.opened

    @opened.setter
    def opened(self, value: float) -> None:
        self._segment.opened = value

    @property
    def _chain(self):
        return self._segment.chain

    @_chain.setter
    def _chain(self, value):
        self._segment.chain = value

    @property
    def _identity(self) -> tuple[int, int] | None:
        return self._segment.identity

    @_identity.setter
    def _identity(self, value: tuple[int, int] | None) -> None:
        self._segment.identity = value

    @property
    def max_bytes(self) -> int:
        """Maximum segment size checked before each append."""
        return self.rotation.max_bytes

    @max_bytes.setter
    def max_bytes(self, value: int) -> None:
        if value <= 0:
            raise ValueError("rotation limits must be positive")
        self.rotation = replace(self.rotation, max_bytes=value)

    @property
    def max_seconds(self) -> float:
        """Maximum segment age checked before each append."""
        return self.rotation.max_seconds

    @max_seconds.setter
    def max_seconds(self, value: float) -> None:
        if value <= 0:
            raise ValueError("rotation limits must be positive")
        self.rotation = replace(self.rotation, max_seconds=value)

    def _open(self, date: str) -> None:
        if self._chain_factory is not None:
            if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", date):
                raise ValueError("invalid received date")
            parts = [
                f"source_id={self.source_id}",
                f"recv_date={date}",
                f"session={self.session_id}",
            ]
            chain = self._chain_factory(parts, create=True)
            self._segment.chain = chain
            self.part += 1
            self._segment.path = self.root.joinpath(
                *parts, f"part-{self.part:06d}.jsonl.open"
            )
            try:
                chain.check()
                fd = os.open(
                    self._segment.path.name,
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                    0o600,
                    dir_fd=chain.fd,
                )
                self.stream = os.fdopen(fd, "wb", buffering=0)
                info = os.fstat(self.stream.fileno())
                self._segment.identity = (info.st_dev, info.st_ino)
                chain.check()
                os.fsync(chain.fd)
                chain.check()
                self._segment.date, self._segment.opened = date, time.monotonic()
            except BaseException:
                if self.stream is not None:
                    self.stream.close()
                    self.stream = None
                chain.close()
                self._segment.chain = None
                raise
            return
        directory = (
            self.root
            / f"source_id={self.source_id}"
            / f"recv_date={date}"
            / f"session={self.session_id}"
        )
        _make_durable_directory(directory)
        self.part += 1
        self._segment.path = directory / f"part-{self.part:06d}.jsonl.open"
        self.stream = self._segment.path.open("xb")
        _sync_directory(directory)
        self._segment.date, self._segment.opened = date, time.monotonic()

    def append(self, record: dict[str, Any]) -> None:
        encoded = (
            json.dumps(
                record, ensure_ascii=False, allow_nan=False, separators=(",", ":")
            )
            + "\n"
        ).encode("utf-8")
        date = record["received_at_utc"][:10]
        if self.stream and (
            date != self._segment.date
            or self.stream.tell() + len(encoded) > self.rotation.max_bytes
            or time.monotonic() - self._segment.opened >= self.rotation.max_seconds
        ):
            self.seal()
        if self.stream is None:
            if self._segment.chain is not None:
                raise RuntimeError("segment sealing incomplete")
            self._open(date)
        assert self.stream is not None
        if self._segment.chain is not None:
            remaining = memoryview(encoded)
            while remaining:
                self._check_anchored_file()
                count = self.stream.write(remaining)
                if not count:
                    raise OSError("short segment write")
                remaining = remaining[count:]
            self._check_anchored_file()
        else:
            self.stream.write(encoded)
        self.written = record["local_seq"]

    def sync(self) -> None:
        if self.stream:
            if self._segment.chain is not None:
                self._check_anchored_file()
            self.stream.flush()
            os.fsync(self.stream.fileno())
            if self._segment.chain is not None:
                self._check_anchored_file()
            self.durable = self.written

    def _check_anchored_file(self) -> None:
        assert self._segment.chain is not None and self._segment.path is not None
        self._segment.chain.check()
        info = os.stat(
            self._segment.path.name,
            dir_fd=self._segment.chain.fd,
            follow_symlinks=False,
        )
        if (
            not stat.S_ISREG(info.st_mode)
            or (info.st_dev, info.st_ino) != self._segment.identity
        ):
            raise OSError("open segment replaced")

    def seal(self) -> None:
        if self._segment.chain is not None:
            if self.stream:
                self.sync()
                self._check_anchored_file()
                self.stream.close()
                self.stream = None
            self._check_anchored_file()
            assert self._segment.path is not None
            target = self._segment.path.with_suffix("")
            os.link(
                self._segment.path.name,
                target.name,
                src_dir_fd=self._segment.chain.fd,
                dst_dir_fd=self._segment.chain.fd,
                follow_symlinks=False,
            )
            self._segment.chain.check()
            os.fsync(self._segment.chain.fd)
            self._check_anchored_file()
            os.unlink(self._segment.path.name, dir_fd=self._segment.chain.fd)
            os.fsync(self._segment.chain.fd)
            self._segment.chain.check()
            self._segment.path = target
            self._segment.chain.close()
            self._segment.chain = None
            return
        if self.stream:
            self.sync()
            self.stream.close()
            self.stream = None
            assert self._segment.path is not None
            target = self._segment.path.with_suffix("")
            os.rename(self._segment.path, target)
            _sync_directory(target.parent)
            self._segment.path = target

    close = seal

    def abort(self) -> None:
        """Release held descriptors without sealing or removing evidence.

        Use after a failed write or sync when retrying publication is unsafe.
        """
        try:
            if self.stream is not None:
                self.stream.close()
        finally:
            self.stream = None
            if self._segment.chain is not None:
                self._segment.chain.close()
                self._segment.chain = None


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

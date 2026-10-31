"""Offline raw-frame ingress using the existing live collector's TickWriter.

No network session is provided here. The caller must stop producing on False.
"""

from __future__ import annotations

import hashlib
import json
import os
import queue
import threading
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path

from dq_live_market_data_collector.tick_file import TickWriter, inspect_file

from openbb_futu.contracts import business_frame


def recover_open_files(root: Path) -> list[dict]:
    """Copy interrupted segments into immutable prefix and tail evidence under the owner lock."""
    reports = []
    for path in sorted(root.glob("source_id=futu/recv_date=*/session=*/*.jsonl.open")):
        inspection = inspect_file(path)
        directory = root / "futu-recovery" / uuid.uuid4().hex
        directory.mkdir(parents=True, mode=0o700)
        with path.open("rb") as source:
            for name, size in (
                ("valid-prefix.jsonl", inspection["valid_bytes"]),
                ("tail.bin", inspection["unread_bytes"]),
            ):
                with (directory / name).open("xb") as target:
                    length = size
                    while length:
                        chunk = source.read(min(length, 1024 * 1024))
                        if not chunk:
                            raise OSError("interrupted source changed during recovery")
                        target.write(chunk)
                        length -= len(chunk)
                    target.flush()
                    os.fsync(target.fileno())
        report = {
            **inspection,
            "evidence_dir": str(directory),
            "gap": "unresolved_after_process_interruption",
        }
        with (directory / "recovery.json").open("x", encoding="utf-8") as target:
            json.dump(report, target)
            target.flush()
            os.fsync(target.fileno())
        for folder in (directory, directory.parent, root):
            fd = os.open(folder, os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
        reports.append(report)
    # A sealed segment can precede the final manifest if the process dies in close().
    for path in sorted(root.glob("source_id=futu/recv_date=*/session=*/*.jsonl")):
        session = path.parent.name.removeprefix("session=")
        inspection = inspect_file(path)
        run_ids = set()
        with path.open(encoding="utf-8") as source:
            for line in source:
                try:
                    run_ids.add(json.loads(line)["collector_run_id"])
                except (ValueError, KeyError, TypeError):
                    run_ids.add("unknown")
        manifest = root / "futu-manifests" / f"{session}.json"
        try:
            with manifest.open(encoding="utf-8") as source:
                recorded = json.load(source)
            listed = {entry["file"] for entry in recorded["files"]}
            if (
                run_ids == {recorded["run_id"]}
                and recorded["session_id"] == session
                and str(path) in listed
            ):
                continue
        except (OSError, ValueError, KeyError, TypeError):
            pass
        directory = root / "futu-recovery" / uuid.uuid4().hex
        directory.mkdir(parents=True, mode=0o700)
        report = {
            **inspection,
            "session_id": session,
            "run_ids": sorted(run_ids),
            "evidence_dir": str(directory),
            "gap": "unresolved_after_process_interruption",
        }
        with (directory / "recovery.json").open("x", encoding="utf-8") as target:
            json.dump(report, target)
            target.flush()
            os.fsync(target.fileno())
        for folder in (directory, directory.parent, root):
            fd = os.open(folder, os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
        reports.append(report)
    return reports


MAX_FRAME_BYTES = 1024 * 1024
MAX_LINE_BYTES = 2 * 1024 * 1024 + 8192
MAX_QUEUE_BYTES = 64 * 1024 * 1024
MAX_SEGMENTS = 10000
MAX_SECRETS = 256


class BoundedWriter(TickWriter):
    """Limit newly created segments without modifying historical evidence."""

    def _open(self, date):
        if self.part >= MAX_SEGMENTS:
            raise ValueError("segment budget exhausted")
        super()._open(date)


@dataclass
class ArchiveIdentity:
    """Identity attached to each archived frame."""

    run_id: str
    transport: str


@dataclass
class ArchiveAccounting:
    """Ingress and queue accounting, guarded by the archive lock."""

    received: int = 0
    enqueued: int = 0
    rejected: int = 0
    queued_bytes: int = 0
    active_secrets: set[str] = field(default_factory=set)


class Archive:
    """Handle Archive."""

    def __init__(
        self,
        root: Path,
        *,
        run_id: str,
        capacity: int = 1000,
        sync_seconds: float = 1,
        max_bytes: int = 128 * 1024 * 1024,
        max_seconds: float = 3600,
        transport: str = "websocket",
    ):
        if capacity < 1 or sync_seconds <= 0 or not run_id:
            raise ValueError("invalid archive settings")
        self.writer = BoundedWriter(root, "futu", max_bytes, max_seconds)
        self.identity = ArchiveIdentity(run_id, transport)
        self.queue: queue.Queue[tuple[dict, int]] = queue.Queue(maxsize=capacity)
        self.sync_seconds = sync_seconds
        self.lock = threading.Lock()
        self.stop = threading.Event()
        self.accounting = ArchiveAccounting()
        self.error: str | None = None
        self.error_errno: int | None = None
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    @property
    def received(self):
        """Return the number of frames accepted for classification."""
        return self.accounting.received

    @property
    def rejected(self):
        """Return the number of rejected frames."""
        return self.accounting.rejected

    @rejected.setter
    def rejected(self, value):
        self.accounting.rejected = value

    def add_secrets(self, values):
        """Retain known secrets until shutdown or fail closed at the budget."""
        with self.lock:
            combined = self.accounting.active_secrets.union(values)
            if len(combined) > MAX_SECRETS or any(
                len(s.encode("utf-8")) > 8192 for s in combined
            ):
                self.error = "secret budget exhausted"
                self.stop.set()
                raise ValueError("secret budget exhausted")
            self.accounting.active_secrets = combined

    def accept(self, raw: str, connection_id: str) -> bool:
        """Handle accept."""
        with self.lock:
            self.accounting.received += 1
            if self.stop.is_set():
                self.accounting.rejected += 1
                return False
            try:
                if (
                    not isinstance(raw, str)
                    or len(raw.encode("utf-8")) > MAX_FRAME_BYTES
                ):
                    raise ValueError("frame budget exhausted")
                record = business_frame(
                    raw,
                    run_id=self.identity.run_id,
                    connection_id=connection_id,
                    local_seq=self.accounting.received,
                    active_secrets=tuple(self.accounting.active_secrets),
                )
                size = len(
                    (
                        json.dumps(
                            record,
                            ensure_ascii=False,
                            allow_nan=False,
                            separators=(",", ":"),
                        )
                        + "\n"
                    ).encode("utf-8")
                )
                if (
                    size > MAX_LINE_BYTES
                    or self.accounting.queued_bytes + size > MAX_QUEUE_BYTES
                ):
                    raise ValueError("archive byte budget exhausted")
                self.queue.put_nowait((record, size))
                self.accounting.queued_bytes += size
            except Exception as exc:
                self.accounting.rejected += 1
                # Do not log raw input or exception text, which might contain secrets.
                self.error = f"ingress failure: {type(exc).__name__}"
                self.error_errno = exc.errno if isinstance(exc, OSError) else None
                self.stop.set()
                return False
            self.accounting.enqueued += 1
            return True

    def accept_record(self, record: dict) -> bool:
        """Enqueue a validated non-WebSocket envelope under the same limits."""
        with self.lock:
            self.accounting.received += 1
            try:
                if self.stop.is_set():
                    raise ValueError("stopped")
                record = {
                    **record,
                    "collector_run_id": self.identity.run_id,
                    "local_seq": self.accounting.received,
                }
                size = len(
                    (
                        json.dumps(
                            record,
                            ensure_ascii=False,
                            allow_nan=False,
                            separators=(",", ":"),
                        )
                        + "\n"
                    ).encode()
                )
                if (
                    size > MAX_LINE_BYTES
                    or self.accounting.queued_bytes + size > MAX_QUEUE_BYTES
                ):
                    raise ValueError("archive byte budget exhausted")
                self.queue.put_nowait((record, size))
                self.accounting.queued_bytes += size
                self.accounting.enqueued += 1
                return True
            except Exception as exc:
                self.accounting.rejected += 1
                self.error = f"ingress failure: {type(exc).__name__}"
                self.stop.set()
                return False

    def _run(self):
        deadline = time.monotonic() + self.sync_seconds
        try:
            while not self.stop.is_set() or not self.queue.empty():
                try:
                    item = self.queue.get(timeout=min(self.sync_seconds, 0.1))
                except queue.Empty:
                    item = None
                if item is not None:
                    record, size = item
                    self.writer.append(record)
                    with self.lock:
                        self.accounting.queued_bytes -= size
                    self.queue.task_done()
                if time.monotonic() >= deadline:
                    self.writer.sync()
                    deadline = time.monotonic() + self.sync_seconds
            self.writer.close()
        except Exception as exc:
            self.error = f"writer failure: {type(exc).__name__}"
            self.error_errno = exc.errno if isinstance(exc, OSError) else None
            self.stop.set()

    def close(self, timeout: float = 10) -> dict:
        """Handle close."""
        self.stop.set()
        self.thread.join(timeout)
        if self.thread.is_alive():
            self.error = "writer shutdown timed out"
        result: dict = {
            "schema_version": "futu_archive_manifest_v1",
            "provider": "futu",
            "api_family": "futunn_http_ws",
            "transport": self.identity.transport,
            "session_id": self.writer.session_id,
            "run_id": self.identity.run_id,
            "received_frames": self.accounting.received,
            "enqueued_frames": self.accounting.enqueued,
            "rejected_frames": self.accounting.rejected,
            "written_seq": self.writer.written,
            "durable_seq": self.writer.durable,
            "queue_depth": self.queue.qsize(),
            "error": self.error,
            "error_errno": self.error_errno,
            "coverage_partial": True,
            "uncertain_loss": bool(
                self.error
                or self.accounting.rejected
                or self.queue.qsize()
                or self.writer.durable != self.accounting.received
            ),
            "files": [],
        }
        if not self.thread.is_alive():
            session = self.writer.session_id
            previous_seq = None
            for path in sorted(
                Path(self.writer.root).glob(
                    f"source_id=futu/recv_date=*/session={session}/*.jsonl"
                )
            ):
                digest = hashlib.sha256()
                with path.open("rb") as source:
                    while chunk := source.read(1024 * 1024):
                        digest.update(chunk)
                inspection = inspect_file(path)
                if inspection["error"] or (
                    previous_seq is not None
                    and inspection["first_seq"] != previous_seq + 1
                ):
                    self.error = result["error"] = "sealed segment validation failed"
                previous_seq = inspection["last_seq"]
                result["files"].append(
                    {
                        **inspection,
                        "bytes": path.stat().st_size,
                        "sha256": digest.hexdigest(),
                    }
                )
            # Exclusive creation ensures a prior run's evidence cannot be overwritten.
            directory = Path(self.writer.root) / "futu-manifests"
            directory.mkdir(parents=True, exist_ok=True)
            manifest = directory / f"{session}.json"
            with manifest.open("x", encoding="utf-8") as stream:
                json.dump(result, stream, ensure_ascii=False, indent=2)
                stream.write("\n")
                stream.flush()
                os.fsync(stream.fileno())
            fd = os.open(directory, os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
            result["manifest"] = str(manifest)
        return result

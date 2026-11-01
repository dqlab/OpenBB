"""Offline raw-frame ingress using the existing live collector's TickWriter.

No network session is provided here. The caller must stop producing on False.
"""

from __future__ import annotations

import contextlib
import ctypes
import errno
import hashlib
import io
import json
import os
import queue
import re
import stat
import sys
import threading
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path

from dq_live_market_data_collector.tick_file import TickWriter, inspect_file

from openbb_futu.contracts import business_frame
from openbb_futu.directory_chain import DirectoryChain
from openbb_futu.read_budget import Budget, BudgetExceeded, EvidenceChanged
from openbb_futu.root_handle import RootHandle


def _rename_noreplace(dir_fd, source, destination):
    """Atomically publish without aliases or replacement; fail closed elsewhere."""
    if sys.platform != "linux":
        raise OSError(errno.ENOTSUP, "atomic no-replace rename unavailable")
    libc = ctypes.CDLL(None, use_errno=True)
    try:
        rename = libc.renameat2
    except AttributeError as exc:
        raise OSError(errno.ENOTSUP, "renameat2 unavailable") from exc
    rename.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
    rename.restype = ctypes.c_int
    if rename(dir_fd, os.fsencode(source), dir_fd, os.fsencode(destination), 1):
        code = ctypes.get_errno()
        raise OSError(code, os.strerror(code), destination)


def _inspect_recovery(path, data, budget):
    """Inspect a bounded snapshot using the tick file prefix semantics."""
    count = offset = 0
    first_seq = last_seq = None
    error = None
    run_ids = set()
    for line in io.BytesIO(data):
        budget.charge_line()
        if len(line) > budget.line_limit:
            raise ValueError("recovery line too large")
        if not line.endswith(b"\n"):
            error = "incomplete_line"
            break
        try:
            record = json.loads(line, parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
        except (ValueError, UnicodeDecodeError):
            error = "invalid_json"
            break
        if not isinstance(record, dict) or "local_seq" not in record:
            error = "missing_sequence"
            break
        seq = record["local_seq"]
        if type(seq) is not int or seq < 1:
            error = "invalid_sequence"
            break
        if last_seq is not None and seq != last_seq + 1:
            error = "noncontiguous_sequence"
            break
        run_id = record.get("collector_run_id")
        if not isinstance(run_id, str) or not run_id:
            run_ids.add("unknown")
        else:
            run_ids.add(run_id)
        first_seq = seq if first_seq is None else first_seq
        last_seq = seq
        count += 1
        offset += len(line)
    return ({"file": str(path), "records": count, "first_seq": first_seq,
             "last_seq": last_seq, "valid_bytes": offset,
             "unread_bytes": len(data) - offset, "error": error}, run_ids)


def recover_open_files(root: Path, *, budget=None, root_handle=None, on_unresolved=None) -> list[dict]:
    """Save interrupted evidence after a bounded, read-only preflight.

    A budget is shared by all dates, sessions, segments and manifests in this
    invocation. Preflight failure leaves source files and existing evidence intact.
    Evidence already written by a failed publication is retained.
    """
    root = Path(root).absolute()
    if root_handle is not None:
        if root != root_handle.path:
            raise ValueError("recovery root does not match held root")
        root_handle.check()
        return _recover_open_files(root, root_handle, budget=budget, on_unresolved=on_unresolved)
    with RootHandle(root) as handle:
        return _recover_open_files(root, handle, budget=budget, on_unresolved=on_unresolved)


class SealedSegmentError(ValueError):
    """A sealed segment failed preflight; expose only canonical generated names."""

    category = "sealed_segment_invalid"

    def __init__(self, root, path, reason):
        relative = path.relative_to(root).as_posix()
        self.relative_path = relative if re.fullmatch(
            r"source_id=futu/recv_date=\d{4}-\d{2}-\d{2}/session=[0-9a-f]{32}/part-\d{6}\.jsonl", relative
        ) else None
        self.reason = reason
        super().__init__("sealed segment " + reason)


def _recover_open_files(root, handle, *, budget, on_unresolved):
    budget = budget if budget is not None else Budget(line_limit=MAX_LINE_BYTES)
    # Keep every scanned ancestor open until publication has finished.
    with contextlib.ExitStack() as stack:
        try:
            source = stack.enter_context(DirectoryChain(handle, ["source_id=futu"]))
        except FileNotFoundError:
            return []

        def entries(chain, on_entry=None):
            chain.check()
            with os.scandir(chain.fd) as scan:
                names = []
                for entry in scan:
                    if on_entry is not None:
                        on_entry(entry.name)
                    if budget.entries_left <= 0:
                        raise BudgetExceeded("directory entry budget exceeded")
                    budget.entries_left -= 1
                    names.append((entry.name, identity(entry.stat(follow_symlinks=False))))
            chain.check()
            return sorted(names)

        def identity(st):
            return (st.st_dev, st.st_ino, st.st_mode, st.st_size,
                    st.st_mtime_ns, st.st_ctime_ns)

        def read(chain, name, maximum):
            chain.check()
            fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK,
                         dir_fd=chain.fd)
            try:
                initial = identity(os.fstat(fd))
                if not stat.S_ISREG(initial[2]):
                    raise ValueError("evidence is not a regular file")
                chunks, total = [], 0
                while True:
                    chunk = os.read(fd, min(budget.chunk_size, maximum - total + 1,
                                            budget.bytes_left + 1))
                    if not chunk:
                        break
                    total += len(chunk)
                    budget.charge_read(len(chunk))
                    if total > maximum:
                        raise BudgetExceeded("evidence byte budget exceeded")
                    chunks.append(chunk)
                if identity(os.fstat(fd)) != initial:
                    raise EvidenceChanged("evidence changed during read")
                check(chain, name, initial)
                return b"".join(chunks), initial
            finally:
                os.close(fd)

        def check(chain, name, expected):
            chain.check()
            try:
                current = os.stat(name, dir_fd=chain.fd, follow_symlinks=False)
            except OSError as exc:
                raise EvidenceChanged("evidence changed") from exc
            if identity(current) != expected:
                raise EvidenceChanged("evidence replaced or changed")

        candidates = []
        notified = False

        def found_entry(name):
            nonlocal notified
            if name.endswith(".jsonl.open") and on_unresolved is not None and not notified:
                notified = True
                on_unresolved()

        for date_name, date_identity in entries(source):
            if not date_name.startswith("recv_date="):
                continue
            date = stack.enter_context(DirectoryChain(handle, ["source_id=futu", date_name]))
            if identity(os.fstat(date.fd)) != date_identity:
                raise EvidenceChanged("discovered date replaced")
            for session_name, session_identity in entries(date):
                if not session_name.startswith("session="):
                    continue
                session = stack.enter_context(DirectoryChain(
                    handle, ["source_id=futu", date_name, session_name]))
                if identity(os.fstat(session.fd)) != session_identity:
                    raise EvidenceChanged("discovered session replaced")
                for name, file_identity in entries(session, found_entry):
                    if name.endswith((".jsonl", ".jsonl.open")):
                        candidates.append((session_name.removeprefix("session="), session,
                                           root / "source_id=futu" / date_name / session_name / name, file_identity))
        pending, identities, absent_manifests = [], [], []
        manifest_dir = None
        for session, chain, path, discovered in candidates:
            if path.name.endswith(".open") and on_unresolved is not None and not notified:
                notified = True
                on_unresolved()
            data, stamp = read(chain, path.name, 1024 * 1024 * 1024)
            if stamp != discovered:
                raise EvidenceChanged("discovered segment replaced")
            identities.append((chain, path.name, stamp))
            inspection, run_ids = _inspect_recovery(path, data, budget)
            if not path.name.endswith(".open"):
                try:
                    if manifest_dir is None:
                        manifest_dir = stack.enter_context(DirectoryChain(handle, ["futu-manifests"]))
                    raw, stamp = read(manifest_dir, f"{session}.json", 8 * 1024 * 1024)
                except FileNotFoundError:
                    recorded = None
                    absent_manifests.append((manifest_dir, f"{session}.json"))
                else:
                    identities.append((manifest_dir, f"{session}.json", stamp))
                    recorded = json.loads(raw)
                    if not isinstance(recorded, dict) or not isinstance(recorded.get("files"), list):
                        raise ValueError("invalid recovery manifest")
                    listed = [entry for entry in recorded["files"] if entry["file"] == str(path)]
                    if inspection["error"] or "unknown" in run_ids:
                        raise SealedSegmentError(root, path, "corrupt_or_incomplete")
                    if (len(listed) == 1 and run_ids == {recorded["run_id"]}
                            and recorded["session_id"] == session):
                        entry = listed[0]
                        if entry.get("bytes") != len(data) or entry.get("sha256") != hashlib.sha256(data).hexdigest():
                            raise SealedSegmentError(root, path, "manifest_mismatch")
                        continue
                if inspection["error"] or "unknown" in run_ids:
                    raise SealedSegmentError(root, path, "unattributable")
            if on_unresolved is not None and not notified:
                notified = True
                on_unresolved()
            pending.append((session, path, data, inspection, run_ids))

        def check_sources():
            for item in identities:
                check(*item)
            for chain, name in absent_manifests:
                handle.check()
                if chain is None:
                    parent, missing = handle.fds[-1], "futu-manifests"
                else:
                    chain.check()
                    parent, missing = chain.fd, name
                try:
                    os.stat(missing, dir_fd=parent, follow_symlinks=False)
                except FileNotFoundError:
                    pass
                else:
                    raise EvidenceChanged("previously absent manifest or directory appeared")

        check_sources()
        return _publish_recovery(root, handle, pending, check_sources)


def _publish_recovery(root, handle, pending, check_sources):
    def identity_for_publish(st):
        # Linking changes ctime, so verify immutable identity and contents metadata.
        return (st.st_dev, st.st_ino, st.st_mode, st.st_size, st.st_mtime_ns)

    # Refuse unsupported platforms before creating any publication artifacts.
    if pending and sys.platform != "linux":
        raise OSError(errno.ENOTSUP, "atomic no-replace rename unavailable")
    reports = []
    for session, path, data, inspection, run_ids in pending:
        check_sources()
        handle.check()
        dirname = uuid.uuid4().hex
        directory = root / "futu-recovery" / dirname
        parent_fd = handle.directory(["futu-recovery"], create=True)
        try:
            os.mkdir(dirname, 0o700, dir_fd=parent_fd)
            handle.check()
            dir_fd = os.open(dirname, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent_fd)
            try:
                def check_directory():
                    handle.check()
                    for parent, name, fd in ((handle.fds[-1], "futu-recovery", parent_fd),
                                              (parent_fd, dirname, dir_fd)):
                        current = os.stat(name, dir_fd=parent, follow_symlinks=False)
                        held = os.fstat(fd)
                        if (current.st_dev, current.st_ino) != (held.st_dev, held.st_ino):
                            raise ValueError("recovery directory replaced")
                check_directory()
                report = {"schema_version": "futu_recovery_unknown_v2",
                          "file": str(path), "records": None, "first_seq": None,
                          "last_seq": None, "valid_bytes": None, "unread_bytes": None,
                          "error": "source_stability_unknown", "snapshot": inspection,
                          "evidence_dir": str(directory),
                          "gap": "unresolved_after_process_interruption",
                          "verification_status": "unknown",
                          "snapshot_status": "source_stability_not_guaranteed"}
                if not path.name.endswith(".open"):
                    report.update(session_id=session, run_ids=sorted(run_ids))
                files = []
                if path.name.endswith(".open"):
                    offset = inspection["valid_bytes"]
                    files.extend((("valid-prefix.jsonl", data[:offset]), ("tail.bin", data[offset:])))
                files.append(("recovery-unknown-v2.json", json.dumps(report).encode()))
                for name, content in files:
                    check_directory()
                    check_sources()
                    temp = ".tmp-" + uuid.uuid4().hex
                    fd = os.open(temp, os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                                 0o600, dir_fd=dir_fd)
                    # Keep the descriptor open through publication and cleanup so that
                    # cleanup can identify the object created by this invocation.
                    try:
                        with os.fdopen(fd, "w+b", closefd=False) as target:
                            target.write(content)
                            target.flush()
                            os.fsync(target.fileno())
                            target.seek(0)
                            if hashlib.sha256(target.read()).digest() != hashlib.sha256(content).digest():
                                raise ValueError("recovery evidence hash mismatch")
                            stamp = os.fstat(fd)
                        check_directory()
                        check_sources()
                        named = os.stat(temp, dir_fd=dir_fd, follow_symlinks=False)
                        if identity_for_publish(named) != identity_for_publish(stamp):
                            raise EvidenceChanged("recovery temporary changed")
                        _rename_noreplace(dir_fd, temp, name)
                        check_directory()
                        check_sources()
                        named = os.stat(name, dir_fd=dir_fd, follow_symlinks=False)
                        if identity_for_publish(named) != identity_for_publish(stamp):
                            raise EvidenceChanged("recovery evidence changed")
                    finally:
                        # An identity check followed by unlink races substitution.
                        # Retain unpublished .tmp files for manual review.
                        os.close(fd)
                os.fsync(dir_fd)
                os.fsync(parent_fd)
                os.fsync(handle.fds[-1])
                check_directory()
                check_sources()
            finally:
                os.close(dir_fd)
        finally:
            os.close(parent_fd)
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

    def seal(self):
        """Record each segment after successful sealing."""
        was_open = self._chain is not None
        identity = self._identity
        super().seal()
        if was_open:
            self.sealed_paths.append(self.path)
            self.sealed_identities.append(identity)

    close = seal


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
        _test_root_handle=None,
    ):
        if capacity < 1 or sync_seconds <= 0 or not run_id:
            raise ValueError("invalid archive settings")
        self._root_handle = _test_root_handle or RootHandle.create(root)
        root_stat = os.fstat(self._root_handle.fds[-1])
        self._root_identity = (root_stat.st_dev, root_stat.st_ino)
        self.writer = BoundedWriter(self._root_handle.path, "futu", max_bytes, max_seconds,
                                    directory_chain_factory=lambda parts, create=False: DirectoryChain(
                                        self._root_handle, parts, create=create))
        self.writer.sealed_paths = []
        self.writer.sealed_identities = []
        self.identity = ArchiveIdentity(run_id, transport)
        self.queue: queue.Queue[tuple[dict, int]] = queue.Queue(maxsize=capacity)
        self.sync_seconds = sync_seconds
        self.lock = threading.Lock()
        self.stop = threading.Event()
        self.accounting = ArchiveAccounting()
        self.publish_attempts = 0
        self.error: str | None = None
        self.error_errno: int | None = None
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    @classmethod
    def _isolated_for_test(cls, root, **kwargs):
        """Construct the anchored path for isolated tests."""
        handle = RootHandle(root)
        try:
            return cls(root, _test_root_handle=handle, **kwargs)
        except BaseException:
            handle.close()
            raise

    def _anchored_manifest(self, result):
        """Inspect the writer's sealed files and publish without replacing evidence."""
        root = self._root_handle
        previous = None
        for path, expected in zip(self.writer.sealed_paths, self.writer.sealed_identities):
            parts = path.relative_to(root.path).parts
            with DirectoryChain(root, parts[:-1]) as chain:
                chain.check()
                fd = os.open(parts[-1], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=chain.fd)
                try:
                    before = os.fstat(fd)
                    named = os.stat(parts[-1], dir_fd=chain.fd, follow_symlinks=False)
                    if (not stat.S_ISREG(before.st_mode) or (before.st_dev, before.st_ino) != expected
                            or (before.st_dev, before.st_ino) != (named.st_dev, named.st_ino)):
                        raise ValueError("sealed segment replaced")
                    if before.st_size > 1024 * 1024 * 1024:
                        raise ValueError("sealed segment too large")
                    with os.fdopen(fd, "rb", closefd=False) as stream:
                        data = stream.read(1024 * 1024 * 1024 + 1)
                    if len(data) != before.st_size:
                        raise ValueError("sealed segment size changed")
                    after = os.fstat(fd)
                    named = os.stat(parts[-1], dir_fd=chain.fd, follow_symlinks=False)
                    chain.check()
                    before_identity = (before.st_dev, before.st_ino, before.st_size,
                                       before.st_mtime_ns, before.st_ctime_ns)
                    after_identity = (after.st_dev, after.st_ino, after.st_size,
                                      after.st_mtime_ns, after.st_ctime_ns)
                    if before_identity != after_identity or (after.st_dev, after.st_ino) != (named.st_dev, named.st_ino):
                        raise ValueError("sealed segment changed")
                finally:
                    os.close(fd)
            inspection, _ = _inspect_recovery(path, data, Budget(line_limit=MAX_LINE_BYTES))
            if inspection["error"] or (previous is not None and inspection["first_seq"] != previous + 1):
                raise ValueError("sealed segment validation failed")
            previous = inspection["last_seq"]
            result["files"].append({**inspection, "bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()})
        stage = "manifest_parent_fsync"
        self.publish_attempts += 1
        attempt = self.publish_attempts
        try:
            with DirectoryChain(root, ["futu-manifests"], create=True) as chain:
                name = f"{self.writer.session_id}.json"
                temp = f".{self.writer.session_id}.{uuid.uuid4().hex}.tmp"
                chain.check()
                stage = "manifest_write"
                fd = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=chain.fd)
                with os.fdopen(fd, "wb") as stream:
                    stream.write((json.dumps(result, ensure_ascii=False, indent=2) + "\n").encode())
                    stream.flush()
                    stage = "manifest_file_fsync"
                    os.fsync(stream.fileno())
                    temp_info = os.fstat(stream.fileno())
                chain.check()
                named = os.stat(temp, dir_fd=chain.fd, follow_symlinks=False)
                if (not stat.S_ISREG(named.st_mode)
                        or (named.st_dev, named.st_ino) != (temp_info.st_dev, temp_info.st_ino)):
                    raise ValueError("manifest temporary replaced")
                stage = "manifest_publish"
                os.link(temp, name, src_dir_fd=chain.fd, dst_dir_fd=chain.fd, follow_symlinks=False)
                stage = "manifest_directory_fsync"
                os.fsync(chain.fd)
                chain.check()
                named = os.stat(name, dir_fd=chain.fd, follow_symlinks=False)
                temporary = os.stat(temp, dir_fd=chain.fd, follow_symlinks=False)
                if any((item.st_dev, item.st_ino) != (temp_info.st_dev, temp_info.st_ino)
                       or not stat.S_ISREG(item.st_mode) for item in (named, temporary)):
                    raise ValueError("manifest publication replaced")
                stage = "manifest_temp_cleanup"
                os.unlink(temp, dir_fd=chain.fd)
                os.fsync(chain.fd)
                chain.check()
        except Exception as exc:
            try:
                root.check()
                event = {"event": "manifest_commit_failed", "run_id": self.identity.run_id,
                         "session_id": self.writer.session_id, "stage": stage,
                         "publish_attempt": attempt,
                         "original_commit_failure": (attempt == 1 and stage != "manifest_temp_cleanup"
                             and not (stage == "manifest_publish" and isinstance(exc, FileExistsError))),
                         "errno": exc.errno if isinstance(exc, OSError) else None}
                event_name = f"futu-events-{self.identity.run_id}.jsonl"
                fd = os.open(event_name, os.O_RDWR | os.O_CREAT | os.O_APPEND | os.O_NOFOLLOW,
                             0o600, dir_fd=root.fds[-1])
                with os.fdopen(fd, "a+b") as log:
                    if not stat.S_ISREG(os.fstat(log.fileno()).st_mode):
                        raise ValueError("event log is not regular")
                    root.check()
                    log.seek(0, os.SEEK_END)
                    if log.tell():
                        log.seek(-1, os.SEEK_END)
                        if log.read(1) != b"\n":
                            log.write(b'\n{"event":"event_log_corrupt_tail"}\n')
                    log.write((json.dumps(event) + "\n").encode())
                    log.flush()
                    os.fsync(log.fileno())
                os.fsync(root.fds[-1])
            except Exception:  # noqa: S110
                pass
            raise
        result["manifest"] = str(root.path / "futu-manifests" / name)
        return result

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
            if len(combined) > MAX_SECRETS or any(len(s.encode("utf-8")) > 8192 for s in combined):
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
                if not isinstance(raw, str) or len(raw.encode("utf-8")) > MAX_FRAME_BYTES:
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
                if size > MAX_LINE_BYTES or self.accounting.queued_bytes + size > MAX_QUEUE_BYTES:
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
                if size > MAX_LINE_BYTES or self.accounting.queued_bytes + size > MAX_QUEUE_BYTES:
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
            # Preserve the original error and all evidence.
            with contextlib.suppress(Exception):
                self.writer.abort()

    def close(self, timeout: float = 10) -> dict:
        """Handle close."""
        self.stop.set()
        self.thread.join(timeout)
        if self.thread.is_alive():
            self.error = self.error or "writer shutdown timed out"
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
        if not self.thread.is_alive() and hasattr(self, "_root_handle"):
            if not self._root_handle.fds:
                self._root_handle = RootHandle(self.writer.root)
                reopened = os.fstat(self._root_handle.fds[-1])
                if (reopened.st_dev, reopened.st_ino) != self._root_identity:
                    self._root_handle.close()
                    raise ValueError("archive root replaced")
            try:
                return self._anchored_manifest(result)
            finally:
                self._root_handle.close()
        if not self.thread.is_alive():
            session = self.writer.session_id
            previous_seq = None
            for path in sorted(Path(self.writer.root).glob(f"source_id=futu/recv_date=*/session={session}/*.jsonl")):
                digest = hashlib.sha256()
                with path.open("rb") as source:
                    while chunk := source.read(1024 * 1024):
                        digest.update(chunk)
                inspection = inspect_file(path)
                if inspection["error"] or (previous_seq is not None and inspection["first_seq"] != previous_seq + 1):
                    self.error = result["error"] = "sealed segment validation failed"
                previous_seq = inspection["last_seq"]
                result["files"].append(
                    {
                        **inspection,
                        "bytes": path.stat().st_size,
                        "sha256": digest.hexdigest(),
                    }
                )
            directory = Path(self.writer.root) / "futu-manifests"
            manifest = directory / f"{session}.json"
            temp = directory / f".{session}.{uuid.uuid4().hex}.tmp"
            self.publish_attempts += 1
            attempt = self.publish_attempts
            stage = "manifest_write"
            try:
                missing = []
                ancestor = directory
                while not ancestor.exists():
                    missing.append(ancestor)
                    ancestor = ancestor.parent
                directory.mkdir(parents=True, exist_ok=True)
                stage = "manifest_parent_fsync"
                for child in reversed(missing):
                    fd = os.open(child.parent, os.O_RDONLY)
                    try:
                        os.fsync(fd)
                    finally:
                        os.close(fd)
                stage = "manifest_write"
                with temp.open("x", encoding="utf-8") as stream:
                    json.dump(result, stream, ensure_ascii=False, indent=2)
                    stream.write("\n")
                    stream.flush()
                    stage = "manifest_file_fsync"
                    os.fsync(stream.fileno())
                stage = "manifest_publish"
                os.link(temp, manifest)
                stage = "manifest_directory_fsync"
                fd = os.open(directory, os.O_RDONLY)
                try:
                    os.fsync(fd)
                finally:
                    os.close(fd)
                stage = "manifest_temp_cleanup"
                temp.unlink()
            except Exception as exc:
                # Failure evidence is best effort; never obscure the original exception.
                try:
                    event = {
                        "event": "manifest_commit_failed",
                        "run_id": self.identity.run_id,
                        "session_id": session,
                        "stage": stage,
                        "publish_attempt": attempt,
                        "original_commit_failure": (
                            attempt == 1
                            and stage != "manifest_temp_cleanup"
                            and (stage == "manifest_directory_fsync" or not manifest.exists())
                            and not (stage == "manifest_publish" and isinstance(exc, FileExistsError))
                        ),
                        "errno": exc.errno if isinstance(exc, OSError) else None,
                    }
                    event_path = Path(self.writer.root) / f"futu-events-{self.identity.run_id}.jsonl"
                    with event_path.open("a+b") as log:
                        log.seek(0, os.SEEK_END)
                        if log.tell():
                            log.seek(log.tell() - 1)
                            if log.read(1) != b"\n":
                                log.write(b'\n{"event":"event_log_corrupt_tail"}\n')
                        log.write((json.dumps(event) + "\n").encode("utf-8"))
                        log.flush()
                        os.fsync(log.fileno())
                except Exception:  # noqa: S110
                    pass  # Preserve the original failure.
                raise
            result["manifest"] = str(manifest)
        return result

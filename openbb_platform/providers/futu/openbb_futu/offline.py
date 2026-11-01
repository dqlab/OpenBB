"""Offline replay and read-only verification of sealed archive evidence."""

import hashlib
import json
import os
import re
import stat
import uuid
from pathlib import Path

from openbb_futu.archive import MAX_FRAME_BYTES, MAX_LINE_BYTES, MAX_SEGMENTS, Archive
from openbb_futu.read_budget import Budget, BudgetExceeded, EvidenceChanged


def _identity(info):
    return (info.st_dev, info.st_ino, info.st_mode, info.st_size, info.st_mtime_ns, info.st_ctime_ns)


def _strict_int(value):
    """Exclude bool and integer subclasses from sequence numbers."""
    return isinstance(value, int) and value.__class__ is int


def _valid_manifest_identity(path, session):
    return (
        isinstance(session, str)
        and bool(re.fullmatch(r"[0-9a-f]{32}", session))
        and path.name == f"{session}.json"
        and path.parent.name == "futu-manifests"
    )


def _sealed_path(path):
    return path.suffix == ".jsonl" and path.resolve() == path


def _segment_matches(entry, digest, size, records, *, first, last):
    expected = {
        "sha256": digest,
        "bytes": size,
        "records": records,
        "first_seq": first,
        "last_seq": last,
        "valid_bytes": size,
        "unread_bytes": 0,
        "error": None,
    }
    return all(entry[key] == value and (type(entry[key]) is int if type(value) is int else True)
               for key, value in expected.items())


def _manifest_matches(manifest, count):
    expected = {
        "schema_version": "futu_archive_manifest_v1",
        "provider": "futu",
        "rejected_frames": 0,
        "queue_depth": 0,
        "received_frames": count,
        "enqueued_frames": count,
        "written_seq": count,
        "durable_seq": count,
    }
    return (
        all(manifest.get(key) == value and (type(manifest.get(key)) is int if type(value) is int else True)
            for key, value in expected.items())
        and not manifest.get("error")
        and not manifest.get("uncertain_loss", False)
    )


def replay(source: Path, output: Path, *, _archive_factory=Archive) -> dict:
    """Feed JSONL frames through the same ingress and writer as the collector."""
    archive = _archive_factory(output, run_id=uuid.uuid4().hex)
    try:
        with source.open("rb") as stream:
            while True:
                line = stream.readline(MAX_FRAME_BYTES + 2)
                if not line:
                    break
                if not line.endswith(b"\n") or len(line) - 1 > MAX_FRAME_BYTES:
                    archive.error = "replay input line exceeds budget or is incomplete"
                    archive.rejected += 1
                    break
                try:
                    raw = line[:-1].decode("utf-8")
                except UnicodeDecodeError:
                    archive.error = "replay input is not UTF-8"
                    archive.rejected += 1
                    break
                if not archive.accept(raw, "offline-replay"):
                    break
    except OSError:
        archive.error = "replay input read failure"
    return archive.close()


def _verification(status, records, errors):
    return {
        "status": status,
        "records": records,
        "errors": errors,
        "integrity_status": status,
        "original_commit_status": "unknown",
        "original_commit_status_basis": "no_trustworthy_commit_confirmation",
    }


def _bounded_lines(stream, declared, budget):
    """Read at most declared bytes plus one probe, retaining at most one line."""
    pending = bytearray()
    total = 0
    while True:
        chunk = stream.read(min(65536, declared - total + 1, budget.bytes_left + 1))
        budget.charge_read(len(chunk))
        if not chunk:
            break
        total += len(chunk)
        if total > declared:
            raise ValueError("segment exceeds declared size")
        start = 0
        while start < len(chunk):
            end = chunk.find(b"\n", start)
            stop = len(chunk) if end < 0 else end + 1
            if len(pending) + stop - start > MAX_LINE_BYTES:
                raise ValueError("line too large")
            pending.extend(chunk[start:stop])
            if end >= 0:
                budget.charge_line()
                yield bytes(pending)
                pending.clear()
            start = stop
    if pending:
        raise ValueError("incomplete line")


def verify(  # noqa: PLR0912
    manifest_path: Path, *, on_record=None, on_manifest=None,
    max_segment_bytes=1024 * 1024 * 1024, budget=None, expected_manifest_identity=None,
) -> dict:
    """Verify all listed segments and their contiguous durable prefix, read only."""
    if type(max_segment_bytes) is not int or max_segment_bytes < 0:
        raise ValueError("invalid segment byte limit")
    budget = budget if budget is not None else Budget()
    manifest_path = Path(manifest_path).absolute()
    manifest_fd = budget.open_regular(manifest_path)
    with os.fdopen(manifest_fd, "rb", buffering=0) as stream:
        before_manifest = os.fstat(stream.fileno())
        if expected_manifest_identity is not None and _identity(before_manifest) != expected_manifest_identity:
            raise EvidenceChanged("manifest replaced")
        if not stat.S_ISREG(before_manifest.st_mode):
            raise ValueError("manifest is not a regular file")
        parts = []
        total = 0
        while True:
            chunk = stream.read(min(65536, 8 * 1024 * 1024 - total + 1, budget.bytes_left + 1))
            budget.charge_read(len(chunk))
            if not chunk:
                break
            parts.append(chunk)
            total += len(chunk)
            if total > 8 * 1024 * 1024:
                return _verification("failed", 0, ["manifest too large"])
        raw_manifest = b"".join(parts)
        if len(raw_manifest) > 8 * 1024 * 1024:
            return _verification("failed", 0, ["manifest too large"])
        manifest = json.loads(raw_manifest)
        if _identity(os.fstat(stream.fileno())) != _identity(before_manifest):
            return _verification("failed", 0, ["manifest changed"])
    if manifest_path.resolve() != manifest_path or _identity(manifest_path.stat()) != _identity(before_manifest):
        return _verification("failed", 0, ["manifest changed"])
    if not isinstance(manifest, dict) or not isinstance(manifest.get("files"), list):
        return _verification("failed", 0, ["invalid manifest"])
    if on_manifest is not None:
        on_manifest(manifest)
    errors = []
    expected = 1
    count = 0
    files = manifest["files"]
    session = manifest.get("session_id")
    if not _valid_manifest_identity(manifest_path, session) or not isinstance(files, list) or len(files) > MAX_SEGMENTS:
        return _verification("failed", 0, ["invalid manifest identity or files"])
    root = manifest_path.parent.parent.resolve()
    paths = []
    try:
        for entry in files:
            path = Path(entry["file"]).absolute()
            relative = path.relative_to(root)
            if (
                len(relative.parts) != 4
                or relative.parts[0] != "source_id=futu"
                or not re.fullmatch(r"recv_date=\d{4}-\d{2}-\d{2}", relative.parts[1])
                or relative.parts[2] != f"session={session}"
                or not _sealed_path(path)
            ):
                raise ValueError("invalid segment path")
            paths.append(path)
        if len(set(paths)) != len(paths):
            raise ValueError("duplicate segment")
        discovered: set = set()
        source = root / "source_id=futu"
        if source.is_symlink():
            raise ValueError("invalid source directory")
        for index, name in enumerate(budget.entries(source)):
            if not name.startswith("recv_date="):
                continue
            date = source / name
            if index >= MAX_SEGMENTS:
                raise ValueError("date directory budget")
            if date.is_symlink():
                raise ValueError("invalid date directory")
            directory = date / f"session={session}"
            if directory.is_symlink():
                raise ValueError("invalid session directory")
            for segment_name in budget.entries(directory):
                if not segment_name.endswith(".jsonl"):
                    continue
                path = directory / segment_name
                if path.is_symlink() or len(discovered) >= MAX_SEGMENTS:
                    raise ValueError("invalid segment or segment budget")
                discovered.add(path.absolute())
        if discovered != set(paths):
            raise ValueError("unlisted or missing sealed segment")
    except BudgetExceeded:
        raise
    except (OSError, ValueError, KeyError, TypeError):
        return _verification("failed", 0, ["invalid segment set or path"])
    identities = []
    for entry in files:
        path = Path(entry["file"])
        digest = hashlib.sha256()
        first = last = None
        records = size = 0
        try:
            if type(entry.get("bytes")) is not int or not 0 <= entry["bytes"] <= max_segment_bytes:
                raise ValueError("invalid segment size")
            fd = budget.open_regular(path)
            with os.fdopen(fd, "rb", buffering=0) as stream:
                initial = os.fstat(stream.fileno())
                if not stat.S_ISREG(initial.st_mode) or initial.st_size != entry["bytes"]:
                    raise ValueError("invalid segment size or type")
                for line in _bounded_lines(stream, entry["bytes"], budget):
                    digest.update(line)
                    size += len(line)
                    if not line.endswith(b"\n"):
                        raise ValueError("incomplete line")
                    record = json.loads(line)
                    seq = record["local_seq"]
                    if not _strict_int(seq) or seq != expected or record["collector_run_id"] != manifest["run_id"]:
                        raise ValueError("sequence or run mismatch")
                    expected += 1
                    records += 1
                    first = seq if first is None else first
                    last = seq
                    if on_record is not None:
                        on_record(record)
                if (
                    _identity(os.fstat(stream.fileno())) != _identity(initial)
                    or path.is_symlink()
                    or _identity(path.stat()) != _identity(initial)
                ):
                    raise ValueError("segment changed")
                identities.append((path, initial))
            if not _segment_matches(entry, digest.hexdigest(), size, records, first=first, last=last):
                raise ValueError("segment metadata mismatch")
            count += records
        except BudgetExceeded:
            raise
        except (OSError, ValueError, KeyError, TypeError, UnicodeError):
            errors.append("invalid segment: " + str(path))
            break
    try:
        if manifest_path.resolve() != manifest_path or _identity(manifest_path.stat()) != _identity(before_manifest):
            raise ValueError("manifest changed")
        if any(
            path.is_symlink() or path.resolve() != path or _identity(path.stat()) != _identity(info)
            for path, info in identities
        ):
            raise ValueError("segment changed")
        discovered_after = set()
        for index, name in enumerate(budget.entries(root / "source_id=futu")):
            if not name.startswith("recv_date="):
                continue
            date = root / "source_id=futu" / name
            if index >= MAX_SEGMENTS or date.is_symlink():
                raise ValueError("invalid date directory")
            directory = date / f"session={session}"
            if directory.is_symlink():
                raise ValueError("invalid session directory")
            for segment_name in budget.entries(directory):
                if not segment_name.endswith(".jsonl"):
                    continue
                path = directory / segment_name
                if path.is_symlink() or len(discovered_after) >= MAX_SEGMENTS:
                    raise ValueError("invalid segment")
                discovered_after.add(path.absolute())
        if discovered_after != set(paths):
            raise ValueError("segment set changed")
    except BudgetExceeded:
        raise
    except (OSError, ValueError):
        errors.append("archive changed during verification")
    if not _manifest_matches(manifest, count):
        errors.append("manifest status or durable prefix mismatch")
    return _verification("verified" if not errors else "failed", count, errors)

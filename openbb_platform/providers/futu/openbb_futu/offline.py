"""Offline replay and read-only verification of sealed archive evidence."""

import hashlib
import json
import re
import uuid
from pathlib import Path

from openbb_futu.archive import MAX_FRAME_BYTES, MAX_SEGMENTS, Archive


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
    return all(entry[key] == value for key, value in expected.items())


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
        all(manifest.get(key) == value for key, value in expected.items())
        and not manifest.get("error")
        and not manifest.get("uncertain_loss", False)
    )


def replay(source: Path, output: Path) -> dict:
    """Feed JSONL frames through the same ingress and writer as the collector."""
    archive = Archive(output, run_id=uuid.uuid4().hex)
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


def verify(manifest_path: Path) -> dict:
    """Verify all listed segments and their contiguous durable prefix, read only."""
    with manifest_path.open(encoding="utf-8") as stream:
        manifest = json.load(stream)
    errors = []
    expected = 1
    count = 0
    files = manifest["files"]
    session = manifest.get("session_id")
    if (
        not _valid_manifest_identity(manifest_path, session)
        or not isinstance(files, list)
        or len(files) > MAX_SEGMENTS
    ):
        return {
            "status": "failed",
            "records": 0,
            "errors": ["invalid manifest identity or files"],
        }
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
        for index, date in enumerate(source.glob("recv_date=*")):
            if index >= MAX_SEGMENTS:
                raise ValueError("date directory budget")
            if date.is_symlink():
                raise ValueError("invalid date directory")
            directory = date / f"session={session}"
            if directory.is_symlink():
                raise ValueError("invalid session directory")
            for path in directory.glob("*.jsonl"):
                if path.is_symlink() or len(discovered) >= MAX_SEGMENTS:
                    raise ValueError("invalid segment or segment budget")
                discovered.add(path.absolute())
        if discovered != set(paths):
            raise ValueError("unlisted or missing sealed segment")
    except (OSError, ValueError, KeyError, TypeError):
        return {
            "status": "failed",
            "records": 0,
            "errors": ["invalid segment set or path"],
        }
    for entry in files:
        path = Path(entry["file"])
        digest = hashlib.sha256()
        first = last = None
        records = size = 0
        try:
            with path.open("rb") as stream:
                for line in stream:
                    digest.update(line)
                    size += len(line)
                    if not line.endswith(b"\n"):
                        raise ValueError("incomplete line")
                    record = json.loads(line)
                    seq = record["local_seq"]
                    if (
                        not _strict_int(seq)
                        or seq != expected
                        or record["collector_run_id"] != manifest["run_id"]
                    ):
                        raise ValueError("sequence or run mismatch")
                    expected += 1
                    records += 1
                    first = seq if first is None else first
                    last = seq
            if not _segment_matches(
                entry, digest.hexdigest(), size, records, first=first, last=last
            ):
                raise ValueError("segment metadata mismatch")
            count += records
        except (OSError, ValueError, KeyError, TypeError, UnicodeError):
            errors.append("invalid segment: " + str(path))
            break
    if not _manifest_matches(manifest, count):
        errors.append("manifest status or durable prefix mismatch")
    return {
        "status": "verified" if not errors else "failed",
        "records": count,
        "errors": errors,
    }

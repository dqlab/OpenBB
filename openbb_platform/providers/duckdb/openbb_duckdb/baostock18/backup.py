"""Bounded manifest transport: no dependency paths are passed through argv."""

import ctypes
import hashlib
import json
import math
import os
import signal
import stat
import subprocess
import time
from itertools import chain, islice
from pathlib import Path

from openbb_duckdb.baostock18.contract import AdmissionError
from openbb_duckdb.baostock18.manifest import sha256

BATCH_PATHS = 512
BLOCK_BYTES = 8 * 1024 * 1024


def manifest_sources(manifest):
    """Read sorted, unique canonical absolute paths, one JSON string per line."""
    previous = ""
    with Path(manifest).open() as stream:
        for line in stream:
            value = json.loads(line)
            if not isinstance(value, str) or value <= previous:
                raise AdmissionError("Dependency manifest must contain sorted unique paths")
            path = Path(value)
            if not path.is_absolute() or path.resolve(strict=True) != path:
                raise AdmissionError("Dependency manifest requires canonical absolute paths")
            previous = value
            yield path


def backup_items(manifest):
    """Stream v2 receipts; preserve support for existing v1 backup receipts."""
    with Path(manifest).open() as stream:
        first = stream.readline()
        if first == '{"format":"baostock18-backup-files-v2"}\n':
            for line in stream:
                yield json.loads(line)
        else:
            stream.seek(0)
            value = json.load(stream)
            if value.get("format") != "baostock18-backup-files-v1" or not value.get("files"):
                raise AdmissionError("Invalid backup manifest")
            yield from value["files"]


def copy_manifest(manifest, destination, *, expected_files, expected_bytes,
                  estimated_seconds, max_seconds, first_source=None):
    """Copy batches of at most 512 paths and rehash every payload before success.

    Each batch has its own JSONL and NUL list. Rsync receives only the list's
    filename, never its contents as argv. A filesystem flush precedes independent
    payload hashing. A deadline requests SIGINT only on this owned rsync process,
    then waits for its exit; no forced kill or deletion is used.
    A failed/partial directory is retained and never has a success receipt.
    """
    if (type(expected_files) is not int or expected_files < 1
            or type(expected_bytes) is not int or expected_bytes < 0
            or not isinstance(estimated_seconds, (int, float))
            or not isinstance(max_seconds, (int, float))
            or not math.isfinite(estimated_seconds) or not math.isfinite(max_seconds)
            or not 0 < estimated_seconds <= max_seconds):
        raise AdmissionError("Measured file/byte plan and bounded time estimate required")
    started = time.monotonic()
    deadline = started + max_seconds
    destination = Path(destination)
    if not destination.is_absolute() or destination.exists():
        raise AdmissionError("Manifest backup requires a new absolute directory")
    manifest = Path(manifest)
    manifest_digest = sha256(manifest)
    destination.mkdir(parents=True, exist_ok=False)
    (destination / "batches").mkdir()

    def check_time():
        if time.monotonic() > deadline:
            raise AdmissionError(f"Backup deadline exceeded ({max_seconds} seconds); abort")

    plan = dict(transport="rsync-files-from-null-batches", max_batch_paths=BATCH_PATHS,
                expected_files=expected_files, expected_bytes=expected_bytes,
                estimated_seconds=estimated_seconds, max_seconds=max_seconds,
                dependency_manifest=str(manifest), dependency_manifest_sha256=manifest_digest)
    with (destination / "plan.json").open("x") as stream:
        json.dump(plan, stream, indent=2)
        stream.flush()
        os.fsync(stream.fileno())
    sources = manifest_sources(manifest)
    if first_source is not None:
        sources = chain([Path(first_source)], sources)
    sources = iter(sources)
    payload = destination / "payload"
    payload.mkdir()
    count = total = batches = 0
    with (destination / "manifest.jsonl").open("x") as ledger:
        ledger.write('{"format":"baostock18-backup-files-v2"}\n')
        while batch := list(islice(sources, BATCH_PATHS)):
            check_time()
            batches += 1
            with (destination / "batches" / f"{batches:08d}.jsonl").open("x") as stream:
                for source in batch:
                    stream.write(json.dumps(str(source)) + "\n")
                stream.flush()
                os.fsync(stream.fileno())
            entries = []
            for source in batch:
                check_time()
                if count and first_source is not None and source == Path(first_source):
                    raise AdmissionError("Duplicate catalog in dependency manifest")
                before = source.lstat()
                if not stat.S_ISREG(before.st_mode):
                    raise AdmissionError("Backup source must be a regular file")
                count += 1
                total += before.st_size
                if count > expected_files or total > expected_bytes:
                    raise AdmissionError("Backup exceeds approved file/byte plan")
                backup = payload / source.relative_to("/")
                source_hash = hashlib.sha256()
                fd = os.open(source, os.O_RDONLY | os.O_NOFOLLOW)
                with os.fdopen(fd, "rb") as inp:
                    opened = os.fstat(inp.fileno())
                    for block in iter(lambda: inp.read(BLOCK_BYTES), b""):
                        check_time()
                        source_hash.update(block)
                    closed = os.fstat(inp.fileno())
                after = source.stat()
                fields = ("st_size", "st_mtime_ns", "st_dev", "st_ino")
                if any(len({getattr(s, key) for s in (before, opened, closed, after)}) != 1
                       for key in fields):
                    raise AdmissionError("Backup source changed while hashing")
                entries.append((source, backup, before, source_hash.hexdigest()))
            file_list = destination / "batches" / f"{batches:08d}.files-from"
            with file_list.open("xb") as stream:
                for source in batch:
                    stream.write(os.fsencode(str(source.relative_to("/"))) + b"\0")
                stream.flush()
                os.fsync(stream.fileno())
            arguments = ["rsync", "--from0", "--files-from=" + str(file_list),
                         "--relative", "--whole-file", "--no-links", "--", "/", str(payload) + "/"]
            # No subprocess timeout helper: those helpers may escalate to SIGKILL.
            with (destination / "batches" / f"{batches:08d}.rsync.log").open("xb") as log:
                process = subprocess.Popen(arguments, stdout=log, stderr=log)  # noqa: S603
                expired = False
                while process.poll() is None:
                    if time.monotonic() > deadline and not expired:
                        process.send_signal(signal.SIGINT)
                        expired = True
                    time.sleep(0.1)
                if expired:
                    raise AdmissionError(
                        f"Backup deadline exceeded ({max_seconds} seconds); rsync exited {process.returncode}"
                    )
                if process.returncode:
                    raise AdmissionError(f"Rsync batch {batches} failed with exit {process.returncode}; see {log.name}")
            fd = os.open(payload, os.O_RDONLY | os.O_DIRECTORY)
            try:
                libc = ctypes.CDLL(None, use_errno=True)
                if libc.syncfs(fd) != 0:
                    raise OSError(ctypes.get_errno(), "Backup syncfs failed")
            finally:
                os.close(fd)
            check_time()
            for source, backup, before, digest in entries:
                backup_hash = hashlib.sha256()
                with backup.open("rb") as stream:
                    for block in iter(lambda: stream.read(BLOCK_BYTES), b""):
                        check_time()
                        backup_hash.update(block)
                after = source.stat()
                if any(getattr(before, key) != getattr(after, key) for key in fields):
                    raise AdmissionError("Backup source changed during rsync")
                if backup_hash.hexdigest() != digest or backup.stat().st_size != before.st_size:
                    raise AdmissionError("Backup copy hash/size mismatch")
                ledger.write(json.dumps(dict(source=str(source), backup=str(backup),
                                             size=before.st_size, sha256=digest)) + "\n")
            ledger.flush()
            os.fsync(ledger.fileno())
        check_time()
    if (count, total) != (expected_files, expected_bytes) or sha256(manifest) != manifest_digest:
        raise AdmissionError("Backup file/byte reconciliation or manifest identity mismatch")
    return dict(**plan, files=count, bytes=total, batches=batches,
                seconds=time.monotonic() - started)

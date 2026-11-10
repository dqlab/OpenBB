# Evidence schemas require exact built-in types, including rejection of bool-as-int.
# pylint: disable=unidiomatic-typecheck

"""The acquisition log: an append-only anchor for the manifest's own digest.

:mod:`.capture_manifest` chains every file *inside* one capture directory, but the
manifest is the chain's root and nothing hashes it.  An edit that rewrites the
status record and restates ``status_sha256`` in the manifest is therefore a
capture that verifies: two files, six untouched, no receipt or core certificate
involved.

This module adds one more file, deliberately **outside** the capture locator::

    <root>/acquisition-log.jsonl
    {"version": "etf-acquisition-log-v1",
     "sequence": 0, "locator": "raw-acquisition-v1/<sha>",
     "manifest_sha256": "<sha256 of that locator's acquire_manifest.json>"}

One line per finalized capture, appended with ``O_APPEND`` under an exclusive
``flock``, never rewritten in place.  :func:`verify` reads the log, requires a
line for the locator being read, and requires that line's ``manifest_sha256`` to
equal the digest of the manifest bytes on disk.  A missing file or a missing line
is ``acquisition_log_missing``; a line that disagrees -- or a log whose sequence
numbers are not ``0..n-1`` in order, whose lines are not canonical JSON with the
exact key set, or that names one locator twice -- is ``acquisition_log_mismatch``.

What this buys, exactly: the cheapest forgery of a capture's status is now three
files (the status record, the manifest and the log line) instead of two, and an
edit that leaves any of the three inconsistent is a typed refusal.  A gap in the
sequence numbers is visible, so deleting a line from the middle of the log makes
every capture in that store refuse, not just the one whose line was removed; a
line removed from the *end* of the log is visible when the capture it named still
exists, because that capture then has no log entry.

What it does **not** buy: an anchor.  The log is as unauthenticated as the
manifest, so a writer who rewrites the status record, the manifest **and** the
matching log line consistently produces a directory that verifies -- the barrier
raised is three coordinated files, not a signature.  Rewriting the log wholesale
(truncating it and renumbering it from ``0`` for the surviving captures) is also
undetectable from inside the store, and so is deleting a capture directory whose
line is then removed from the tail.  Those need an append-only medium this unit
does not have (a remote log, a signed head, a WORM volume).  And the log records
one digest per capture, so it says nothing about files it does not name.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
from pathlib import Path
from typing import Any

VERSION = "etf-acquisition-log-v1"
NAME = "acquisition-log.jsonl"
MAX_BYTES = 8 << 20
FIELDS = ("version", "sequence", "locator", "manifest_sha256")
_HASH = re.compile(r"[0-9a-f]{64}\Z")
_LOCATOR = re.compile(r"[A-Za-z0-9_.-]+/[0-9a-f]{64}\Z")


class LogError(ValueError):
    """A typed refusal about the anchor; nothing is appended or repaired."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


def _require(condition: bool, code: str) -> None:
    if not condition:
        raise LogError(code)


def canonical(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
    )


def path(root: Path | str) -> Path:
    """The log for one evidence root: a sibling of ``raw-acquisition-v1/``."""
    return Path(os.path.abspath(root)) / NAME


def _hash(value: Any) -> bool:
    return type(value) is str and bool(_HASH.fullmatch(value))


def _parse(content: bytes) -> list[dict[str, Any]]:
    """Every line, validated: canonical, exact key set, sequence ``0..n-1``."""
    if not content:
        return []
    # A partial trailing line is a torn append, not a short log: refuse it.
    _require(content.endswith(b"\n"), "acquisition_log_mismatch")
    rows: list[dict[str, Any]] = []
    for index, line in enumerate(content.split(b"\n")[:-1]):
        try:
            value = json.loads(line)
        except (ValueError, UnicodeError, RecursionError) as exc:
            raise LogError("acquisition_log_mismatch") from exc
        _require(
            type(value) is dict and set(value) == set(FIELDS),
            "acquisition_log_mismatch",
        )
        _require(value["version"] == VERSION, "acquisition_log_mismatch")
        # Stated, not implied: a deleted line leaves a gap, a duplicated one a
        # repeat, and either is refused for the whole store rather than one row.
        _require(
            type(value["sequence"]) is int and value["sequence"] == index,
            "acquisition_log_mismatch",
        )
        locator = value["locator"]
        _require(
            type(locator) is str and bool(_LOCATOR.fullmatch(locator)),
            "acquisition_log_mismatch",
        )
        _require(_hash(value["manifest_sha256"]), "acquisition_log_mismatch")
        # The bytes are canonical, so a re-indented or re-ordered line is refused
        # before anything is compared to it.
        _require(canonical(value).encode() == line, "acquisition_log_mismatch")
        rows.append(value)
    # One final capture, one line: the same locator twice is not a history.
    _require(
        len({row["locator"] for row in rows}) == len(rows), "acquisition_log_mismatch"
    )
    return rows


def entries(root: Path | str) -> list[dict[str, Any]]:
    """Every entry, in order; a missing or malformed log is a typed refusal."""
    target = path(root)
    _require(target.exists(), "acquisition_log_missing")
    _require(target.is_file() and not target.is_symlink(), "acquisition_log_mismatch")
    content = target.read_bytes()
    _require(len(content) <= MAX_BYTES, "acquisition_log_mismatch")
    return _parse(content)


def _read_all(descriptor: int) -> bytes:
    os.lseek(descriptor, 0, os.SEEK_SET)
    chunks: list[bytes] = []
    while True:
        chunk = os.read(descriptor, 1 << 20)
        if not chunk:
            return b"".join(chunks)
        chunks.append(chunk)


def append(root: Path | str, locator: str, manifest_sha256: str) -> dict[str, Any]:
    """Append (or confirm) the line for one finalized capture.

    The write is one ``O_APPEND`` write of one line under an exclusive ``flock``,
    so two captures finalizing at once get distinct sequence numbers and no line
    is ever rewritten.  A locator that already has a line must have the same
    digest -- a replay of the same capture is a no-op, a different digest is a
    refusal, never an append.
    """
    _require(
        type(locator) is str and bool(_LOCATOR.fullmatch(locator)),
        "acquisition_log_mismatch",
    )
    _require(_hash(manifest_sha256), "acquisition_log_mismatch")
    target = path(root)
    try:
        descriptor = os.open(
            target, os.O_RDWR | os.O_CREAT | os.O_APPEND | os.O_NOFOLLOW, 0o600
        )
    except OSError as exc:  # a symlink, a directory, a permission refusal
        raise LogError("acquisition_log_mismatch") from exc
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        content = _read_all(descriptor)
        _require(len(content) <= MAX_BYTES, "acquisition_log_mismatch")
        rows = _parse(content)
        for row in rows:
            if row["locator"] == locator:
                _require(
                    row["manifest_sha256"] == manifest_sha256,
                    "acquisition_log_mismatch",
                )
                return row
        row = {
            "version": VERSION,
            "sequence": len(rows),
            "locator": locator,
            "manifest_sha256": manifest_sha256,
        }
        line = (canonical(row) + "\n").encode()
        written = os.write(descriptor, line)
        _require(written == len(line), "acquisition_log_mismatch")
        os.fsync(descriptor)
        return row
    finally:
        os.close(descriptor)


def verify(root: Path | str, locator: str, manifest_sha256: str) -> dict[str, Any]:
    """The line for ``locator`` must exist and must state this manifest digest."""
    rows = entries(root)
    matches = [row for row in rows if row["locator"] == locator]
    _require(bool(matches), "acquisition_log_missing")
    _require(len(matches) == 1, "acquisition_log_mismatch")
    _require(
        matches[0]["manifest_sha256"] == manifest_sha256, "acquisition_log_mismatch"
    )
    return matches[0]


def digest(root: Path | str, locator: str) -> str:
    """The digest of one capture's manifest bytes, exactly as they are on disk."""
    target = Path(os.path.abspath(root)) / locator / "acquire_manifest.json"
    _require(target.is_file() and not target.is_symlink(), "missing_manifest")
    return hashlib.sha256(target.read_bytes()).hexdigest()

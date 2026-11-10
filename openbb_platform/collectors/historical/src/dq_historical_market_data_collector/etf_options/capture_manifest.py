# Evidence schemas require exact built-in types, including rejection of bool-as-int.
# pylint: disable=unidiomatic-typecheck

"""The capture's integrity manifest: one record that chains every file it owns.

``raw-acquisition-v1`` chains its own evidence: each receipt binds the task
digest, ``terminal.json`` binds the receipts' digests, and ``durable.json`` binds
the terminal's.  What that chain cannot bind is a file that lives *beside* it --
the per-request status record (:mod:`.capture_status`) -- because the receipt's
key set is fixed and ``openbb_collector_core`` is read-only for this unit.

A status record bound only to the capture's *identity* (task digest, task id,
generation, receipt sha256 by index) can be relabelled in one field with every
other file untouched: ``503`` becomes ``200`` and all three readers -- ``read``,
``verify`` and ``main`` -- agree with the forgery, because nothing outside the
file says what the status was.

``acquire_manifest.json`` closes that.  It is written once, into the same
immutable locator directory, and states the sha256 of every file the finalized
capture consists of::

    <root>/raw-acquisition-v1/<locator>/acquire_manifest.json
    {"version": "etf-acquire-manifest-v1",
     "task_id": ..., "generation": ...,
     "task_sha256": <sha256 of task.json>,
     "receipts": [<sha256 of responses/000000/receipt.json>, ...],
     "status_sha256": <sha256 of status.json>,
     "terminal_sha256": <sha256 of terminal.json>,
     "durable_sha256": <sha256 of durable.json>}

:func:`check` recomputes all of them from the bytes on disk and refuses typed
(``status_binding``) on any disagreement, so any edit that leaves this record
inconsistent with the bytes it names is a refusal, not a claim.

The manifest is the chain's root, so it needs an anchor of its own: the digest of
these bytes is appended, out of band, to ``<root>/acquisition-log.jsonl``
(:mod:`.capture_log`), which lives outside the capture locator.  :func:`check`
verifies the log line for this locator as well.

Two deliberate properties:

* **No clock field.**  The record is a pure statement about bytes, so a replay
  recomputes exactly the content that was installed: the no-clobber equality
  holds and a replay cannot rewrite it.
* **Written for a finalized capture only.**  ``terminal.json``/``durable.json``
  are what make a capture finalized, and they are installed once; a capture that
  is still resumable has no terminal to chain, so it has no manifest either, and
  ``acquire.verify`` refuses it as not complete (it can never report success).
  A process that dies between the status install and this install leaves a
  capture that refuses ``missing_manifest`` -- fail closed, never silently
  unbound.  The same is true one step later: the log line is appended after the
  manifest is installed, so a death in between leaves ``acquisition_log_missing``.

What this is **not**: a signature, and not a trust anchor either.  The guarantee
is exactly this, and no wider:

* any **single-file** edit -- of ``status.json``, of this manifest, of a receipt,
  of ``terminal.json`` or ``durable.json`` -- is a typed refusal;
* any edit that leaves **this manifest or the acquisition log** inconsistent with
  the bytes on disk is a typed refusal.  That includes the previously accepted
  ``{status.json, acquire_manifest.json}`` forgery: restating ``status_sha256``
  changes the manifest's own digest, and the log line still states the old one
  (``acquisition_log_mismatch``);
* a **coordinated** rewrite of the status record, this manifest **and** the
  matching log line is *not* detectable: three files, no secret, nothing signed.
  Rewriting the log wholesale, or deleting a capture together with its log line,
  is likewise outside what this can see.

So the barrier is "three coordinated files, and no leftover inconsistency", never
"any proper subset".  Closing the three-file case needs an append-only medium this
unit does not own (a remote log, a signed head, a WORM volume).
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import uuid
from pathlib import Path
from typing import Any

from . import capture_log

VERSION = "etf-acquire-manifest-v1"
NAME = "acquire_manifest.json"
MAX_BYTES = 65536
FIELDS = (
    "version",
    "task_id",
    "generation",
    "task_sha256",
    "receipts",
    "status_sha256",
    "terminal_sha256",
    "durable_sha256",
)
#: The core layout this manifest hashes.  ``raw-acquisition-v1`` fixes all four.
TASK = "task.json"
TERMINAL = "terminal.json"
DURABLE = "durable.json"
RESPONSES = "responses"
RECEIPT = "receipt.json"
_HASH = re.compile(r"[0-9a-f]{64}\Z")


class ManifestError(ValueError):
    """A typed refusal about the integrity chain; nothing is repaired."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


def _require(condition: bool, code: str) -> None:
    if not condition:
        raise ManifestError(code)


def canonical(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
    )


def _encode(record: dict) -> bytes:
    return (canonical(record) + "\n").encode()


def path(root: Path | str, locator: str) -> Path:
    return Path(os.path.abspath(root)) / locator / NAME


def _hash(value: Any) -> bool:
    return type(value) is str and bool(_HASH.fullmatch(value))


def _content(target: Path) -> bytes:
    """Read one chained file; a symlink or a non-regular file is a refusal."""
    _require(target.is_file() and not target.is_symlink(), "chain_incomplete")
    return target.read_bytes()


def _sha(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def receipt_path(directory: Path, index: int) -> Path:
    return directory / RESPONSES / f"{index:06d}" / RECEIPT


def build(
    root: Path | str,
    locator: str,
    state: dict[str, Any],
    *,
    status_name: str,
) -> dict[str, Any]:
    """The manifest a finalized capture on disk implies.

    ``status_name`` is supplied by the owner of the status record
    (:mod:`.capture_status`) so no second copy of that filename exists here.
    """
    _require(state["complete"] is True, "chain_incomplete")
    _require(state["locator"] == locator, "status_binding")
    directory = Path(os.path.abspath(root)) / locator
    task = state["task"]
    return {
        "version": VERSION,
        "task_id": task["task_id"],
        "generation": task["generation"],
        "task_sha256": _sha(_content(directory / TASK)),
        "receipts": [
            _sha(_content(receipt_path(directory, index)))
            for index in range(len(state["responses"]))
        ],
        "status_sha256": _sha(_content(directory / status_name)),
        "terminal_sha256": _sha(_content(directory / TERMINAL)),
        "durable_sha256": _sha(_content(directory / DURABLE)),
    }


def _install(target: Path, content: bytes) -> None:
    """Atomic no-clobber install; an existing record must be byte-identical."""
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        _require(not target.is_symlink() and target.is_file(), "invalid_manifest")
        _require(target.read_bytes() == content, "status_binding")
        return
    temporary = target.with_name(f".{target.name}.{uuid.uuid4().hex}.pending")
    with temporary.open("xb") as stream:
        stream.write(content)
        stream.flush()
        os.fsync(stream.fileno())
    try:
        os.link(temporary, target)
    except FileExistsError:
        _require(target.read_bytes() == content, "status_binding")
    _sync(target.parent)
    temporary.unlink()
    _sync(target.parent)


def _sync(directory: Path) -> None:
    descriptor = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _reject_duplicates(pairs):
    result = {}
    for key, value in pairs:
        _require(key not in result, "invalid_manifest")
        result[key] = value
    return result


def _decode(content: bytes) -> dict:
    try:
        value = json.loads(content, object_pairs_hook=_reject_duplicates)
    except (ValueError, UnicodeError, RecursionError) as exc:
        raise ManifestError("invalid_manifest") from exc
    _require(type(value) is dict, "invalid_manifest")
    return value


def write(
    root: Path | str,
    state: dict[str, Any],
    *,
    status_name: str,
) -> dict[str, Any]:
    """Install the manifest of a finalized capture and return it.

    Called once, immediately after the status record is installed.  An existing
    manifest must be byte-identical: the record has no clock, so a replay of the
    same capture recomputes the same bytes, and any difference means the tree
    changed after it was written -- a typed refusal, never a rewrite.

    Once the manifest is on disk its digest is appended to the acquisition log,
    which is what makes a later rewrite of this file together with the status
    record visible (``acquisition_log_mismatch``).  The append comes second and
    is idempotent for an identical digest, so a replay adds nothing.
    """
    record = build(root, state["locator"], state, status_name=status_name)
    content = _encode(record)
    _install(path(root, state["locator"]), content)
    try:
        capture_log.append(root, state["locator"], _sha(content))
    except capture_log.LogError as exc:
        raise ManifestError(str(exc)) from exc
    return record


def read(root: Path | str, locator: str) -> dict[str, Any]:
    """Read the manifest of one capture, structurally and canonically."""
    target = path(root, locator)
    _require(target.is_file() and not target.is_symlink(), "missing_manifest")
    content = target.read_bytes()
    _require(len(content) <= MAX_BYTES, "manifest_size_limit")
    record = _decode(content)
    _require(_encode(record) == content, "noncanonical_manifest")
    _require(
        set(record) == set(FIELDS) and record["version"] == VERSION, "invalid_manifest"
    )
    _require(
        all(
            _hash(record[key])
            for key in (
                "task_sha256",
                "status_sha256",
                "terminal_sha256",
                "durable_sha256",
            )
        ),
        "invalid_manifest",
    )
    receipts = record["receipts"]
    _require(
        type(receipts) is list and all(_hash(item) for item in receipts),
        "invalid_manifest",
    )
    _require(
        type(record["task_id"]) is str
        and bool(record["task_id"])
        and type(record["generation"]) is str
        and bool(record["generation"]),
        "invalid_manifest",
    )
    return record


def check(
    root: Path | str,
    locator: str,
    state: dict[str, Any],
    *,
    status_name: str,
) -> dict[str, Any]:
    """Recompute every chained hash and refuse on any disagreement.

    The manifest is the only file outside ``raw-acquisition-v1``'s own chain, so
    this is where a lone edit of ``status.json`` -- the review's 503 -> 200 --
    stops being invisible: the status bytes no longer hash to what the manifest
    states (``status_binding``).
    """
    record = read(root, locator)
    task = state["task"]
    _require(
        record["task_id"] == task["task_id"]
        and record["generation"] == task["generation"],
        "status_binding",
    )
    expected = build(root, locator, state, status_name=status_name)
    _require(record == expected, "status_binding")
    # The manifest is the root of the chain, so the bytes above are not enough on
    # their own: the out-of-band log states what this manifest's digest was when
    # the capture was finalized.  An edit of the manifest -- alone, or together
    # with the status record whose hash it restates -- no longer matches it.
    try:
        capture_log.verify(root, locator, capture_log.digest(root, locator))
    except capture_log.LogError as exc:
        raise ManifestError(str(exc)) from exc
    return record

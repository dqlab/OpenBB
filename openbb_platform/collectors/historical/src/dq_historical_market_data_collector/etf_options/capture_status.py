# Evidence schemas require exact built-in types, including rejection of bool-as-int.
# pylint: disable=unidiomatic-typecheck

"""The HTTP status of each captured request, as evidence rather than a console field.

``raw-acquisition-v1``'s receipt has a fixed key set with **no** status slot, and
``openbb_collector_core`` is read-only for this unit, so the status is persisted
beside the receipts it describes, inside the same immutable locator directory::

    <root>/raw-acquisition-v1/<locator>/status.json
    {"version": "etf-capture-status-v1",
     "task_sha256": ..., "task_id": ..., "generation": ...,
     "statuses": [{"index": 0, "sha256": <receipt sha>, "http_status": 503}],
     "observed_at": "2026-10-20T21:40:00.000000Z"}

Why a sibling file rather than a slot in the receipt: the receipt is written by a
frozen dependency this unit may not edit.  Why it is still evidence rather than a
log line: it lives in the capture directory, it is installed no-clobber, it is
canonical JSON with an exact key set, it is **bound** -- to the task digest and to
each receipt's sha256 by index -- and, for a finalized capture, it is **chained**:
``acquire_manifest.json`` beside it states the sha256 of every file the capture
consists of, this record included, and :func:`read` recomputes them all
(:mod:`.capture_manifest`).  Identity binding alone could not see a lone
``503`` -> ``200`` edit; the chain can, because the status bytes no longer hash to
what the manifest states (``status_binding``).

The manifest is the root of that chain, so its own digest is anchored outside the
capture directory, in ``<root>/acquisition-log.jsonl`` (:mod:`.capture_log`): a
forgery that rewrites this record *and* restates its hash in the manifest leaves
the manifest bytes different from the digest the log states
(``acquisition_log_mismatch``).  Three coordinated files are needed now, not two
-- and a coordinated rewrite of all three is still undetectable without a
signature.  :func:`read` is the one function that enforces this; it always
requires the capture's ``state``, so no caller can read a status without it.

An ``http_status`` of ``null`` is not a default and not a hole: it means no
transport status was observed for that index (a durable prefix resumed from a
crashing process, or the aggregate lane where the provider performs the GETs), and
:func:`unrecorded` names those indices.  A capture with an unrecorded request is
reported ``status_unrecorded`` -- never ``captured``.
"""

from __future__ import annotations

import hashlib
import json
import os
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from . import capture_manifest

VERSION = "etf-capture-status-v1"
NAME = "status.json"
MAX_BYTES = 65536
FIELDS = (
    "version",
    "task_sha256",
    "task_id",
    "generation",
    "statuses",
    "observed_at",
)
STATUS_FIELDS = ("index", "sha256", "http_status")
#: The only terminal that means "the source answered and the bytes are its 200".
CAPTURED = "captured"
TERMINALS = (
    CAPTURED,
    "http_error",
    "transport_error",
    "status_unrecorded",
    "incomplete",
    "cancelled",
    "refused",
)


class StatusError(ValueError):
    """A typed refusal about a status record; nothing is repaired."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


def _require(condition: bool, code: str) -> None:
    if not condition:
        raise StatusError(code)


def canonical(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
    )


def envelope_sha(value: Any) -> str:
    """The digest ``raw-acquisition-v1`` uses for a task or a receipt."""
    return hashlib.sha256((canonical(value) + "\n").encode()).hexdigest()


def _now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def path(root: Path | str, locator: str) -> Path:
    return Path(os.path.abspath(root)) / locator / NAME


def _encode(record: dict) -> bytes:
    _json(record)
    return (canonical(record) + "\n").encode()


def _json(value: Any, depth: int = 0) -> None:
    _require(depth <= 32, "invalid_status")
    if type(value) is dict:
        _require(all(type(key) is str for key in value), "invalid_status")
        for item in value.values():
            _json(item, depth + 1)
    elif type(value) is list:
        for item in value:
            _json(item, depth + 1)
    else:
        _require(type(value) in (str, int, bool, type(None)), "invalid_status")


def _install(target: Path, content: bytes) -> None:
    """Atomic no-clobber install; an existing record must be byte-identical."""
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        _require(target.read_bytes() == content, "status_conflict")
        _sync_file(target)
        return
    temporary = target.with_name(f".{target.name}.{uuid.uuid4().hex}.pending")
    with temporary.open("xb") as stream:
        stream.write(content)
        stream.flush()
        os.fsync(stream.fileno())
    try:
        os.link(temporary, target)
    except FileExistsError:
        _require(target.read_bytes() == content, "status_conflict")
    _sync(target.parent)
    temporary.unlink()
    _sync(target.parent)


def _sync(directory: Path) -> None:
    descriptor = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _sync_file(target: Path) -> None:
    with target.open("rb") as stream:
        os.fsync(stream.fileno())


def _reject_duplicates(pairs):
    result = {}
    for key, value in pairs:
        _require(key not in result, "invalid_status")
        result[key] = value
    return result


def _decode(content: bytes) -> dict:
    try:
        value = json.loads(content, object_pairs_hook=_reject_duplicates)
    except (ValueError, UnicodeError, RecursionError) as exc:
        raise StatusError("invalid_status") from exc
    _require(type(value) is dict, "invalid_status")
    return value


def write(
    root: Path | str,
    state: dict[str, Any],
    statuses: dict[int, int | None],
) -> dict[str, Any]:
    """Persist the per-request statuses of ``state`` and return the record.

    ``statuses`` maps a response index to the transport status observed by *this*
    invocation.  An index it does not name is recorded as unrecorded rather than
    guessed: the bytes may still be evidence, but this process did not see the
    answer that produced them.  An existing record is never rewritten; it is read
    back and compared, so a replay cannot relabel a capture.
    """
    task = state["task"]
    receipts = [item["receipt"] for item in state["responses"]]
    _require(
        all(type(index) is int and 0 <= index < len(receipts) for index in statuses),
        ("invalid_status"),
    )
    rows = []
    for index, receipt in enumerate(receipts):
        status = statuses.get(index)
        if status is not None:
            _require(
                type(status) is int and 100 <= status <= 599,
                "invalid_status",
            )
        rows.append(
            {
                "index": index,
                "sha256": receipt["sha256"],
                "http_status": status,
            }
        )
    record = {
        "version": VERSION,
        "task_sha256": envelope_sha(task),
        "task_id": task["task_id"],
        "generation": task["generation"],
        "statuses": rows,
        "observed_at": _now(),
    }
    target = path(root, state["locator"])
    if (
        not target.exists()
        and state["complete"]
        and capture_manifest.path(root, state["locator"]).exists()
    ):
        # A finalized capture that has lost its status record cannot be repaired by
        # inventing one.  The manifest already states the digest of the bytes that
        # are gone; re-installing a fresh record -- every index unrecorded -- would
        # be a rewrite of the evidence this function exists to preserve.  Refuse
        # before writing anything: nothing is installed, nothing is repaired.
        raise StatusError("missing_status")
    if target.exists():
        stored = read(root, state["locator"], state)
        if not statuses:
            # Nothing was observed by this invocation (a replay): the record the
            # capture was written under is the answer, and it is not rewritten.
            return stored
        # Byte-for-byte equality of everything except the clock: two invocations
        # describing the same capture must not disagree about a status.
        _require(
            {key: value for key, value in stored.items() if key != "observed_at"}
            == {key: value for key, value in record.items() if key != "observed_at"},
            "status_conflict",
        )
        return stored
    _install(target, _encode(record))
    if state["complete"]:
        # Chain the record into the capture's own integrity manifest, so a later
        # lone edit of this file is a typed refusal rather than a claim.  Only a
        # finalized capture has a terminal and a durability certificate to chain;
        # a resumable one has no manifest and cannot verify as complete.
        try:
            capture_manifest.write(root, state, status_name=NAME)
        except capture_manifest.ManifestError as exc:
            raise StatusError(str(exc)) from exc
    return read(root, state["locator"], state)


def read(
    root: Path | str,
    locator: str,
    state: dict[str, Any],
) -> dict[str, Any]:
    """Read a status record and verify it describes the capture in ``state``.

    ``state`` is required.  It is what makes this function the one entry point
    that always enforces both bindings -- the identity binding and, for a
    finalized capture, the integrity chain and its out-of-band anchor.  An
    optional argument would let a caller read a status without either, which is
    exactly the bypass this signature exists to remove.
    """
    target = path(root, locator)
    _require(target.is_file() and not target.is_symlink(), "missing_status")
    content = target.read_bytes()
    _require(len(content) <= MAX_BYTES, "status_size_limit")
    record = _decode(content)
    _require(_encode(record) == content, "noncanonical_status")
    _require(
        set(record) == set(FIELDS) and record["version"] == VERSION, "invalid_status"
    )
    rows = record["statuses"]
    _require(type(rows) is list, "invalid_status")
    for index, row in enumerate(rows):
        _require(
            type(row) is dict
            and set(row) == set(STATUS_FIELDS)
            and row["index"] == index
            and (row["sha256"] is None or type(row["sha256"]) is str),
            "invalid_status",
        )
        status = row["http_status"]
        _require(
            status is None or (type(status) is int and 100 <= status <= 599),
            "invalid_status",
        )
    task = state["task"]
    receipts = [item["receipt"] for item in state["responses"]]
    _require(
        record["task_sha256"] == envelope_sha(task)
        and record["task_id"] == task["task_id"]
        and record["generation"] == task["generation"],
        "status_binding",
    )
    _require(
        [(row["index"], row["sha256"]) for row in rows]
        == [(index, receipt["sha256"]) for index, receipt in enumerate(receipts)],
        "status_binding",
    )
    # Identity is not content: the same task digest and receipt shas say nothing
    # about which status was written.  For a finalized capture the manifest does,
    # and the acquisition log anchors the manifest -- so every hash in it and the
    # log line are checked here.  This `read` is what `verify`, the acquire CLI and
    # the collector's `_status_record` all call.
    if state["complete"]:
        try:
            capture_manifest.check(root, locator, state, status_name=NAME)
        except capture_manifest.ManifestError as exc:
            raise StatusError(str(exc)) from exc
    return record


def unrecorded(record: dict[str, Any] | None) -> list[int]:
    """The response indices whose transport status this capture cannot state."""
    if record is None:
        return []
    return [row["index"] for row in record["statuses"] if row["http_status"] is None]


def status_of(record: dict[str, Any] | None, index: int = 0) -> int | None:
    """The persisted status for one response index, or ``None`` when unrecorded."""
    if record is None:
        return None
    rows = record["statuses"]
    return rows[index]["http_status"] if 0 <= index < len(rows) else None


def observed(
    responses: list[dict[str, Any]], statuses: list[int | None]
) -> dict[int, int | None]:
    """Map the statuses one invocation observed onto the receipts it produced.

    A fetch adapter appends exactly one status per call, and ``capture`` appends
    one receipt per call *after* the durable prefix it resumed, so the observed
    statuses line up with the tail of the captured receipts.  A prefix resumed
    from another process has no observed status and stays unrecorded: this
    process did not see the answer that produced those bytes.
    """
    captured = [
        index
        for index, item in enumerate(responses)
        if item["receipt"]["status"] == "captured"
    ]
    tail = captured[len(captured) - len(statuses) :] if statuses else []
    return dict(zip(tail, statuses, strict=True))


def terminal(state: dict[str, Any], record: dict[str, Any] | None) -> str:
    """The typed verdict this capture supports; ``captured`` is the only success.

    A 503 with a body is ``http_error``, not ``captured``: the bytes are kept (they
    are the evidence of the refusal) but nothing in this lane may call that a
    completed acquisition.
    """
    if state["reason"] == "cancelled":
        return "cancelled"
    receipts = [item["receipt"] for item in state["responses"]]
    if any(receipt["status"] == "failed" for receipt in receipts):
        return "transport_error"
    if not receipts or not state["complete"]:
        return "incomplete"
    if record is None or unrecorded(record):
        return "status_unrecorded"
    if any(not 200 <= row["http_status"] < 300 for row in record["statuses"]):
        return "http_error"
    return CAPTURED

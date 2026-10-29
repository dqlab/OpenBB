"""Independent raw-acquisition-v1 evidence. No canonical publication or ACK.

Only trusted local code supplies fetch adapters/source configuration. Tasks are
literal JSON data, never code or provider import paths. POSIX local filesystems
with atomic rename/link, flock and fsync are required; NAS is not qualified.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import stat
import time
import uuid
from collections.abc import Callable
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from threading import Event, get_ident
from typing import Any

from .models import ProviderFailure

VERSION = "raw-acquisition-v1"
HTTP_VERSION = "raw-acquisition-v2"
LENGTH_VERSION = "raw-acquisition-v3"
MAX_REQUESTS = 256
MAX_TASK_BYTES = 262144
MAX_BODY_BYTES = 16 * 1024 * 1024
MAX_SOURCE_RESPONSE_BYTES = 50_000_000
MAX_CAPTURE_BODY_BYTES = MAX_SOURCE_RESPONSE_BYTES + 1
MAX_TOTAL_BYTES = 128 * 1024 * 1024
MAX_META_BYTES = 65536
_HASH = re.compile(r"[0-9a-f]{64}\Z")
_CLOCK = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z\Z")


class RawAcquisitionError(ValueError):
    """Safe protocol error; never provider exception messages."""


@dataclass(frozen=True)
class RawResponse:
    """Bytes at the adapter boundary, not necessarily bytes on the network."""

    body: bytes
    evidence_kind: str = "raw-bytes"
    transformation: str | None = None
    source_time: str | None = None
    media_type: str = "application/octet-stream"
    http_status: int | None = None
    length_state: str | None = None


def _require(condition: bool, code: str = "invalid_raw_evidence") -> None:
    if not condition:
        raise RawAcquisitionError(code)


def _json_types(value: Any, depth: int = 0) -> None:
    _require(depth <= 32, "json_depth_limit")
    if type(value) is dict:
        _require(all(type(key) is str for key in value), "non_json_key")
        for item in value.values():
            _json_types(item, depth + 1)
    elif type(value) is list:
        for item in value:
            _json_types(item, depth + 1)
    else:
        _require(type(value) in (str, int, float, bool, type(None)), "non_json_value")
        if type(value) is float:
            _require(math.isfinite(value), "nonfinite_json")


def _encode(value: Any) -> bytes:
    _json_types(value)
    try:
        return (
            json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n"
        ).encode()
    except (ValueError, UnicodeError) as exc:
        raise RawAcquisitionError("invalid_json") from exc


def _sha(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _keys(value: Any, fields: str) -> None:
    _require(type(value) is dict and set(value) == set(fields.split()), "invalid_fields")


def _text(value: Any, limit: int = 256) -> bool:
    return type(value) is str and 0 < len(value) <= limit


def validate_task(task: dict[str, Any]) -> dict[str, Any]:
    """Validate transport structure only; return a detached literal JSON tree."""
    encoded = _encode(task)
    _require(len(encoded) <= MAX_TASK_BYTES, "task_size_limit")
    _keys(task, "version task_id generation source requests")
    _require(task["version"] in (VERSION, HTTP_VERSION, LENGTH_VERSION), "unsupported_version")
    _require(_text(task["task_id"]) and _text(task["generation"]), "invalid_identity")
    _keys(task["source"], "provider model")
    _require(all(_text(v) for v in task["source"].values()), "invalid_source")
    requests = task["requests"]
    _require(type(requests) is list and 1 <= len(requests) <= MAX_REQUESTS, "request_limit")
    identifiers = set()
    for request in requests:
        _keys(request, "request_id params")
        _require(
            _text(request["request_id"]) and type(request["params"]) is dict, "invalid_request"
        )
        _require(request["request_id"] not in identifiers, "duplicate_request")
        identifiers.add(request["request_id"])
    return json.loads(encoded)


def plan_capture(task: dict[str, Any], *, response_max_bytes: int = MAX_BODY_BYTES) -> dict:
    """Pure worst-case reservation; the caller splits tasks when fits is false.

    response_max_bytes is the adapter's inclusive bound, not an observed average.
    This plan changes neither task identity nor provider limits.
    """
    authority = validate_task(task)
    _require(
        type(response_max_bytes) is int and 1 <= response_max_bytes <= MAX_CAPTURE_BODY_BYTES,
        "adapter_response_limit",
    )
    count = len(authority["requests"])
    required = count * response_max_bytes
    return {
        "request_count": count,
        "response_max_bytes": response_max_bytes,
        "required_body_bytes": required,
        "max_requests_per_task": min(MAX_REQUESTS, MAX_TOTAL_BYTES // response_max_bytes),
        "fits": required <= MAX_TOTAL_BYTES,
    }


def plan_process_capture(task: dict[str, Any], source: Any) -> dict:
    """Pure ProcessClient admission: compact JSON cannot exceed transport + LF.

    ProcessClient uses ASCII-escaped JSON with default separators. We encode
    the same parsed object with compact separators, sorted keys and one newline.
    """
    authority = validate_task(task)
    _require(
        authority["source"] == {"provider": source.provider, "model": source.model},
        "source_configuration_mismatch",
    )
    maximum = getattr(source, "max_response_bytes", None)
    _require(
        type(maximum) is int and 1 <= maximum <= MAX_SOURCE_RESPONSE_BYTES,
        "source_response_limit",
    )
    plan = plan_capture(authority, response_max_bytes=maximum + 1)
    return {**plan, "source_max_response_bytes": maximum}


def capture_locator(task_id: str, generation: str, *, version: str = VERSION) -> str:
    _require(_text(task_id) and _text(generation), "invalid_identity")
    _require(version in (VERSION, HTTP_VERSION, LENGTH_VERSION), "unsupported_version")
    return version + "/" + _sha(_encode([version, task_id, generation]))


def _path(root: Path, relative: str = "") -> Path:
    root = Path(os.path.abspath(root))
    _require(type(relative) is str, "unsafe_path")
    if relative:
        _require(
            not relative.startswith("/") and "\\" not in relative and ":" not in relative,
            "unsafe_path",
        )
        _require(all(part not in ("", ".", "..") for part in relative.split("/")), "unsafe_path")
    target = root / relative
    for part in [target, *target.parents]:
        _require(not part.is_symlink(), "symlink_evidence")
    return target


def _execution_owner() -> tuple:
    # A copied ContextVar is not authority in another thread, task or fork.
    from asyncio import current_task

    try:
        task = current_task()
    except RuntimeError:
        task = None
    return os.getpid(), get_ident(), task


@dataclass
class _CancellationObligation:
    """One live capture's cancellation latch, independent of exception priority."""

    owner: tuple = field(default_factory=_execution_owner)
    live: bool = False
    accepted: bool = False
    persisted: bool = False

    def accept(self) -> None:
        if self.live and _active_cancellation() is self:
            self.accepted = True

    def observe(self, exc: BaseException) -> None:
        if not isinstance(exc, Exception):
            self.accept()


_cancellation: ContextVar[_CancellationObligation | None] = ContextVar(
    "raw_capture_cancellation", default=None
)
# The physical invocation stack is authoritative, not re-entered ContextVars.
# Different execution owners use different keys; nested calls restore their top.
_capture_owners: dict[tuple, _CancellationObligation] = {}
if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_capture_owners.clear)


def _active_cancellation() -> _CancellationObligation | None:
    return _capture_owners.get(_execution_owner())


@contextmanager
def _capture_cancellation():
    obligation = _CancellationObligation()
    parent = _capture_owners.get(obligation.owner)
    token = _cancellation.set(obligation)
    try:
        # Keep even an inactive inner invocation on top through setup/release:
        # a copied live parent must not accept an inner post-cutoff failure.
        _capture_owners[obligation.owner] = obligation
        yield obligation
    finally:
        # Copies retain the same object: invalidate it before restoring a parent.
        obligation.live = False
        if parent is None:
            _capture_owners.pop(obligation.owner, None)
        else:
            _capture_owners[obligation.owner] = parent
        _cancellation.reset(token)


@contextmanager
def _accept_cancellation(obligation: _CancellationObligation):
    # The enclosing invocation already hides any parent's obligation, including
    # during pre-ownership setup and final native ownership release.
    obligation.live = True
    try:
        yield
    finally:
        obligation.live = False


@contextmanager
def _opened(path: Path, mode: str):
    """Own entry/body/exit explicitly; never infer a primary from ambient state."""
    obligation = _active_cancellation()
    stream = path.open(mode)
    primary = None
    try:
        entered = stream.__enter__()
        yield entered
    except BaseException as exc:
        primary = exc
        if obligation is not None:
            obligation.observe(exc)
        raise
    finally:
        try:
            stream.__exit__(
                type(primary) if primary is not None else None,
                primary,
                primary.__traceback__ if primary is not None else None,
            )
        except BaseException as secondary:
            if obligation is not None:
                obligation.observe(secondary)
            if primary is None:
                raise
            if secondary is not primary:
                primary.add_note("file_exit_failed:" + type(secondary).__name__)


def _read(path: Path, maximum: int) -> bytes:
    _path(path.parent, path.name)
    _require(path.is_file(), "missing_or_nonregular_evidence")
    with _opened(path, "rb") as stream:
        _require(stat.S_ISREG(os.fstat(stream.fileno()).st_mode), "not_regular_file")
        content = stream.read(maximum + 1)
    _require(len(content) <= maximum, "evidence_size_limit")
    return content


def _decode(content: bytes) -> Any:
    def pairs(items):
        result = {}
        for key, value in items:
            _require(key not in result, "duplicate_json_key")
            result[key] = value
        return result

    try:
        value = json.loads(content, object_pairs_hook=pairs)
        _require(_encode(value) == content, "noncanonical_json")
        return value
    except (UnicodeError, ValueError, RecursionError) as exc:
        raise RawAcquisitionError("invalid_json_evidence") from exc


def _clock(value: Any) -> None:
    _require(type(value) is str and bool(_CLOCK.fullmatch(value)), "invalid_clock")
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise RawAcquisitionError("invalid_clock") from exc


def _now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _receipt(value: Any, task_sha: str, request: dict, index: int, version: str) -> None:
    fields = (
        "version task_sha256 request_sha256 index status acquired_at monotonic_ns "
        "evidence_kind transformation source_time media_type body_path sha256 size error"
    )
    _require(type(value) is dict, "invalid_fields")
    _keys(
        value,
        fields
        + (
            " http_status"
            if version in (HTTP_VERSION, LENGTH_VERSION) and "http_status" in value
            else ""
        )
        + (" length_state" if version == LENGTH_VERSION else ""),
    )
    if version == LENGTH_VERSION:
        _require(
            value["length_state"] in ("complete", "oversize_complete_unknown", "unknown"),
            "invalid_length_state",
        )
        _require(
            (value["status"] == "failed") == (value["length_state"] == "unknown"),
            "invalid_length_state",
        )
    if "http_status" in value:
        _require(
            type(value["http_status"]) is int and 100 <= value["http_status"] <= 599,
            "invalid_http_status",
        )
        _require(
            value["status"] == "captured" and value["evidence_kind"] == "raw-bytes",
            "invalid_http_status",
        )
    _require(value["version"] == version and value["task_sha256"] == task_sha, "receipt_binding")
    _require(value["request_sha256"] == _sha(_encode(request)), "request_binding")
    _require(type(value["index"]) is int and value["index"] == index, "receipt_index")
    _clock(value["acquired_at"])
    _require(
        type(value["monotonic_ns"]) is int and 0 <= value["monotonic_ns"] < 2**63,
        "invalid_monotonic_clock",
    )
    if value["status"] == "failed":
        _require(value["error"] == "provider_failure", "invalid_failure")
        _require(
            all(
                value[k] is None
                for k in (
                    "evidence_kind",
                    "transformation",
                    "source_time",
                    "media_type",
                    "body_path",
                    "sha256",
                    "size",
                )
            ),
            "failure_has_body",
        )
        return
    _require(value["status"] == "captured" and value["error"] is None, "invalid_response_status")
    _require(
        value["evidence_kind"] in ("raw-bytes", "provider-transformed"), "invalid_evidence_kind"
    )
    if version == LENGTH_VERSION:
        _require(value["evidence_kind"] == "raw-bytes", "invalid_length_evidence")
        if value["length_state"] == "oversize_complete_unknown":
            _require(type(value["size"]) is int and value["size"] > 0, "invalid_length_state")
    _require(
        (value["evidence_kind"] == "raw-bytes" and value["transformation"] is None)
        or (value["evidence_kind"] == "provider-transformed" and _text(value["transformation"])),
        "missing_transformation_label",
    )
    _require(
        value["source_time"] is None or _text(value["source_time"], 4096), "invalid_source_time"
    )
    _require(_text(value["media_type"]), "invalid_media_type")
    _require(value["body_path"] == f"responses/{index:06d}/body.bin", "unsafe_body_path")
    _require(
        type(value["size"]) is int and 0 <= value["size"] <= MAX_CAPTURE_BODY_BYTES,
        "body_size_limit",
    )
    _require(
        type(value["sha256"]) is str and bool(_HASH.fullmatch(value["sha256"])), "invalid_hash"
    )


def read_capture(
    root: Path | str, task_id: str, generation: str, *, version: str = VERSION
) -> dict[str, Any]:
    """Read exact task only, including interrupted/cancelled audit evidence."""
    locator = capture_locator(task_id, generation, version=version)
    directory = _path(Path(root), locator)
    task_bytes = _read(_path(directory, "task.json"), MAX_TASK_BYTES)
    task = validate_task(_decode(task_bytes))
    _require(
        task["version"] == version
        and task["task_id"] == task_id
        and task["generation"] == generation,
        "task_binding",
    )
    task_sha = _sha(task_bytes)
    responses, hashes = [], []
    total, missing = 0, False
    for index, request in enumerate(task["requests"]):
        folder = _path(directory, f"responses/{index:06d}")
        if not folder.exists():
            missing = True
            continue
        _require(not missing, "noncontiguous_responses")
        receipt_bytes = _read(_path(folder, "receipt.json"), MAX_META_BYTES)
        receipt = _decode(receipt_bytes)
        _receipt(receipt, task_sha, request, index, version)
        body = None
        if receipt["status"] == "captured":
            body = _read(_path(directory, receipt["body_path"]), MAX_CAPTURE_BODY_BYTES)
            _require(
                len(body) == receipt["size"] and _sha(body) == receipt["sha256"], "body_checksum"
            )
            total += len(body)
            _require(total <= MAX_TOTAL_BYTES, "capture_size_limit")
        else:
            _require(not _path(folder, "body.bin").exists(), "failure_has_body")
        responses.append({"receipt": receipt, "body": body})
        hashes.append(_sha(receipt_bytes))
    successful = sum(r["receipt"]["status"] == "captured" for r in responses)
    status = (
        "captured" if successful == len(task["requests"]) else "partial" if successful else "failed"
    )
    terminal_path = _path(directory, "terminal.json")
    terminal = None
    complete = terminal_path.exists()
    if complete:
        terminal = _decode(_read(terminal_path, MAX_META_BYTES))
        _keys(terminal, "version task_sha256 status receipts finalized_at")
        _require(
            terminal["version"] == version and terminal["task_sha256"] == task_sha,
            "terminal_binding",
        )
        _require(
            not missing and terminal["status"] == status and terminal["receipts"] == hashes,
            "terminal_conflict",
        )
        _clock(terminal["finalized_at"])
    durable_path = _path(directory, "durable.json")
    if complete:
        complete = durable_path.exists()
        if complete:
            durable = _decode(_read(durable_path, MAX_META_BYTES))
            _require(
                durable
                == {
                    "version": version,
                    "task_sha256": task_sha,
                    "terminal_sha256": _sha(_encode(terminal)),
                },
                "invalid_durability_certificate",
            )
    reason = "finished" if complete else "interrupted"
    cancel_path = _path(directory, "cancel.json")
    if cancel_path.exists():
        cancel = _decode(_read(cancel_path, MAX_META_BYTES))
        _require(
            cancel == {"version": version, "task_sha256": task_sha, "reason": "cancelled"},
            "invalid_cancellation",
        )
        complete, reason = False, "cancelled"
    if not complete:
        status = "partial" if successful else "failed"
    return {
        "task": task,
        "locator": locator,
        "status": status,
        "reason": reason,
        "complete": complete,
        "responses": responses,
    }


@contextmanager
def _directory(directory: Path):
    """Own a directory descriptor, including cancellation-safe release."""
    obligation = _active_cancellation()
    fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
    primary = None
    try:
        yield fd
    except BaseException as exc:
        primary = exc
        if obligation is not None:
            obligation.observe(exc)
        raise
    finally:
        try:
            os.close(fd)
        except BaseException as secondary:
            if obligation is not None:
                obligation.observe(secondary)
            if primary is None:
                raise
            if secondary is not primary:
                primary.add_note("directory_close_failed:" + type(secondary).__name__)


def _sync(directory: Path) -> None:
    with _directory(directory) as fd:
        os.fsync(fd)


def _sync_file(path: Path) -> None:
    _path(path.parent, path.name)
    _require(path.is_file(), "missing_or_nonregular_evidence")
    with _opened(path, "rb") as stream:
        _require(stat.S_ISREG(os.fstat(stream.fileno()).st_mode), "not_regular_file")
        os.fsync(stream.fileno())


def _establish_durability(directory: Path, state: dict) -> None:
    """Re-establish exact validated evidence, never discover or rewrite history."""
    for name in (".lock", "task.json", "terminal.json"):
        _sync_file(_path(directory, name))
    for index, item in enumerate(state["responses"]):
        folder = _path(directory, f"responses/{index:06d}")
        _sync_file(folder / "receipt.json")
        if item["body"] is not None:
            _sync_file(folder / "body.bin")
        _sync(folder)
    _sync(_path(directory, "responses"))
    for folder in (directory, *directory.parents):
        _sync(folder)
    terminal = _read(directory / "terminal.json", MAX_META_BYTES)
    _install(
        directory / "durable.json",
        _encode(
            {
                "version": state["task"]["version"],
                "task_sha256": _sha(_encode(state["task"])),
                "terminal_sha256": _sha(terminal),
            }
        ),
    )


def _mkdir(directory: Path) -> None:
    _path(directory)
    if not directory.exists():
        _mkdir(directory.parent)
        directory.mkdir(exist_ok=True)
        _sync(directory.parent)
    _require(directory.is_dir(), "not_directory")


def _install(path: Path, content: bytes, *, _parent_fd: int | None = None) -> None:
    """Atomic no-clobber install. Retain interrupted staging for audit."""

    def sync_parent():
        if _parent_fd is None:
            _sync(path.parent)
        else:
            os.fsync(_parent_fd)

    _path(path.parent, path.name)
    if path.exists():
        _require(_read(path, len(content)) == content, "immutable_conflict")
        _sync_file(path)
        sync_parent()
        return
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.pending")
    with _opened(temporary, "xb") as stream:
        stream.write(content)
        stream.flush()
        os.fsync(stream.fileno())
    try:
        os.link(temporary, path)
    except FileExistsError:
        _require(_read(path, len(content)) == content, "immutable_conflict")
    sync_parent()
    temporary.unlink()  # only this invocation's redundant hardlink, never evidence
    sync_parent()


def _install_response(directory: Path, index: int, receipt: dict, body: bytes | None) -> None:
    responses = _path(directory, "responses")
    _mkdir(responses)
    target = _path(responses, f"{index:06d}")
    _require(not target.exists(), "immutable_response_conflict")
    stage = _path(responses, f".pending-{index:06d}")
    _require(not stage.exists(), "pending_response_conflict")
    _mkdir(stage)
    # Metadata first: exact pending locator can recover original clocks/hashes
    # after the body install, without scanning history or refetching the source.
    _install(stage / "receipt.json", _encode(receipt))
    if body is not None:
        _install(stage / "body.bin", body)
    _sync(stage)
    # Exclusive task flock protects the absent destination from another writer.
    os.rename(stage, target)
    _sync(responses)


def _recover_pending(directory: Path, state: dict) -> None:
    authority = state["task"]
    prefix = len(state["responses"])
    pending = None
    for index in range(len(authority["requests"])):
        stage = _path(directory, f"responses/.pending-{index:06d}")
        if stage.exists():
            _require(
                index == prefix and not state["complete"] and state["reason"] != "cancelled",
                "unexpected_pending_response",
            )
            pending = (index, stage)
    if pending is None:
        return
    index, stage = pending
    receipt_path = _path(stage, "receipt.json")
    receipt = _decode(_read(receipt_path, MAX_META_BYTES))
    _receipt(
        receipt, _sha(_encode(authority)), authority["requests"][index], index, authority["version"]
    )
    paths = [receipt_path]
    body_path = _path(stage, "body.bin")
    if receipt["status"] == "captured":
        body = _read(body_path, MAX_CAPTURE_BODY_BYTES)
        _require(
            len(body) == receipt["size"] and _sha(body) == receipt["sha256"], "pending_checksum"
        )
        total = len(body) + sum(len(r["body"]) for r in state["responses"] if r["body"] is not None)
        _require(total <= MAX_TOTAL_BYTES, "capture_size_limit")
        paths.append(body_path)
    else:
        _require(not body_path.exists(), "failure_has_body")
    for path in paths:
        _sync_file(path)
    _sync(stage)
    os.rename(stage, _path(directory, f"responses/{index:06d}"))
    _sync(stage.parent)


@contextmanager
def _ownership(directory: Path, *, retain_for_workers=False):
    import fcntl

    lock = _path(directory, ".lock")
    stream = lock.open("a+b")
    primary = None
    try:
        try:
            fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RawAcquisitionError("acquisition_busy") from exc
        try:
            yield stream.fileno()
        finally:
            # Campaign spawn workers hold this same open file description.
            # Legacy raw acquisition may fork persistent children and must unlock.
            if not retain_for_workers:
                fcntl.flock(stream.fileno(), fcntl.LOCK_UN)
    except BaseException as exc:
        primary = exc
        raise
    finally:
        try:
            stream.close()
        except BaseException as secondary:
            if primary is None:
                raise
            primary.add_note("ownership_close_failed:" + type(secondary).__name__)


@_capture_cancellation()
def capture(
    root: Path | str,
    task: dict[str, Any],
    fetch: Callable[[dict[str, Any]], RawResponse],
    *,
    stop: Event | None = None,
    response_max_bytes: int = MAX_BODY_BYTES,
    _cleanup: Callable[[], None] | None = None,
) -> dict[str, Any]:
    """Capture each explicit request once per generation; resume durable prefix.

    Only ProviderFailure becomes a safe failed outcome. Other exceptions remain
    operational failures. A terminal failed request is not retried in this
    generation. New refresh/retry tasks must use a new generation.
    """
    obligation = _active_cancellation()
    assert obligation is not None
    authority = validate_task(task)
    plan = plan_capture(authority, response_max_bytes=response_max_bytes)
    _require(plan["fits"], "task_evidence_budget_exceeded")
    _require(callable(fetch), "invalid_fetch_adapter")
    _require(_cleanup is None or callable(_cleanup), "invalid_cleanup")
    _require(os.name == "posix", "unsupported_durability_platform")
    stop = stop if stop is not None else Event()
    task_bytes = _encode(authority)
    task_sha = _sha(task_bytes)
    directory = _path(
        Path(root),
        capture_locator(
            authority["task_id"], authority["generation"], version=authority["version"]
        ),
    )
    if directory.exists():
        _require(_path(directory, "task.json").is_file(), "missing_task_authority")
    _mkdir(directory)
    # Reserve the veto parent descriptor before entering fallible acquisition
    # scopes. Cancellation need not reacquire a directory resource after failure.
    with (
        _ownership(directory),
        _directory(directory) as veto_parent_fd,
        _accept_cancellation(obligation),
    ):

        def cancel():
            obligation.accept()
            if obligation.persisted:
                return
            _install(
                directory / "cancel.json",
                _encode(
                    {
                        "version": authority["version"],
                        "task_sha256": task_sha,
                        "reason": "cancelled",
                    }
                ),
                _parent_fd=veto_parent_fd,
            )
            obligation.persisted = True

        cleanup_pending = _cleanup

        def cleanup():
            nonlocal cleanup_pending
            callback, cleanup_pending = cleanup_pending, None
            if callback is not None:
                callback()

        try:
            _install(directory / "task.json", task_bytes)
            state = read_capture(
                root, authority["task_id"], authority["generation"], version=authority["version"]
            )
            if state["reason"] == "cancelled":
                return state
            _recover_pending(directory, state)
            state = read_capture(
                root, authority["task_id"], authority["generation"], version=authority["version"]
            )
            total = sum(
                len(item["body"]) for item in state["responses"] if item["body"] is not None
            )
            # On resume the retained prefix may have a different declared bound.
            remaining = len(authority["requests"]) - len(state["responses"])
            _require(
                total + remaining * response_max_bytes <= MAX_TOTAL_BYTES,
                "task_evidence_budget_exceeded",
            )
            for index in range(len(state["responses"]), len(authority["requests"])):
                if stop.is_set() or obligation.accepted:
                    cancel()
                    break
                request = authority["requests"][index]
                response, failed = None, False
                try:
                    response = fetch(json.loads(_encode(request)))
                except ProviderFailure as exc:
                    if exc.code == "cancelled":
                        cancel()
                        break
                    failed = True
                acquired_at, monotonic_ns = _now(), time.monotonic_ns()
                receipt = {
                    "version": authority["version"],
                    "task_sha256": task_sha,
                    "request_sha256": _sha(_encode(request)),
                    "index": index,
                    "status": "failed" if failed else "captured",
                    "acquired_at": acquired_at,
                    "monotonic_ns": monotonic_ns,
                    "evidence_kind": None,
                    "transformation": None,
                    "source_time": None,
                    "media_type": None,
                    "body_path": None,
                    "sha256": None,
                    "size": None,
                    "error": "provider_failure" if failed else None,
                }
                if authority["version"] == LENGTH_VERSION:
                    receipt["length_state"] = "unknown" if failed else response.length_state
                body = None
                if not failed:
                    _require(
                        type(response) is RawResponse and type(response.body) is bytes,
                        "invalid_response_adapter",
                    )
                    body = response.body
                    _require(len(body) <= response_max_bytes, "adapter_response_limit_exceeded")
                    total += len(body)
                    _require(total <= MAX_TOTAL_BYTES, "capture_size_limit")
                    if response.http_status is not None:
                        receipt["http_status"] = response.http_status
                    receipt.update(
                        evidence_kind=response.evidence_kind,
                        transformation=response.transformation,
                        source_time=response.source_time,
                        media_type=response.media_type,
                        body_path=f"responses/{index:06d}/body.bin",
                        sha256=_sha(body),
                        size=len(body),
                    )
                _receipt(receipt, task_sha, request, index, authority["version"])
                _require(len(_encode(receipt)) <= MAX_META_BYTES, "receipt_size_limit")
                _install_response(directory, index, receipt, body)
            cleanup()
            state = read_capture(
                root, authority["task_id"], authority["generation"], version=authority["version"]
            )
            if stop.is_set() or obligation.accepted:
                cancel()
            elif state["reason"] != "cancelled":
                outcomes = state["responses"]
                successful = sum(r["receipt"]["status"] == "captured" for r in outcomes)
                status = (
                    "captured"
                    if successful == len(outcomes)
                    else "partial"
                    if successful
                    else "failed"
                )
                if not _path(directory, "terminal.json").exists():
                    _install(
                        directory / "terminal.json",
                        _encode(
                            {
                                "version": authority["version"],
                                "task_sha256": task_sha,
                                "status": status,
                                "receipts": [_sha(_encode(item["receipt"])) for item in outcomes],
                                "finalized_at": _now(),
                            }
                        ),
                    )
                _establish_durability(directory, state)
            state = read_capture(
                root, authority["task_id"], authority["generation"], version=authority["version"]
            )
            # Close acceptance only after the protected snapshot and its native
            # cleanup. Sample the Event at this cutoff, still under ownership:
            # unlike raising handlers, Event-only handlers do not set the latch.
            obligation.live = False
            if stop.is_set() or obligation.accepted:
                cancel()
                # The successful veto invalidates this already-validated
                # snapshot. Do not introduce another fallible success read.
                state.update(
                    complete=False,
                    reason="cancelled",
                    status="partial"
                    if any(r["receipt"]["status"] == "captured" for r in state["responses"])
                    else "failed",
                )
            return state
        except BaseException as primary:
            # Never replace an acquisition/storage/cancellation primary with a
            # secondary cleanup or veto-persistence failure. No exception text.
            obligation.observe(primary)
            if obligation.accepted or stop.is_set():
                try:
                    cancel()
                except BaseException as secondary:
                    primary.add_note("cancellation_persistence_failed:" + type(secondary).__name__)
            try:
                cleanup()
            except BaseException as secondary:
                obligation.observe(secondary)
                primary.add_note("worker_cleanup_failed:" + type(secondary).__name__)
            # Cleanup may accept a stop or raise cancellation while handling an
            # operational primary. Retain both facts before releasing ownership.
            if obligation.accepted or stop.is_set():
                try:
                    cancel()
                except BaseException as secondary:
                    primary.add_note("cancellation_persistence_failed:" + type(secondary).__name__)
            raise


def capture_process(
    root: Path | str, task: dict[str, Any], source: Any, *, client=None, stop: Event | None = None
) -> dict[str, Any]:
    """Existing ProcessClient→QueryExecutor facade; never claim wire-byte capture.

    `source` is trusted host configuration, not imported/deserialized from a task.
    A supplied client remains caller-owned. An internally created client is closed
    before terminal finalization, including failure paths.
    """
    from .process import ProcessClient

    authority = validate_task(task)
    plan = plan_process_capture(authority, source)
    _require(plan["fits"], "task_evidence_budget_exceeded")
    owned = client is None
    client = ProcessClient(stop=stop) if owned else client
    stop = stop if stop is not None else client.stop

    def fetch(request):
        result = client.fetch(authority["task_id"], source, request["params"])
        # The already-transported body is opaque evidence, not bounded control
        # JSON. Do not impose the task metadata's nesting/business constraints.
        body = (
            json.dumps(
                {
                    "rows": result.rows,
                    "warning_count": result.warning_count,
                    "metadata": result.metadata,
                },
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
            + "\n"
        ).encode()
        return RawResponse(
            body,
            "provider-transformed",
            "openbb-process-response-json-v1",
            media_type="application/json",
        )

    return capture(
        root,
        authority,
        fetch,
        stop=stop,
        response_max_bytes=plan["response_max_bytes"],
        _cleanup=client.close if owned else None,
    )

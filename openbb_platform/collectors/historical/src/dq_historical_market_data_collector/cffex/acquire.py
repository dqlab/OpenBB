"""CFFEX daily acquisition only: one literal plan in, literal source bytes out.

This module is the acquisition *task* of the CFFEX daily lane, separated from the
collection task. It consumes a literal, immutable JSON plan (dates or a date
range plus the collector's transport authority), performs exactly one literal
HTTP GET per requested date per attempt through
:mod:`dq_historical_market_data_collector.cffex.source` -- the provider's own
transport helper, whose response body is taken *before* any parser sees it --
and writes every returned body into a private ``raw-acquisition-v2``
content-addressed evidence root with the standard receipts. It then exits.

What it does **not** do, by construction and by import closure:

* no *collector* configuration file is read and no collector configuration
  module is imported: ``cffex.config``, the journal/``storage`` layer,
  ``normalize``, ``parse``, ``runtime`` and ``cli`` are outside this module's
  closure. The plan supplies the transport authority as literal data instead;
* no journal, database, catalog or central store is opened (no ``sqlite3``, no
  ``duckdb``, no ``dq_quant_invest_data``);
* nothing is parsed, decoded, normalized, unit-converted, date-stamped or
  committed: a malformed body is still evidence;
* no delivery is written and no business predicate is evaluated.

The transport itself is *not* stdlib, and this task does not claim it is:
``.source`` calls the provider's ``make_request``, which imports ``pandas`` and
``openbb_core`` and reads the user-level provider settings file
``~/.openbb_platform/system_settings.json`` through ``SystemService()`` the first
time a request is made. Both packages are outside this task's freeze roots (the
round-2 carry-over recorded in ``CURRENT_GATE.md``) and neither is a collector
module. A plan that is validated but never requested imports neither the
provider nor ``pandas``.

The downstream collection task (``runtime.Collector``) reads these bytes back.
That is not a hope about format: the capture identity below is derived by the
same function (:mod:`.identity`) the collector uses, and the task document it
writes is byte-identical to the collector's, so ``Collector.backfill`` finds this
generation, replays it and commits rows with **zero** source calls. What this
task never does is judge the bytes.

Plan format (``cffex-acquire-plan-v2``), exactly these keys::

    {
      "version":  "cffex-acquire-plan-v2",
      "plan_id":  "<stable literal label, 1..200 chars>",
      "source_id": "<the collector's source_id, 1..80 chars>",
      "units":    {"price_unit": "...", "volume_unit": "...",
                   "open_interest_unit": "...", "turnover_unit": "..."},
      "token":    "configured",
      "attempt":  0,
      "max_attempts": 2,
      "source": {
        "provider": "akshare",
        "model": "AkshareFuturesHistDailyCffex",
        "credentials_env": {},
        "timeout_seconds": 60.0,
        "min_interval_seconds": 2.0,
        "max_rows": 10000,
        "max_response_bytes": 8000000,
        "retries": 2,
        "retry_delay_seconds": 2.0
      },
      "requests": {"dates": ["2026-09-18", "2026-09-21"]}
    }

``source``, ``units``, ``source_id``, ``token`` and ``attempt`` are the
collector's *capture authority* written out literally: the same provider, the
same limits, the same declared units and the same freshness token that
``Config`` would hand ``Collector``. The plan does not name a configuration file
and cannot read one; it carries the values the collector would have read. The
numbers are normalised to the collector's field types before the fingerprint is
derived, so ``60`` and ``60.0`` name the same authority.

``requests`` carries exactly one of:

* ``{"dates": [...]}`` -- an explicit, duplicate-free list of ISO trading dates;
* ``{"range": {"start": "YYYY-MM-DD", "end": "YYYY-MM-DD",
               "weekdays_only": true}}`` -- an inclusive calendar range. No
  exchange calendar is consulted here (there is none without configuration), so
  ``weekdays_only`` selects Monday-Friday and ``false`` selects every calendar
  day. A closed-day response is captured like any other body and is refused
  downstream, never silently skipped.

Identity is derived, not invented (:mod:`.identity`)::

    task_id     = sha256(kind, source_id, provider, model, date)
    fingerprint = sha256(version=2, source_id, source, units, evidence_version)
    base        = sha256(fingerprint, source_id, date, attempt, token,
                         evidence_version)
    generation  = sha256(base, epoch)

``task_id``, ``fingerprint``, ``base`` and ``generation`` are the collector's own
formulas, called from the module the collector calls. So:

* **this task's evidence is the collection task's evidence.** ``Collector``
  resolves the same ``(task_id, generation)``, finds a complete capture, and
  replays it with zero source calls;
* **re-running a plan is a zero-call replay** of warm evidence;
* **a terminal transport failure is retryable under the same ``plan_id``**: the
  next run opens the next ``epoch`` (the failed generation stays immutable as
  evidence of the attempt), bounded by the plan's ``max_attempts`` per run and
  by ``MAX_EVIDENCE_EPOCHS`` overall;
* **a cancelled generation keeps its ``cancel.json`` veto** and is never
  refetched, by this task or by a rerun of the plan. A run that stops is reported
  ``status="cancelled"`` and exits ``4``, and so is a later replay of that vetoed
  generation (``cancelled`` counts the vetoed entries) -- never ``failed``;
* a genuinely fresh capture of the same date under the same authority requires a
  new ``token`` (the collector mints one for ``--refresh``) or a different
  authority (a new fingerprint) -- not a new plan label. ``plan_id`` names the
  *plan*, not the capture.

``--root`` is the private evidence root (the directory a configured run calls
``evidence``/``<root>/evidence``). Nothing outside ``<root>/raw-acquisition-v2/``
is created.

Exit codes, aligned with the collection CLI's map (``cffex/cli.py``): ``0``
complete, ``1`` failed, ``2`` refused plan or private state, ``3`` partial, ``4``
cancelled.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import signal
import sys
import threading
from dataclasses import asdict, dataclass, field
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from openbb_collector_core.raw_acquisition import (
    MAX_SOURCE_RESPONSE_BYTES,
    RawAcquisitionError,
    RawResponse,
    capture,
    capture_locator,
    plan_capture,
    read_capture,
    validate_task,
)

from . import identity
from .source import CffexCsvClient

__all__ = [
    "EVIDENCE_VERSION",
    "MAX_DATES",
    "PLAN_VERSION",
    "EvidenceSource",
    "PlanError",
    "entries",
    "load_plan",
    "main",
    "run",
    "task_for",
    "validate_plan",
]

PLAN_VERSION = "cffex-acquire-plan-v2"
EVIDENCE_VERSION = identity.EVIDENCE_VERSION
MAX_PLAN_BYTES = 1024 * 1024
MAX_DATES = 366
MAX_ROWS = 100_000
MAX_ATTEMPT_INDEX = 2
MAX_ATTEMPTS = 5
MAX_EVIDENCE_EPOCHS = 4096
EXIT_CODES = {"complete": 0, "failed": 1, "partial": 3, "cancelled": 4}

# The transport limits this task actually uses, and the full authority block the
# collector's ``SourceLimits`` dumps (the extra two are fingerprint material: the
# acquire task never retries, but a capture is scoped to the collector's retry
# policy because that policy is part of the authority that shaped it). The three
# float fields are normalised to ``float`` so ``60`` and ``60.0`` name one
# authority, matching what the collector's validated model dumps.
TRANSPORT_FIELDS = (
    "provider",
    "model",
    "credentials_env",
    "max_rows",
    "max_response_bytes",
    "timeout_seconds",
    "min_interval_seconds",
)
PLAN_SOURCE_FIELDS = (
    "provider",
    "credentials_env",
    "timeout_seconds",
    "min_interval_seconds",
    "max_rows",
    "max_response_bytes",
    "retries",
    "retry_delay_seconds",
    "model",
)
PLAN_FIELDS = (
    "version",
    "plan_id",
    "source_id",
    "units",
    "token",
    "attempt",
    "max_attempts",
    "source",
    "requests",
)

_SAFE_CODE = re.compile(r"[a-z0-9_]{1,64}\Z")


class PlanError(ValueError):
    """A literal plan this task refuses; ``code`` is the machine contract."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


def _require(condition: bool, code: str) -> None:
    if not condition:
        raise PlanError(code)


def _text(value: Any, limit: int = 256) -> bool:
    return type(value) is str and 0 < len(value) <= limit


def encode(value: Any) -> bytes:
    """Canonical bytes; the only encoding used for identities and the plan digest."""
    try:
        return (
            json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n"
        ).encode()
    except (ValueError, TypeError, UnicodeError) as exc:
        raise PlanError("invalid_json") from exc


def digest(value: Any) -> str:
    return hashlib.sha256(encode(value)).hexdigest()


def loads(body: bytes) -> Any:
    """Strict JSON: UTF-8 only, duplicate keys refused, no NaN/Infinity admitted."""
    _require(type(body) is bytes, "invalid_json")
    try:
        text = body.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise PlanError("invalid_json") from exc

    def pairs(items):
        result: dict[str, Any] = {}
        for key, value in items:
            _require(key not in result, "duplicate_json_key")
            result[key] = value
        return result

    try:
        value = json.loads(text, object_pairs_hook=pairs)
    except PlanError:
        raise
    except (ValueError, RecursionError) as exc:
        raise PlanError("invalid_json") from exc
    # Reject NaN/Infinity (JSON's non-finite extensions) before anything is derived.
    encode(value)
    return value


def read_plan(path: Path | str) -> tuple[Any, str]:
    """Read a literal plan file; return it and the sha256 of the bytes read."""
    target = Path(path)
    _require(not target.is_symlink(), "unsafe_plan_path")
    _require(target.is_file(), "plan_unreadable")
    try:
        body = target.read_bytes()
    except OSError as exc:
        raise PlanError("plan_unreadable") from exc
    _require(len(body) <= MAX_PLAN_BYTES, "plan_size_limit")
    return loads(body), hashlib.sha256(body).hexdigest()


def load_plan(path: Path | str) -> Any:
    """Read a literal plan file; never interprets it as code or a module path."""
    return read_plan(path)[0]


@dataclass(frozen=True)
class EvidenceSource:
    """Transport configuration only, supplied by the literal plan."""

    provider: str
    model: str
    credentials_env: dict[str, str] = field(default_factory=dict)
    max_rows: int = 10000
    max_response_bytes: int = 8_000_000
    timeout_seconds: float = 60
    min_interval_seconds: float = 2

    def model_dump(self, **_: Any) -> dict[str, Any]:
        return asdict(self)


def _units(value: Any) -> dict[str, str]:
    """The collector's declared units, verbatim; they bind a capture to a payload."""
    _require(type(value) is dict, "invalid_units")
    _require(set(value) <= set(identity.UNIT_FIELDS), "unknown_unit_field")
    _require(
        all(type(item) is str and 0 < len(item) <= 64 for item in value.values()),
        "invalid_unit",
    )
    return dict(value)


def _source(value: Any) -> dict[str, Any]:
    """The collector's transport authority, normalised to its JSON field types."""
    _require(type(value) is dict and set(value) == set(PLAN_SOURCE_FIELDS), "invalid_source")
    _require(value["provider"] == "akshare", "unsupported_provider")
    _require(value["model"] in identity.ALLOWED_MODELS, "unsupported_model")
    credentials = value["credentials_env"]
    _require(
        type(credentials) is dict
        and all(_text(key, 80) and _text(item, 80) for key, item in credentials.items()),
        "invalid_credentials_env",
    )
    max_rows, max_bytes = value["max_rows"], value["max_response_bytes"]
    _require(type(max_rows) is int and 1 <= max_rows <= MAX_ROWS, "invalid_max_rows")
    _require(
        type(max_bytes) is int and 1 <= max_bytes <= MAX_SOURCE_RESPONSE_BYTES,
        "invalid_max_response_bytes",
    )
    retries = value["retries"]
    _require(type(retries) is int and 0 <= retries <= MAX_ATTEMPT_INDEX, "invalid_retries")
    timeout = value["timeout_seconds"]
    interval = value["min_interval_seconds"]
    delay = value["retry_delay_seconds"]

    _require(type(timeout) in (int, float) and 0 < timeout <= 300, "invalid_timeout")
    _require(type(interval) in (int, float) and 0 <= interval <= 3600, "invalid_min_interval")
    _require(type(delay) in (int, float) and 0 <= delay <= 60, "invalid_retry_delay")
    return {
        "provider": value["provider"],
        "credentials_env": dict(credentials),
        "timeout_seconds": float(timeout),
        "min_interval_seconds": float(interval),
        "max_rows": max_rows,
        "max_response_bytes": max_bytes,
        "retries": retries,
        "retry_delay_seconds": float(delay),
        "model": value["model"],
    }


def _day(value: Any) -> date:
    _require(type(value) is str and len(value) == 10, "invalid_date")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise PlanError("invalid_date") from exc
    _require(str(parsed) == value, "invalid_date")
    return parsed


def _expand(requests: Any) -> list[date]:
    """``{"dates": ...}`` or ``{"range": ...}`` -> the requested dates, in order."""
    _require(type(requests) is dict, "unknown_requests")
    _require(set(requests) in ({"dates"}, {"range"}), "unknown_requests")
    if "dates" in requests:
        items = requests["dates"]
        _require(type(items) is list, "invalid_dates")
        days = [_day(item) for item in items]
    else:
        span = requests["range"]
        _require(
            type(span) is dict and set(span) == {"start", "end", "weekdays_only"},
            "unknown_range",
        )
        _require(type(span["weekdays_only"]) is bool, "invalid_weekdays_only")
        start, end = _day(span["start"]), _day(span["end"])
        _require(start <= end, "invalid_range_bounds")
        _require((end - start).days + 1 <= MAX_DATES, "range_span_limit")
        days = [
            start + timedelta(days=offset)
            for offset in range((end - start).days + 1)
            if not span["weekdays_only"] or (start + timedelta(days=offset)).weekday() < 5
        ]
    _require(1 <= len(days) <= MAX_DATES, "plan_entry_limit")
    _require(len(set(days)) == len(days), "duplicate_date")
    return days


def validate_plan(plan: Any) -> dict[str, Any]:
    """Structural and transport validation only; no business rule is applied.

    Runs to completion before the caller may touch a source, a socket or a
    filesystem path, so a malformed plan cannot create a store or a request.
    """
    _require(type(plan) is dict and set(plan) == set(PLAN_FIELDS), "unknown_plan")
    _require(len(encode(plan)) <= MAX_PLAN_BYTES, "plan_size_limit")
    _require(plan["version"] == PLAN_VERSION, "unsupported_version")
    _require(_text(plan["plan_id"], 200), "invalid_plan_id")
    _require(_text(plan["source_id"], 80), "invalid_source_id")
    units = _units(plan["units"])
    _require(_text(plan["token"], 128), "invalid_token")
    attempt = plan["attempt"]
    _require(type(attempt) is int and 0 <= attempt <= MAX_ATTEMPT_INDEX, "invalid_attempt")
    max_attempts = plan["max_attempts"]
    _require(
        type(max_attempts) is int and 1 <= max_attempts <= MAX_ATTEMPTS, "invalid_max_attempts"
    )
    source = _source(plan["source"])
    days = _expand(plan["requests"])
    authority = {
        "version": PLAN_VERSION,
        "plan_id": plan["plan_id"],
        "source_id": plan["source_id"],
        "units": units,
        "token": plan["token"],
        "attempt": attempt,
        "max_attempts": max_attempts,
        "source": source,
        "fingerprint": identity.fingerprint(plan["source_id"], source, units),
        "dates": [day.isoformat() for day in days],
    }
    # The tasks below are what the store's authority will contain; encoding them
    # here proves they are representable before anything is written.
    for day in days:
        encode(task_for(authority, day))
    return authority


def base_for(authority: dict[str, Any], day: date | str) -> str:
    """The collector's per-(date, attempt, token) capture base."""
    return identity.capture_base(
        authority["fingerprint"],
        authority["source_id"],
        day,
        authority["attempt"],
        authority["token"],
    )


def task_for(authority: dict[str, Any], day: date | str, epoch: int = 0) -> dict[str, Any]:
    """The literal ``raw-acquisition-v2`` task for one requested date and epoch.

    Byte-for-byte what ``Collector._plan`` builds for the same configuration
    (same task id, same generation, same ``request_id == source_id``), which is
    what lets the collector replay this capture instead of fetching again.
    """
    source = authority["source"]
    value = day if isinstance(day, str) else day.isoformat()
    return validate_task(
        {
            "version": EVIDENCE_VERSION,
            "task_id": identity.task_identity(
                authority["source_id"], source["provider"], source["model"], value
            ),
            "generation": identity.generation(base_for(authority, value), epoch),
            "source": {"provider": source["provider"], "model": source["model"]},
            "requests": [
                {
                    "request_id": authority["source_id"],
                    "params": {"date": value},
                }
            ],
        }
    )


def entries(authority: dict[str, Any]) -> list[dict[str, Any]]:
    """One entry per requested date: identity, base and nothing derived from config."""
    result = []
    for value in authority["dates"]:
        task = task_for(authority, value)
        base = base_for(authority, value)
        result.append(
            {
                "entry_id": digest(
                    {
                        "kind": "cffex-daily",
                        "trade_date": value,
                        "task_id": task["task_id"],
                        "base": base,
                    }
                ),
                "trade_date": value,
                "task_id": task["task_id"],
                "base": base,
            }
        )
    return result


def _code(exc: BaseException) -> str:
    text = str(exc)
    if isinstance(exc, ValueError) and _SAFE_CODE.match(text):
        return text
    return "acquisition_exception"


def existing_state(root: Path | str, task_id: str, generation: str) -> dict[str, Any] | None:
    """Durable capture state, or ``None`` when this generation was never written.

    Unreadable state is a typed refusal, never "absent": refetching over corrupt
    evidence would destroy the only copy of what the source actually sent.
    """
    if not (Path(root) / capture_locator(task_id, generation, version=EVIDENCE_VERSION)).exists():
        if (Path(root) / capture_locator(task_id, generation)).exists():
            raise RawAcquisitionError("legacy_http_status_missing")
        return None
    try:
        return read_capture(root, task_id, generation, version=EVIDENCE_VERSION)
    except RawAcquisitionError:
        raise
    except Exception as exc:  # noqa: BLE001 - every failure here is a refusal
        raise RawAcquisitionError("evidence_state_unavailable") from exc


def _scan(
    root: Path | str, task_id: str, base: str, start: int = 0
) -> dict[str, Any]:
    """The generation this entry should use, scanning epochs in order.

    ``absent`` and ``resume`` both capture; ``replay`` is a completed capture and
    makes no call; ``cancelled`` is the durable veto and is terminal. A completed
    generation whose receipt failed (``status`` ``failed``) or whose responses are
    incomplete (``partial``) is *not* terminal: it is evidence of an attempt, and
    the scan moves to the next epoch, which is how the same ``plan_id`` retries.
    """
    skipped: list[dict[str, Any]] = []
    for epoch in range(start, MAX_EVIDENCE_EPOCHS):
        generation = identity.generation(base, epoch)
        state = existing_state(root, task_id, generation)
        if state is None:
            return {
                "epoch": epoch,
                "generation": generation,
                "state": None,
                "kind": "absent",
                "skipped": skipped,
            }
        choice = {
            "epoch": epoch,
            "generation": generation,
            "state": state,
            "skipped": skipped,
        }
        if state["reason"] == "cancelled":
            return {**choice, "kind": "cancelled"}
        if state["complete"] and state["status"] == "captured" and _http_ok(state):
            return {**choice, "kind": "replay"}
        if state["reason"] == "interrupted":
            return {**choice, "kind": "resume"}
        skipped.append({"epoch": epoch, "status": state["status"], "reason": state["reason"]})
    raise RawAcquisitionError("evidence_generation_exhausted")


def _http_ok(state: dict[str, Any]) -> bool:
    """Raw HTTP evidence is successful only with a bound 2xx status."""
    return all(
        item["receipt"].get("evidence_kind") != "raw-bytes"
        or (
            type(item["receipt"].get("http_status")) is int
            and 200 <= item["receipt"]["http_status"] < 300
        )
        for item in state["responses"]
    )


def _http_status(state: dict[str, Any]) -> int | None:
    return next(
        (
            item["receipt"].get("http_status")
            for item in state["responses"]
            if item["receipt"].get("evidence_kind") == "raw-bytes"
        ),
        None,
    )


def _adapter(client: Any, source: EvidenceSource, task_id: str, counter: dict[str, Any]):
    """Take the transport body verbatim: no decode, no parse, no label but ``raw-bytes``."""

    def fetch(request: dict[str, Any]) -> RawResponse:
        counter["calls"] += 1
        result = client.fetch(task_id, source, request["params"])
        body = getattr(result, "body", None)
        if type(body) is not bytes:
            # A non-bytes result would be a provider-transformed payload and is
            # not evidence this task is allowed to produce.
            raise ValueError("invalid_source_body_type")
        counter["status"] = getattr(result, "status", None)
        return RawResponse(
            body,
            "raw-bytes",
            None,
            media_type=getattr(result, "media_type", None) or "text/csv",
            http_status=counter["status"],
        )

    return fetch


def _receipts(state: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        {
            name: item["receipt"][name]
            for name in (
                "index",
                "status",
                "acquired_at",
                "evidence_kind",
                "transformation",
                "media_type",
                "sha256",
                "size",
                "error",
            )
        }
        for item in state["responses"]
    ]


def _entry_result(
    entry: dict[str, Any],
    choice: dict[str, Any],
    *,
    attempts: int,
    history: list[dict[str, Any]],
    calls: int,
    code: str | None = None,
    http_status: int | None = None,
) -> dict[str, Any]:
    """One entry's verdict.

    ``complete`` is *judged*, not copied from the store: it is true only when this
    entry ended ``captured``, i.e. every request slot holds a body that the store
    verified. The store's own "terminal record exists" flag is reported as
    ``terminal`` so the two can never be confused (a failed request is terminal
    evidence and an incomplete acquisition at the same time).

    ``status`` is the store's verdict, with one substitution this task owns: a
    generation the store vetoed reports ``status == "failed"`` or ``"partial"``
    and ``reason == "cancelled"``, and an entry with that reason is reported as
    ``status == "cancelled"`` (``code == "cancelled"``, ``complete == false``).
    The veto is then visible at the plan level and in the exit code (4) instead of
    being flattened into "the source failed".
    """
    state = choice["state"]
    if state is None:
        status, reason, terminal, locator, responses = (
            "error",
            code or "not_captured",
            False,
            None,
            [],
        )
    else:
        status = "cancelled" if state["reason"] == "cancelled" else state["status"]
        if status == "captured" and not _http_ok(state):
            status = "failed"
        reason = state["reason"]
        http_status = _http_status(state)
        if status == "failed" and state["status"] == "captured":
            reason = "http_status_missing" if http_status is None else "http_status_failure"
        terminal = state["complete"]
        locator = state["locator"]
        responses = _receipts(state)
        if reason == "finished" and status != "captured":
            # A terminal record exists but not every slot captured a body: report
            # the receipt's own failure code, not the store's "finished" wording.
            failures = [
                item["error"]
                for item in responses
                if item["status"] != "captured" and item["error"]
            ]
            reason = failures[0] if failures else "incomplete_capture"
    return {
        "entry_id": entry["entry_id"],
        "trade_date": entry["trade_date"],
        "task_id": entry["task_id"],
        "epoch": choice["epoch"],
        "generation": choice["generation"],
        "locator": locator,
        "status": status,
        "reason": reason,
        "code": code if code is not None else ("cancelled" if status == "cancelled" else None),
        "complete": status == "captured",
        "terminal": terminal,
        "replayed": choice["kind"] == "replay",
        "attempts": attempts,
        "source_calls": calls,
        "http_status": http_status,
        "responses": responses,
        "epochs": history,
    }


def _run_entry(
    root: Path,
    authority: dict[str, Any],
    entry: dict[str, Any],
    *,
    client: Any,
    source: EvidenceSource,
    stop: threading.Event,
    budget: int,
) -> dict[str, Any]:
    """Capture one date, retrying a failed generation under the same plan.

    ``source_calls`` is the sum over every attempt this run made, not the last
    attempt's counter: two real provider calls must be reported as two, whether
    the second one captured or the retries were exhausted.
    """
    attempts = 0
    history: list[dict[str, Any]] = []
    last: dict[str, Any] | None = None
    start = 0
    while True:
        choice = _scan(root, entry["task_id"], entry["base"], start)
        if choice["kind"] in ("replay", "cancelled"):
            return _entry_result(
                entry,
                choice,
                attempts=attempts,
                history=history,
                calls=sum(item["source_calls"] for item in history),
            )
        if attempts >= authority["max_attempts"]:
            # Bounded: this run opens no further epoch for this date. The last
            # attempt's own receipt is what is reported; ``attempts_exhausted``
            # says this run stopped there rather than that the source was happy.
            return _entry_result(
                entry,
                choice if last is None else last,
                attempts=attempts,
                history=history,
                calls=sum(item["source_calls"] for item in history),
                code="attempts_exhausted",
            )
        counter: dict[str, Any] = {"calls": 0, "status": None}
        try:
            state = capture(
                root,
                task_for(authority, entry["trade_date"], choice["epoch"]),
                _adapter(client, source, entry["task_id"], counter),
                stop=stop,
                response_max_bytes=budget,
            )
        except ValueError as exc:
            # One entry's protocol refusal (oversize body, unsafe path, busy
            # generation) is a reported verdict; earlier evidence is kept and a
            # refusal is not retried as if it were a transport failure.
            return _entry_result(
                entry,
                choice,
                attempts=attempts,
                history=history,
                calls=sum(item["source_calls"] for item in history) + counter["calls"],
                code=_code(exc),
                http_status=counter["status"],
            )
        attempts += 1
        history.append(
            {
                "epoch": choice["epoch"],
                "status": state["status"],
                "reason": state["reason"],
                "source_calls": counter["calls"],
            }
        )
        last = {**choice, "state": state}
        if state["status"] == "captured" and state["complete"] and _http_ok(state):
            return _entry_result(
                entry,
                {**choice, "state": state, "kind": "captured"},
                attempts=attempts,
                history=history,
                calls=sum(item["source_calls"] for item in history),
                http_status=counter["status"],
            )
        if state["reason"] == "cancelled":
            return _entry_result(
                entry,
                {**choice, "state": state, "kind": "cancelled"},
                attempts=attempts,
                history=history,
                calls=sum(item["source_calls"] for item in history),
                http_status=counter["status"],
            )
        start = choice["epoch"] + 1


def run(
    root: Path | str,
    plan: Any,
    *,
    stop: threading.Event | None = None,
    sender: Any = None,
    plan_file_sha256: str | None = None,
) -> dict[str, Any]:
    """Validate, then capture each requested date once per attempt; never parse or commit.

    Ordering is the contract: the whole plan (structure, dates, per-task evidence
    budget) and every generation it names are validated before the first source
    call, and each returned body is written to the immutable store before this
    function looks at it again. ``sender`` is the transport seam used by offline
    tests; production always resolves the provider's own ``make_request``.
    """
    authority = validate_plan(plan)
    source = EvidenceSource(
        **{name: authority["source"][name] for name in TRANSPORT_FIELDS}
    )
    items = entries(authority)
    evidence_root = Path(os.path.abspath(root))
    _require(not evidence_root.is_symlink(), "unsafe_evidence_root")
    _require(
        not evidence_root.exists() or evidence_root.is_dir(), "invalid_evidence_root"
    )
    budget = source.max_response_bytes + 1
    for item in items:
        _require(
            plan_capture(
                task_for(authority, item["trade_date"]), response_max_bytes=budget
            )["fits"],
            "task_evidence_budget_exceeded",
        )
    # Read (and therefore verify) the generation every entry resolves to before
    # the first side effect: unreadable private state refuses with nothing
    # mutated. Epochs opened later by a retry are absent by construction.
    for item in items:
        _scan(evidence_root, item["task_id"], item["base"])

    stop = stop if stop is not None else threading.Event()
    client = CffexCsvClient(stop, sender=sender)
    results: list[dict[str, Any]] = []
    try:
        for item in items:
            results.append(
                _run_entry(
                    evidence_root,
                    authority,
                    item,
                    client=client,
                    source=source,
                    stop=stop,
                    budget=budget,
                )
            )
            if stop.is_set():
                # A stop is a run-level veto: the dates after this one are left
                # untouched (no cancellation record for a request never made) and
                # resume on the next run. The cancelled generation keeps its
                # ``cancel.json`` and is never refetched under this plan id. This
                # is the *observed* stop, not the entry's status: a replay of an
                # older veto reports ``cancelled`` too but does not stop the run.
                break
    finally:
        client.close()

    # A plan is ``cancelled`` when this run was stopped or when any entry it
    # processed still carries a standing veto, and ``cancelled`` counts exactly
    # those entries. Both are typed outcomes with exit code 4, so a scheduler can
    # tell "operator veto outstanding" from "the source failed".
    cancelled = stop.is_set() or any(item["status"] == "cancelled" for item in results)
    captured = sum(item["status"] == "captured" for item in results)
    overall = (
        "cancelled"
        if cancelled
        else "complete"
        if captured == len(results)
        else "partial"
        if captured
        else "failed"
    )
    attempted = len(results)
    return {
        "status": overall,
        "command": "acquire",
        "plan_version": PLAN_VERSION,
        "plan_id": authority["plan_id"],
        # Digests of two different things, deliberately both reported: the
        # normalised plan this run validated, and (when the plan came from a
        # file) the bytes the operator actually wrote.
        "plan_sha256": digest(authority),
        "plan_file_sha256": plan_file_sha256,
        "collector_fingerprint": authority["fingerprint"],
        "evidence_token": authority["token"],
        "attempt": authority["attempt"],
        "evidence_version": EVIDENCE_VERSION,
        "evidence_root": str(evidence_root),
        "entries": len(items),
        "attempted": attempted,
        "unprocessed": len(items) - attempted,
        "captured": captured,
        "replayed": sum(item["replayed"] for item in results),
        "retried": sum(item["attempts"] > 1 for item in results),
        "failed": sum(item["status"] in ("error", "failed") for item in results),
        "cancelled": sum(item["status"] == "cancelled" for item in results),
        "source_calls": sum(item["source_calls"] for item in results),
        "results": results,
    }


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(
        prog="cffex acquire",
        description=(
            "Persist literal CFFEX daily source bytes for a plan; "
            "no collector config, journal or catalogue."
        ),
    )
    result.add_argument("--plan", type=Path, required=True, help="Literal plan JSON file")
    result.add_argument("--root", type=Path, required=True, help="Private evidence root")
    return result


def emit(payload: dict[str, Any]) -> None:
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True, allow_nan=False), flush=True)


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(sys.argv[1:] if argv is None else argv)
    stop = threading.Event()
    previous: dict[Any, Any] = {}
    try:
        if threading.current_thread() is threading.main_thread():
            for signum in (signal.SIGINT, signal.SIGTERM):
                previous[signum] = signal.signal(signum, lambda *_: stop.set())
        plan, file_digest = read_plan(args.plan)
        report = run(args.root, plan, stop=stop, plan_file_sha256=file_digest)
    except PlanError as exc:
        emit({"status": "error", "command": "acquire", "code": exc.code})
        return 2
    except (RawAcquisitionError, ValueError) as exc:
        emit({"status": "error", "command": "acquire", "code": _code(exc)})
        return 2
    except OSError as exc:
        emit(
            {
                "status": "error",
                "command": "acquire",
                "code": "evidence_write_failed",
                "error_type": type(exc).__name__,
            }
        )
        return 2
    except Exception as exc:  # noqa: BLE001 - never leak vendor text or paths
        emit(
            {
                "status": "error",
                "command": "acquire",
                "code": "acquisition_exception",
                "error_type": type(exc).__name__,
            }
        )
        return 2
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)
    emit(report)
    return EXIT_CODES[report["status"]]


if __name__ == "__main__":
    raise SystemExit(main())

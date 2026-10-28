"""CFFEX acquisition (raw evidence only) and the downstream business consumer.

Two halves live here, and the boundary between them is a hard rule:

``acquire``
    Consumes a literal, immutable plan (JSON data only: one requested date, the
    provider's transport limits, the capture generation) and writes every
    returned response -- empty, malformed, partial or rejected -- into a private
    content-addressed ``raw-acquisition-v1`` store, before any business predicate
    can see it. For the daily lane the response is the *source CSV body bytes*,
    taken from the transport helper (:mod:`.source`) before any parser touches
    them; that is what makes a malformed body evidence instead of an exception.
    It never opens the journal, never reads a calendar, never resolves a contract,
    never derives a trading date, never validates OHLC/units and never touches a
    central store.

``Collector``
    Everything else: plan derivation from configuration, calendar and journal
    watermarks; bounded batching and the deferred tail; one acquisition round per
    attempt; the raw delivery; normalization, contract/date/unit/OHLC/shrink
    judgement; revisions, checkpoints, export, status and the update cursor. It
    reads captured bytes back, so a restart or a replay never refetches.

The daily and rolling lanes share one capture format and one delivery format, and
:func:`read_delivery` consumes either lane without a lane argument. Publication
into the central catalog is **not** implemented here: this module stops at the
local delivery plus the local journal/export, and ``REPORT.md`` states that
plainly rather than implying a publication loop that is not wired.
"""

from __future__ import annotations

import json
import os
import threading
import uuid
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from openbb_collector_core.models import ProviderFailure
from openbb_collector_core.process import ProcessClient
from openbb_collector_core.raw_acquisition import (
    RawResponse,
    capture,
    plan_process_capture,
    read_capture,
    validate_task,
)

from . import identity
from .config import (
    CalendarSource,
    Config,
    ConfigurationError,
    SourceLimits,
    TradingCalendar,
    eligible_day,
    iso_date,
    load_calendar,
)
from .normalize import (
    DATE_SOURCES,
    DataError,
    UnitAuthority,
    accept_calendar,
    declared_units,
    normalize,
    source_dates,
)
from .parse import SourceBodyError, parse_daily_csv
from .source import CffexCsvClient
from .storage import Journal, encoded, protected_target, sync_directory, utc_now

PLAN_VERSION = "cffex-raw-plan-v1"
DELIVERY_VERSION = "cffex-raw-delivery-v2"
CALENDAR_DELIVERY_VERSION = "cffex-raw-calendar-v1"
EVIDENCE_VERSION = "raw-acquisition-v1"
MAX_PLAN_ENTRIES = 366
MAX_EVIDENCE_EPOCHS = 4096

DELIVERY_FIELDS = (
    "version",
    "request",
    "source",
    "capture",
    "rows",
    "row_provenance",
    "provenance",
    "metadata",
    "warning_count",
    "observed_at",
    "captured_at",
    "event_time_precision",
    "publication_time",
    "evidence",
    "plan",
)
CAPTURE_FIELDS = (
    "evidence_version",
    "locator",
    "task_id",
    "generation",
    "evidence_kind",
    "transformation",
    "media_type",
    "sha256",
    "size",
    "row_sha256",
)
CALENDAR_FIELDS = (
    "version",
    "request",
    "source",
    "capture",
    "rows",
    "row_provenance",
    "provenance",
    "warning_count",
    "observed_at",
    "evidence",
    "plan",
)

__all__ = [
    "CALENDAR_DELIVERY_VERSION",
    "DELIVERY_VERSION",
    "EVIDENCE_VERSION",
    "MAX_PLAN_ENTRIES",
    "PLAN_VERSION",
    "Collector",
    "EvidenceStateError",
    "NoSourceClient",
    "acquire",
    "read_delivery",
    "refresh_calendar",
]


class EvidenceStateError(RuntimeError):
    """Private evidence or a delivery is not in a state this build understands."""

    def __init__(self, code: str, detail: dict[str, Any] | None = None):
        self.code = code
        self.detail = detail or {}
        super().__init__(code)


class NoSourceClient:
    """A client that refuses every request: the consuming path may not fetch."""

    def fetch(self, *_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("consuming captured evidence performed a source call")

    def close(self) -> None:
        return None


# ---------------------------------------------------------------------------
# literal plan
# ---------------------------------------------------------------------------

SOURCE_FIELDS = (
    "provider",
    "model",
    "credentials_env",
    "max_rows",
    "max_response_bytes",
    "timeout_seconds",
    "min_interval_seconds",
)
ENTRY_FIELDS = ("entry_id", "kind", "trade_date", "source_id", "attempt", "source", "task")
ENTRY_KINDS = ("daily", "calendar")


def canonical(value: Any) -> bytes:
    """Canonical JSON bytes; the only encoding used for identities."""
    return (
        json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n"
    ).encode()


def digest(value: Any) -> str:
    import hashlib

    return hashlib.sha256(canonical(value)).hexdigest()


@dataclass(frozen=True)
class EvidenceSource:
    """Transport configuration only, supplied by trusted host configuration."""

    provider: str
    model: str
    credentials_env: dict[str, str] = field(default_factory=dict)
    max_rows: int = 10000
    max_response_bytes: int = 8_000_000
    timeout_seconds: float = 60
    min_interval_seconds: float = 2

    def model_dump(self, **_: Any) -> dict[str, Any]:
        return asdict(self)


def source_of(source: SourceLimits) -> EvidenceSource:
    payload = source.model_dump(mode="json")
    return EvidenceSource(**{name: payload[name] for name in SOURCE_FIELDS})


def _require(condition: bool, code: str) -> None:
    if not condition:
        raise ValueError(code)


def _text(value: Any, limit: int = 256) -> bool:
    return type(value) is str and 0 < len(value) <= limit


def entry_identity(entry: dict[str, Any]) -> str:
    return digest({name: entry[name] for name in ENTRY_FIELDS if name != "entry_id"})


def validate_plan(plan: Any) -> dict[str, Any]:
    """Structural and transport validation only: no business rule is applied."""
    _require(
        type(plan) is dict and set(plan) == {"version", "generation", "entries"}, "unknown_plan"
    )
    _require(plan["version"] == PLAN_VERSION, "unsupported_version")
    _require(_text(plan["generation"]), "invalid_plan_generation")
    entries = plan["entries"]
    _require(type(entries) is list and 1 <= len(entries) <= MAX_PLAN_ENTRIES, "plan_entry_limit")
    for entry in entries:
        _require(type(entry) is dict and set(entry) == set(ENTRY_FIELDS), "unknown_entry")
        _require(_text(entry["source_id"], 80), "invalid_entry_name")
        _require(entry["kind"] in ENTRY_KINDS, "invalid_entry_kind")
        if entry["kind"] == "daily":
            day = date.fromisoformat(entry["trade_date"])
            _require(str(day) == entry["trade_date"], "invalid_entry_date")
        else:
            # A calendar capture has no trading date at all; it must not invent
            # one from the range it happens to support.
            _require(entry["trade_date"] is None, "invalid_entry_date")
        _require(type(entry["attempt"]) is int and 0 <= entry["attempt"] <= 16, "invalid_attempt")
        source = entry["source"]
        _require(type(source) is dict and set(source) == set(SOURCE_FIELDS), "invalid_source")
        _require(_text(source["provider"], 80) and _text(source["model"], 80), "invalid_source")
        _require(
            type(source["max_rows"]) is int and 1 <= source["max_rows"] <= 100_000,
            "invalid_max_rows",
        )
        _require(
            type(source["max_response_bytes"]) is int
            and 1 <= source["max_response_bytes"] <= 50_000_000,
            "invalid_max_response_bytes",
        )
        _require(
            type(source["credentials_env"]) is dict
            and all(
                _text(key, 80) and _text(item, 80)
                for key, item in source["credentials_env"].items()
            ),
            "invalid_credentials_env",
        )
        task = validate_task(entry["task"])
        _require(
            task["source"] == {"provider": source["provider"], "model": source["model"]},
            "source_configuration_mismatch",
        )
        _require(entry["entry_id"] == entry_identity(entry), "entry_identity_conflict")
    return json.loads(canonical(plan))


# ---------------------------------------------------------------------------
# acquisition half
# ---------------------------------------------------------------------------


def _safe_code(exc: BaseException) -> str:
    text = str(exc)
    if isinstance(exc, ValueError) and text.isidentifier() and len(text) <= 64:
        return text
    return "acquisition_exception"


def existing_state(root: Path | str, task_id: str, generation: str) -> dict[str, Any] | None:
    """Durable capture state, or ``None`` when this generation was never written.

    Unreadable state is never treated as absent: that would silently refetch over
    corrupt evidence. It becomes a typed refusal with the underlying reason.
    """
    from openbb_collector_core.raw_acquisition import capture_locator

    if not (Path(root) / capture_locator(task_id, generation)).exists():
        return None
    try:
        return read_capture(root, task_id, generation)
    except EvidenceStateError:
        raise
    except Exception as exc:  # noqa: BLE001 - every failure here is a refusal
        raise EvidenceStateError(
            "evidence_state_unreadable",
            {"task_id": task_id, "generation": generation, "reason": _safe_code(exc)},
        ) from exc


def _fetch_adapter(client: Any, task_id: str, source: EvidenceSource, counter: dict[str, Any]):
    """One adapter, two declared evidence kinds, decided by what the client returns.

    * a :class:`~.source.SourceBody` is a literal transport response: its bytes are
      stored exactly as received (``raw-bytes``, no transformation label) and only
      the HTTP status is carried alongside;
    * an OpenBB ``Response`` is the provider-transformed object the pre-existing
      doubles and the calendar lane produce: rows already parsed and, for the daily
      model, already date-stamped by the provider adapter. It keeps the
      ``provider-transformed`` label so nothing downstream can mistake it for source
      bytes.
    """
    def fetch(request: dict[str, Any]) -> RawResponse:
        counter["calls"] += 1
        result = client.fetch(task_id, source, request["params"])
        body = getattr(result, "body", None)
        if type(body) is bytes:  # literal source response
            counter.setdefault("http_status", []).append(getattr(result, "status", None))
            return RawResponse(
                body,
                "raw-bytes",
                None,
                media_type=getattr(result, "media_type", None) or "text/csv",
            )
        counter.setdefault("http_status", []).append(None)
        encoded = json.dumps(
            {
                "rows": result.rows,
                "warning_count": result.warning_count,
                "metadata": result.metadata,
            },
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        return RawResponse(
            (encoded + "\n").encode(),
            "provider-transformed",
            "openbb-process-response-json-v1",
            media_type="application/json",
        )

    return fetch


def acquire(
    root: Path | str,
    plan: dict[str, Any],
    *,
    client: Any = None,
    stop: threading.Event | None = None,
) -> list[dict[str, Any]]:
    """Capture every plan entry once per generation; resume a durable prefix.

    Ordering matters and is the whole point of this function:

    1. the plan is validated structurally and every entry's worst-case evidence
       budget is preflighted -- a rejection here happens before the first source
       call and before the first byte is written;
    2. every generation named by the plan is read back and verified -- corrupt or
       unknown private state refuses typed, mutating nothing;
    3. only then is any provider called, and each response is written to the
       immutable store before returning to the caller.

    A transport failure is never turned into an empty success: it becomes a
    ``failed`` receipt (or an error result), and the caller decides.
    """
    authority = validate_plan(plan)
    sources = [EvidenceSource(**entry["source"]) for entry in authority["entries"]]
    for entry, source in zip(authority["entries"], sources, strict=True):
        if not plan_process_capture(entry["task"], source)["fits"]:
            raise ValueError("task_evidence_budget_exceeded")
    for entry in authority["entries"]:
        try:
            existing_state(root, entry["task"]["task_id"], entry["task"]["generation"])
        except EvidenceStateError:
            raise
        except Exception as exc:  # noqa: BLE001 - every failure is a refusal here
            raise EvidenceStateError(
                "evidence_state_unreadable",
                {
                    "task_id": entry["task"]["task_id"],
                    "generation": entry["task"]["generation"],
                    "reason": _safe_code(exc),
                },
            ) from exc
    owned = client is None
    caller_client = client if client is not None else ProcessClient(stop)
    results: list[dict[str, Any]] = []
    try:
        for entry, source in zip(authority["entries"], sources, strict=True):
            task = entry["task"]
            counter = {"calls": 0}
            try:
                state = capture(
                    root,
                    task,
                    _fetch_adapter(caller_client, task["task_id"], source, counter),
                    stop=stop,
                    response_max_bytes=source.max_response_bytes + 1,
                )
            except ValueError as exc:
                # A protocol refusal (oversized response, unsafe path, ...) is
                # this attempt's verdict: reported, retried, never fatal.
                results.append(
                    {
                        "entry_id": entry["entry_id"],
                        "task_id": task["task_id"],
                        "generation": task["generation"],
                        "locator": None,
                        "status": "error",
                        "reason": _safe_code(exc),
                        "complete": False,
                        "responses": [],
                        "source_calls": counter["calls"],
                        "http_status": None,
                    }
                )
                continue
            # Anything else (a provider exception that is not a ProviderFailure,
            # an OSError from the store, a KeyboardInterrupt) is not a data
            # verdict: it escapes so the session is cleaned up and fails loudly.
            results.append(
                {
                    "entry_id": entry["entry_id"],
                    "task_id": task["task_id"],
                    "generation": task["generation"],
                    "locator": state["locator"],
                    "status": state["status"],
                    "reason": state["reason"],
                    "complete": state["complete"],
                    "responses": [
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
                    ],
                    "source_calls": counter["calls"],
                    "http_status": (counter.get("http_status") or [None])[0],
                }
            )
    finally:
        if owned:
            caller_client.close()
    return results


def _empty_provenance() -> dict[str, Any]:
    return {
        "evidence_kind": None,
        "transformation": None,
        "layout": None,
        "date_source": None,
        "date_column": None,
        "unit_source": None,
        "units": {},
        "filtered": {},
        "transforms": [],
        "http_status": None,
        "parse_error": None,
        "response_count": 0,
    }


def payload(
    state: dict[str, Any], *, http_status: int | None = None
) -> tuple[list[Any], dict[str, Any], int, dict[str, Any]]:
    """Rows as captured, plus the provenance of exactly how they were obtained.

    A captured body that cannot be read is **not** an exception here: the bytes are
    already in the store, so the delivery is written and the failure becomes a typed
    parse error that the downstream predicates reject. That is what makes a
    malformed body evidence rather than a hole in the record.
    """
    rows: list[Any] = []
    metadata: dict[str, Any] = {}
    warnings = 0
    provenance = _empty_provenance()
    provenance["http_status"] = http_status
    for item in state["responses"]:
        receipt = item["receipt"]
        if receipt["status"] != "captured" or item["body"] is None:
            continue
        provenance["response_count"] += 1
        provenance["evidence_kind"] = receipt["evidence_kind"]
        provenance["transformation"] = receipt["transformation"]
        if receipt["evidence_kind"] == "raw-bytes":
            try:
                parsed = parse_daily_csv(item["body"])
            except SourceBodyError as exc:
                provenance["parse_error"] = provenance["parse_error"] or exc.code
                continue
            rows.extend(parsed.rows)
            part = parsed.provenance()
            provenance["layout"] = part["layout"]
            provenance["date_column"] = part["date_column"]
            provenance["unit_source"] = part["unit_source"]
            provenance["units"] = {**provenance["units"], **part["units"]}
            provenance["filtered"] = {
                key: provenance["filtered"].get(key, 0) + value
                for key, value in part["filtered"].items()
            }
            provenance["transforms"] = list(part["transforms"])
            # The layout states no date, so the request is the only date there is.
            provenance["date_source"] = (
                "row" if part["date_column"] else "request_parameter"
            )
            continue
        try:
            body = json.loads(item["body"])
        except (TypeError, ValueError):
            provenance["parse_error"] = provenance["parse_error"] or "invalid_capture_body"
            continue
        if (
            type(body) is not dict
            or set(body) != {"rows", "warning_count", "metadata"}
            or type(body["rows"]) is not list
            or type(body["metadata"]) is not dict
        ):
            provenance["parse_error"] = provenance["parse_error"] or "invalid_capture_body"
            continue
        rows.extend(body["rows"])
        metadata.update(body["metadata"])
        warnings += int(body["warning_count"] or 0)
        provenance["date_source"] = provenance["date_source"] or "row"
        provenance["unit_source"] = provenance["unit_source"] or "absent"
    return rows, metadata, warnings, provenance


def unit_authority(provenance: dict[str, Any], declared: dict[str, str]) -> UnitAuthority:
    """The units this payload's values are in: the source's, or the declared ones.

    Never a default. When the body states nothing and configuration declares
    nothing, the authority is ``absent`` and the day is refused
    ``units_undetermined`` instead of being committed as ``provider_defined``.
    """
    if provenance.get("unit_source") == "source":
        return UnitAuthority(authority="source")
    return declared_units(declared)


def row_provenance(
    rows: list[dict[str, Any]], provenance: dict[str, Any]
) -> list[dict[str, Any]]:
    """One provenance record per delivered row, aligned by position."""
    result = []
    for index, item in enumerate(rows):
        result.append(
            {
                "source_row": index,
                "source_sha256": digest(item),
                "date_source": provenance.get("date_source") or "row",
                "unit_source": provenance.get("unit_source") or "absent",
            }
        )
    return result


# ---------------------------------------------------------------------------
# delivery half: one versioned envelope for both lanes
# ---------------------------------------------------------------------------


def _load_json(path: Path, version: str) -> dict[str, Any]:
    try:
        body = json.loads(path.read_bytes())
    except (OSError, ValueError) as exc:
        raise EvidenceStateError("invalid_delivery", {"path": str(path)}) from exc
    if type(body) is not dict or body.get("version") != version:
        raise EvidenceStateError("unsupported_delivery_version", {"path": str(path)})
    return body


def _capture_block(body: dict[str, Any], path: Path) -> dict[str, Any]:
    capture = body.get("capture")
    if type(capture) is not dict or set(capture) != set(CAPTURE_FIELDS):
        raise EvidenceStateError("invalid_delivery", {"path": str(path), "missing": "capture"})
    if not _text(capture["locator"]) or not _text(capture["task_id"], 256):
        raise EvidenceStateError("invalid_delivery", {"path": str(path), "capture": "shape"})
    if not _text(capture["generation"], 256) or type(capture["row_sha256"]) is not list:
        raise EvidenceStateError("invalid_delivery", {"path": str(path), "capture": "shape"})
    if not _hash(capture["sha256"]) or type(capture["size"]) is not int or capture["size"] < 0:
        raise EvidenceStateError("invalid_delivery", {"path": str(path), "capture": "digest"})
    return capture


def _hash(value: Any) -> bool:
    return type(value) is str and len(value) == 64 and all(
        character in "0123456789abcdef" for character in value
    )


def _rows_of(body: dict[str, Any], path: Path) -> list[dict[str, Any]]:
    rows = body.get("rows")
    if type(rows) is not list or not all(type(item) is dict for item in rows):
        raise EvidenceStateError("invalid_delivery", {"path": str(path), "rows": "shape"})
    return rows


def read_delivery(path: Path | str, capture_root: Path | str | None = None) -> dict[str, Any]:
    """Read a daily raw delivery and verify it against the immutable capture.

    The delivery is a copy, so it is only accepted when it still *is* a copy: every
    row digest must match the envelope, and the rows must be exactly what the
    captured bytes re-derive to. A rewritten price, a rewritten symbol or a
    fabricated body is refused typed -- there is no read path that skips
    verification, and no lane argument by design.
    """
    path = Path(path)
    body = _load_json(path, DELIVERY_VERSION)
    if set(body) != set(DELIVERY_FIELDS):
        raise EvidenceStateError(
            "invalid_delivery",
            {"path": str(path), "unexpected": sorted(set(body) - set(DELIVERY_FIELDS))[:4]},
        )
    if type(body["request"]) is not dict:
        raise EvidenceStateError("invalid_delivery", {"path": str(path), "request": "shape"})
    if body["request"].get("date") != path.parent.name:
        raise EvidenceStateError(
            "delivery_binding", {"path": str(path), "request": body["request"].get("date")}
        )
    rows = _rows_of(body, path)
    capture = _capture_block(body, path)
    provenance = body["provenance"]
    if type(provenance) is not dict:
        raise EvidenceStateError("invalid_delivery", {"path": str(path), "provenance": "shape"})
    if provenance.get("requested_date") != body["request"]["date"]:
        raise EvidenceStateError("invalid_delivery", {"path": str(path), "provenance": "date"})
    # A payload that could not be parsed has no date source to declare; one that
    # parsed must say exactly where its date came from.
    if provenance.get("parse_error") is None:
        if provenance.get("date_source") not in DATE_SOURCES:
            raise EvidenceStateError(
                "invalid_delivery", {"path": str(path), "provenance": "date_source"}
            )
    elif type(provenance["parse_error"]) is not str or not provenance["parse_error"]:
        raise EvidenceStateError(
            "invalid_delivery", {"path": str(path), "provenance": "parse_error"}
        )
    row_prov = body["row_provenance"]
    if type(row_prov) is not list or len(row_prov) != len(rows):
        raise EvidenceStateError("invalid_delivery", {"path": str(path), "row_provenance": "len"})
    if len(capture["row_sha256"]) != len(rows):
        raise EvidenceStateError("invalid_delivery", {"path": str(path), "row_sha256": "len"})
    for index, item in enumerate(rows):
        if capture["row_sha256"][index] != digest(item):
            raise EvidenceStateError(
                "delivery_row_digest", {"path": str(path), "row": index}
            )
        if (
            type(row_prov[index]) is not dict
            or row_prov[index].get("source_sha256") != digest(item)
        ):
            raise EvidenceStateError(
                "delivery_row_digest", {"path": str(path), "row": index, "field": "provenance"}
            )
    if capture_root is None:
        # Nothing may be consumed without re-reading the bytes it claims to copy.
        raise EvidenceStateError("delivery_unbound", {"path": str(path)})
    try:
        state = read_capture(capture_root, capture["task_id"], capture["generation"])
    except Exception as exc:  # noqa: BLE001 - every failure here is a refusal
        raise EvidenceStateError(
            "evidence_state_unreadable",
            {"path": str(path), "reason": _safe_code(exc)},
        ) from exc
    if state["locator"] != capture["locator"]:
        raise EvidenceStateError(
            "delivery_capture_mismatch", {"path": str(path), "reason": "locator"}
        )
    captured_rows, _, _, captured = payload(state, http_status=provenance.get("http_status"))
    # A delivery may legitimately record that its capture did not parse -- that is
    # evidence, not corruption. What it may not do is claim a different outcome
    # than the bytes produce.
    if captured["parse_error"] != provenance.get("parse_error"):
        raise EvidenceStateError(
            "delivery_capture_mismatch",
            {"path": str(path), "reason": "parse_error", "capture": captured["parse_error"]},
        )
    if canonical(captured_rows) != canonical(rows):
        raise EvidenceStateError(
            "delivery_capture_mismatch", {"path": str(path), "reason": "rows"}
        )
    body_sha = None
    for item in state["responses"]:
        if item["receipt"]["status"] == "captured" and item["body"] is not None:
            body_sha = item["receipt"]["sha256"]
            break
    if body_sha != capture["sha256"]:
        raise EvidenceStateError(
            "delivery_capture_mismatch", {"path": str(path), "reason": "body_sha256"}
        )
    return body


def read_calendar_delivery(
    path: Path | str, capture_root: Path | str | None = None
) -> dict[str, Any]:
    """Read a calendar delivery. A daily envelope is refused by version, always."""
    path = Path(path)
    body = _load_json(path, CALENDAR_DELIVERY_VERSION)
    if set(body) != set(CALENDAR_FIELDS):
        raise EvidenceStateError(
            "invalid_delivery",
            {"path": str(path), "unexpected": sorted(set(body) - set(CALENDAR_FIELDS))[:4]},
        )
    rows = _rows_of(body, path)
    capture = _capture_block(body, path)
    if len(capture["row_sha256"]) != len(rows):
        raise EvidenceStateError("invalid_delivery", {"path": str(path), "row_sha256": "len"})
    if len(body["row_provenance"]) != len(rows):
        raise EvidenceStateError("invalid_delivery", {"path": str(path), "row_provenance": "len"})
    for index, item in enumerate(rows):
        if capture["row_sha256"][index] != digest(item):
            raise EvidenceStateError("delivery_row_digest", {"path": str(path), "row": index})
    if capture_root is None:
        raise EvidenceStateError("delivery_unbound", {"path": str(path)})
    try:
        state = read_capture(capture_root, capture["task_id"], capture["generation"])
    except Exception as exc:  # noqa: BLE001 - every failure here is a refusal
        raise EvidenceStateError(
            "evidence_state_unreadable", {"path": str(path), "reason": _safe_code(exc)}
        ) from exc
    if state["locator"] != capture["locator"]:
        raise EvidenceStateError(
            "delivery_capture_mismatch", {"path": str(path), "reason": "locator"}
        )
    if canonical(payload(state)[0]) != canonical(rows):
        raise EvidenceStateError(
            "delivery_capture_mismatch", {"path": str(path), "reason": "rows"}
        )
    return body


# ---------------------------------------------------------------------------
# downstream half
# ---------------------------------------------------------------------------


def check_response_budget(rows: list[Any], source: Any) -> None:
    """The one response/row budget predicate. Both entry points call it.

    ``backfill``/``update`` and ``consume`` see the same captured rows, so they must
    apply the same bound; a delivery that is over budget cannot commit through one
    path and fail through the other.
    """
    if len(rows) > source.max_rows or len(encoded(rows).encode()) > source.max_response_bytes:
        raise DataError("response_limit")


class Collector:
    """Plan, acquire, then judge: the only collection path for CFFEX.

    Constructor and public methods keep the legacy identity
    (``backfill``/``update``/``_collect`` and the same report keys) so existing
    operators see the same commands; what changed is that acquisition now writes
    bytes first and judgement reads them back.
    """

    def __init__(
        self,
        config: Config,
        journal: Journal,
        calendar: TradingCalendar,
        client: Any = None,
        stop: threading.Event | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ):
        self.config, self.journal, self.calendar = config, journal, calendar
        self.stop = stop or threading.Event()
        # The daily lane's transport runs here, in the collector process: the
        # capture has to precede the provider's parser, and that parser lives on
        # the far side of ``ProcessClient``. The calendar lane still crosses the
        # process boundary (``refresh_calendar`` receives a ``ProcessClient``).
        self.client = client if client is not None else CffexCsvClient(self.stop)
        self.clock = clock

    def close(self) -> None:
        self.client.close()

    @property
    def evidence_root(self) -> Path:
        return self.config.evidence

    # -- plan derivation ----------------------------------------------------

    def _sequence(self) -> range:
        return range(self.config.source.retries + 1)

    def _task_id(self, day: date) -> str:
        source = self.config.source
        return identity.task_identity(
            self.config.source_id, source.provider, source.model, day
        )

    def _base(self, day: date, attempt: int, token: str) -> str:
        return identity.capture_base(
            self.config.fingerprint(), self.config.source_id, day, attempt, token
        )

    def _generation(self, task_id: str, base: str) -> tuple[str, bool]:
        """Pick the capture generation for one attempt; reuse exact evidence.

        An absent generation captures. An interrupted one resumes its durable
        prefix. A completed capture that downstream has not rejected replays its
        bytes with no source call. A rejected, cancelled, failed or partial
        generation advances to the next epoch, which is the only way a provider
        may be called again -- and the old generation stays immutable.

        The formulas themselves live in :mod:`.identity`, which the
        acquisition-only task also calls: ``cffex acquire`` files its capture
        under this same ``(task_id, generation)``, so this scan finds it and
        replays it instead of fetching the day again.
        """
        for epoch in range(MAX_EVIDENCE_EPOCHS):
            generation = identity.generation(base, epoch)
            state = existing_state(self.evidence_root, task_id, generation)
            if state is None:
                return generation, False
            if self.journal.verdict(state["locator"]) == "rejected":
                continue
            if state["complete"] and state["status"] == "captured":
                return generation, True
            if state["reason"] == "interrupted":
                return generation, True
        raise EvidenceStateError("evidence_generation_exhausted", {"task_id": task_id})

    def _plan(
        self,
        day: date,
        attempt: int,
        token: str,
        replayed: dict[tuple[date, int], bool] | None = None,
    ) -> dict[str, Any]:
        source = self.config.source
        evidence_source = source_of(source)
        task_id = self._task_id(day)
        generation, replayed_state = self._generation(task_id, self._base(day, attempt, token))
        if replayed is not None:
            replayed[(day, attempt)] = replayed_state
        entry: dict[str, Any] = {
            "kind": "daily",
            "trade_date": day.isoformat(),
            "source_id": self.config.source_id,
            "attempt": attempt,
            "source": evidence_source.model_dump(),
            "task": {
                "version": EVIDENCE_VERSION,
                "task_id": task_id,
                "generation": generation,
                "source": {"provider": source.provider, "model": source.model},
                "requests": [
                    {
                        "request_id": self.config.source_id,
                        "params": {"date": day.isoformat()},
                    }
                ],
            },
        }
        entry["entry_id"] = entry_identity(entry)
        return {
            "version": PLAN_VERSION,
            "generation": digest(
                {
                    "base": self._base(day, attempt, token),
                    "generation": generation,
                    "entry_id": entry["entry_id"],
                }
            ),
            "entries": [entry],
        }

    def _plan_session(
        self, days: list[date], refresh_days: set[date], max_days: int | None
    ) -> dict[str, Any]:
        """Select work, resolve every generation, and preflight the whole batch.

        The preflight reads (and therefore verifies) every generation this
        session could touch *before* the run record is created, so unreadable
        private state refuses with no journal row, no evidence mutation and no
        source call.
        """
        if max_days is not None and (type(max_days) is not int or max_days < 1):
            raise ConfigurationError("invalid_max_days")
        source = self.config.source
        pending = [
            day
            for day in days
            if day in refresh_days
            or not self.journal.complete(day.isoformat(), source.provider, source.model)
        ]
        selected = pending[:max_days] if max_days is not None else pending
        fresh = any(day in refresh_days for day in selected)
        token, token_value = (
            self.journal.evidence_token(self.clock().astimezone(UTC).isoformat())
            if fresh
            else ("configured", None)
        )
        plans: dict[tuple[date, int], dict[str, Any]] = {}
        replays: dict[tuple[date, int], bool] = {}
        for day in selected:
            for attempt in self._sequence():
                plans[(day, attempt)] = self._plan(day, attempt, token, replays)
        if token_value is not None:
            self.journal.commit_evidence_token(token_value)
        return {
            "planned": len(days),
            "skipped": len(days) - len(pending),
            "selected": selected,
            "deferred": len(pending) - len(selected),
            "plans": plans,
            "replays": replays,
            "token": token,
        }

    # -- public commands ----------------------------------------------------

    def backfill(
        self,
        start: date | None = None,
        end: date | None = None,
        refresh: bool = False,
        max_days: int | None = None,
    ) -> dict[str, Any]:
        cutoff = eligible_day(self.config, self.clock())
        start = start or self.config.start_date
        end = end or cutoff
        if start < self.config.start_date:
            raise ConfigurationError("start_before_configured_origin")
        if end > cutoff:
            raise ConfigurationError("publication_cutoff_exceeded")
        days = self.calendar.days(start, end)
        session = self._plan_session(days, set(days) if refresh else set(), max_days)
        return self._collect("backfill", session, start, end)

    def update(self, as_of: date | None = None) -> dict[str, Any]:
        end = eligible_day(self.config, self.clock(), as_of)
        rolling_start = max(self.config.start_date, end - timedelta(days=6))
        persisted = self.journal.get_meta("update_origin")
        origin = (
            iso_date(persisted) if persisted else (self.config.update_start_date or rolling_start)
        )
        if self.config.update_start_date:
            origin = min(origin, self.config.update_start_date)
        if end < origin:
            raise ConfigurationError("as_of_before_update_origin")
        # Validate the complete catch-up range before writing cursors or requesting data.
        days = self.calendar.days(origin, end)
        session = self._plan_session(days, {day for day in days if day >= rolling_start}, None)
        self.journal.set_meta("update_origin", origin.isoformat())
        previous_cursor = self.journal.get_meta("update_cursor")
        result = self._collect("update", session, origin, end)
        try:
            # Advance only through contiguous successful dates; closed days are explicitly
            # covered by the calendar, not inferred from empty vendor responses.
            if result["status"] == "interrupted" or self.stop.is_set():
                result["status"] = "interrupted"
                result["update_cursor"] = None
                self.journal.abort_run(result["run_id"], result, "interrupted")
                return result
            cursor_end = max(end, iso_date(previous_cursor)) if previous_cursor else end
            cursor_days = self.calendar.days(origin, cursor_end)
            cursor = cursor_end
            for day in cursor_days:
                if not self.journal.complete(
                    day.isoformat(), self.config.source.provider, self.config.source.model
                ):
                    cursor = day - timedelta(days=1)
                    break
            result["update_cursor"] = cursor.isoformat()
            with self.journal.transaction():
                if self.stop.is_set():
                    raise KeyboardInterrupt
                self.journal.set_meta("update_cursor", cursor.isoformat())
                self.journal.finish_run(result["run_id"], result)
                # A signal before the atomic commit must roll back both assertions.
                if self.stop.is_set():
                    raise KeyboardInterrupt
        except BaseException as exc:
            result["status"] = "interrupted" if isinstance(exc, KeyboardInterrupt) else "failed"
            result["update_cursor"] = None
            result["errors"].append(
                {"code": "storage_or_runtime_failure", "error_type": type(exc).__name__}
            )
            try:
                self.journal.abort_run(
                    result["run_id"],
                    result,
                    (
                        "interrupted"
                        if result["status"] == "interrupted"
                        else "storage_or_runtime_failure"
                    ),
                )
            except BaseException as cleanup_error:
                exc.add_note(f"CFFEX cleanup failed: {type(cleanup_error).__name__}")
            raise
        return result

    def consume(
        self, start: date | None = None, end: date | None = None, limit: int | None = None
    ) -> dict[str, Any]:
        """Re-consume already-delivered raw evidence; never calls a provider.

        This is the reachable downstream consumer: it reads the one versioned
        delivery format, applies the business predicates and commits revisions.
        It exists so the delivery can be exercised without acquisition, and so a
        crash between delivery and commit is recoverable offline.
        """
        cutoff = eligible_day(self.config, self.clock())
        start = start or self.config.start_date
        end = end or cutoff
        if start < self.config.start_date:
            raise ConfigurationError("start_before_configured_origin")
        if end > cutoff:
            raise ConfigurationError("publication_cutoff_exceeded")
        days = self.calendar.days(start, end)
        report: dict[str, Any] = {
            "status": "running",
            "command": "consume",
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
            "end_inclusive": True,
            "planned_days": len(days),
            "requested_days": len(days[:limit] if limit else days),
            "completed_days": 0,
            "failed_days": 0,
            "missing_evidence_days": 0,
            "provider_calls": 0,
            "rows_committed": 0,
            "changed_days": 0,
            "unchanged_days": 0,
            "errors": [],
            "provider": self.config.source.provider,
            "model": self.config.source.model,
        }
        report["run_id"] = self.journal.start_run("consume", report)
        try:
            for day in days[:limit] if limit else days:
                path = self.delivered(day)
                if path is None:
                    report["missing_evidence_days"] += 1
                    continue
                delivery = read_delivery(path, capture_root=self.config.evidence)
                attempt_id = self.journal.begin_attempt(report["run_id"], day.isoformat())
                rows = delivery["rows"]
                metadata = delivery["metadata"]
                observed = delivery["observed_at"]
                try:
                    if delivery["provenance"]["parse_error"]:
                        raise DataError(delivery["provenance"]["parse_error"])
                    check_response_budget(rows, self.config.source)
                    records = normalize(
                        rows,
                        day,
                        date_source=delivery["provenance"]["date_source"],
                        units=unit_authority(delivery["provenance"], self.config.units),
                    )
                    self.journal.validate_contract_set(day.isoformat(), records)
                    changed = self.journal.commit_day(
                        attempt_id,
                        day.isoformat(),
                        records,
                        metadata,
                        observed,
                        self.config.source.provider,
                        self.config.source.model,
                    )
                except DataError as exc:
                    code = str(exc)
                    self.journal.fail_attempt(attempt_id, day.isoformat(), code)
                    report["failed_days"] += 1
                    report["errors"].append({"trading_date": day.isoformat(), "code": code})
                    continue
                self.journal.record_verdict(
                    delivery["evidence"]["locator"],
                    task_id=delivery["evidence"]["task_id"],
                    generation=delivery["evidence"]["generation"],
                    day=day.isoformat(),
                    attempt=delivery["plan"]["entries"][0]["attempt"],
                    plan_sha256=digest(delivery["plan"]),
                    entry_id=delivery["evidence"]["entry_id"],
                    verdict="accepted",
                    code=None,
                    raw_path=path.name,
                    detail={"consumed": True, "source_calls": 0},
                )
                report["completed_days"] += 1
                report["rows_committed"] += len(records)
                report["changed_days" if changed else "unchanged_days"] += 1
            if self.stop.is_set():
                report["status"] = "interrupted"
            elif report["failed_days"]:
                report["status"] = (
                    "partial" if report["completed_days"] else "failed"
                )
            else:
                report["status"] = "complete"
            report["unprocessed_days"] = 0
            if report["status"] == "interrupted":
                self.journal.abort_run(report["run_id"], report, "interrupted")
            else:
                self.journal.finish_run(report["run_id"], report)
        except BaseException as exc:
            report["status"] = "interrupted" if isinstance(exc, KeyboardInterrupt) else "failed"
            report["errors"].append(
                {"code": "storage_or_runtime_failure", "error_type": type(exc).__name__}
            )
            try:
                self.journal.abort_run(
                    report["run_id"],
                    report,
                    "interrupted"
                    if report["status"] == "interrupted"
                    else "storage_or_runtime_failure",
                )
            except BaseException as cleanup_error:
                exc.add_note(f"CFFEX cleanup failed: {type(cleanup_error).__name__}")
            raise
        return report

    def delivered(self, day: date) -> Path | None:
        """The newest raw delivery for one trading date, if any was written."""
        folder = Path(self.config.root) / "raw" / day.isoformat()
        if not folder.is_dir():
            return None
        files = sorted(
            folder.glob("*.json"), key=lambda item: (item.stat().st_mtime_ns, item.name)
        )
        return files[-1] if files else None

    # -- one session --------------------------------------------------------

    def _collect(
        self, command: str, session: dict[str, Any], start: date, end: date
    ) -> dict[str, Any]:
        source = self.config.source
        selected: list[date] = session["selected"]
        report = {
            "status": "running",
            "command": command,
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
            "end_inclusive": True,
            "planned_days": session["planned"],
            "skipped_days": session["skipped"],
            "requested_days": len(selected),
            "deferred_days": session["deferred"],
            "completed_days": 0,
            "failed_days": 0,
            "provider_calls": 0,
            "evidence_captures": 0,
            "evidence_replays": 0,
            "rows_received": 0,
            "rows_committed": 0,
            "changed_days": 0,
            "unchanged_days": 0,
            "warning_count": 0,
            "errors": [],
            "provider": source.provider,
            "model": source.model,
        }
        report["run_id"] = self.journal.start_run(command, report)
        try:
            if self.stop.is_set():
                report["status"] = "interrupted"
            for day in selected:
                if self.stop.is_set():
                    report["status"] = "interrupted"
                    break
                error = None
                for attempt in self._sequence():
                    if self.stop.is_set():
                        break
                    attempt_id = self.journal.begin_attempt(report["run_id"], day.isoformat())
                    plan = session["plans"][(day, attempt)]
                    entry = plan["entries"][0]
                    results = acquire(
                        self.evidence_root, plan, client=self.client, stop=self.stop
                    )
                    result = results[0]
                    report["provider_calls"] += result["source_calls"]
                    # A generation that already existed is replayed, not captured.
                    replayed = session["replays"].get((day, attempt))
                    report["evidence_replays" if replayed else "evidence_captures"] += 1
                    observed = self.clock().astimezone(UTC).isoformat()
                    if result["status"] != "captured" or not result["complete"]:
                        error = result["reason"] or "acquisition_incomplete"
                        self.journal.fail_attempt(attempt_id, day.isoformat(), error)
                        if attempt < source.retries and not self.stop.wait(
                            source.retry_delay_seconds
                        ):
                            continue
                        break
                    state = read_capture(
                        self.evidence_root, result["task_id"], result["generation"]
                    )
                    delivery, raw_path = self._deliver(attempt_id, day, plan, state, result)
                    report["rows_received"] += len(delivery["rows"])
                    report["warning_count"] += delivery["warning_count"]
                    try:
                        if delivery["provenance"]["parse_error"]:
                            raise DataError(delivery["provenance"]["parse_error"])
                        check_response_budget(delivery["rows"], source)
                        rows = normalize(
                            delivery["rows"],
                            day,
                            date_source=delivery["provenance"]["date_source"],
                            units=unit_authority(delivery["provenance"], self.config.units),
                        )
                        self.journal.validate_contract_set(day.isoformat(), rows)
                        changed = self.journal.commit_day(
                            attempt_id,
                            day.isoformat(),
                            rows,
                            delivery["metadata"],
                            observed,
                            source.provider,
                            source.model,
                        )
                    except DataError as exc:
                        # Business rejection: the capture and the delivery stay
                        # readable and the verdict keeps replay from reusing it.
                        error = str(exc)
                        self.journal.fail_attempt(attempt_id, day.isoformat(), error)
                        self.journal.record_verdict(
                            result["locator"],
                            task_id=result["task_id"],
                            generation=result["generation"],
                            day=day.isoformat(),
                            attempt=attempt,
                            plan_sha256=digest(plan),
                            entry_id=entry["entry_id"],
                            verdict="rejected",
                            code=error,
                            raw_path=raw_path,
                            detail={
                                "requested_date": day.isoformat(),
                                "source_dates": source_dates(delivery["rows"]),
                                "capture": result["status"],
                                "reason": result["reason"],
                            },
                        )
                        if attempt < source.retries and not self.stop.wait(
                            source.retry_delay_seconds
                        ):
                            continue
                        break
                    self.journal.record_verdict(
                        result["locator"],
                        task_id=result["task_id"],
                        generation=result["generation"],
                        day=day.isoformat(),
                        attempt=attempt,
                        plan_sha256=digest(plan),
                        entry_id=entry["entry_id"],
                        verdict="accepted",
                        code=None,
                        raw_path=raw_path,
                        detail={
                            "requested_date": day.isoformat(),
                            "source_dates": source_dates(delivery["rows"]),
                            "capture": result["status"],
                            "source_calls": result["source_calls"],
                        },
                    )
                    report["completed_days"] += 1
                    report["rows_committed"] += len(rows)
                    report["changed_days" if changed else "unchanged_days"] += 1
                    error = None
                    break
                if self.stop.is_set():
                    report["status"] = "interrupted"
                    break
                if error:
                    report["failed_days"] += 1
                    report["errors"].append({"trading_date": day.isoformat(), "code": error})
            if self.stop.is_set():
                report["status"] = "interrupted"
            if report["status"] != "interrupted":
                if report["failed_days"]:
                    report["status"] = (
                        "partial"
                        if report["completed_days"] or report["skipped_days"]
                        else "failed"
                    )
                else:
                    report["status"] = "partial" if report["deferred_days"] else "complete"
            report["unprocessed_days"] = (
                len(selected) - report["completed_days"] - report["failed_days"]
            )
            if report["status"] == "interrupted":
                self.journal.abort_run(report["run_id"], report, "interrupted")
            elif command != "update":
                with self.journal.transaction():
                    if self.stop.is_set():
                        raise KeyboardInterrupt
                    self.journal.finish_run(report["run_id"], report)
                    # Cancellation before commit must not assert successful completion.
                    if self.stop.is_set():
                        raise KeyboardInterrupt
            # Updates remain running until cursor and summary commit atomically.
        except BaseException as exc:
            report["status"] = "interrupted" if isinstance(exc, KeyboardInterrupt) else "failed"
            report["errors"].append(
                {"code": "storage_or_runtime_failure", "error_type": type(exc).__name__}
            )
            report["unprocessed_days"] = (
                len(selected) - report["completed_days"] - report["failed_days"]
            )
            try:
                self.journal.abort_run(
                    report["run_id"],
                    report,
                    (
                        "interrupted"
                        if report["status"] == "interrupted"
                        else "storage_or_runtime_failure"
                    ),
                )
            except BaseException as cleanup_error:
                # Recovery retries cleanup if the journal itself is unavailable.
                # Never replace the original failure with a secondary cleanup failure.
                exc.add_note(f"CFFEX cleanup failed: {type(cleanup_error).__name__}")
            raise
        return report

    def _deliver(
        self,
        attempt_id: str,
        day: date,
        plan: dict[str, Any],
        state: dict[str, Any],
        result: dict[str, Any],
    ) -> tuple[dict[str, Any], str]:
        """Write the versioned raw delivery *before* any predicate reads it.

        The envelope is bound to the capture it copies: the captured body digest,
        the per-row digests and one provenance record per row. ``read_delivery``
        re-reads the capture and refuses any row that the bytes do not re-derive,
        so the delivery cannot drift from its evidence.
        """
        rows, metadata, warnings, provenance = payload(
            state, http_status=result.get("http_status")
        )
        entry = plan["entries"][0]
        receipts = [
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
        observed = self.clock().astimezone(UTC).isoformat()
        authority = unit_authority(provenance, self.config.units)
        captured_at = receipts[0]["acquired_at"] if receipts else None
        delivery = {
            "version": DELIVERY_VERSION,
            "request": {"date": day.isoformat()},
            "source": entry["source"],
            "capture": {
                "evidence_version": EVIDENCE_VERSION,
                "locator": state["locator"],
                "task_id": result["task_id"],
                "generation": result["generation"],
                "evidence_kind": provenance["evidence_kind"],
                "transformation": provenance["transformation"],
                "media_type": receipts[0]["media_type"] if receipts else None,
                "sha256": receipts[0]["sha256"] if receipts else "0" * 64,
                "size": receipts[0]["size"] if receipts else 0,
                "row_sha256": [digest(item) for item in rows],
            },
            "rows": rows,
            "row_provenance": row_provenance(rows, provenance),
            "provenance": {
                **provenance,
                "requested_date": day.isoformat(),
                "row_count": len(rows),
                "unit_authority": authority.authority,
            },
            "metadata": {
                **metadata,
                "provider": self.config.source.provider,
                "model": self.config.source.model,
            },
            "warning_count": warnings,
            "observed_at": observed,
            "captured_at": captured_at,
            "event_time_precision": "date",
            "publication_time": None,
            "evidence": {
                "version": EVIDENCE_VERSION,
                "locator": state["locator"],
                "task_id": result["task_id"],
                "generation": result["generation"],
                "entry_id": entry["entry_id"],
                "plan_sha256": digest(plan),
                "status": state["status"],
                "complete": state["complete"],
                "captured_at": captured_at,
                "source_calls": result["source_calls"],
                "responses": receipts,
            },
            "plan": plan,
        }
        path = self.journal.archive(attempt_id, delivery)
        self.journal.attach_raw(attempt_id, path, observed, warnings)
        return delivery, path


# ---------------------------------------------------------------------------
# calendar: capture first, accept downstream
# ---------------------------------------------------------------------------


def refresh_calendar(
    config: Config, journal: Journal, client: Any, stop: threading.Event | None = None
) -> dict[str, Any]:
    """Capture the reference calendar, then let the downstream accept it.

    The response is written to the same immutable store as a trading day before
    any date, coverage or session-removal rule runs; a rejected calendar is
    captured evidence, not a silent success and not an overwritten file.
    """
    target = config.calendar_file
    if protected_target(config.root, target, config.evidence):
        raise ConfigurationError("protected_calendar_path")
    report: dict[str, Any] = {
        "command": "calendar-refresh",
        "status": "running",
        "provider_calls": 0,
        "raw_paths": [],
        "errors": [],
        "warning_count": 0,
    }
    report["run_id"] = journal.start_run("calendar-refresh", report)
    try:
        report.update(_refresh_calendar(config, journal, client, stop, report))
    except BaseException as exc:
        code = (
            exc.code if isinstance(exc, ProviderFailure) else "calendar_storage_or_runtime_failure"
        )
        report["status"] = "interrupted" if code == "cancelled" else "failed"
        report["errors"].append({"code": code, "error_type": type(exc).__name__})
        journal.finish_run(report["run_id"], report)
        raise
    journal.finish_run(report["run_id"], report)
    return report


def _refresh_calendar(
    config: Config,
    journal: Journal,
    client: Any,
    stop: threading.Event | None,
    report: dict[str, Any],
) -> dict[str, Any]:
    """Acquire through the same killable boundary; replace only accepted coverage."""
    stop = stop or threading.Event()
    source = config.source
    settings = source.model_dump(exclude={"model"})
    calendar_model = "AkshareToolTradeDateHistSina"
    calendar_source = CalendarSource(**{**settings, "model": calendar_model})
    token, token_value = journal.evidence_token(datetime.now(UTC).isoformat())
    task_id = digest(
        {"kind": "cffex-calendar", "source_id": config.source_id, "provider": source.provider}
    )
    previous = load_calendar(config.calendar_file) if config.calendar_file.exists() else None
    journal.commit_evidence_token(token_value)
    for retry in range(source.retries + 1):
        if stop.is_set():
            raise ProviderFailure("cancelled")
        generation = digest(
            {
                "task_id": task_id,
                "attempt": retry,
                "token": token,
                "version": EVIDENCE_VERSION,
            }
        )
        task = {
            "version": EVIDENCE_VERSION,
            "task_id": task_id,
            "generation": generation,
            "source": {"provider": source.provider, "model": calendar_model},
            "requests": [{"request_id": "calendar", "params": {}}],
        }
        entry: dict[str, Any] = {
            "kind": "calendar",
            "trade_date": None,
            "source_id": config.source_id,
            "attempt": retry,
            "source": source_of(calendar_source).model_dump(),
            "task": task,
        }
        entry["entry_id"] = entry_identity(entry)
        plan = {
            "version": PLAN_VERSION,
            "generation": digest([task_id, retry, token]),
            "entries": [entry],
        }
        try:
            results = acquire(config.evidence, plan, client=client, stop=stop)
            result = results[0]
            report["provider_calls"] += result["source_calls"]
            if result["status"] != "captured" or not result["complete"]:
                raise DataError(result["reason"] or "acquisition_incomplete")
            state = read_capture(config.evidence, result["task_id"], result["generation"])
            rows, _, warnings, provenance = payload(state, http_status=result.get("http_status"))
            report["warning_count"] += warnings
            captured_at = (
                result["responses"][0]["acquired_at"] if result["responses"] else None
            )
            raw_path = journal.archive(
                uuid.uuid4().hex,
                {
                    # A calendar delivery is its own schema and its own version:
                    # no reader may mistake it for a daily delivery, and no daily
                    # delivery may be read as one.
                    "version": CALENDAR_DELIVERY_VERSION,
                    "request": {"date": "calendar"},
                    "source": entry["source"],
                    "capture": {
                        "evidence_version": EVIDENCE_VERSION,
                        "locator": state["locator"],
                        "task_id": result["task_id"],
                        "generation": result["generation"],
                        "evidence_kind": provenance["evidence_kind"],
                        "transformation": provenance["transformation"],
                        "media_type": (
                            result["responses"][0]["media_type"] if result["responses"] else None
                        ),
                        "sha256": (
                            result["responses"][0]["sha256"]
                            if result["responses"]
                            else "0" * 64
                        ),
                        "size": (
                            result["responses"][0]["size"] if result["responses"] else 0
                        ),
                        "row_sha256": [digest(item) for item in rows],
                    },
                    "rows": rows,
                    "row_provenance": row_provenance(rows, provenance),
                    "provenance": {
                        **provenance,
                        "requested_date": None,
                        "row_count": len(rows),
                        "unit_authority": "not_applicable",
                    },
                    "warning_count": warnings,
                    "observed_at": utc_now(),
                    "evidence": {
                        "version": EVIDENCE_VERSION,
                        "locator": state["locator"],
                        "task_id": result["task_id"],
                        "generation": result["generation"],
                        "entry_id": entry["entry_id"],
                        "plan_sha256": digest(plan),
                        "status": state["status"],
                        "complete": state["complete"],
                        "captured_at": captured_at,
                        "source_calls": result["source_calls"],
                        "responses": result["responses"],
                    },
                    "plan": plan,
                },
            )
            report["raw_paths"].append(raw_path)
            calendar = accept_calendar(rows, previous, source.max_rows)
        except (ProviderFailure, ValueError, TypeError) as exc:
            code = (
                exc.code
                if isinstance(exc, ProviderFailure)
                else (str(exc) if isinstance(exc, DataError) else "invalid_calendar_response")
            )
            report["errors"].append({"code": code})
            if retry < source.retries and not stop.wait(source.retry_delay_seconds):
                continue
            raise ProviderFailure(code) from exc
        target = config.calendar_file
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = target.with_name(f".{target.name}.{uuid.uuid4().hex}.tmp")
        try:
            with temporary.open("x", encoding="utf-8") as stream:
                stream.write(encoded(calendar.model_dump(mode="json")) + "\n")
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, target)
            sync_directory(target.parent)
        finally:
            temporary.unlink(missing_ok=True)
        return {
            "status": "complete",
            "trading_dates": len(calendar.trading_dates),
            "coverage_start": calendar.coverage_start.isoformat(),
            "coverage_end": calendar.coverage_end.isoformat(),
            "calendar_file": str(target),
            "raw_path": raw_path,
        }
    raise AssertionError("unreachable")

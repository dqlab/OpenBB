# Preserve the acquisition/delivery protocol together during this bounded promotion.
# pylint: disable=too-many-lines
# Evidence schemas require exact built-in types, including rejection of bool-as-int.
# pylint: disable=unidiomatic-typecheck
# OpenBB and collector callback signatures retain protocol parameters.
# pylint: disable=unused-argument

"""Capture first, judge later: the ETF option chain, split in two halves.

Acquisition (:func:`acquire`, :func:`existing_state`, :mod:`.source`) validates
plans, preflights the evidence budget, verifies private capture state and persists
each transport response with its receipt before returning the body bytes.

Downstream (:class:`Collector`, :mod:`.parse`, :mod:`.quality`, :mod:`.storage`,
:mod:`.delivery`, ``cli``, ``scheduled``) decodes captured bodies, applies identity,
quality, shrinkage and coverage checks, writes bound raw deliveries and commits
revisions. Export refuses unknown state.

The lane used to ask the provider for parsed rows and judge them in the same
breath.  It now asks the transport for bytes, stores them in a private immutable
``raw-acquisition-v1`` capture, writes a ``etf-raw-delivery-v2`` document bound to
that capture, and only then lets any business predicate run.
"""

from __future__ import annotations

import json
import os
import threading
import time
import uuid
from collections.abc import Callable
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, cast

from openbb_collector_core.models import ProviderFailure
from openbb_collector_core.process import ProcessClient
from openbb_collector_core.raw_acquisition import (
    RawResponse,
    capture,
    capture_locator,
    plan_process_capture,
    read_capture,
    validate_task,
)

from . import capture_status, identity
from . import parse as parser
from .acquire import PLAN_VERSION as ACQUIRE_PLAN_VERSION
from .config import SHANGHAI, Config, ConfigurationError, TradingCalendar, load_calendar
from .quality import (
    DataError,
    contracts,
    coverage,
    coverage_accepts,
    quotes,
    unit_authority,
)
from .quality import unit_labels as unit_label_map
from .source import EtfChainClient, plan_kind
from .storage import Journal, digest, encoded, protected_target, sync_directory

#: The unsubstituted provider boundary.  A substituted one (an offline double set
#: over ``runtime.ProcessClient``) cannot produce source bytes, so the document
#: lane stands down -- see :meth:`Collector._client_for`.
_REAL_PROCESS_CLIENT = ProcessClient

EVIDENCE_VERSION = "raw-acquisition-v1"
DELIVERY_VERSION = "etf-raw-delivery-v3"
CALENDAR_DELIVERY_VERSION = "etf-raw-calendar-v1"
PLAN_VERSION = "etf-raw-plan-v1"
PROVIDER_TRANSFORMATION = "openbb-process-response-json-v1"
MAX_EVIDENCE_EPOCHS = 64
#: The provider model that returns a contract master (both exchanges).
MASTER_MODEL = "AkshareEtfOptionContracts"

#: A v3 delivery carries no HTTP status key.  The status is not lost: it is
#: persisted beside the receipts it describes, per request, in the capture locator
#: (``capture_status``: ``status.json``), bound to the task digest and to each
#: receipt's sha256, and chained by ``capture_manifest`` (``acquire_manifest.json``):
#: the status bytes, the task, every receipt and the core terminal/durable
#: certificates are hashed in one record that the reader recomputes.  That is the
#: only place a status can be *checked* rather than believed, because
#: ``raw-acquisition-v1``'s receipt has no slot for one. The collector reads it back
#: for every attempt, including a
#: replayed one -- and refuses a non-2xx attempt (``source_body_http_status``); the
#: journal's attempt row (:meth:`Journal.attach_evidence`) records the same value
#: as a run observation.  Adding the field to a delivery is still refused as an
#: unknown key (``invalid_delivery``): one fact, one place.
DELIVERY_FIELDS = (
    "version",
    "kind",
    "request",
    "source",
    "capture",
    "rows",
    "row_sha256",
    "row_provenance",
    "provenance",
    "metadata",
    "warning_count",
    "observed_at",
    "captured_at",
    "evidence",
    "plan",
)
CAPTURE_FIELDS = (
    "sha256",
    "size",
    "evidence_kind",
    "transformation",
    "locator",
    "media_type",
)
#: Every key the reader re-derives or cross-checks; an unknown key is a schema
#: change, not a harmless extra.
PROVENANCE_FIELDS = (
    "kind",
    "date_source",
    "date_column",
    "unit_source",
    "unit_labels",
    "transforms",
    "row_count",
    "parse_error",
)
UNIT_SOURCES = ("source", "configuration", "absent")
#: ``evidence`` is a block of facts about the capture, and every one of them is
#: re-derived: the entry id from the task and the reading, the plan digest from the
#: task, the status and completeness from the capture itself.
EVIDENCE_FIELDS = ("entry_id", "plan_sha256", "status", "complete")
#: The aggregate envelope is not a document capture, but it may not be unlabelled
#: either: it states its own kind, transformation, date source and losses.
AGGREGATE_PROVENANCE_FIELDS = (
    "evidence_kind",
    "transformation",
    "capture",
    "sha256",
    "size",
    "media_type",
    "date_source",
    "unit_source",
    "unit_labels",
    "transforms",
    "parse_error",
    "http_status_available",
    "lost",
    "retained_verbatim",
)
#: Exactly what the aggregate lane cannot give a reader, in words an operator can
#: act on.  The provider issues ~5xN GETs per underlying (category index, one
#: expiry validation per month, two discovery lists per month, then quote
#: batches); this lane stores the provider's model rows, so those bodies are gone.
AGGREGATE_LOSS = {
    "per_get_bodies": "not stored: the provider aggregates its own discovery and quote GETs",
    "per_get_status": "unavailable: no per-GET HTTP status reaches this lane",
    "per_get_media_type": "unavailable: one envelope media type is recorded, not per GET",
    "batch_framing": "lost: the provider re-groups quote batches before this capture",
    "json_key_order": "lost: category/expiry JSON is stored as re-serialised dicts",
}
AGGREGATE_RETAINED = {
    "discovery_text": "verbatim, in each row's source_row.discovery.raw_response",
    "per_symbol_quote_payload": "verbatim, in each row's source_row.quote.raw_payload",
    "category_and_expiry_json": "as dicts, in source_row.category / source_row.expiry_validation",
}

CALENDAR_FIELDS = (
    "version",
    "request",
    "capture",
    "trading_dates",
    "provenance",
    "captured_at",
    "evidence",
)
_HEX = 64


class EvidenceStateError(RuntimeError):
    """A typed refusal about private evidence; never a data verdict."""

    def __init__(self, code: str, detail: dict[str, Any] | None = None):
        self.code = code
        self.detail = detail or {}
        super().__init__(code)


def _require(condition: bool, code: str) -> None:
    if not condition:
        raise EvidenceStateError(code)


def _safe_code(exc: BaseException) -> str:
    text = str(exc)
    if isinstance(exc, ValueError) and text.isidentifier() and len(text) <= 64:
        return text
    return "evidence_state_invalid"


def source_of(limits: Any) -> Any:
    """The evidence-source view of a configured source (provider/model/limit)."""
    return limits


def master_request(exchange: str, settings: Any) -> dict[str, Any]:
    """One exchange's master request, built in exactly one place.

    The acquisition plan and the collection run must spell this identically: the
    parameters are part of the task id, so one extra key would address different
    bytes and the hand-off would silently re-fetch.
    """
    request = {"exchange": exchange, "source": settings.source}
    if settings.underlyings:
        request["underlyings"] = ",".join(settings.underlyings)
    return request


def acquisition_plan(config: Config, target: date) -> dict[str, Any]:
    """The literal ``etf-acquire-plan-v2`` for one day: documents, not rows.

    One master per configured exchange that is addressable as a single document.
    Quote blocks are deliberately absent -- their membership is a function of the
    master's *content*, and acquisition may not read content.  The identity token
    is a stable function of (fingerprint, source, day), so re-planning the same day
    addresses the same evidence and a completed acquisition is reused as is.
    """
    fingerprint = digest(config.fingerprint())
    token = digest(
        {"plan": fingerprint, "source_id": config.source_id, "date": target.isoformat()}
    )
    source = config.source(MASTER_MODEL)
    requests = []
    for exchange, settings in sorted(config.exchanges.items()):
        params = master_request(exchange, settings)
        kind = plan_kind(MASTER_MODEL, params)
        if kind is None:
            # The Sina master is an aggregate of many GETs.  It is not one document
            # and this plan will not pretend that it is.
            raise ConfigurationError("exchange_not_addressable_as_one_document")
        requests.append(
            {
                # The same request id the collector's own task carries: the task
                # bytes are sha-bound, so this string is part of the identity.
                "request_id": f"{config.source_id}:{MASTER_MODEL}",
                "kind": kind,
                "exchange": exchange,
                "params": params,
            }
        )
    return {
        "version": ACQUIRE_PLAN_VERSION,
        "identity": {
            "source_id": config.source_id,
            "date": target.isoformat(),
            "fingerprint": fingerprint,
            "token": token,
            "attempt": 0,
        },
        "source": {
            "provider": source.provider,
            "model": source.model,
            "max_response_bytes": source.max_response_bytes,
            "timeout_seconds": source.timeout_seconds,
        },
        "requests": requests,
    }


def existing_state(
    root: Path | str, task_id: str, generation: str
) -> dict[str, Any] | None:
    """Durable capture state, or ``None`` when this generation was never written.

    Unreadable state is never treated as absent: that would refetch over corrupt
    evidence.  It becomes a typed refusal naming the underlying reason.
    """
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


def _fetch_adapter(client: Any, task_id: str, source: Any, counter: dict[str, Any]):
    """One adapter, two declared evidence kinds, decided by what the client returns.

    * a :class:`.source.SourceBody` is a literal transport response: its bytes are
      stored exactly as received (``raw-bytes``, no transformation label) and only
      the HTTP status travels alongside;
    * an OpenBB ``Response`` is the provider-transformed object the pre-existing
      doubles and the aggregate Sina master produce: rows already parsed (and, for
      the Sina master, strike/expiry already derived by the provider adapter).  It
      keeps the ``provider-transformed`` label so nothing downstream can mistake
      it for source bytes.
    """

    def fetch(request: dict[str, Any]) -> RawResponse:
        counter["calls"] += 1
        result = client.fetch(task_id, source, request["params"])
        body = getattr(result, "body", None)
        if type(body) is bytes:  # literal source response
            counter.setdefault("http_status", []).append(
                getattr(result, "status", None)
            )
            return RawResponse(
                body,
                "raw-bytes",
                None,
                media_type=getattr(result, "media_type", None)
                or "application/octet-stream",
            )
        counter.setdefault("http_status", []).append(None)
        payload = json.dumps(
            {
                "rows": result.rows,
                "warning_count": result.warning_count,
                "metadata": result.metadata,
            },
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=True,
        )
        return RawResponse(
            (payload + "\n").encode(),
            "provider-transformed",
            PROVIDER_TRANSFORMATION,
            media_type="application/json",
        )

    return fetch


def acquire(
    root: Path | str,
    entries: list[dict[str, Any]],
    *,
    client: Any,
    stop: threading.Event | None = None,
) -> list[dict[str, Any]]:
    """Capture every entry once per generation; resume a durable prefix.

    Ordering is the point: every entry is validated and its worst-case evidence
    budget preflighted, then every generation is read back and verified, and only
    then is any source called.  Every response -- including a non-2xx body and a
    body that will turn out to be malformed -- is written before returning.
    """
    results: list[dict[str, Any]] = []
    for entry in entries:
        task = validate_task(entry["task"])
        source = entry["source"]
        if not plan_process_capture(task, source)["fits"]:
            raise ValueError("task_evidence_budget_exceeded")
        existing_state(root, task["task_id"], task["generation"])
    for entry in entries:
        task = entry["task"]
        source = entry["source"]
        counter: dict[str, Any] = {"calls": 0}
        try:
            state = capture(
                root,
                task,
                _fetch_adapter(entry["client"], task["task_id"], source, counter),
                stop=stop,
                response_max_bytes=source.max_response_bytes,
            )
        except ValueError as exc:
            # A protocol refusal (oversize body, unsafe path, budget) is this
            # attempt's verdict: reported, retried, never fatal.
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
        # Anything else (an OSError from the store, a KeyboardInterrupt) is not a
        # data verdict: it escapes so the session fails loudly.
        try:
            record = capture_status.write(
                root,
                state,
                capture_status.observed(
                    state["responses"], counter.get("http_status", [])
                ),
            )
        except capture_status.StatusError as exc:
            # The capture is on disk but its integrity chain does not add up.  A
            # typed refusal about evidence, before any verdict, delivery or commit.
            raise EvidenceStateError(
                "evidence_status_unreadable", {"reason": str(exc)}
            ) from exc
        results.append(
            {
                "entry_id": entry["entry_id"],
                "task_id": task["task_id"],
                "generation": task["generation"],
                "locator": state["locator"],
                "status": state["status"],
                "reason": state["reason"],
                "complete": state["complete"],
                "terminal": capture_status.terminal(state, record),
                "responses": state["responses"],
                "source_calls": counter["calls"],
                # Read back from the record, so a replayed capture reports the
                # status it was taken under rather than ``None`` for "no call".
                "http_status": capture_status.status_of(record, 0),
            }
        )
    return results


def check_response_budget(rows: list[Any], source: Any) -> None:
    """One bound predicate, called by the collection path that commits."""
    if (
        len(rows) > source.max_rows
        or len(encoded(rows).encode()) > source.max_response_bytes
    ):
        raise DataError("response_limit")


# ---------------------------------------------------------------------------
# delivery half: one versioned envelope per captured response
# ---------------------------------------------------------------------------


def row_provenance(rows: list[dict], provenance: dict) -> list[dict]:
    return [
        {
            "source_row": index,
            "source_sha256": digest(item),
            "date_source": provenance["date_source"],
            "unit_source": provenance["unit_source"],
        }
        for index, item in enumerate(rows)
    ]


def _capture_block(state: dict) -> dict[str, Any]:
    (response,) = state["responses"]
    receipt = response["receipt"]
    body = response["body"]
    return {
        "sha256": receipt["sha256"] if body is not None else None,
        "size": receipt["size"] if body is not None else None,
        "evidence_kind": receipt["evidence_kind"],
        "transformation": receipt["transformation"],
        "locator": state["locator"],
        "media_type": receipt["media_type"],
    }


def build_delivery(  # pylint: disable=too-many-arguments
    *,
    kind: str,
    day: date,
    request: dict,
    source: dict,
    state: dict,
    rows: list[dict],
    provenance: dict,
    metadata: Any,
    warning_count: int,
    observed_at: str,
    entry_id: str,
    plan_sha256: str,
) -> dict[str, Any]:
    """Assemble the bound delivery document.  No judgement happens here."""
    capture_block = _capture_block(state)
    return {
        "version": DELIVERY_VERSION,
        "kind": kind,
        "request": {"date": day.isoformat(), **request},
        "source": source,
        "capture": capture_block,
        "rows": rows,
        "row_sha256": [digest(item) for item in rows],
        "row_provenance": row_provenance(rows, provenance),
        "provenance": provenance,
        "metadata": metadata,
        "warning_count": warning_count,
        "observed_at": observed_at,
        "captured_at": state["responses"][0]["receipt"]["acquired_at"],
        "evidence": {
            "entry_id": entry_id,
            "plan_sha256": plan_sha256,
            "status": state["status"],
            "complete": state["complete"],
        },
        "plan": {
            "version": PLAN_VERSION,
            "task_id": state["task"]["task_id"],
            "generation": state["task"]["generation"],
        },
    }


def _hash(value: Any) -> bool:
    return (
        isinstance(value, str)
        and len(value) == _HEX
        and all(character in "0123456789abcdef" for character in value)
    )


def _load(path: Path) -> dict[str, Any]:
    _require(path.is_file(), "missing_or_nonregular_evidence")
    try:
        return json.loads(path.read_bytes())
    except (ValueError, UnicodeError, OSError) as exc:
        raise EvidenceStateError("invalid_delivery", {"path": path.name}) from exc


def _units_consistent(provenance: dict) -> bool:
    """Labels present ⇔ the authority is not ``absent``.

    ``unit_source``/``unit_labels`` are the only provenance pair the bytes cannot
    state: units are declared by the deployment, and ``raw-acquisition-v1`` stores
    no unit column.  The reader cannot invent them, so it enforces that the two
    fields agree and refuses everything else typed.  Everything else in
    ``provenance`` is re-derived from the capture and compared exactly.
    """
    authority = provenance["unit_source"]
    labels = provenance["unit_labels"]
    if authority not in UNIT_SOURCES:
        return False
    if type(labels) is not dict or set(labels) != set(parser.UNIT_KEYS):
        return False
    if any(value is not None and type(value) is not str for value in labels.values()):
        return False
    present = [value for value in labels.values() if value is not None]
    if not present:
        return authority == "absent"
    return len(present) == len(labels) and authority in {"source", "configuration"}


def _label_mismatch(field: str, expected: Any, observed: Any) -> EvidenceStateError:
    return EvidenceStateError(
        "delivery_label_mismatch",
        {"field": field, "expected": expected, "observed": observed},
    )


def _timestamp(value: Any) -> datetime | None:
    """An ISO-8601 instant with an offset, or ``None`` when it is not one."""
    if type(value) is not str or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else None


def read_delivery(
    path: Path | str, capture_root: Path | str | None = None
) -> dict[str, Any]:
    """Read a raw delivery and re-derive its rows *and its labels* from the capture.

    Every check below is a refusal, not a repair.  The rows a caller gets back are
    the ones the captured bytes produce -- the envelope's own ``rows`` are only
    compared, never trusted -- and the label fields are re-derived from the same
    reading rather than believed:

    * ``capture.sha256/size/media_type/evidence_kind/transformation`` and
      ``captured_at`` are compared with the receipt;
    * ``request`` is compared with the capture's task *by key set and by value* --
      an extra key is a claim the task does not support;
    * ``source`` is compared with the capture's task;
    * ``evidence`` is rebuilt: the entry id from (task, generation, params, kind),
      the plan digest from the task's generation, status/completeness from the
      capture;
    * ``row_provenance`` is rebuilt from the re-derived rows and the checked
      provenance block, so a per-row ``date_source``/``unit_source`` copy cannot
      contradict the top-level label;
    * ``metadata`` and ``warning_count`` are this lane's one shape (a document
      capture has no provider metadata and no provider warnings), and
      ``observed_at`` must be an offset-bearing instant no earlier than the
      capture it describes -- a delivery cannot be observed before its bytes;
    * ``provenance.kind/date_source/date_column/transforms/row_count`` are rebuilt
      from the document kind, and ``parse_error`` from the re-derivation;
    * ``unit_source``/``unit_labels`` cannot come from bytes, so they are required
      to agree with each other (:func:`_units_consistent`).
    """
    path = Path(path)
    body = _load(path)
    _require(
        isinstance(body, dict)
        and set(body) == set(DELIVERY_FIELDS)
        and body.get("version") == DELIVERY_VERSION,
        "invalid_delivery",
    )
    request = body["request"]
    _require(
        isinstance(request, dict) and request.get("date") == path.parent.name,
        "invalid_delivery",
    )
    _require(body["kind"] in parser.TRANSFORMS, "invalid_delivery")
    rows = body["rows"]
    _require(isinstance(rows, list), "invalid_delivery")
    _require(
        len(rows) == len(body["row_sha256"]) == len(body["row_provenance"]),
        "delivery_row_digest",
    )
    for row, sha, provenance in zip(
        rows, body["row_sha256"], body["row_provenance"], strict=True
    ):
        _require(isinstance(row, dict) and digest(row) == sha, "delivery_row_digest")
        _require(
            isinstance(provenance, dict) and provenance.get("source_sha256") == sha,
            "delivery_row_digest",
        )
    # The lane's own shape, and nothing else: a document capture carries no
    # provider metadata block and no provider warnings, so a value here would be
    # an invented observation.
    if body["metadata"] != {} or type(body["metadata"]) is not dict:
        raise _label_mismatch("metadata", {}, body["metadata"])
    if body["warning_count"] != 0 or type(body["warning_count"]) is not int:
        raise _label_mismatch("warning_count", 0, body["warning_count"])
    capture_block = body["capture"]
    _require(
        isinstance(capture_block, dict)
        and set(capture_block) == set(CAPTURE_FIELDS)
        and (capture_block["sha256"] is None or _hash(capture_block["sha256"])),
        "invalid_delivery",
    )
    if capture_block["evidence_kind"] == "raw-bytes":
        _require(capture_block["transformation"] is None, "invalid_delivery")
    provenance_block = body["provenance"]
    _require(
        isinstance(provenance_block, dict)
        and set(provenance_block) == set(PROVENANCE_FIELDS),
        "invalid_delivery",
    )
    plan = body["plan"]
    _require(
        isinstance(plan, dict)
        and plan.get("version") == PLAN_VERSION
        and isinstance(plan.get("task_id"), str)
        and isinstance(plan.get("generation"), str),
        "invalid_delivery",
    )
    # No mode reads a delivery without re-reading its evidence.
    _require(capture_root is not None, "delivery_unbound")
    try:
        state = read_capture(capture_root, plan["task_id"], plan["generation"])
    except EvidenceStateError:
        raise
    except Exception as exc:  # noqa: BLE001
        raise EvidenceStateError(
            "evidence_state_unreadable", {"reason": _safe_code(exc)}
        ) from exc
    _require(state["locator"] == capture_block["locator"], "delivery_capture_mismatch")
    response = state["responses"][0]
    receipt = response["receipt"]
    _require(response["body"] is not None, "delivery_capture_mismatch")
    _require(receipt["sha256"] == capture_block["sha256"], "delivery_capture_mismatch")
    _require(
        receipt["evidence_kind"] == capture_block["evidence_kind"],
        "delivery_capture_mismatch",
    )
    _require(
        receipt["transformation"] == capture_block["transformation"],
        "delivery_capture_mismatch",
    )
    # Re-derived from the receipt, not trusted: size, media type, capture clock.
    if receipt["size"] != capture_block["size"]:
        raise _label_mismatch("capture.size", receipt["size"], capture_block["size"])
    if receipt["media_type"] != capture_block["media_type"]:
        raise _label_mismatch(
            "capture.media_type", receipt["media_type"], capture_block["media_type"]
        )
    if receipt["acquired_at"] != body["captured_at"]:
        raise _label_mismatch(
            "captured_at", receipt["acquired_at"], body["captured_at"]
        )
    # The plan block, the source block and the request params come from the task.
    task = state["task"]
    if body["source"] != task["source"]:
        raise _label_mismatch("source", task["source"], body["source"])
    params = task["requests"][0]["params"]
    # ``exchange`` is a property of the request, not of the task's parameters (a
    # quote block is addressed by symbol only), so it is allowed here and must be
    # one of the lane's exchanges.  Anything else is an extra claim.
    allowed = set(params) | {"date", "exchange"}
    if set(request) != allowed or request.get("exchange") not in identity.EXCHANGES:
        raise _label_mismatch(
            "request",
            sorted(allowed),
            sorted(request),
        )
    for key, value in params.items():
        if request.get(key) != value:
            raise _label_mismatch("request." + key, value, request.get(key))
    # ``evidence`` is rebuilt, not read: every field is a fact about this capture.
    evidence_block = body["evidence"]
    _require(
        isinstance(evidence_block, dict)
        and set(evidence_block) == set(EVIDENCE_FIELDS),
        "invalid_delivery",
    )
    expected_evidence = {
        "entry_id": digest(
            {
                "task_id": plan["task_id"],
                "generation": plan["generation"],
                "params": params,
                "kind": body["kind"],
            }
        ),
        "plan_sha256": task["generation"],
        "status": state["status"],
        "complete": state["complete"],
    }
    for field in EVIDENCE_FIELDS:
        if evidence_block[field] != expected_evidence[field]:
            raise _label_mismatch(
                "evidence." + field, expected_evidence[field], evidence_block[field]
            )
    # ``observed_at`` is the one label no captured byte can state: it is the
    # reader's clock at delivery time, so it is *checked for shape* (an
    # offset-bearing instant) and disclosed as an observation, never presented as
    # evidence about the source.  Ordering it against ``captured_at`` would only
    # test the two clocks against each other, which is not a property of the bytes.
    _require(_timestamp(body["observed_at"]) is not None, "invalid_delivery")
    _require(_timestamp(body["captured_at"]) is not None, "invalid_delivery")
    rederived, parse_error = _rederive(state, body["kind"])
    _require(rederived == rows, "delivery_capture_mismatch")
    _require(provenance_block["parse_error"] == parse_error, "delivery_parse_error")
    expected = parser.provenance(
        body["kind"],
        rederived,
        provenance_block["unit_source"],
        provenance_block["unit_labels"],
    )
    for field in ("kind", "date_source", "date_column", "transforms", "row_count"):
        if provenance_block[field] != expected[field]:
            raise _label_mismatch(
                "provenance." + field, expected[field], provenance_block[field]
            )
    _require(_units_consistent(provenance_block), "delivery_label_mismatch")
    # The per-row copies of the two provenance labels are compared with the
    # top-level block, so a consumer that reads ``row_provenance`` for the date or
    # unit authority is reading the same value the reader just checked.
    if body["row_provenance"] != row_provenance(rederived, provenance_block):
        raise EvidenceStateError("delivery_label_mismatch", {"field": "row_provenance"})
    return {**body, "rows": rederived, "rederived": True}


def _rederive(state: dict, kind: str) -> tuple[list[dict], str | None]:
    """Re-run the structural reader on the captured bytes.

    The kind is *not* taken from the capture: it is taken from the delivery and
    checked by consequence -- a delivery that names the wrong reader produces
    different rows and is refused ``delivery_capture_mismatch``.
    """
    request = state["task"]["requests"][0]["params"]
    receipt = state["responses"][0]["receipt"]
    body = state["responses"][0]["body"]
    try:
        return (
            parser.rows_of(body, kind, request, receipt["acquired_at"]),
            None,
        )
    except parser.ParseError as exc:
        return [], exc.code


def read_calendar_delivery(
    path: Path | str, capture_root: Path | str | None = None
) -> dict:
    """The calendar envelope has its own version and its own reader.

    Neither reader accepts the other's version: one string never describes two
    schemas.
    """
    path = Path(path)
    body = _load(path)
    _require(
        isinstance(body, dict)
        and set(body) == set(CALENDAR_FIELDS)
        and body.get("version") == CALENDAR_DELIVERY_VERSION,
        "unsupported_delivery_version",
    )
    _require(isinstance(body["capture"], dict), "invalid_delivery")
    _require(
        isinstance(body["trading_dates"], list) and bool(body["trading_dates"]),
        "invalid_delivery",
    )
    _require(capture_root is not None, "delivery_unbound")
    return body


def read_aggregate_delivery(
    path: Path | str, capture_root: Path | str | None = None
) -> dict:
    """Read the provider-envelope lane's own label, and bind it to its capture.

    The aggregate lane (the Sina SSE master, and the calendar) is not a document:
    the provider issues its own discovery and quote GETs and returns model rows.
    That lane therefore has no ``etf-raw-delivery-v3`` -- but it may not be
    anonymous either, so the archived envelope carries its own label block and
    this reader checks it against the capture it names:

    * the body must equal the rows the captured envelope yields, and
    * ``evidence_kind``/``transformation``/date source/unit authority must be the
      aggregate lane's declared values, and
    * ``http_status_available`` must be ``False``: this lane cannot observe a
      per-GET status, so it may not claim one.
    """
    path = Path(path)
    body = _load(path)
    _require(isinstance(body, dict), "invalid_delivery")
    label = body.get("provenance")
    _require(
        isinstance(label, dict) and set(label) == set(AGGREGATE_PROVENANCE_FIELDS),
        "aggregate_unlabelled",
    )
    label = cast(dict[str, Any], label)
    _require(label["evidence_kind"] == "provider-transformed", "invalid_delivery")
    _require(label["transformation"] == PROVIDER_TRANSFORMATION, "invalid_delivery")
    _require(label["http_status_available"] is False, "aggregate_status_claimed")
    _require(
        label["date_source"] == parser.DATE_SOURCE[parser.PROVIDER_ENVELOPE_KIND],
        "invalid_delivery",
    )
    _require(
        label["transforms"] == parser.TRANSFORMS[parser.PROVIDER_ENVELOPE_KIND],
        "invalid_delivery",
    )
    _require(label["lost"] == AGGREGATE_LOSS, "invalid_delivery")
    _require(label["retained_verbatim"] == AGGREGATE_RETAINED, "invalid_delivery")
    plan = body.get("plan")
    _require(
        isinstance(plan, dict)
        and plan.get("version") == PLAN_VERSION
        and isinstance(plan.get("task_id"), str)
        and isinstance(plan.get("generation"), str),
        "invalid_delivery",
    )
    plan = cast(dict[str, Any], plan)
    _require(capture_root is not None, "delivery_unbound")
    state = read_capture(capture_root, plan["task_id"], plan["generation"])
    _require(state["locator"] == label["capture"], "delivery_capture_mismatch")
    receipt = state["responses"][0]["receipt"]
    body_bytes = state["responses"][0]["body"]
    _require(body_bytes is not None, "delivery_capture_mismatch")
    _require(receipt["sha256"] == label["sha256"], "delivery_capture_mismatch")
    _require(receipt["size"] == label["size"], "delivery_label_mismatch")
    _require(receipt["media_type"] == label["media_type"], "delivery_label_mismatch")
    _require(
        receipt["evidence_kind"] == label["evidence_kind"], "delivery_capture_mismatch"
    )
    _require(
        receipt["transformation"] == label["transformation"],
        "delivery_capture_mismatch",
    )
    rederived, parse_error = _rederive(state, parser.PROVIDER_ENVELOPE_KIND)
    _require(rederived == body["rows"], "delivery_capture_mismatch")
    _require(label["parse_error"] == parse_error, "delivery_parse_error")
    _require(_units_consistent(label), "delivery_label_mismatch")
    return {**body, "rows": rederived, "rederived": True}


def build_calendar_delivery(
    evidence: dict[str, Any], day: date, rows: list[dict]
) -> dict[str, Any]:
    """The calendar envelope carries the same capture binding, under its own version."""
    return {
        "version": CALENDAR_DELIVERY_VERSION,
        "request": {"date": day.isoformat()},
        "capture": {
            "sha256": evidence["sha256"],
            "size": evidence["size"],
            "evidence_kind": evidence["evidence_kind"],
            "transformation": evidence["transformation"],
            "locator": evidence["locator"],
            "media_type": evidence["media_type"],
        },
        "trading_dates": [row["trade_date"] for row in rows],
        "provenance": {
            "date_source": "row",
            "unit_source": "not_applicable",
            "transforms": [],
            "parse_error": None,
        },
        "captured_at": evidence["captured_at"],
        "evidence": {
            "entry_id": evidence["entry_id"],
            "status": evidence["status"],
            "complete": evidence["complete"],
        },
    }


class Collector:  # pylint: disable=too-many-instance-attributes
    """Plan, acquire, then judge: the only collection path for ETF options.

    Constructor and public methods keep the legacy identity (``collect`` and the
    same report keys) so existing operators see the same command; what changed is
    that acquisition now writes bytes first and judgement reads them back.
    """

    def __init__(  # pylint: disable=too-many-positional-arguments
        self,
        config: Config,
        journal: Journal,
        calendar: TradingCalendar | None,
        client: Any = None,
        stop: threading.Event | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        handoff: dict[str, Any] | None = None,
    ):
        self.config, self.journal, self.calendar = config, journal, calendar
        self.stop = stop or threading.Event()
        self.injected = client is not None
        # Document-addressed transport for everything that is one HTTP response;
        # the provider process boundary is kept for the aggregate Sina master and
        # for the calendar lane.
        self.client = client if client is not None else EtfChainClient(self.stop)
        self.aggregate = client if client is not None else ProcessClient(self.stop)
        self.clock = clock
        self.deadline = 0.0
        self.last_call = -float("inf")
        self.report: dict = {}
        # The one representation of the configuration fingerprint that identities
        # are derived from: a hand-off plan states the same string, so "the same
        # configuration" is a value comparison rather than a structural guess.
        self.fingerprint = digest(self.config.fingerprint())
        # A hand-off identity (``etf-acquire-plan-v2``'s ``identity`` block): when
        # present its token replaces the per-run one, which is the entire
        # difference between "this run's evidence" and "the evidence the
        # acquisition task already wrote for this configuration and day".
        self.handoff = self._handoff(handoff)
        # One acquisition session identity.  The capture generation is derived
        # from it, so a *run* re-reads the current snapshot while a restarted or
        # retried acquisition inside the same run resumes exact evidence.
        self.token = "unstarted"
        if self.handoff is not None:
            self.token = self.handoff["token"]

    def _handoff(self, handoff: dict[str, Any] | None) -> dict[str, Any] | None:
        """Validate a hand-off identity against *this* configuration and day.

        A plan is data, so it may name another deployment's fingerprint or another
        day; adopting either would let one configuration's bytes answer for
        another's.  Both are refused typed, before anything is read or fetched.
        """
        if handoff is None:
            return None
        try:
            block = identity.block(handoff)
        except ValueError as exc:
            raise ConfigurationError(str(exc)) from exc
        if block["fingerprint"] != self.fingerprint:
            raise ConfigurationError("handoff_fingerprint_mismatch")
        if block["source_id"] != self.config.source_id:
            raise ConfigurationError("handoff_source_mismatch")
        return block

    def close(self) -> None:
        self.client.close()
        if self.aggregate is not self.client:
            self.aggregate.close()

    @property
    def evidence_root(self) -> Path:
        return self.config.evidence_root

    @property
    def unit_source(self) -> str:
        return unit_authority(False, self.config.unit_authority())

    @property
    def unit_labels(self) -> dict:
        """The declared labels, or all-``None`` when nothing was declared."""
        return unit_label_map(self.unit_source, self.config.unit_authority())

    # -- plan derivation ----------------------------------------------------

    def _status_record(self, state: dict) -> dict:
        """The persisted per-request status record of a capture, verified.

        A capture whose record is missing or does not bind to its receipts is a
        typed refusal, never "no status observed": the difference between a 503
        body and a 200 body has to survive the read, or persisting it would be
        decoration.
        """
        try:
            return capture_status.read(self.evidence_root, state["locator"], state)
        except capture_status.StatusError as exc:
            raise EvidenceStateError(
                "evidence_status_unreadable", {"reason": str(exc)}
            ) from exc

    def _client_for(self, model: str, request: dict) -> tuple[Any, str | None]:
        """Which client addresses this request, and as what kind of document.

        An injected client is not the transport: it is a provider-shaped double
        (the pre-existing tests, the zero-central child) and therefore always
        produces ``provider-transformed`` evidence, one model call per request.
        """
        if self.injected:
            return self.aggregate, None
        if type(self.aggregate) is not _REAL_PROCESS_CLIENT:
            # The provider process boundary itself has been substituted (an
            # offline double installed over ``runtime.ProcessClient``).  A
            # substituted boundary cannot produce source bytes, so every request
            # goes through it and the evidence is labelled ``provider-transformed``.
            return self.aggregate, None
        kind = self.client.plan_kind(model, request)
        if kind is None:
            return self.aggregate, None
        return self.client, kind

    def _task_id(self, day: date, exchange: str, model: str, params: dict) -> str:
        """The capture task id, from the same function the acquisition task uses."""
        return identity.task_id(
            source_id=self.config.source_id,
            model=model,
            exchange=exchange,
            date=day.isoformat(),
            params=params,
        )

    def _base(self, day: date, attempt: int) -> str:
        """The generation family for one attempt.

        ``token`` is the journal run id by default, which is what keeps a capture
        generation per run.  A hand-off plan supplies a stable token instead, so
        the acquisition task and the collector derive the *same* generation from
        the same identity block and a completed acquisition is reused as is.
        """
        return identity.base(
            fingerprint=self.fingerprint,
            source_id=self.config.source_id,
            date=day.isoformat(),
            attempt=attempt,
            token=self.token,
        )

    def _generation(self, task_id: str, base: str) -> tuple[str, dict | None]:
        """Pick the capture generation for one attempt; reuse exact evidence.

        An absent generation captures.  An interrupted one resumes its durable
        prefix.  A completed capture that downstream has not rejected replays its
        bytes with no source call.  A cancelled one replays its veto, also with no
        source call.  A rejected, failed or partial generation advances to the
        next epoch, which is the only way a source may be called again -- and the
        old generation stays immutable.
        """
        for epoch in range(MAX_EVIDENCE_EPOCHS):
            # The one derivation: `acquire` addresses the same locator with the
            # same function, so this must not be re-spelled here.
            generation = identity.generation(base, epoch)
            state = existing_state(self.evidence_root, task_id, generation)
            if state is None:
                return generation, None
            if self.journal.verdict(state["locator"]) == "rejected":
                continue
            if state["complete"] and state["status"] == "captured":
                return generation, state
            if state["reason"] in {"interrupted", "cancelled"}:
                return generation, state
        raise EvidenceStateError("evidence_generation_exhausted", {"task_id": task_id})

    def _entry(
        self, day: date, exchange: str, model: str, params: dict, attempt: int
    ):  # pylint: disable=too-many-positional-arguments
        source = self.config.source(model)
        client, kind = self._client_for(model, params)
        task_id = self._task_id(day, exchange, model, params)
        generation, state = self._generation(task_id, self._base(day, attempt))
        task = {
            "version": EVIDENCE_VERSION,
            "task_id": task_id,
            "generation": generation,
            "source": {"provider": source.provider, "model": source.model},
            "requests": [
                {"request_id": f"{self.config.source_id}:{model}", "params": params}
            ],
        }
        entry = {
            "kind": kind,
            "task": task,
            "source": source,
            "client": client,
            "replayed": state,
        }
        entry["entry_id"] = digest(
            {
                "task_id": task_id,
                "generation": generation,
                "params": params,
                "kind": kind,
            }
        )
        return entry

    # -- run bookkeeping ----------------------------------------------------

    def _check(self) -> None:
        if self.stop.is_set():
            raise ProviderFailure("cancelled")
        if time.monotonic() >= self.deadline:
            raise ProviderFailure("run_budget_exceeded")
        if self.report.get("trading_date"):
            now = self.clock()
            if now.tzinfo is None:
                raise ConfigurationError("invalid_clock")
            if (
                now.astimezone(SHANGHAI).date().isoformat()
                != self.report["trading_date"]
            ):
                raise DataError("live_source_date_only")

    def _begin(self, command: str) -> dict:
        self.deadline = time.monotonic() + self.config.max_run_seconds
        self.report = {
            "command": command,
            "status": "running",
            "provider_calls": 0,
            "errors": [],
            "warning_count": 0,
            "captures": [],
            "response_complete": False,
            "pricing_eligible_count": 0,
            "row_count": 0,
            "changed_snapshots": 0,
            "resumed_rows": 0,
        }
        self.report["run_id"] = self.journal.start_run(self.report)
        # A hand-off identity keeps its stable token: the run id would re-address
        # the evidence the acquisition task already wrote, which is the split this
        # round closes.  The run is still recorded; it just is not the identity.
        if self.handoff is None:
            self.token = self.report["run_id"]
        return self.report

    def _cleanup(self, exc: BaseException) -> None:
        self.report["status"] = (
            "interrupted"
            if (
                self.stop.is_set()
                or isinstance(exc, KeyboardInterrupt)
                or isinstance(exc, ProviderFailure)
                and exc.code == "cancelled"
            )
            else "failed"
        )
        self.report["response_complete"] = False
        try:
            self.journal.abort_run(self.report["run_id"], self.report)
        except BaseException as secondary:
            exc.add_note(f"ETF cleanup failed: {type(secondary).__name__}")

    # -- one acquisition + delivery + verdict -------------------------------

    def _request(
        self,
        model: str,
        request: dict,
        validator: Callable,
        capture_id: str | None = None,
        *,
        day: date,
        exchange: str,
    ) -> tuple[Any, str, datetime, dict[str, Any]]:
        """Acquire one response, deliver its rows, then judge them.

        The order is load-bearing: capture, then the bound delivery, then the
        business validator.  A malformed body is delivered with a typed
        ``parse_error`` and rejected afterwards -- the bytes are never lost and
        nothing is committed.
        """
        source = self.config.source(model)
        for retry in range(source.retries + 1):
            self._check()
            if self.report["provider_calls"] >= self.config.max_requests:
                raise ProviderFailure("request_budget_exceeded")
            delay = max(
                0, source.min_interval_seconds - (time.monotonic() - self.last_call)
            )
            if delay >= self.deadline - time.monotonic():
                raise ProviderFailure("run_budget_exceeded")
            if self.stop.wait(delay):
                raise ProviderFailure("cancelled")
            self._check()
            remaining = self.deadline - time.monotonic()
            bounded = source.model_copy(
                update={"timeout_seconds": min(source.timeout_seconds, remaining)}
            )
            entry = self._entry(day, exchange, model, request, retry)
            replayed = entry["replayed"]
            attempt = self.journal.begin_attempt(
                self.report["run_id"],
                capture_id,
                model,
                request,
                stage=entry["kind"] or "provider-envelope",
                evidence_locator=(replayed or {}).get("locator"),
            )
            observed = self.clock()
            if observed.tzinfo is None:
                raise ConfigurationError("invalid_clock")
            if replayed is not None and replayed["reason"] == "cancelled":
                # A durable veto: this generation may not call the source again.
                self.journal.finish_attempt(attempt, "cancelled")
                raise ProviderFailure("cancelled")
            results = acquire(
                self.evidence_root,
                [entry],
                client=entry["client"],
                stop=self.stop,
            )
            result = results[0]
            calls = result["source_calls"]
            self.report["provider_calls"] += calls
            if calls:
                self.last_call = time.monotonic()
            if result["status"] == "error":
                self.journal.finish_attempt(attempt, result["reason"])
                if retry == source.retries:
                    raise ProviderFailure(result["reason"])
                continue
            state = existing_state(
                self.evidence_root, result["task_id"], result["generation"]
            )
            if state is None:  # pragma: no cover - acquire just wrote it
                raise EvidenceStateError("evidence_state_unreadable")
            # The status is read back from the capture, not taken from this
            # invocation's counter: a replayed capture has no call but still has a
            # status, and the attempt row must record the one the bytes came under.
            status = capture_status.status_of(self._status_record(state), 0)
            self.journal.attach_evidence(attempt, state["locator"], status)
            if not state["responses"]:
                # Cancelled before the source was called: the generation carries a
                # durable veto and no receipt.  Nothing to deliver, nothing lost.
                self.journal.finish_attempt(attempt, "cancelled")
                raise ProviderFailure("cancelled")
            receipt = state["responses"][0]["receipt"]
            evidence = {
                "locator": state["locator"],
                "sha256": receipt["sha256"],
                "size": receipt["size"],
                "evidence_kind": receipt["evidence_kind"],
                "transformation": receipt["transformation"],
                "media_type": receipt["media_type"],
                "captured_at": receipt["acquired_at"],
                "http_status": status,
                "status": state["status"],
                "complete": state["complete"],
                "entry_id": entry["entry_id"],
            }
            if state["responses"][0]["body"] is None:
                # The transport failed: no bytes existed, so there is nothing to
                # deliver.  This is a receipt, not a data verdict.
                self.journal.finish_attempt(attempt, "provider_failure")
                if retry == source.retries:
                    raise ProviderFailure("provider_failure")
                self._retry_wait(source, exc=None)
                continue
            kind = entry["kind"]
            rows, parse_error = _rederive(state, kind or parser.PROVIDER_ENVELOPE_KIND)
            if kind is None:
                # Aggregate lane: the document is the provider's own response, so
                # the envelope is kept in its pre-existing shape (including the
                # non-finite tokens the legacy readers tolerate) -- plus a label
                # block of its own, because a reader of this file must not have to
                # follow a pointer to learn what it is holding.
                envelope = {
                    "request": request,
                    "source": bounded.model_dump(mode="json"),
                    "rows": rows,
                    "metadata": {"evidence": state["locator"]},
                    "warning_count": 0,
                    "provider_error": None,
                    "observed_at": observed.astimezone(UTC).isoformat(),
                    # Legacy field, kept for the pre-existing readers.  This lane
                    # never observes a per-GET status (the provider performs the
                    # GETs), so nothing is claimed here.
                    "http_status": None,
                    "provenance": {
                        "evidence_kind": "provider-transformed",
                        "transformation": PROVIDER_TRANSFORMATION,
                        "capture": state["locator"],
                        "sha256": receipt["sha256"],
                        "size": receipt["size"],
                        "media_type": receipt["media_type"],
                        "date_source": parser.DATE_SOURCE[
                            parser.PROVIDER_ENVELOPE_KIND
                        ],
                        "unit_source": self.unit_source,
                        "unit_labels": self.unit_labels,
                        "transforms": list(
                            parser.TRANSFORMS[parser.PROVIDER_ENVELOPE_KIND]
                        ),
                        "parse_error": parse_error,
                        "http_status_available": False,
                        "lost": dict(AGGREGATE_LOSS),
                        "retained_verbatim": dict(AGGREGATE_RETAINED),
                    },
                    "plan": {
                        "version": PLAN_VERSION,
                        "task_id": state["task"]["task_id"],
                        "generation": state["task"]["generation"],
                    },
                }
                self.journal.archive(attempt, envelope)
            else:
                provenance = parser.provenance(
                    kind, rows, self.unit_source, self.unit_labels
                )
                provenance["parse_error"] = parse_error
                delivery = build_delivery(
                    kind=kind,
                    day=day,
                    request={"exchange": exchange, **request},
                    source={"provider": source.provider, "model": source.model},
                    state=state,
                    rows=rows,
                    provenance=provenance,
                    metadata={},
                    warning_count=0,
                    observed_at=observed.astimezone(UTC).isoformat(),
                    entry_id=entry["entry_id"],
                    plan_sha256=entry["task"]["generation"],
                )
                # Written before any predicate can reject the day: the bytes and
                # the reading of them are durable even when the reader refuses.
                self.journal.archive(attempt, delivery, day=day.isoformat())
            if parse_error is not None:
                self.journal.record_verdict(state["locator"], "rejected", parse_error)
                self.journal.finish_attempt(attempt, parse_error)
                if retry == source.retries:
                    raise DataError(parse_error)
                self._retry_wait(source, exc=None)
                continue
            try:
                self.check_http(status)
                self._check()
                if len(rows) > source.max_rows:
                    raise DataError("response_limit")
                result_rows = validator(rows, observed)
                # The bound applies to the delivered rows, after the reading and
                # before the commit: both dimensions, one predicate.
                check_response_budget(rows, source)
            except (ProviderFailure, DataError) as exc:
                code = exc.code if isinstance(exc, ProviderFailure) else str(exc)
                self.journal.record_verdict(state["locator"], "rejected", code)
                self.journal.finish_attempt(attempt, code)
                if code == "cancelled" or retry == source.retries:
                    raise
                self._retry_wait(source, exc=exc)
                continue
            self.journal.record_verdict(state["locator"], "accepted", None)
            self.journal.finish_attempt(attempt)
            return result_rows, attempt, observed, evidence
        raise AssertionError("unreachable")

    def _retry_wait(self, source: Any, exc: BaseException | None) -> None:
        wait = min(source.retry_delay_seconds, max(0, self.deadline - time.monotonic()))
        if self.stop.wait(wait):
            raise ProviderFailure("cancelled") from exc

    @staticmethod
    def check_http(status: int | None) -> None:
        """A non-2xx body is captured, then refused by content.

        The provider raised on status before any bytes existed; this lane keeps
        the body (it may explain the refusal) and refuses the attempt with a typed
        code instead of dropping it.
        """
        if status is not None and not 200 <= status < 300:
            raise DataError("source_body_http_status")

    # -- public commands ----------------------------------------------------

    def collect(
        self,
        target: date | None = None,
        resume: bool = True,
        approve_universe_change: bool = False,
    ) -> dict:
        now = self.clock()
        if now.tzinfo is None:
            raise ConfigurationError("invalid_clock")
        local = now.astimezone(SHANGHAI)
        target = target or local.date()
        if target != local.date():
            raise ConfigurationError("live_source_date_only")
        # A hand-off identity is one day's identity: adopting another day's token
        # would let yesterday's bytes answer for today without a source call.
        if self.handoff is not None and self.handoff["date"] != target.isoformat():
            raise ConfigurationError("handoff_date_mismatch")
        if self.calendar is None:
            raise ConfigurationError("calendar_required")
        session = self.calendar.is_session(target)
        if session and local.time() < self.config.not_before:
            raise ConfigurationError("capture_before_cutoff")
        report = self._begin("collect")
        report.update(
            trading_date=target.isoformat(),
            config_hash=digest(self.config.identity()),
            config=self.config.identity(),
            approve_universe_change=approve_universe_change,
            requested_exchanges=len(self.config.exchanges),
            unit_source=self.unit_source,
        )
        ready = []
        try:
            self._check()
            if not session:
                report.update(
                    status="skipped", skip_reason="validated_calendar_closed_day"
                )
            else:
                for exchange, settings in sorted(self.config.exchanges.items()):
                    capture_id = None
                    try:
                        self._check()
                        request = master_request(exchange, settings)
                        metadata, attempt, observed, _ = self._request(
                            "AkshareEtfOptionContracts",
                            request,
                            lambda rows, _, ex=exchange, policy=settings: contracts(
                                rows, ex, policy.source, target, policy.underlyings
                            ),
                            day=target,
                            exchange=exchange,
                        )
                        capture_id, universe_hash = self.journal.start_capture(
                            report["run_id"],
                            target.isoformat(),
                            exchange,
                            report["config_hash"],
                            metadata,
                            observed.astimezone(UTC).isoformat(),
                            unit_source=self.unit_source,
                            evidence_locator=self.journal.evidence_of(attempt),
                        )
                        self.journal.db.execute(
                            "UPDATE attempts SET capture_id=? WHERE id=?",
                            (capture_id, attempt),
                        )
                        report["captures"].append(capture_id)
                        self.journal.validate_universe(
                            target.isoformat(),
                            exchange,
                            metadata,
                            approve_universe_change,
                        )
                        staged = (
                            self.journal.resume(
                                target.isoformat(),
                                exchange,
                                report["config_hash"],
                                universe_hash,
                            )
                            if resume
                            else []
                        )
                        if staged:
                            subset = [
                                row
                                for row in metadata
                                if row["symbol"]
                                in {quote["symbol"] for quote in staged}
                            ]
                            staged = quotes(
                                staged,
                                subset,
                                target,
                                observed,
                                self.clock(),
                                self.config.quote_not_before,
                            )
                        report["resumed_rows"] += len(staged)
                        done = {row["symbol"] for row in staged}
                        pending = [row for row in metadata if row["symbol"] not in done]
                        for offset in range(0, len(pending), self.config.block_size):
                            block = pending[offset : offset + self.config.block_size]
                            block_rows, attempt, observed, _ = self._request(
                                "AkshareEtfOptionQuotes",
                                {"symbols": ",".join(row["symbol"] for row in block)},
                                lambda rows, captured, expected=block: quotes(
                                    rows,
                                    expected,
                                    target,
                                    captured,
                                    self.clock(),
                                    self.config.quote_not_before,
                                ),
                                capture_id,
                                day=target,
                                exchange=exchange,
                            )
                            self.journal.checkpoint(
                                target.isoformat(),
                                exchange,
                                report["config_hash"],
                                universe_hash,
                                block_rows,
                                observed.isoformat(),
                                attempt,
                            )
                            staged.extend(block_rows)
                        self._check()
                        # Full exact-universe reconciliation after checkpoint merge.
                        complete_rows = quotes(
                            staged,
                            metadata,
                            target,
                            self.clock(),
                            self.clock(),
                            self.config.quote_not_before,
                        )
                        self.journal.ready(
                            capture_id,
                            complete_rows,
                            self.clock().astimezone(UTC).isoformat(),
                        )
                        ready.append(capture_id)
                        report["row_count"] += len(complete_rows)
                        report["pricing_eligible_count"] += sum(
                            row["pricing_eligible"] for row in complete_rows
                        )
                        report["warning_count"] += sum(
                            bool(row["quality_flags"]) for row in complete_rows
                        )
                    except (ProviderFailure, DataError) as exc:
                        code = (
                            exc.code if isinstance(exc, ProviderFailure) else str(exc)
                        )
                        if capture_id:
                            self.journal.fail_capture(capture_id, code)
                        report["errors"].append({"exchange": exchange, "code": code})
                        if code == "cancelled":
                            raise
                report["response_complete"] = (
                    not report["errors"]
                    and len(ready) == len(self.config.exchanges)
                    and report["row_count"] > 0
                )
                report["status"] = (
                    "complete" if report["response_complete"] else "failed"
                )
            with self.journal.transaction():
                self._check()
                for capture_id in ready:
                    report["changed_snapshots"] += self.journal.commit_capture(
                        capture_id, approve_universe_change
                    )
                    self.journal.db.execute(
                        "DELETE FROM checkpoints WHERE "
                        "(trading_date,exchange,config_hash,universe_hash) IN "
                        "(SELECT trading_date,exchange,config_hash,universe_hash "
                        "FROM captures WHERE id=?)",
                        (capture_id,),
                    )
                self.journal.finish_run(report["run_id"], report)
                self._check()
            # A cancellation delivered immediately after the commit must still be
            # reported as interrupted, while the durable good acquisition is kept.
            if self.stop.is_set():
                raise ProviderFailure("cancelled")
        except (ProviderFailure, DataError) as exc:
            report["errors"].append(
                {"code": exc.code if isinstance(exc, ProviderFailure) else str(exc)}
            )
            self._cleanup(exc)
        except BaseException as exc:
            self._cleanup(exc)
            raise
        return report

    def refresh_calendar(self) -> dict:
        report = self._begin("calendar-refresh")
        try:
            target = self.config.calendar_file
            if protected_target(self.config.root, target):
                raise ConfigurationError("protected_calendar_path")
            day = self.clock().astimezone(SHANGHAI).date()

            def validate(rows: list[dict], observed: datetime) -> TradingCalendar:
                days = coverage(rows)
                calendar = TradingCalendar(
                    coverage_start=days[0],
                    coverage_end=days[-1],
                    trading_dates=days,
                    source="AkshareToolTradeDateHistSina",
                    captured_at=observed.astimezone(UTC),
                )
                previous = load_calendar(target) if target.exists() else None
                coverage_accepts(
                    previous, calendar.coverage_start, calendar.coverage_end, days
                )
                return calendar

            calendar, attempt, _observed, evidence = self._request(
                "AkshareToolTradeDateHistSina",
                {},
                validate,
                day=day,
                exchange="SSE",
            )
            self._check()
            calendar_delivery = build_calendar_delivery(
                evidence,
                day,
                [{"trade_date": item.isoformat()} for item in calendar.trading_dates],
            )
            calendar_path = (
                self.config.root / "raw" / day.isoformat() / f"{attempt}.calendar.json"
            )
            calendar_path.parent.mkdir(parents=True, exist_ok=True)
            if not calendar_path.exists():
                temporary = calendar_path.with_name(f".{calendar_path.name}.tmp")
                try:
                    with temporary.open("xb") as binary_stream:
                        binary_stream.write(encoded(calendar_delivery).encode())
                        binary_stream.flush()
                        os.fchmod(binary_stream.fileno(), 0o444)
                        os.fsync(binary_stream.fileno())
                    os.link(temporary, calendar_path)
                    sync_directory(calendar_path.parent)
                finally:
                    temporary.unlink(missing_ok=True)
            target.parent.mkdir(parents=True, exist_ok=True)
            temporary = target.with_name(f".{target.name}.{uuid.uuid4().hex}.tmp")
            try:
                with temporary.open("x", encoding="utf-8") as stream:
                    stream.write(encoded(calendar.model_dump(mode="json")) + "\n")
                    stream.flush()
                    os.fsync(stream.fileno())
                self._check()
                os.replace(temporary, target)
                sync_directory(target.parent)
            finally:
                temporary.unlink(missing_ok=True)
            report.update(
                status="complete",
                coverage_start=calendar.coverage_start.isoformat(),
                coverage_end=calendar.coverage_end.isoformat(),
                trading_dates=len(calendar.trading_dates),
                calendar_delivery=evidence["locator"],
            )
            with self.journal.transaction():
                self._check()
                self.journal.finish_run(report["run_id"], report)
                self._check()
            if self.stop.is_set():
                raise ProviderFailure("cancelled")
        except (ProviderFailure, DataError) as exc:
            report["errors"].append(
                {"code": exc.code if isinstance(exc, ProviderFailure) else str(exc)}
            )
            self._cleanup(exc)
        except BaseException as exc:
            self._cleanup(exc)
            raise
        return report


def refresh_calendar(
    config: Config, journal: Journal, client: Any, stop: threading.Event | None = None
) -> dict:
    return Collector(config, journal, None, client, stop).refresh_calendar()

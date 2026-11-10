# Evidence schemas require exact built-in types, including rejection of bool-as-int.
# pylint: disable=unidiomatic-typecheck

"""Acquisition-only task for the ETF option chain: a literal plan in, bytes out.

This is the lane's *acquisition boundary as a process*: it reads one literal JSON
plan, performs exactly the GETs that plan names through :mod:`.source` (the
transport boundary that returns ``response.content`` bytes), writes the private
``raw-acquisition-v1`` evidence root and exits.  It does not parse, normalize,
label, judge, commit or publish anything, and it has no journal, no config file
and no catalog/database of any kind:

```python
import etf_options.acquire   # pulls in .source and collector-core evidence only
```

Nothing here imports ``.config``, ``.quality``, ``.storage``, ``.delivery``,
``.runtime``, ``duckdb``, ``sqlite3`` or any ``dq_quant_invest_data`` module --
there is no code path from this module to the central store, so "no central
access" is structural rather than asserted.  The evidence it writes is the same
``raw-acquisition-v1`` shape ``etf_options.runtime`` produces for a document, so a
downstream reader (``runtime.read_capture``/``read_delivery``) can consume it.

Why not the generic ``acquire-raw``?  That entry point dispatches through
``raw_acquisition.capture_process`` -> ``ProcessClient``, i.e. it stores the
provider's *transformed rows* (``provider-transformed``), never the exchange's
document bytes.  The literal lane is only reachable at the transport call, which
is what this module drives.

Plan (``etf-acquire-plan-v2``), one document per entry::

    {
      "version": "etf-acquire-plan-v2",
      "identity": {"source_id": "etf-options", "date": "2026-10-20",
                   "fingerprint": "<64 hex: digest of the collector's fingerprint>",
                   "token": "<opaque run token>", "attempt": 0},
      "source": {"provider": "akshare", "model": "AkshareEtfOptionQuotes",
                 "max_response_bytes": 1000000, "timeout_seconds": 15},
      "requests": [
        {"request_id": "sse-quotes-1", "kind": "quote-batch", "exchange": "SSE",
         "params": {"symbols": "10000001,10000002"}},
        {"request_id": "sse-master", "kind": "sse-official-master", "exchange": "SSE",
         "params": {"exchange": "SSE", "source": "official"}}
      ]
    }

``identity`` is literal data, not configuration: it is the block the collector
would itself use for the same day (``.identity``), and :mod:`.identity` derives
``task_id``/``generation`` from it with the same functions the collection half
uses.  That is the hand-off: ``acquire`` writes a locator the collector's own
``existing_state`` then finds, so a completed acquisition is reused with **no**
source call.  ``etf_options.cli plan --config ... --out ...`` writes exactly such
a plan.  ``etf-acquire-plan-v1`` is still accepted; it has no identity block, so
its entries are addressed by an entry-local derivation and can only be replayed
by the same plan (never consumed by the collector).

An entry may carry literal ``task_id``/``generation`` (64 hex each) instead of
deriving them, but not both that and an identity block.  Exit codes: ``0`` every
entry captured, verified and 2xx, ``1`` anything else (including a captured non-2xx
body), ``2`` the plan itself was refused, ``4`` cancelled.  The per-request HTTP
status is *persisted* (``capture_status``) and **chained**: ``acquire_manifest.json``
in the same locator states the sha256 of the task, every receipt, the status record
and the core terminal/durable certificates, ``verify`` recomputes all of them, and
the manifest's own digest is anchored outside the locator in
``<root>/acquisition-log.jsonl``.  It decides the terminal: a 503 body is kept as
evidence and reported ``http_error``, never ``complete`` -- relabelling it ``200``
in place is ``status_binding``, refused, exit 1, and relabelling it *and*
restating ``status_sha256`` in the manifest is ``acquisition_log_mismatch``,
also refused.  A writer who rewrites all three (status record, manifest, log line)
is not detectable here; there is no signature.

CLI::

    python -m dq_historical_market_data_collector.etf_options.acquire \
        --plan plan.json --root /var/lib/etf-options/evidence

The same command is wired as ``etf_options.cli acquire --plan ... --root ...``.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import signal
import threading
from pathlib import Path
from typing import Any

from openbb_collector_core.models import ProviderFailure
from openbb_collector_core.raw_acquisition import (
    RawResponse,
    capture,
    plan_capture,
    read_capture,
    validate_task,
)

from . import capture_manifest, capture_status, identity
from . import source as lane_source

PLAN_VERSION = "etf-acquire-plan-v2"
PLAN_VERSIONS = ("etf-acquire-plan-v1", PLAN_VERSION)
TASK_VERSION = "raw-acquisition-v1"
MAX_PLAN_BYTES = 1 << 20
MAX_ENTRIES = 64
MAX_RESPONSE_BYTES = 50_000_000
PLAN_FIELDS = ("version", "source", "requests")
PLAN_FIELDS_BLOCK = (*PLAN_FIELDS, "identity")
SOURCE_FIELDS = ("provider", "model", "max_response_bytes", "timeout_seconds")
REQUEST_FIELDS = (
    "request_id",
    "kind",
    "params",
    "model",
    "task_id",
    "generation",
    "exchange",
)
SYMBOL = re.compile(r"[0-9]{8}\Z")
HASH = re.compile(r"[0-9a-f]{64}\Z")
#: A document kind belongs to exactly one provider model.  The client decides the
#: URL and the transport helper from ``(model, params)``; the plan may not claim a
#: kind the client would not produce.
KIND_MODELS = {
    lane_source.QUOTE_KIND: "AkshareEtfOptionQuotes",
    lane_source.SSE_MASTER_KIND: "AkshareEtfOptionContracts",
    lane_source.SZSE_MASTER_KIND: "AkshareEtfOptionContracts",
}


class PlanError(ValueError):
    """A typed refusal about the plan; nothing was captured."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


def _require(condition: Any, code: str) -> None:
    if not condition:
        raise PlanError(code)


def _plain(value: Any, depth: int = 0) -> None:
    """JSON data only: no callables, no NaN, no depth bomb, no non-string keys."""
    _require(depth <= 32, "plan_depth_limit")
    if type(value) is dict:
        _require(all(type(key) is str for key in value), "non_json_key")
        for item in value.values():
            _plain(item, depth + 1)
    elif type(value) is list:
        for item in value:
            _plain(item, depth + 1)
    elif type(value) is float:
        _require(math.isfinite(value), "nonfinite_json")
    else:
        _require(type(value) in (str, int, bool, type(None)), "non_json_value")


def encode(value: Any) -> bytes:
    """Canonical bytes for a plan or a task; never evaluates or imports anything."""
    _plain(value)
    try:
        return (
            json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
            + "\n"
        ).encode()
    except (
        ValueError,
        UnicodeError,
    ) as exc:  # pragma: no cover - _plain already refused
        raise PlanError("invalid_json") from exc


def digest(value: Any) -> str:
    return hashlib.sha256(encode(value)).hexdigest()


class PlanSource:
    """The transport view of a planned source: limits only, no configuration."""

    __slots__ = ("provider", "model", "max_response_bytes", "timeout_seconds")

    def __init__(
        self, provider: str, model: str, max_response_bytes: int, timeout_seconds: float
    ) -> None:
        self.provider = provider
        self.model = model
        self.max_response_bytes = max_response_bytes
        self.timeout_seconds = timeout_seconds


def _source_of(value: Any) -> PlanSource:
    _require(
        type(value) is dict and set(value) == set(SOURCE_FIELDS), "invalid_plan_source"
    )
    _require(
        all(type(value[key]) is str and value[key] for key in ("provider", "model")),
        ("invalid_plan_source"),
    )
    _require(
        type(value["max_response_bytes"]) is int
        and 1 <= value["max_response_bytes"] <= MAX_RESPONSE_BYTES,
        "invalid_response_limit",
    )
    _require(
        type(value["timeout_seconds"]) in (int, float)
        and 0 < value["timeout_seconds"] <= 600,
        "invalid_timeout",
    )
    return PlanSource(
        value["provider"],
        value["model"],
        value["max_response_bytes"],
        value["timeout_seconds"],
    )


def _params_of(kind: str, params: Any) -> dict[str, Any]:
    _require(type(params) is dict, "invalid_request_params")
    if kind == lane_source.QUOTE_KIND:
        symbols = params.get("symbols")
        _require(type(symbols) is str and symbols, "invalid_symbols")
        tokens = symbols.split(",")
        _require(
            1 <= len(tokens) <= 50 and all(SYMBOL.match(token) for token in tokens),
            "invalid_symbols",
        )
        _require(len(set(tokens)) == len(tokens), "duplicate_symbols")
        _require(set(params) == {"symbols"}, "invalid_request_params")
    else:
        _require(
            params.get("source") == "official"
            and params.get("exchange") in {"SSE", "SZSE"},
            "invalid_request_params",
        )
    return dict(params)


def validate_plan(plan: Any) -> dict[str, Any]:
    """Structural validation only: no rows, no judgement, no I/O."""
    _require(type(plan) is dict, "invalid_plan")
    version = plan.get("version")
    _require(version in PLAN_VERSIONS, "unsupported_plan_version")
    block = None
    if version == PLAN_VERSION:
        _require(set(plan) == set(PLAN_FIELDS_BLOCK), "invalid_plan")
        try:
            block = identity.block(plan["identity"])
        except ValueError as exc:
            raise PlanError(str(exc)) from exc
    else:
        _require(set(plan) == set(PLAN_FIELDS), "invalid_plan")
    source = _source_of(plan["source"])
    entries = plan["requests"]
    _require(
        type(entries) is list and 1 <= len(entries) <= MAX_ENTRIES, "request_limit"
    )
    validated = []
    for entry in entries:
        _require(
            type(entry) is dict
            and set(entry) <= set(REQUEST_FIELDS)
            and "request_id" in entry,
            "invalid_request",
        )
        _require(set(entry) >= {"request_id", "kind", "params"}, "invalid_request")
        _require(
            type(entry["request_id"]) is str and 0 < len(entry["request_id"]) <= 256,
            "invalid_request",
        )
        # A request id is the *task's* request id, and a plan may cover two
        # exchanges of the same model, so the same id legitimately appears once per
        # task.  What may not repeat is a task: two entries that derive the same
        # (task_id, generation) would address one locator twice (``duplicate_task``).
        kind = entry["kind"]
        _require(kind in KIND_MODELS, "unknown_document_kind")
        # One plan may cover several documents; each document still names exactly
        # one provider model, and the plan-level model is the default.  A model
        # that does not produce this kind is refused, never guessed.
        model = entry.get("model", source.model)
        _require(type(model) is str and model, "invalid_request")
        _require(KIND_MODELS[kind] == model, "plan_kind_conflict")
        entry_source = PlanSource(
            source.provider, model, source.max_response_bytes, source.timeout_seconds
        )
        params = _params_of(kind, entry["params"])
        # One authority for which URL and which transport helper a request uses:
        # the client's own classifier.  A plan whose kind disagrees is refused.
        _require(lane_source.plan_kind(model, params) == kind, "plan_kind_conflict")
        if block is not None:
            # The shared identity needs the exchange, which is not always in the
            # request parameters (a quote block is addressed by symbol only), so
            # an identity-bearing plan states it per entry and may not also
            # restate a literal task/generation.
            _require(entry.get("exchange") in identity.EXCHANGES, "invalid_identity")
            _require(not ({"task_id", "generation"} & set(entry)), "ambiguous_identity")
        for field in ("task_id", "generation"):
            if field in entry:
                _require(
                    type(entry[field]) is str and bool(HASH.fullmatch(entry[field])),
                    "invalid_identity",
                )
        validated.append(
            {
                "request_id": entry["request_id"],
                "kind": kind,
                "params": params,
                "model": model,
                "source": entry_source,
                "exchange": entry.get("exchange"),
                "task_id": entry.get("task_id"),
                "generation": entry.get("generation"),
            }
        )
    return {"source": source, "requests": validated, "identity": block}


def task_of(
    source: PlanSource, entry: dict[str, Any], block: dict[str, Any] | None = None
) -> dict[str, Any]:
    """The raw-acquisition task for one planned document.

    With an identity block the task id and generation are the *same functions* the
    collection half uses, so the collector's own ``existing_state`` finds this
    locator and reuses it without a second source call.  Without one they are
    derived from the entry itself, deterministically -- never random, so re-running
    one plan addresses the same evidence and resumes it.
    """
    if block is not None:
        task_id = identity.task_id(
            source_id=block["source_id"],
            model=source.model,
            exchange=entry["exchange"],
            date=block["date"],
            params=entry["params"],
        )
        generation = identity.generation(
            identity.base(
                fingerprint=block["fingerprint"],
                source_id=block["source_id"],
                date=block["date"],
                attempt=block["attempt"],
                token=block["token"],
            ),
            0,
        )
    else:
        task_id = entry["task_id"] or digest(
            {
                "version": "etf-acquire-task-v1",
                "provider": source.provider,
                "model": source.model,
                "kind": entry["kind"],
                "params": entry["params"],
            }
        )
        generation = entry["generation"] or digest(
            {"version": "etf-acquire-generation-v1", "task_id": task_id, "attempt": 0}
        )
    return validate_task(
        {
            "version": TASK_VERSION,
            "task_id": task_id,
            "generation": generation,
            "source": {"provider": source.provider, "model": source.model},
            "requests": [
                {"request_id": entry["request_id"], "params": entry["params"]}
            ],
        }
    )


def _fetch_adapter(client: Any, source: PlanSource, counter: dict[str, Any]):
    """One GET, bytes only: never a provider-transformed payload, never a decode."""

    def fetch(request: dict[str, Any]) -> RawResponse:
        counter["calls"] += 1
        try:
            result = client.fetch("etf-acquire", source, request["params"])
        except ProviderFailure:  # pylint: disable=try-except-raise
            raise
        except OSError as exc:
            # A transport error is an OSError; it becomes a typed failed receipt
            # (no bytes existed) rather than an empty success.
            raise ProviderFailure("provider_failure") from exc
        body = getattr(result, "body", None)
        if type(body) is not bytes:
            # An injected provider-shaped client returns parsed rows.  Storing
            # that as evidence would be a mislabelled capture, so refuse loudly.
            raise ValueError("provider_transformed_payload_not_accepted")
        counter["status"].append(getattr(result, "status", None))
        counter["media_type"].append(getattr(result, "media_type", None))
        return RawResponse(
            body,
            "raw-bytes",
            None,
            media_type=getattr(result, "media_type", None)
            or "application/octet-stream",
        )

    return fetch


def _private_root(root: Path | str) -> Path:
    path = Path(os.path.abspath(root))
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    return path


def _code(exc: BaseException) -> str:
    """A safe, typed reason string: never an exception repr with paths in it."""
    text = str(exc)
    return text if text.isidentifier() and len(text) <= 64 else "acquisition_refused"


def acquire_plan(
    root: Path | str,
    plan: dict[str, Any],
    *,
    client: Any = None,
    stop: threading.Event | None = None,
) -> list[dict[str, Any]]:
    """Capture every planned document once per generation and return the result.

    The whole plan is validated and budget-preflighted before the first source
    call, so a deterministic refusal mutates nothing.  Every response -- a non-2xx
    body, an empty body, a body that a later reader will reject -- is stored
    before this function returns; nothing is parsed and nothing is committed.
    """
    authority = validate_plan(plan)
    source = authority["source"]
    block = authority["identity"]
    tasks = [task_of(entry["source"], entry, block) for entry in authority["requests"]]
    for task in tasks:
        if not plan_capture(task, response_max_bytes=source.max_response_bytes)["fits"]:
            raise PlanError("task_evidence_budget_exceeded")
    _require(
        len({(task["task_id"], task["generation"]) for task in tasks}) == len(tasks),
        "duplicate_task",
    )
    root = _private_root(root)
    if client is None:
        client = lane_source.EtfChainClient(stop)
    results: list[dict[str, Any]] = []
    for entry, task in zip(authority["requests"], tasks, strict=True):
        counter: dict[str, Any] = {"calls": 0, "status": [], "media_type": []}
        try:
            state = capture(
                root,
                task,
                _fetch_adapter(client, entry["source"], counter),
                stop=stop,
                response_max_bytes=source.max_response_bytes,
            )
        except ValueError as exc:
            # Protocol refusal (oversize body, unsafe path, budget): this entry
            # fails, the evidence written so far stays, the next entry may run.
            results.append(
                {
                    "request_id": entry["request_id"],
                    "exchange": entry["exchange"],
                    "kind": entry["kind"],
                    "task_id": task["task_id"],
                    "generation": task["generation"],
                    "locator": None,
                    "status": "error",
                    "reason": _code(exc),
                    "complete": False,
                    "terminal": "refused",
                    "sha256": None,
                    "size": None,
                    "evidence_kind": None,
                    "transformation": None,
                    "media_type": None,
                    "http_status": None,
                    "source_calls": counter["calls"],
                }
            )
            continue
        responses = state["responses"]
        receipt = (responses[0] or {}).get("receipt") or {} if responses else {}
        try:
            record = capture_status.write(
                root, state, capture_status.observed(responses, counter["status"])
            )
        except capture_status.StatusError as exc:
            # The capture is on disk but its integrity chain does not add up (a
            # lone status edit, a replaced record, a missing or edited manifest).
            # That is a refusal, not an acquisition: the bytes stay, the typed
            # code is reported, and `verify` reaches the same verdict by reading
            # the directory again rather than trusting this result.
            results.append(
                {
                    "request_id": entry["request_id"],
                    "exchange": entry["exchange"],
                    "kind": entry["kind"],
                    "task_id": task["task_id"],
                    "generation": task["generation"],
                    "locator": state["locator"],
                    "status": "error",
                    "reason": str(exc),
                    "complete": state["complete"],
                    "terminal": "refused",
                    "sha256": receipt.get("sha256"),
                    "size": receipt.get("size"),
                    "evidence_kind": receipt.get("evidence_kind"),
                    "transformation": receipt.get("transformation"),
                    "media_type": receipt.get("media_type"),
                    "http_status": None,
                    "status_record": capture_status.NAME,
                    "integrity_manifest": capture_manifest.NAME,
                    "source_calls": counter["calls"],
                }
            )
            continue
        results.append(
            {
                "request_id": entry["request_id"],
                "exchange": entry["exchange"],
                "kind": entry["kind"],
                "task_id": task["task_id"],
                "generation": task["generation"],
                "locator": state["locator"],
                "status": state["status"],
                "reason": state["reason"],
                "complete": state["complete"],
                # The typed verdict that decides the exit code.  ``complete`` above
                # only says the capture is finalized; ``terminal`` says whether the
                # source answered 2xx.  A 503 keeps its body and is ``http_error``.
                "terminal": capture_status.terminal(state, record),
                "sha256": receipt.get("sha256"),
                "size": receipt.get("size"),
                "evidence_kind": receipt.get("evidence_kind"),
                "transformation": receipt.get("transformation"),
                "media_type": receipt.get("media_type"),
                # The status is read back from the persisted record, so a replay
                # reports the status the capture was actually taken under instead
                # of the count of calls this invocation happened to make.
                "http_status": capture_status.status_of(record, 0),
                "status_record": capture_status.NAME,
                # The manifest that chains this record, the task and the receipts.
                "integrity_manifest": capture_manifest.NAME,
                "source_calls": counter["calls"],
            }
        )
    return results


def verify(root: Path | str, results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Read every capture back and report the locator and body digest it verifies.

    The status record and its integrity manifest are part of what is verified: a
    capture whose per-request status cannot be read back, does not bind to the
    receipts on disk, or no longer hashes to what ``acquire_manifest.json`` states
    is not verified, so ``main()`` cannot report success for it.
    """
    verified = []
    for result in results:
        if not result["locator"]:
            verified.append(
                {"request_id": result["request_id"], "verified": False, "reason": None}
            )
            continue
        try:
            # The core chain is read first, and a tamper there is a *verdict*, not
            # an exception escaping to the caller: `verify` is handed results from
            # an earlier run and must answer "not verified, this reason" for every
            # result it was given.  `main` reaches the same refusal through
            # `acquire_plan`; this makes the two paths agree.
            state = read_capture(root, result["task_id"], result["generation"])
            receipt = state["responses"][0]["receipt"]
            record = capture_status.read(root, state["locator"], state)
        except ValueError as exc:
            verified.append(
                {
                    "request_id": result["request_id"],
                    "verified": False,
                    "reason": _code(exc),
                    "locator": result["locator"],
                    "sha256": result["sha256"],
                    "size": result["size"],
                    "complete": False,
                }
            )
            continue
        verified.append(
            {
                "request_id": result["request_id"],
                "verified": state["locator"] == result["locator"]
                and receipt["sha256"] == result["sha256"]
                and receipt["size"] == result["size"]
                and state["complete"],
                "locator": state["locator"],
                "sha256": receipt["sha256"],
                "size": receipt["size"],
                "complete": state["complete"],
                "http_status": capture_status.status_of(record, 0),
                "terminal": capture_status.terminal(state, record),
            }
        )
    return verified


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(
        prog="dq-etf-option-chain-collector acquire",
        description="Persist literal exchange document bytes for a plan; parse nothing.",
    )
    result.add_argument("--plan", required=True, type=Path)
    result.add_argument("--root", required=True, type=Path)
    return result


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    stop = threading.Event()
    previous: dict[int, Any] = {}
    if threading.current_thread() is threading.main_thread():
        for signum in (signal.SIGINT, signal.SIGTERM):
            previous[signum] = signal.signal(signum, lambda *_: stop.set())
    try:
        try:
            payload = args.plan.read_bytes()
            if len(payload) > MAX_PLAN_BYTES:
                raise PlanError("plan_size_limit")
            plan = json.loads(payload)
        except PlanError as exc:
            print(
                json.dumps({"status": "failed", "code": exc.code}, sort_keys=True),
                flush=True,
            )
            return 2
        except (ValueError, UnicodeError):
            print(
                json.dumps(
                    {"status": "failed", "code": "invalid_json"}, sort_keys=True
                ),
                flush=True,
            )
            return 2
        version = plan.get("version") if isinstance(plan, dict) else None
        try:
            results = acquire_plan(args.root, plan, stop=stop)
        except PlanError as exc:
            print(
                json.dumps({"status": "failed", "code": exc.code}, sort_keys=True),
                flush=True,
            )
            return 2
        verified = verify(args.root, results)
        terminals: dict[str, int] = {}
        for item in results:
            terminals[item["terminal"]] = terminals.get(item["terminal"], 0) + 1
        ok = all(item["terminal"] == capture_status.CAPTURED for item in results)
        verified_all = all(item["verified"] for item in verified)
        settled = (
            "complete"
            if ok and verified_all
            else "cancelled" if stop.is_set() else "failed"
        )
        payload = {
            "command": "acquire",
            "status": settled,
            "plan_version": version,
            "identity": plan.get("identity") if isinstance(plan, dict) else None,
            "root": str(Path(os.path.abspath(args.root))),
            "results": results,
            "verified": verified,
            "verified_all": verified_all,
            # The verdicts, by count: a non-2xx capture appears here as
            # ``http_error`` even though its bytes are on disk and ``complete``.
            "terminals": terminals,
            "non_2xx": [
                {"request_id": item["request_id"], "http_status": item["http_status"]}
                for item in results
                if item["terminal"] == "http_error"
            ],
        }
        print(json.dumps(payload, sort_keys=True), flush=True)
        if stop.is_set():
            return 4
        return 0 if ok and verified_all else 1
    finally:
        for previous_signum, handler in previous.items():
            signal.signal(previous_signum, handler)


if __name__ == "__main__":
    raise SystemExit(main())

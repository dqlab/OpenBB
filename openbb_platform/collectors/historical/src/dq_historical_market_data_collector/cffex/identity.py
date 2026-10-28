"""The one capture identity shared by the acquire task and the collector.

A ``raw-acquisition-v2`` generation is addressed by ``(task_id, generation)``.
Two lanes write into that store -- the acquisition-only task
(:mod:`.acquire`) and the collection task (:mod:`.runtime`, ``Collector``) --
and the whole point of separating them is that the bytes the acquire task
captured are the bytes the collector judges. That only works if both lanes
derive the *same* names for the same day. So the formulas live here, once, and
both modules call them:

    task_id     = sha256(kind, source_id, provider, model, date)
    fingerprint = sha256(version=2, source_id, source, units, evidence_version)
    base        = sha256(fingerprint, source_id, date, attempt, token, evidence_version)
    generation  = sha256(base, epoch)
    locator     = raw-acquisition-v2/sha256(version, task_id, generation)   (the store)

``fingerprint`` describes the transport authority that shapes a capture -- the
provider, its limits and the declared units -- so evidence captured under one
authority is never reused under another. ``token`` is the collector's freshness
token: ``"configured"`` for an ordinary backfill, a per-run token when the
operator asked for a refresh. ``epoch`` is the only way a provider may be asked
again for the same ``(task_id, base)``: a completed capture under epoch *n*
stays immutable when epoch *n+1* is written.

This module is stdlib-only on purpose: :mod:`.acquire` imports it, and the
acquire task's import closure may not contain configuration. The *collector*
imports it too (``config.Config.fingerprint`` and ``runtime.Collector``), which
is what makes coincidence structural rather than a convention both sides must
remember.
"""

from __future__ import annotations

import hashlib
import json
from datetime import date
from typing import Any

EVIDENCE_VERSION = "raw-acquisition-v2"
# Keep the historical generation formula so legacy evidence remains discoverable.
_IDENTITY_VERSION = "raw-acquisition-v1"
KIND = "cffex-daily"

# ``Config.fingerprint`` material version. Bumping it renames every capture of
# every day, so it changes only with a deliberate evidence-migration decision.
FINGERPRINT_VERSION = 2

# The only unit fields a payload may declare (mirrored by ``config.UNIT_FIELDS``,
# which imports this tuple rather than restating it).
UNIT_FIELDS = ("price_unit", "volume_unit", "open_interest_unit", "turnover_unit")

# The provider models a collector configuration may name. A plan that named
# anything else would file evidence under an identity no configuration can
# derive, so the acquire task refuses it instead.
ALLOWED_MODELS = ("AkshareFuturesHistDailyCffex", "AkshareCffexDaily")


def canonical(value: Any) -> bytes:
    """Canonical JSON bytes; the only encoding used for identities and digests."""
    return (
        json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n"
    ).encode()


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value)).hexdigest()


def _iso(day: date | str) -> str:
    return day if isinstance(day, str) else day.isoformat()


def task_identity(
    source_id: str, provider: str, model: str, day: date | str, kind: str = KIND
) -> str:
    """The daily task id: one date, one source, one provider model."""
    return digest(
        {
            "kind": kind,
            "source_id": source_id,
            "provider": provider,
            "model": model,
            "date": _iso(day),
        }
    )


def fingerprint(source_id: str, source: dict[str, Any], units: dict[str, str]) -> str:
    """Stable identity of the transport authority that shapes a capture.

    Clocks, calendars and cursors are deliberately excluded: only the provider,
    its limits, the source identity and the declared units decide whether two
    captures of the same day are comparable.

    The material is encoded **without** a trailing newline, unlike
    :func:`canonical`. That is the encoding the value has had since the
    collection lane introduced it; changing it would rename every existing
    capture, so it is preserved here verbatim and is not interchangeable with
    :func:`digest`.
    """
    material = {
        "version": FINGERPRINT_VERSION,
        "source_id": source_id,
        "source": source,
        "units": dict(units),
        "evidence_version": _IDENTITY_VERSION,
    }
    body = json.dumps(material, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(body).hexdigest()


def capture_base(
    fingerprint_value: str,
    source_id: str,
    day: date | str,
    attempt: int,
    token: str,
) -> str:
    """The per-(date, attempt, token) base a generation is derived from."""
    return digest(
        {
            "fingerprint": fingerprint_value,
            "source_id": source_id,
            "date": _iso(day),
            "attempt": attempt,
            "token": token,
            "evidence_version": _IDENTITY_VERSION,
        }
    )


def generation(base: str, epoch: int) -> str:
    """The epoch-th generation under one base; earlier epochs stay immutable."""
    return digest({"base": base, "epoch": epoch})

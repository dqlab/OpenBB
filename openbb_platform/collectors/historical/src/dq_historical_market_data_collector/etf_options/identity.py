# Keep acquisition-only imports free of provider/parsing/journal initialization.
# pylint: disable=import-outside-toplevel
# Evidence schemas require exact built-in types, including rejection of bool-as-int.
# pylint: disable=unidiomatic-typecheck

"""One identity for the acquisition and the collection half of this lane.

Both halves write into the same ``raw-acquisition-v1`` namespace, so the task id
and the capture generation must be a function of the same inputs on both sides:

```
task_id    = digest({kind, source_id, model, exchange, date, params})
base       = digest({fingerprint, source_id, date, attempt, token,
                     evidence_version})
generation = digest({base, epoch})
```

``acquire`` derives these from a literal *identity block* in its plan; ``runtime``
derives them from the configuration fingerprint and (by default) the journal run
id.  Given the same block, both address the same locator -- which is what makes a
completed acquisition reusable by the collector with no second source call.  The
run id is what keeps the collector's default identity per-run; a hand-off plan
replaces it with a stable token, and that is the whole difference.

Standard library only, deliberately: :mod:`.acquire` must be able to learn its own
identity without importing :mod:`.storage` (sqlite3) or :mod:`.config`.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

#: The identity contract's own version.  Bumping it re-addresses every capture.
IDENTITY_VERSION = "etf-options-identity-v1"
#: The evidence scheme whose locator these identities address.
EVIDENCE_VERSION = "raw-acquisition-v1"
#: The one namespace prefix ``raw-acquisition-v1``'s ``task_id`` is derived from.
TASK_KIND = "etf-options"
#: Fields of a literal identity block.  ``exchange`` is not one of them: it is a
#: property of the request, so it travels with the request (the plan's entry) and
#: is part of ``task_id`` there.
BLOCK_FIELDS = ("source_id", "date", "fingerprint", "token", "attempt")
#: Kinds of request this lane addresses as one document.
EXCHANGES = ("SSE", "SZSE")


def canonical(value: Any) -> str:
    """The collector's canonical JSON: sorted keys, compact, non-ASCII kept."""
    return json.dumps(
        value,
        sort_keys=True,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
    )


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def task_id(
    *, source_id: str, model: str, exchange: str, date: str, params: dict
) -> str:
    """The ``raw-acquisition-v1`` task id for one document request.

    ``params`` is the whole request parameter block, so two blocks that differ in
    one symbol are two tasks -- identity follows the bytes that would be fetched.
    """
    return digest(
        {
            "kind": TASK_KIND,
            "source_id": source_id,
            "model": model,
            "exchange": exchange,
            "date": date,
            "params": params,
        }
    )


def base(
    *, fingerprint: Any, source_id: str, date: str, attempt: int, token: str
) -> str:
    """The generation family for one (configuration, day, attempt, token).

    ``attempt`` is the retry ordinal: a new attempt may refetch, so it must be a
    new generation.  ``epoch`` (see :func:`generation`) advances when downstream
    rejects a capture, which is the only other way a source may be called again.
    """
    return digest(
        {
            "fingerprint": fingerprint,
            "source_id": source_id,
            "date": date,
            "attempt": attempt,
            "token": token,
            "evidence_version": EVIDENCE_VERSION,
        }
    )


def generation(base_value: str, epoch: int) -> str:
    return digest({"base": base_value, "epoch": epoch})


def block(value: Any) -> dict[str, Any]:
    """Validate and detach a literal identity block (raises :class:`ValueError`)."""
    if type(value) is not dict or set(value) != set(BLOCK_FIELDS):
        raise ValueError("invalid_identity")
    if type(value["source_id"]) is not str or not 0 < len(value["source_id"]) <= 64:
        raise ValueError("invalid_identity")
    if type(value["date"]) is not str or not _is_date(value["date"]):
        raise ValueError("invalid_identity")
    if type(value["fingerprint"]) is not str or not _is_hash(value["fingerprint"]):
        raise ValueError("invalid_identity")
    if type(value["token"]) is not str or not 0 < len(value["token"]) <= 256:
        raise ValueError("invalid_identity")
    if type(value["attempt"]) is not int or not 0 <= value["attempt"] < 64:
        raise ValueError("invalid_identity")
    return dict(value)


def _is_date(value: str) -> bool:
    from datetime import date as _date

    try:
        return _date.fromisoformat(value).isoformat() == value
    except ValueError:
        return False


def _is_hash(value: str) -> bool:
    return len(value) == 64 and all(
        character in "0123456789abcdef" for character in value
    )

"""Bounded, read-only, explicit-basis consumption at an actual visibility time."""

# Only a constant schema and generated '?' placeholders enter SQL text.
# ruff: noqa: S608

from contextlib import contextmanager
from datetime import date, datetime
from pathlib import Path

import duckdb
from openbb_duckdb.baostock18.contract import SCHEMA, AdmissionError, require_basis


@contextmanager
def _existing_read_lock(path):
    """Share an existing lock O_RDONLY; fail immediately on a writer, never create."""
    lock = path.with_suffix(".market.lock")
    if not lock.exists():
        yield
        return
    import fcntl  # pylint: disable=import-outside-toplevel

    with lock.open("rb") as stream:
        try:
            fcntl.flock(stream.fileno(), fcntl.LOCK_SH | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise AdmissionError("Market catalog busy; reader did not wait or create a lock") from exc
        try:
            yield
        finally:
            fcntl.flock(stream.fileno(), fcntl.LOCK_UN)


def read_bars(database_path, *, symbols, start_date: date, end_date: date,
              basis: str | None = None, as_of: datetime | None = None,
              fixed_qfq_basis_date=None, limit=100_000) -> list[dict]:
    """Read only the explicitly admitted clean snapshot; no old-table fallback.

    as_of is the local dataset visibility/withdrawal time, not a claim of
    historical market availability. With no earlier event the result is empty.
    """
    require_basis(basis, fixed_qfq_basis_date)
    if not 1 <= limit <= 100_000:
        raise AdmissionError("Reader limit must be 1..100,000")
    if not start_date or not end_date or start_date > end_date:
        raise AdmissionError("A valid bounded start_date/end_date is required")
    if as_of is not None and (as_of.tzinfo is None or as_of.utcoffset() is None):
        raise AdmissionError("as_of must include an explicit timezone")
    normalized = sorted({s.strip().upper() for s in symbols if s.strip()})
    if not normalized or len(normalized) > 100:
        raise AdmissionError("Reader requires 1..100 explicit symbols")
    path = Path(database_path).expanduser().resolve(strict=True)
    if path.with_name(path.name + ".access-windows").exists():
        raise AdmissionError("Access-window policy requires a platform-aware reader; no bypass")
    with _existing_read_lock(path), duckdb.connect(str(path), read_only=True, config={
        "enable_external_access": False, "threads": 2, "memory_limit": "512MB",
    }) as connection:
        connection.execute("SET TimeZone='UTC'")
        if as_of is None:
            event = connection.execute(
                f"SELECT e.snapshot_id,e.known_at FROM {SCHEMA}.dataset_pointer p "
                f"JOIN {SCHEMA}.visibility_event e USING(event_id) WHERE p.adjustment_basis=?", [basis],
            ).fetchone()
        else:
            event = connection.execute(
                f"SELECT snapshot_id,known_at FROM {SCHEMA}.visibility_event "
                "WHERE adjustment_basis=? AND known_at<=? ORDER BY event_id DESC LIMIT 1", [basis, as_of],
            ).fetchone()
        if not event or event[0] is None:
            return []
        placeholders = ",".join("?" for _ in normalized)
        result = connection.execute(
            f"SELECT b.*,l.raw18_json,l.flags_json,?::TIMESTAMPTZ AS known_at,"
            "s.source_available_at,s.availability_basis,'unadjusted' AS base_adjustment "
            f"FROM {SCHEMA}.bar_version b JOIN {SCHEMA}.literal_version l USING(snapshot_id,value_id) "
            f"JOIN {SCHEMA}.snapshot s USING(snapshot_id) JOIN {SCHEMA}.basis_admission a "
            "ON a.snapshot_id=b.snapshot_id AND a.adjustment_basis=b.adjustment_basis "
            "WHERE b.snapshot_id=? AND b.adjustment_basis=? AND s.state='ready' "
            f"AND l.disposition='clean' AND b.symbol IN ({placeholders}) AND b.date BETWEEN ? AND ? "
            "ORDER BY b.symbol,b.date LIMIT ?",
            [event[1], event[0], basis, *normalized, start_date, end_date, limit + 1],
        )
        columns = [col[0] for col in result.description]
        rows = [dict(zip(columns, row)) for row in result.fetchall()]
        if len(rows) > limit:
            raise AdmissionError("Result exceeds limit; narrow symbols or dates")
        return rows

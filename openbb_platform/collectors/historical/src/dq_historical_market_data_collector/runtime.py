"""Bounded collection sessions, resumable scheduling and non-destructive resets."""

from __future__ import annotations

import json
import threading
import time
from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from .config import CollectorConfig, load_config
from .delivery import deliver
from .planner import Chunk, chunks, due_days, expected_times, reset_period
from .providers import (
    OpenBBClient,
    ProviderClient,
    ProviderFailure,
    request_parameters,
    validate_providers,
)
from .quality import digest, normalize
from .storage import Journal, atomic_json


class Collector:
    def __init__(
        self,
        config: CollectorConfig,
        journal: Journal,
        client: ProviderClient | None = None,
        stop: threading.Event | None = None,
        emit: Callable[[dict[str, Any]], None] | None = None,
        clock: Callable[[], datetime] | None = None,
        config_path: Path | None = None,
    ) -> None:
        if client is None:
            validate_providers(config)
        self.config = config
        self.journal = journal
        self.stop = stop or threading.Event()
        self.client = client or OpenBBClient(self.stop)
        self.emit = emit or (lambda _: None)
        self.clock = clock or (lambda: datetime.now(UTC))
        self.config_path = config_path
        self._last_scheduled: dict[str, float] = {}

    def deliver_pending(self) -> None:
        if self.config.delivery is None:
            return
        try:
            result = deliver(self.config, self.journal)
            for event in result["events"]:
                self.emit(event)
        except Exception:
            self.emit({"event": "session_delivery_pending", "error": "delivery_queue_error"})

    def reset_if_due(self) -> None:
        current = reset_period(self.config.schedule, self.clock())
        previous = self.journal.get_meta("reset_period")
        if previous is None:
            self.journal.set_meta("reset_period", current)
        elif previous != current:
            self.client.reset()
            generation = int(self.journal.get_meta("generation") or 0) + 1
            self.journal.set_meta("generation", str(generation))
            self.journal.set_meta("reset_period", current)
            self.journal.set_meta("reload_pending", "1")
            self.emit({"event": "provider_reset", "period": current, "generation": generation})

    def reload_if_pending(self) -> None:
        if self.journal.get_meta("reload_pending") != "1":
            return
        if self.config_path:
            updated = load_config(self.config_path)
            if updated.storage != self.config.storage:
                raise ValueError("Reset reload cannot change storage configuration")
            # Validate request budgets before replacing the running configuration.
            validate_providers(updated)
            next(chunks(updated, self.clock().date()), None)
            self.config = updated
        self.journal.set_meta("reload_pending", "0")
        self.emit({"event": "configuration_reloaded"})

    def collect(
        self,
        *,
        as_of: date | None = None,
        refresh: bool = False,
        scheduled_day: date | None = None,
        refresh_recent: bool = False,
    ) -> dict[str, Any]:
        today = self.clock().astimezone(ZoneInfo(self.config.schedule.timezone)).date()
        as_of = as_of or today
        last_allowed = today
        if scheduled_day and self.config.schedule.include_current_session:
            if scheduled_day not in due_days(self.config.schedule, self.clock()):
                raise ValueError("The scheduled session is not due")
            last_allowed += timedelta(days=1)
        if as_of > last_allowed:
            raise ValueError("as_of cannot be in the future")
        self.journal.recover()
        fingerprint = self.config.fingerprint()
        session_id = self.journal.start(
            fingerprint,
            scheduled_day.isoformat() if scheduled_day else None,
        )
        summary: dict[str, Any] = {
            "completed_chunks": 0,
            "failed_chunks": 0,
            "resumed_chunks": 0,
            "deferred": False,
            "generation": int(self.journal.get_meta("generation") or 0),
        }
        status = "complete"
        count = 0
        try:
            for chunk in chunks(self.config, as_of):
                if self.stop.is_set():
                    status = "interrupted"
                    break
                self.reset_if_due()
                key = digest([fingerprint, chunk.payload()])
                collection = self.config.collections[chunk.collection_id]
                recent = (
                    refresh_recent
                    and chunk.end > as_of - timedelta(days=collection.refresh_days)
                    and not (
                        scheduled_day
                        and self.journal.completed_for_day(key, scheduled_day.isoformat())
                    )
                )
                if self.journal.completed(key) and not (refresh or recent):
                    summary["resumed_chunks"] += 1
                    continue
                if count >= self.config.max_chunks_per_session:
                    summary["deferred"] = True
                    status = "partial"
                    break
                count += 1
                if self._collect_chunk(session_id, key, chunk):
                    summary["completed_chunks"] += 1
                else:
                    summary["failed_chunks"] += 1
                    status = "partial"
                self.journal.export_pending()
            if self.stop.is_set():
                status = "interrupted"
            self.journal.export_pending()
        except Exception:
            status = "storage_or_runtime_error"
            summary["reason"] = "collection_or_output_failed"
        report = self.journal.finish(session_id, status, summary)
        self.emit({"event": "session_report", **report})
        self.deliver_pending()
        return report

    def _collect_chunk(self, session_id: str, key: str, chunk: Chunk) -> bool:
        collection = self.config.collections[chunk.collection_id]
        instrument = self.config.instruments[chunk.instrument_id]
        expected = expected_times(collection, instrument, chunk)
        if expected == set():
            self.journal.record_attempt(
                session_id,
                key,
                chunk.collection_id,
                chunk.instrument_id,
                collection.primary,
                False,
                [],
                [],
                0,
                {"code": "calendar_closed", "coverage": "complete"},
                True,
            )
            return True
        for source_index, source_id in enumerate([collection.primary, *collection.secondary]):
            source = self.config.sources[source_id]
            for retry in range(source.retries + 1):
                if self.stop.is_set():
                    return False
                try:
                    response = self.client.fetch(source_id, source, instrument, collection, chunk)
                    if len(response.rows) > source.max_rows:
                        raise ProviderFailure("response_row_limit")
                    observed_at = self.clock().astimezone(UTC).isoformat()
                    raw = {
                        "session_id": session_id,
                        "chunk": chunk.payload(),
                        "source": source_id,
                        "request": request_parameters(
                            source_id, source, instrument, collection, chunk
                        ),
                        "observed_at": observed_at,
                        "rows": response.rows,
                        "retry": retry,
                        "provider_metadata": response.metadata,
                        "warnings": response.warning_count,
                    }
                    raw_hash = self.journal.archive(raw)
                    records, rejected, outside, coverage = normalize(
                        response.rows,
                        source_id,
                        source,
                        instrument,
                        collection,
                        chunk,
                        observed_at,
                        raw_hash,
                    )
                    success = bool(records) and not rejected and not coverage["missing"]
                    code = (
                        "ok"
                        if success
                        else "invalid_rows"
                        if rejected
                        else "coverage_gaps"
                        if coverage["missing"]
                        else "no_data"
                    )
                    categories = response.metadata.get("gateway_error_categories", [])
                    if not records and categories:
                        code = categories[0]
                    detail = {
                        "code": code,
                        "retry": retry,
                        "raw_hash": raw_hash,
                        "coverage": coverage,
                        "warning_count": response.warning_count,
                        "gateway_error_categories": categories,
                        "gateway_error_codes": response.metadata.get("gateway_error_codes", []),
                    }
                    self.journal.record_attempt(
                        session_id,
                        key,
                        chunk.collection_id,
                        chunk.instrument_id,
                        source_id,
                        source_index > 0,
                        records,
                        rejected,
                        outside,
                        detail,
                        success,
                    )
                    if success:
                        return True
                except ProviderFailure as exc:
                    self.journal.record_attempt(
                        session_id,
                        key,
                        chunk.collection_id,
                        chunk.instrument_id,
                        source_id,
                        source_index > 0,
                        [],
                        [],
                        0,
                        {"code": exc.code, "retry": retry},
                        False,
                    )
                if retry < source.retries:
                    if self.stop.wait(source.retry_delay_seconds * (2**retry)):
                        return False
        return False

    def run(self, max_seconds: float | None = None) -> None:
        if max_seconds is not None and max_seconds <= 0:
            raise ValueError("max_seconds must be positive")
        timer = threading.Timer(max_seconds, self.stop.set) if max_seconds is not None else None
        if timer:
            timer.start()
        try:
            self.journal.recover()
            while not self.stop.is_set():
                try:
                    self.reset_if_due()
                    self.reload_if_pending()
                    self.deliver_pending()
                    now = self.clock()
                    for day in due_days(self.config.schedule, now):
                        fingerprint = self.config.fingerprint()
                        token = f"{fingerprint}:{day.isoformat()}"
                        if self.journal.scheduled_complete(fingerprint, day.isoformat()):
                            continue
                        if time.monotonic() - self._last_scheduled.get(token, -float("inf")) < (
                            self.config.schedule.retry_seconds
                        ):
                            continue
                        self._last_scheduled[token] = time.monotonic()
                        # as_of is exclusive: the morning schedule collects completed prior days.
                        report = self.collect(
                            as_of=day
                            + timedelta(days=int(self.config.schedule.include_current_session)),
                            scheduled_day=day,
                            refresh_recent=True,
                        )
                        if report["status"] != "complete":
                            self.emit({"event": "scheduled_session_incomplete", "day": str(day)})
                        if self.stop.is_set():
                            break
                    # Keep scheduler bookkeeping bounded across continuous operation.
                    valid = {
                        f"{self.config.fingerprint()}:{d}"
                        for d in due_days(
                            self.config.schedule,
                            self.clock(),
                        )
                    }
                    self._last_scheduled = {
                        k: v for k, v in self._last_scheduled.items() if k in valid
                    }
                except Exception:
                    self.emit({"event": "scheduler_error", "code": "runtime_or_reload_failed"})
                    if self.stop.wait(self.config.schedule.retry_seconds):
                        break
                self.stop.wait(self.config.schedule.heartbeat_seconds)
        finally:
            if timer:
                timer.cancel()
            self.client.close()

    def acceptance(
        self,
        *,
        as_of: date,
        required_sources: list[str],
        min_rows: int = 1,
    ) -> dict[str, Any]:
        if (
            min_rows < 1
            or not required_sources
            or set(required_sources) - self.config.sources.keys()
        ):
            raise ValueError("Require configured sources and a positive row threshold")
        report = self.collect(as_of=as_of, refresh=True)
        checks = []
        for source in required_sources:
            for collection_id, collection in self.config.collections.items():
                if source not in [collection.primary, *collection.secondary]:
                    continue
                for instrument_id in collection.instrument_ids:
                    rows = self.journal.db.execute(
                        "SELECT coalesce(sum(accepted+duplicates),0) FROM attempts "
                        "WHERE session_id=? AND source=? AND collection_id=? "
                        "AND instrument_id=? AND success=1",
                        (report["session_id"], source, collection_id, instrument_id),
                    ).fetchone()[0]
                    checks.append(
                        {
                            "source": source,
                            "collection_id": collection_id,
                            "instrument_id": instrument_id,
                            "validated_rows": rows,
                            "passed": rows >= min_rows,
                        }
                    )
        represented = {c["source"] for c in checks}
        passed = (
            bool(checks)
            and represented == set(required_sources)
            and all(c["passed"] for c in checks)
            and report["status"] == "complete"
            and report["pending_exports"] == 0
        )
        result = {
            "event": "acceptance_report",
            "session_id": report["session_id"],
            "passed": passed,
            "checks": checks,
            "session_status": report["status"],
        }
        atomic_json(
            self.journal.root / "acceptance" / f"{report['session_id']}.json",
            result,
            True,
        )
        self.emit(result)
        return result

    def close(self) -> None:
        self.client.close()


def emit_json(payload: dict[str, Any]) -> None:
    print(json.dumps(payload, sort_keys=True), flush=True)

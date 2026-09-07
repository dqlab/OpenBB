"""Paced snapshot collection, durable session state, and continuous recovery."""

from __future__ import annotations

import json
import threading
import time
import uuid
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from .config import CollectorConfig
from .delivery import deliver
from .providers import OpenBBClient, ProviderClient, ProviderFailure, validate_providers
from .quality import digest, normalize
from .schedule import active_window, reset_period, slot_number, window_on
from .storage import Journal, atomic_json


class Collector:
    def __init__(
        self,
        config: CollectorConfig,
        journal: Journal,
        *,
        provider: ProviderClient | None = None,
        clock: Callable[[], datetime] | None = None,
        emit: Callable[[dict[str, Any]], None] | None = None,
    ) -> None:
        if provider is None:
            validate_providers(config)
        self.config = config
        self.journal = journal
        self.stop = threading.Event()
        self.provider = provider or OpenBBClient(stop=self.stop)
        self.clock = clock or (lambda: datetime.now(UTC))
        self.emit = emit or (lambda event: None)
        self.config_hash = config.fingerprint()
        self._closed = False

    def deliver_pending(self) -> None:
        if self.config.delivery is None:
            return
        try:
            result = deliver(self.config, self.journal)
            for event in result["events"]:
                self.emit(event)
        except Exception:
            self.emit({"event": "session_delivery_pending", "error": "delivery_queue_error"})

    def _lifecycle(self, now: datetime) -> int:
        period = reset_period(self.config.reset, now)
        previous = self.journal.get_meta("lifecycle")
        state = json.loads(previous) if previous else {"period": period, "generation": 0}
        if period != state["period"]:
            # Calls are synchronous: this boundary has no request in flight.
            self.provider.reset()
            state = {"period": period, "generation": state["generation"] + 1}
            event = {
                "event": "provider_reset",
                **state,
                "at": now.isoformat(),
                "reason": self.config.reset.period,
            }
            atomic_json(
                self.journal.root / "resets" / f"{digest(event)}.json", event, immutable=True
            )
            self.emit(event)
        self.journal.set_meta("lifecycle", json.dumps(state))
        return state["generation"]

    def _calendar_recovery(self, now: datetime, generation: int) -> None:
        value = self.journal.get_meta("calendar_cursor")
        cursor = json.loads(value) if value else None
        if cursor is None or cursor["config_hash"] != self.config_hash:
            previous = now
        else:
            previous = datetime.fromisoformat(cursor["at"])
        # Recover long outages in bounded, resumable chunks. No provider requests.
        until = min(now, previous + timedelta(days=31))
        if until > previous:
            for collection_id, collection in self.config.collections.items():
                tz = ZoneInfo(collection.schedule.timezone)
                day = previous.astimezone(tz).date() - timedelta(days=1)
                last_day = until.astimezone(tz).date()
                while day <= last_day:
                    window = window_on(collection.schedule, day)
                    if window and previous < window.end <= until:
                        session_id = self.journal.ensure_session(
                            collection_id, collection, self.config_hash, window, generation
                        )
                        self.journal.finish_session(session_id, "completed", "session_closed")
                    day += timedelta(days=1)
        self.journal.set_meta(
            "calendar_cursor",
            json.dumps({"config_hash": self.config_hash, "at": until.isoformat()}),
        )

    def _flush(self, now: datetime) -> list[dict[str, Any]]:
        try:
            self.journal.export_pending()
            self.journal.set_meta("export_error", "")
        except Exception as exc:
            # Keep journaled observations pending; do not recollect them.
            code = type(exc).__name__
            previous = self.journal.get_meta("export_error")
            self.journal.set_meta("export_error", code)
            if previous != code:
                self.emit({"event": "output_pending", "error": code})
        reports = self.journal.write_reports(now)
        for report in reports:
            self.emit({"event": "session_report", **report})
        self.deliver_pending()
        return reports

    def step(self) -> list[dict[str, Any]]:
        now = self.clock().astimezone(UTC)
        for session in self.journal.running_sessions():
            if session["config_hash"] != self.config_hash:
                self.journal.finish_session(session["id"], "interrupted", "configuration_changed")
            elif datetime.fromisoformat(session["end"]) <= now:
                self.journal.finish_session(session["id"], "completed", "session_closed")
        generation = self._lifecycle(now)
        self._calendar_recovery(now, generation)
        reports = self._flush(now)
        for collection_id, collection in self.config.collections.items():
            for instrument_id in collection.instrument_ids:
                if self.stop.is_set():
                    break
                current = self.clock().astimezone(UTC)
                window = active_window(collection.schedule, current)
                if window is None:
                    continue
                session_id = self.journal.ensure_session(
                    collection_id, collection, self.config_hash, window, generation
                )
                slot = slot_number(window, current, collection.frequency_seconds)
                if self.journal.poll_exists(session_id, instrument_id, slot):
                    continue
                self._poll(collection_id, instrument_id, session_id, slot, current, window.end)
        finished_at = self.clock().astimezone(UTC)
        reports.extend(self._flush(finished_at))
        self.journal.set_meta("heartbeat", finished_at.isoformat())
        return reports

    def _poll(
        self,
        collection_id: str,
        instrument_id: str,
        session_id: str,
        slot: int,
        attempted_at: datetime,
        session_end: datetime,
    ) -> None:
        collection = self.config.collections[collection_id]
        instrument = self.config.instruments[instrument_id]
        poll_id = digest([session_id, instrument_id, slot])
        attempts: list[dict[str, Any]] = []
        records: list[dict[str, Any]] = []
        selected_source: str | None = None
        selected_fallback = False
        for priority, source_id in enumerate([collection.primary, *collection.secondary]):
            source = self.config.sources[source_id]
            for attempt_number in range(source.retries + 1):
                if self.stop.is_set() or self.clock().astimezone(UTC) >= session_end:
                    break
                attempt: dict[str, Any] = {
                    "source": source_id,
                    "attempt": attempt_number + 1,
                    "status": "provider_error",
                    "input_rows": 0,
                    "rejected": 0,
                    "warning_count": 0,
                    "quarantine": [],
                }
                attempts.append(attempt)
                try:
                    response = self.provider.fetch(source_id, source, instrument)
                except ProviderFailure as exc:
                    attempt["status"] = exc.code
                    continue
                except Exception:
                    attempt["status"] = "provider_error"
                    continue
                received_at = self.clock().astimezone(UTC)
                attempt["input_rows"] = len(response.rows)
                attempt["warning_count"] = response.warning_count
                codes = response.metadata.get("gateway_error_codes", [])
                attempt["gateway_error_codes"] = sorted(
                    {code for code in codes if isinstance(code, int) and not isinstance(code, bool)}
                )
                raw_hash = self.journal.archive(
                    {
                        "request_id": uuid.uuid4().hex,
                        "poll_id": poll_id,
                        "session_id": session_id,
                        "source": source_id,
                        "instrument_id": instrument_id,
                        "requested_at": attempted_at.isoformat(),
                        "received_at": received_at.isoformat(),
                        "provider_metadata": response.metadata,
                        "rows": response.rows,
                        "warning_count": response.warning_count,
                    }
                )
                attempt["raw_hash"] = raw_hash
                pending_revisions: dict[tuple[str, str], str] = {}
                for index, row in enumerate(response.rows):
                    record, reasons = normalize(
                        row,
                        source_id=source_id,
                        source=source,
                        instrument_id=instrument_id,
                        instrument=instrument,
                        collection_id=collection_id,
                        collection=collection,
                        session_id=session_id,
                        poll_id=poll_id,
                        received_at=received_at,
                        raw_hash=raw_hash,
                    )
                    if received_at >= session_end:
                        record = None
                        reasons.append("received_after_session_close")
                    if record and record["revision_basis"] == "provider":
                        identity = (record["observation_key"], record["source_revision"])
                        values_hash = record["content_hash"]
                        if self.journal.revision_conflict(record) or (
                            identity in pending_revisions
                            and pending_revisions[identity] != values_hash
                        ):
                            record = None
                            reasons.append("source_revision_conflict")
                        else:
                            pending_revisions[identity] = values_hash
                    if record:
                        records.append(record)
                    else:
                        attempt["rejected"] += 1
                        attempt["quarantine"].append({"row": index, "reasons": reasons})
                if records:
                    attempt["status"] = "accepted"
                    selected_source = source_id
                    selected_fallback = priority > 0
                    break
                attempt["status"] = "quality_rejected" if response.rows else "no_data"
                if 10197 in attempt["gateway_error_codes"]:
                    attempt["status"] = "competing_session"
            if records:
                break
        if not attempts:
            return
        self.journal.commit_poll(
            poll_id=poll_id,
            session_id=session_id,
            instrument_id=instrument_id,
            slot=slot,
            attempted_at=attempted_at,
            source_id=selected_source,
            fallback=selected_fallback,
            attempts=attempts,
            records=records,
        )

    def run(self, max_seconds: float | None = None) -> None:
        deadline = time.monotonic() + max_seconds if max_seconds is not None else None
        timer = threading.Timer(max_seconds, self.stop.set) if max_seconds is not None else None
        if timer:
            timer.daemon = True
            timer.start()
        self.emit(
            {
                "event": "collector_started",
                "config_hash": self.config_hash,
                "collections": len(self.config.collections),
            }
        )
        try:
            while not self.stop.is_set():
                if deadline is not None and time.monotonic() >= deadline:
                    break
                self.step()
                delay = self.config.heartbeat_seconds
                if deadline is not None:
                    delay = min(delay, max(0, deadline - time.monotonic()))
                self.stop.wait(delay)
        finally:
            if timer:
                timer.cancel()
            self.shutdown("bounded_run" if deadline is not None else "shutdown")

    def shutdown(self, reason: str = "shutdown") -> None:
        if self._closed:
            return
        self._closed = True
        self.provider.close()
        now = self.clock().astimezone(UTC)
        for session in self.journal.running_sessions():
            completed = datetime.fromisoformat(session["end"]) <= now
            self.journal.finish_session(
                session["id"],
                "completed" if completed else "interrupted",
                "session_closed" if completed else reason,
            )
        self._flush(now)

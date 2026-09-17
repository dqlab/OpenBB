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

from openbb_collector_core.incremental import heartbeat

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
            heartbeat(
                self.config,
                self.journal,
                "live",
                self.clock(),
                {
                    name: "open" if active_window(collection.schedule, self.clock()) else "closed"
                    for name, collection in self.config.collections.items()
                },
            )
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
        self.journal.set_meta("heartbeat", self.clock().astimezone(UTC).isoformat())
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
        reports = self._retry_due(generation)
        reports.extend(self._flush(self.clock().astimezone(UTC)))
        last_flush = self.clock().astimezone(UTC)
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
                if self.journal.has_pending_retry(session_id, instrument_id):
                    continue
                self._poll(collection_id, instrument_id, session_id, slot, current, window.end)
                # Long universes must publish and heartbeat while a pass is in progress.
                polled_at = self.clock().astimezone(UTC)
                if (polled_at - last_flush).total_seconds() >= self.config.heartbeat_seconds:
                    reports.extend(self._flush(polled_at))
                    last_flush = self.clock().astimezone(UTC)
        finished_at = self.clock().astimezone(UTC)
        reports.extend(self._flush(finished_at))
        return reports

    def _retry_due(self, generation: int) -> list[dict[str, Any]]:
        reports = []
        for state in self.journal.due_retries(self.clock().astimezone(UTC), self.config_hash):
            if self.stop.is_set():
                break
            now = self.clock().astimezone(UTC)
            collection = self.config.collections.get(state["collection_id"])
            window = active_window(collection.schedule, now) if collection else None
            if (
                state["config_hash"] != self.config_hash
                or window is None
                or datetime.fromisoformat(state["end"]) <= now
                or collection.quote_retry is None
            ):
                self.journal.finish_retry(state)
            else:
                session_id = self.journal.ensure_session(
                    state["collection_id"], collection, self.config_hash, window, generation
                )
                if session_id != state["session_id"]:
                    self.journal.finish_retry(state)
                else:
                    self._poll(
                        state["collection_id"],
                        state["instrument_id"],
                        session_id,
                        state["slot"],
                        now,
                        window.end,
                    )
            reports.extend(self._flush(self.clock().astimezone(UTC)))
        return reports

    @staticmethod
    def _retryable(attempts: list[dict[str, Any]]) -> bool:
        reasons_allowed = {
            "missing_last_price",
            "invalid_last_price",
            "missing_bid",
            "missing_ask",
            "missing_bid_size",
            "missing_ask_size",
            "freshness_unknown",
        }
        for attempt in attempts:
            if set(attempt.get("gateway_error_codes", [])) & {200, 354, 10186, 10197}:
                return False
            if attempt["status"] in {"no_data", "timeout", "provider_timeout"}:
                continue
            reasons = {reason for row in attempt["quarantine"] for reason in row["reasons"]}
            if (
                attempt["status"] != "quality_rejected"
                or not reasons
                or not reasons <= reasons_allowed
            ):
                return False
        return bool(attempts)

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
        pending = self.journal.retry_state(poll_id)
        attempts: list[dict[str, Any]] = json.loads(pending["attempts"]) if pending else []
        previous_count = len(attempts)
        rounds = pending["rounds"] + 1 if pending else 1
        first_attempted_at = (
            datetime.fromisoformat(pending["first_attempted_at"]) if pending else attempted_at
        )
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
                if collection.quote_retry is not None:
                    attempt["retry_round"] = rounds
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
        if len(attempts) == previous_count:
            return
        policy = collection.quote_retry
        if not records and policy and rounds < policy.max_attempts:
            now = self.clock().astimezone(UTC)
            window = active_window(collection.schedule, now)
            if window and self._retryable(attempts[previous_count:]):
                due = max(
                    now + timedelta(seconds=policy.delay_seconds),
                    window.start + timedelta(seconds=policy.opening_delay_seconds),
                )
                if due < session_end:
                    self.journal.defer_retry(
                        poll_id=poll_id,
                        session_id=session_id,
                        collection_id=collection_id,
                        instrument_id=instrument_id,
                        slot=slot,
                        first_attempted_at=first_attempted_at,
                        next_attempt_at=due,
                        rounds=rounds,
                        attempts=attempts,
                    )
                    self.emit(
                        {
                            "event": "quote_retry_scheduled",
                            "instrument_id": instrument_id,
                            "round": rounds,
                            "next_attempt_at": due.isoformat(),
                        }
                    )
                    return
        self.journal.commit_poll(
            poll_id=poll_id,
            session_id=session_id,
            instrument_id=instrument_id,
            slot=slot,
            attempted_at=first_attempted_at,
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

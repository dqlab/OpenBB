"""Date-exclusive deterministic chunks and explicit calendar coverage."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from zoneinfo import ZoneInfo

from openbb_collector_core.providers import request_limits

from .config import BAR_SECONDS, Calendar, Collection, CollectorConfig, Instrument, Schedule


@dataclass(frozen=True)
class Chunk:
    collection_id: str
    instrument_id: str
    start: date
    end: date

    def payload(self) -> dict[str, str]:
        return {
            "collection_id": self.collection_id,
            "instrument_id": self.instrument_id,
            "start": self.start.isoformat(),
            "end": self.end.isoformat(),
        }


def chunks(config: CollectorConfig, as_of: date) -> Iterator[Chunk]:
    for collection_id, collection in config.collections.items():
        end = min(collection.end_date or as_of, as_of)
        sources = [config.sources[s] for s in [collection.primary, *collection.secondary]]
        days = min(collection.chunk_days, *(s.max_chunk_days for s in sources))
        limits = [request_limits(s.provider, s.model, collection.frequency) for s in sources]
        days = min(days, *(limit[0] for limit in limits))
        # Budget the provider's overlap as well as the requested day span.
        capacity = min(
            source.max_rows * BAR_SECONDS[collection.frequency] // 86400 - limit[1]
            for source, limit in zip(sources, limits, strict=True)
        )
        if capacity < 1:
            raise ValueError("max_rows is too small for a full day plus provider overlap")
        days = min(days, capacity)
        for instrument_id in collection.instrument_ids:
            start = collection.start_date
            while start < end:
                stop = min(start + timedelta(days=days), end)
                yield Chunk(collection_id, instrument_id, start, stop)
                start = stop


def local_boundary(day: date, wall_time: time, tz: ZoneInfo) -> datetime | None:
    value = datetime.combine(day, wall_time, tzinfo=tz)
    back = value.astimezone(UTC).astimezone(tz)
    if back.replace(tzinfo=None) != value.replace(tzinfo=None):
        return None
    # Repeated scheduler times run once, using the first occurrence.
    return value.astimezone(UTC)


def reset_period(schedule: Schedule, now: datetime) -> str:
    local = now.astimezone(ZoneInfo(schedule.timezone))
    if schedule.reset == "weekly":
        year, week, _ = local.isocalendar()
        return f"{year}-W{week:02d}"
    if schedule.reset == "monthly":
        return local.strftime("%Y-%m")
    return "never"


def due_days(schedule: Schedule, now: datetime) -> list[date]:
    if now.tzinfo is None:
        raise ValueError("Scheduler requires an aware clock")
    tz = ZoneInfo(schedule.timezone)
    today = now.astimezone(tz).date()
    result = []
    for offset in reversed(range(schedule.catch_up_days)):
        day = today - timedelta(days=offset)
        if day.weekday() not in schedule.weekdays or day in schedule.holidays:
            continue
        due = local_boundary(day, schedule.at, tz)
        if due and due <= now:
            result.append(day)
    return result


def expected_times(
    collection: Collection,
    instrument: Instrument,
    chunk: Chunk,
) -> set[str] | None:
    calendar: Calendar | None = collection.calendar
    if calendar is None:
        return None
    tz = ZoneInfo(instrument.timezone)
    expected: set[str] = set()
    day = chunk.start
    while day < chunk.end:
        if day in calendar.overrides:
            hours = calendar.overrides[day]
        elif day.weekday() in calendar.weekdays and day not in calendar.holidays:
            hours = calendar
        else:
            hours = None
        if hours is not None:
            if collection.frequency == "1d":
                expected.add(day.isoformat())
            else:
                end_day = day + timedelta(days=1) if hours.end <= hours.start else day
                start = local_boundary(day, hours.start, tz)
                end = local_boundary(end_day, hours.end, tz)
                if start is None or end is None:
                    raise ValueError("Coverage session has a nonexistent local time")
                while start < end:
                    expected.add(start.isoformat())
                    start += timedelta(seconds=BAR_SECONDS[collection.frequency])
        day += timedelta(days=1)
    return expected

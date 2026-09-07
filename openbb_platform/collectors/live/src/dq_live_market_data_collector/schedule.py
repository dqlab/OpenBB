"""Explicit local session calendars and polling slots; no inferred exchange holidays."""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

from .config import ResetConfig, Schedule


@dataclass(frozen=True)
class Window:
    day: date
    start: datetime
    end: datetime


def window_on(schedule: Schedule, day: date) -> Window | None:
    if day in schedule.overrides:
        hours = schedule.overrides[day]
        if hours is None:
            return None
    elif day in schedule.holidays or day.weekday() not in schedule.weekdays:
        return None
    else:
        hours = schedule
    tz = ZoneInfo(schedule.timezone)
    end_day = day + timedelta(days=1) if hours.end <= hours.start else day
    # For a repeated hour, include both folds. Skip nonexistent boundary times.
    start = datetime.combine(day, hours.start, tzinfo=tz).replace(fold=0)
    end = datetime.combine(end_day, hours.end, tzinfo=tz).replace(fold=1)
    for boundary in (start, end):
        if boundary.astimezone(UTC).astimezone(tz).replace(tzinfo=None) != boundary.replace(
            tzinfo=None
        ):
            return None
    return Window(day, start.astimezone(UTC), end.astimezone(UTC))


def active_window(schedule: Schedule, now: datetime) -> Window | None:
    if now.tzinfo is None:
        raise ValueError("Scheduler requires a timezone-aware clock")
    day = now.astimezone(ZoneInfo(schedule.timezone)).date()
    for candidate in (day - timedelta(days=1), day):
        window = window_on(schedule, candidate)
        if window and window.start <= now < window.end:
            return window
    return None


def slot_number(window: Window, now: datetime, frequency: float) -> int:
    return max(0, math.floor((now - window.start).total_seconds() / frequency))


def expected_slots(start: datetime, end: datetime, frequency: float) -> int:
    return max(0, math.ceil((end - start).total_seconds() / frequency))


def reset_period(config: ResetConfig, now: datetime) -> str:
    local = now.astimezone(ZoneInfo(config.timezone))
    if config.period == "weekly":
        year, week, _ = local.isocalendar()
        return f"{year}-W{week:02d}"
    if config.period == "monthly":
        return local.strftime("%Y-%m")
    return "never"

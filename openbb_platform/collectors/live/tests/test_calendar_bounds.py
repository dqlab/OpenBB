from datetime import date, datetime

import pytest
from pydantic import ValidationError

from dq_live_market_data_collector.config import Schedule
from dq_live_market_data_collector.schedule import active_window, window_on


def test_bounds_use_overnight_start_day_and_close_is_exclusive():
    schedule = Schedule(timezone="America/Chicago", start="17:00", end="16:00",
                        weekdays=[6,0,1,2,3], valid_from=date(2026,9,13),
                        valid_through=date(2026,9,17))
    assert window_on(schedule, date(2026,9,6)) is None
    assert active_window(schedule, datetime.fromisoformat("2026-09-18T15:59:59-05:00"))
    assert active_window(schedule, datetime.fromisoformat("2026-09-18T16:00:00-05:00")) is None
    assert window_on(schedule, date(2026,9,20)) is None
    schedule.overrides[date(2026,9,20)] = schedule
    assert window_on(schedule, date(2026,9,20)) is None


@pytest.mark.parametrize("bounds", [
    {"valid_from":"2026-09-13"}, {"valid_through":"2026-09-17"},
    {"valid_from":"2026-09-17","valid_through":"2026-09-13"},
])
def test_incomplete_or_reversed_bounds_fail(bounds):
    with pytest.raises(ValidationError):
        Schedule(start="09:30",end="16:00",**bounds)

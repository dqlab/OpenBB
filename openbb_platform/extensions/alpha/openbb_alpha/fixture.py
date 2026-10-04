"""Seeded no-corporate-action fixture, never claimed to be an exchange calendar."""

import math
import random
from datetime import UTC, date, datetime, time, timedelta

from openbb_core.provider.standard_models.alpha_research import DatasetInput


def synthetic(instruments=10, sessions=320, seed=1729):
    rng = random.Random(seed)
    calendar = []
    day = date(2023, 1, 2)
    while len(calendar) < sessions:
        if day.weekday() < 5:
            calendar.append(
                {
                    "session": day,
                    "open_time": datetime.combine(day, time(9, 30), UTC),
                    "decision_time": datetime.combine(day, time(16), UTC),
                }
            )
        day += timedelta(days=1)
    names = tuple(f"SYN:{i:03d}" for i in range(instruments))
    rows = []
    for i, instrument in enumerate(names):
        price = 50 + i * 3
        for slot, session in enumerate(calendar):
            open_price = price * math.exp(rng.gauss(0, 0.002))
            price = open_price * math.exp(0.0001 * (i - 4) + rng.gauss(0, 0.012))
            rows.append(
                {
                    "instrument_id": instrument,
                    "session": session["session"],
                    "available_at": session["decision_time"],
                    "eligible": True,
                    "price": price,
                    "raw_close": price,
                    "raw_share_volume": 0 if slot % 43 == 0 else rng.randint(100, 10000),
                    "open": open_price,
                }
            )
    return DatasetInput(
        instruments=names,
        calendar=tuple(calendar),
        rows=tuple(rows),
        calendar_name="synthetic_weekdays_not_exchange_calendar",
        timezone="UTC",
        currency="USD",
        price_convention="synthetic_no_actions",
        open_convention="synthetic_no_actions",
        availability_policy="observed",
        quality_flags=("synthetic", "not_investment_evidence"),
        source={
            "vendor": "synthetic",
            "access_adapter": "in_memory_fixture",
            "source_dataset": "seeded_no_actions_v1",
            "snapshot": f"seed:{seed}",
        },
    )

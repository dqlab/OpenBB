from datetime import UTC, datetime

import pytest

from openbb_collector_core.baostock_daily_plan import daily_plan, shanghai_date


def evidence(operation, params, fields, rows):
    return dict(
        version="baostock-source-evidence-v1",
        operation=operation,
        params=params,
        status="complete",
        reason="exhausted",
        error_code="0",
        cleanup="ok",
        pages=[dict(error_code="0", fields=fields, rows=rows)],
    )


def inputs():
    day = "2026-09-16"
    calendar = evidence(
        "BaostockTradeDates",
        dict(start_date=day, end_date=day),
        ["calendar_date", "is_trading_day"],
        [[day, "1"]],
    )
    stock = evidence(
        "BaostockStockBasic",
        {},
        ["code", "code_name", "ipoDate", "outDate", "type", "status"],
        [["sh.510300", "ETF", "", "", "5", "1"]],
    )
    config = dict(
        security_type="5",
        status="1",
        start_date=day,
        end_date=day,
        window_days=1,
        fields="date,code,close",
        adjustflags=["3", "2", "1"],
        generation="daily",
    )
    return config, day, calendar, stock


def test_single_day_and_all_adjustments():
    config, day, calendar, stock = inputs()
    plan = daily_plan(config, day, calendar, stock)["plan"]
    params = [t["requests"][0]["params"] for t in plan["tasks"]]
    assert [p["adjustflag"] for p in params] == ["3", "2", "1"]
    assert all(p["start_date"] == p["end_date"] == day for p in params)


def test_closed_and_unknown_calendar_fail_closed():
    config, day, calendar, stock = inputs()
    calendar["pages"][0]["rows"][0][1] = "0"
    assert daily_plan(config, day, calendar, None) == {"status": "closed", "date": day}
    calendar["pages"][0]["rows"][0][1] = "?"
    with pytest.raises(ValueError, match="unknown_trading_day"):
        daily_plan(config, day, calendar, stock)
    calendar["status"] = "partial"
    with pytest.raises(ValueError, match="incomplete_sdk_evidence"):
        daily_plan(config, day, calendar, stock)


def test_shanghai_date_and_naive_rejected():
    assert shanghai_date(datetime(2026, 9, 15, 16, tzinfo=UTC)) == "2026-09-16"
    with pytest.raises(ValueError, match="naive_datetime"):
        shanghai_date(datetime(2026, 9, 16))


def test_market_selection_is_configurable():
    config, day, calendar, stock = inputs()
    stock["pages"][0]["rows"].append(["sz.159001", "ETF", "", "", "5", "1"])
    config["markets"] = ["sz"]
    tasks = daily_plan(config, day, calendar, stock)["plan"]["tasks"]
    assert len(tasks) == 3
    assert all(task["requests"][0]["params"]["code"] == "sz.159001" for task in tasks)

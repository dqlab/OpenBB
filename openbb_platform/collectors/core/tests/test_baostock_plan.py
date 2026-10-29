import pytest

from openbb_collector_core.baostock_plan import make_plan


def inputs():
    config = dict(
        security_type="5",
        status="1",
        start_date="2024-01-01",
        end_date="2024-01-04",
        window_days=3,
        fields="date,code,close",
        adjustflags=["3", "2", "1"],
        generation="g1",
    )
    snapshot = dict(
        operation="query_stock_basic",
        params={},
        error_code="0",
        fields=["code", "code_name", "ipoDate", "outDate", "type", "status"],
        rows=[
            ["sh.510300", "ETF", "", "", "5", "1"],
            ["sh.600000", "stock", "", "", "1", "1"],
            ["sz.159001", "old", "", "", "5", "0"],
        ],
    )
    return config, snapshot


def test_selection_ranges_flags_and_stock_parameterization():
    config, snapshot = inputs()
    tasks = make_plan(config, snapshot)["tasks"]
    assert len(tasks) == 6
    params = [t["requests"][0]["params"] for t in tasks]
    assert [(p["start_date"], p["end_date"], p["adjustflag"]) for p in params] == [
        ("2024-01-01", "2024-01-03", f) for f in ("3", "2", "1")
    ] + [("2024-01-04", "2024-01-04", f) for f in ("3", "2", "1")]
    config["security_type"] = "1"
    assert all(
        t["requests"][0]["params"]["code"] == "sh.600000"
        for t in make_plan(config, snapshot)["tasks"]
    )


def test_reject_failed_and_duplicate_snapshot():
    config, snapshot = inputs()
    snapshot["error_code"] = "1"
    with pytest.raises(ValueError):
        make_plan(config, snapshot)
    snapshot["error_code"] = "0"
    snapshot["rows"].append(snapshot["rows"][0])
    with pytest.raises(ValueError):
        make_plan(config, snapshot)

"""Exercise the orchestration CLI with spawned, network-trapped SDK processes."""

import json
import os
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path

from test_baostock_campaign import BOOTSTRAP
from test_baostock_daily_plan import inputs

from openbb_collector_core.baostock_daily_plan import shanghai_date

EXTENSION = """
if os.environ.get("CAMPAIGN_OFFLINE") == "1":
    old_resolve = helpers.resolve_method
    def reference_resolve(method):
        if method not in ("query_trade_dates", "query_stock_basic"):
            return old_resolve(method)
        def query(**params):
            log("query", method=method, params=params)
            if method == "query_trade_dates" and os.environ.get("DAY_MARKER"):
                Path(os.environ["DAY_MARKER"]).touch()
            result = Result()
            if os.environ.get("REFERENCE_FAIL") == method:
                result.error_code = "503"
            if method == "query_trade_dates":
                result.fields = ["calendar_date", "is_trading_day"]
                result.data = [[params["start_date"], "1"]]
            else:
                result.fields = ["code", "code_name", "ipoDate", "outDate", "type", "status"]
                result.data = [["sh.600000", "stock", "", "", "1", "1"]]
            return result
        return query
    helpers.resolve_method = reference_resolve
"""


def setup(tmp_path, budget=5, history=30):
    import openbb_baostock

    import openbb_collector_core

    day = shanghai_date()
    config, _, _, _ = inputs()
    config.update(
        start_date=day, end_date=day, security_type="1", fields="date,code,close", adjustflags=["3"]
    )
    (tmp_path / "config").write_text(json.dumps(config))
    boot = tmp_path / "bootstrap"
    boot.mkdir()
    (boot / "sitecustomize.py").write_text(BOOTSTRAP + EXTENSION)
    calls = tmp_path / "calls"
    calls.touch()
    env = dict(
        os.environ,
        OPENBB_AUTO_BUILD="false",
        PYTHONDONTWRITEBYTECODE="1",
        CAMPAIGN_OFFLINE="1",
        CALLS=str(calls),
        SCENARIO="success",
        OPENBB_CAMPAIGN_PACE_SECONDS="0",
        PYTHONPATH=os.pathsep.join(
            [
                str(boot),
                str(Path(openbb_baostock.__file__).parents[1]),
                str(Path(openbb_collector_core.__file__).parents[1]),
            ]
        ),
    )
    cmd = [
        sys.executable,
        "-m",
        "openbb_collector_core.baostock_daily_orchestrator",
        "--config",
        str(tmp_path / "config"),
        "--work",
        str(tmp_path / "work"),
        "--root",
        str(tmp_path / "root"),
        "--reference-budget",
        str(budget),
        "--history-daily-budget",
        str(history),
        "--request-budget",
        "10",
        "--seconds",
        "60",
        "--timeout",
        "15",
        "--backoff-seconds",
        "0",
    ]
    return cmd, env, calls, day


def invoke(cmd, env):
    p = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=90)
    assert p.returncode in (0, 1), p.stdout + p.stderr
    return json.loads(p.stdout)


def test_success_replay_and_failure_retry(tmp_path):
    cmd, env, calls, day = setup(tmp_path)
    env["REFERENCE_FAIL"] = "query_stock_basic"
    assert invoke(cmd, env)["status"] == "partial"
    refs = tmp_path / "work" / "references" / day
    assert (refs / "calendar.json").exists()
    assert len(list(refs.glob("attempt-*.json"))) == 2
    assert any("invalid_evidence" in p.read_text() for p in refs.glob("attempt-*.json"))
    env.pop("REFERENCE_FAIL")
    assert invoke(cmd, env)["status"] == "complete"
    before = calls.read_text()
    assert invoke(cmd, env)["status"] == "complete"
    assert calls.read_text() == before
    events = [json.loads(line) for line in before.splitlines()]
    assert sum(e.get("method") == "query_trade_dates" for e in events) == 1
    assert sum(e.get("method") == "query_stock_basic" for e in events) == 2


def test_reference_and_history_budgets(tmp_path):
    cmd, env, calls, _ = setup(tmp_path, budget=1)
    result = invoke(cmd, env)
    assert result["reason"] == "reference_budget_exhausted"
    before = calls.read_text()
    assert invoke(cmd, env)["reason"] == "reference_budget_exhausted"
    assert calls.read_text() == before


def test_wrong_day_refuses_without_queries(tmp_path):
    cmd, env, calls, day = setup(tmp_path)
    tomorrow = (date.fromisoformat(day) + timedelta(days=1)).isoformat()
    p = subprocess.run(
        cmd + ["--date", tomorrow], env=env, capture_output=True, text=True, timeout=30
    )
    assert (
        p.returncode == 2 and json.loads(p.stdout)["error"] == "date_must_be_current_shanghai_day"
    )
    assert calls.read_text() == ""


def test_terminal_history_failure_is_reported(tmp_path):
    cmd, env, _, _ = setup(tmp_path)
    config = json.loads((tmp_path / "config").read_text())
    config["adjustflags"] = ["3", "2"]
    (tmp_path / "config").write_text(json.dumps(config))
    env["SCENARIO"] = "flag2"
    result = invoke(cmd + ["--max-attempts", "1"], env)
    assert result["status"] == "failed"
    assert result["failures"]


def test_first_success_and_history_budget(tmp_path):
    cmd, env, calls, _ = setup(tmp_path, history=1)
    result = invoke(cmd, env)
    assert result["status"] == "partial"
    assert result["campaign"]["daily_budget_exhausted"]
    before = calls.read_text()
    assert invoke(cmd, env)["status"] == "partial"
    assert calls.read_text() == before


def test_date_change_stops_before_next_reference(tmp_path):
    cmd, env, calls, day = setup(tmp_path)
    marker = tmp_path / "changed"
    env["DAY_MARKER"] = str(marker)
    tomorrow = (date.fromisoformat(day) + timedelta(days=1)).isoformat()
    runner = (
        "import os; from pathlib import Path; "
        "from openbb_collector_core import baostock_daily_orchestrator as o; "
        f"o.shanghai_date=lambda: {tomorrow!r} if "
        f"Path(os.environ['DAY_MARKER']).exists() else {day!r}; "
        "raise SystemExit(o.main())"
    )
    changed = [cmd[0], "-c", runner] + cmd[3:]
    assert invoke(changed, env)["reason"] == "date_changed"
    events = [json.loads(line) for line in calls.read_text().splitlines()]
    assert sum(e.get("method") == "query_trade_dates" for e in events) == 1
    assert not any(e.get("method") == "query_stock_basic" for e in events)
    env.pop("DAY_MARKER")
    assert invoke(cmd, env)["status"] == "complete"
    before = calls.read_text()
    assert invoke(cmd, env)["status"] == "complete"
    assert calls.read_text() == before


def test_first_run_success_and_zero_query_replay(tmp_path):
    cmd, env, calls, _ = setup(tmp_path)
    assert invoke(cmd, env)["status"] == "complete"
    before = calls.read_text()
    assert invoke(cmd, env)["status"] == "complete"
    assert calls.read_text() == before

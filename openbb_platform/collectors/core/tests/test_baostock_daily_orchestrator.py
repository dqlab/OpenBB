"""Exercise the orchestration CLI with spawned, network-trapped SDK processes."""

import json
import os
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path

import pytest
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
                if os.environ.get("TEST_ETF"):
                    result.data = [["sh.510300", "ETF", "", "", "5", "1"]]
                else:
                    result.data = [["sh.600000", "stock", "", "", "1", "1"]]
            return result
        return query
    helpers.resolve_method = reference_resolve
"""


def setup(tmp_path, budget=30, history=30):
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


def test_reference_budget_counts_each_admission_and_survives_restart(tmp_path):
    cmd, env, calls, day = setup(tmp_path, budget=1)
    assert invoke(cmd, env)["reason"] == "reference_budget_exhausted"
    events = [json.loads(line) for line in calls.read_text().splitlines()]
    assert [e["event"] for e in events if e["event"] in ("login", "query", "logout")] == ["login"]
    records = list((tmp_path / "work" / "references" / day).glob("call-*.json"))
    assert len(records) == 1
    assert json.loads(records[0].read_text())["status"] == "unknown"
    before = calls.read_text()
    assert invoke(cmd, env)["reason"] == "reference_budget_exhausted"
    assert calls.read_text() == before


def test_parent_kill_keeps_source_lock_until_child_exits(tmp_path):
    import time

    cmd, env, calls, _ = setup(tmp_path)
    boot = tmp_path / "bootstrap/sitecustomize.py"
    boot.write_text(
        boot.read_text()
        + """\nif os.environ.get("CAMPAIGN_OFFLINE") == "1":
    previous = helpers.resolve_method
    def delayed(m):
        q = previous(m)
        def f(**p):
            if m == "query_trade_dates":
                log("reference_started"); time.sleep(3)
            return q(**p)
        return f
    helpers.resolve_method = delayed
"""
    )
    proc = subprocess.Popen(cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        for _ in range(200):
            if "reference_started" in calls.read_text():
                break
            time.sleep(0.05)
        else:
            raise AssertionError("reference worker did not start")
        proc.kill()
        proc.wait()
        retry = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=10)
        assert retry.returncode == 2
        assert len([x for x in calls.read_text().splitlines() if "reference_started" in x]) == 1
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait()


def test_reference_login_crossing_day_does_not_admit_query(tmp_path):
    cmd, env, calls, day = setup(tmp_path)
    marker = tmp_path / "midnight"
    env["MIDNIGHT_MARKER"] = str(marker)
    boot = tmp_path / "bootstrap/sitecustomize.py"
    boot.write_text(
        boot.read_text()
        + """\nif os.environ.get("MIDNIGHT_MARKER"):
    from openbb_collector_core import baostock_daily_orchestrator as orch
    real_day = orch.shanghai_date
    orch.shanghai_date = lambda: ("2099-01-01" if Path(os.environ["MIDNIGHT_MARKER"]).exists()
                                 else real_day())
    old_login = helpers.bs.login
    def login():
        result = old_login()
        Path(os.environ["MIDNIGHT_MARKER"]).touch()
        time.sleep(.2)
        return result
    helpers.bs.login = login
"""
    )
    cmd = [
        cmd[0],
        "-c",
        "from openbb_collector_core.baostock_daily_orchestrator import main; "
        "raise SystemExit(main())",
    ] + cmd[3:]
    assert invoke(cmd, env)["reason"] == "date_changed"
    events = [json.loads(line) for line in calls.read_text().splitlines()]
    assert not any(e.get("method") == "query_trade_dates" for e in events)


def test_visible_pointer_requires_successful_restart_sync(tmp_path):
    cmd, env, calls, _ = setup(tmp_path)
    boot = tmp_path / "bootstrap/sitecustomize.py"
    boot.write_text(
        boot.read_text()
        + """\nif os.environ.get("FAIL_POINTER_SYNC"):
    from openbb_collector_core import baostock_daily as d
    original = d.atomic_json
    def injected(path, value):
        if path.name == "calendar.json" and "references" in str(path):
            sync = d.campaign.raw._sync
            def fail(directory): raise OSError("injected fsync failure")
            d.campaign.raw._sync = fail
            try: return original(path, value)
            finally: d.campaign.raw._sync = sync
        return original(path, value)
    d.atomic_json = injected
if os.environ.get("FAIL_RESTART_SYNC"):
    from openbb_collector_core import baostock_campaign as c
    original_sync = c.raw._sync
    def fail_restart(directory):
        if Path(directory).name == os.environ["FAIL_RESTART_SYNC"]:
            raise OSError("injected restart fsync failure")
        return original_sync(directory)
    c.raw._sync = fail_restart
"""
    )
    env["FAIL_POINTER_SYNC"] = "1"
    assert invoke(cmd, env)["status"] == "partial"
    before = calls.read_text()
    env.pop("FAIL_POINTER_SYNC")
    env["FAIL_RESTART_SYNC"] = shanghai_date()
    failed = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=30)
    assert failed.returncode == 2
    assert calls.read_text() == before
    env.pop("FAIL_RESTART_SYNC")
    assert invoke(cmd, env)["status"] == "complete"
    events = [json.loads(line) for line in calls.read_text().splitlines()]
    assert sum(e.get("method") == "query_trade_dates" for e in events) == 1


def test_page_calls_consume_budget(tmp_path):
    cmd, env, calls, day = setup(tmp_path, budget=3)
    boot = tmp_path / "bootstrap/sitecustomize.py"
    boot.write_text(
        boot.read_text()
        + """\nif os.environ.get("CAMPAIGN_OFFLINE") == "1":
    previous = helpers.resolve_method
    helpers.BAOSTOCK_PER_PAGE_COUNT = 1
    def paged(m):
        query = previous(m)
        def f(**p):
            result = query(**p)
            old_next = result.next
            def next_page():
                log("page"); return old_next()
            result.next = next_page
            return result
        return f
    helpers.resolve_method = paged
"""
    )
    assert invoke(cmd, env)["reason"] == "reference_budget_exhausted"
    records = [
        json.loads(p.read_text())
        for p in (tmp_path / "work" / "references" / day).glob("call-*.json")
    ]
    assert {r["kind"] for r in records} == {"login", "query", "page"}
    assert sum(json.loads(line)["event"] == "page" for line in calls.read_text().splitlines()) == 1


def test_crash_after_pointer_preserves_evidence_and_call_budget(tmp_path):
    cmd, env, calls, day = setup(tmp_path)
    boot = tmp_path / "bootstrap/sitecustomize.py"
    boot.write_text(boot.read_text() + '''\nif os.environ.get("CRASH_POINTER"):
    from openbb_collector_core import baostock_daily as d
    original = d.atomic_json
    def crash(path, value):
        original(path, value)
        if path.name == "calendar.json" and "references" in str(path):
            os._exit(77)
    d.atomic_json = crash
''')
    env["CRASH_POINTER"] = "1"
    assert subprocess.run(cmd, env=env, capture_output=True, timeout=30).returncode == 77
    refs = tmp_path / "work" / "references" / day
    before = len(list(refs.glob("call-*.json")))
    assert before == 3
    env.pop("CRASH_POINTER")
    assert invoke(cmd, env)["status"] == "complete"
    assert len(list(refs.glob("call-*.json"))) == before + 3
    events = [json.loads(line) for line in calls.read_text().splitlines()]
    assert sum(e.get("method") == "query_trade_dates" for e in events) == 1


def test_legacy_failure_with_unknown_usage_is_not_reset(tmp_path):
    cmd, env, calls, day = setup(tmp_path)
    env["REFERENCE_FAIL"] = "query_trade_dates"
    assert invoke(cmd, env)["status"] == "partial"
    directory = tmp_path / "work" / "references" / day
    for attempt in directory.glob("attempt-*.json"):
        record = json.loads(attempt.read_text())
        record.pop("call_journal")
        attempt.write_text(json.dumps(record))
    before = calls.read_text()
    env.pop("REFERENCE_FAIL")
    assert invoke(cmd, env)["reason"] == "legacy_reference_calls_unknown"
    assert calls.read_text() == before


def test_call_reservation_sync_failure_cannot_restore_budget(tmp_path):
    cmd, env, calls, day = setup(tmp_path, budget=1)
    boot = tmp_path / "bootstrap/sitecustomize.py"
    boot.write_text(boot.read_text() + '''\nif os.environ.get("FAIL_CALL_SYNC"):
    from openbb_collector_core import baostock_daily as d
    original = d.atomic_json
    def injected(path, value):
        if path.name.startswith("call-"):
            sync = d.campaign.raw._sync
            def fail(directory): raise OSError("call reservation sync failed")
            d.campaign.raw._sync = fail
            try: return original(path, value)
            finally: d.campaign.raw._sync = sync
        return original(path, value)
    d.atomic_json = injected
''')
    env["FAIL_CALL_SYNC"] = "1"
    failed = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=30)
    assert failed.returncode == 2
    assert len(list((tmp_path / "work" / "references" / day).glob("call-*.json"))) == 1
    env.pop("FAIL_CALL_SYNC")
    assert invoke(cmd, env)["reason"] == "reference_budget_exhausted"
    assert not any(json.loads(line)["event"] == "login" for line in calls.read_text().splitlines())


def test_full_fields_and_adjustflags_with_call_budget(tmp_path):
    cmd, env, calls, _ = setup(tmp_path, history=100)
    config_path = tmp_path / "config"
    config = json.loads(config_path.read_text())
    config["fields"] = (
        "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,"
        "tradestatus,pctChg,peTTM,pbMRQ,psTTM,pcfNcfTTM,isST"
    )
    config["adjustflags"] = ["3", "2", "1"]
    config_path.write_text(json.dumps(config))
    assert invoke(cmd, env)["status"] == "complete"
    queries = [json.loads(line) for line in calls.read_text().splitlines()]
    queries = [e for e in queries if e.get("method") == "query_history_k_data_plus"]
    assert {e["params"]["adjustflag"] for e in queries} == {"1", "2", "3"}
    assert all(e["params"]["fields"] == config["fields"] for e in queries)
    assert len(queries) == 3


@pytest.mark.parametrize("session_pages", ["0", "1"])
@pytest.mark.parametrize("policy", ["legacy", "conservative"])
def test_history_worker_retains_campaign_lock_after_parent_death(tmp_path, session_pages, policy):
    """An orphaned SDK query must exclude every recovery until it exits."""
    import signal
    import time

    cmd, env, calls, _ = setup(tmp_path, history=100)
    env["SCENARIO"] = "delay"
    env["TEST_ETF"] = "1"
    env["OPENBB_CAMPAIGN_SESSION_PAGES"] = session_pages
    config = json.loads((tmp_path / "config").read_text())
    config["unknown_attempt_policy"] = policy
    config["security_type"] = "5"
    if session_pages == "1":
        config["adjustflags"] = ["3", "2"]
    (tmp_path / "config").write_text(json.dumps(config))
    boot = tmp_path / "bootstrap/sitecustomize.py"
    boot.write_text(boot.read_text().replace("time.sleep(15)", "time.sleep(60)"))
    process = subprocess.Popen(cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                               start_new_session=True)
    worker = None
    try:
        for _ in range(400):
            events = [json.loads(line) for line in calls.read_text().splitlines()]
            queries = [event for event in events
                       if event.get("method") == "query_history_k_data_plus"]
            if queries:
                worker = queries[0]["pid"]
                break
            time.sleep(0.05)
        assert worker is not None
        os.kill(process.pid, signal.SIGKILL)
        process.wait(timeout=5)
        os.kill(worker, 0)
        for _ in range(2):
            retry = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=20)
            assert retry.returncode == 2
            assert json.loads(retry.stdout)["error"] == "acquisition_busy"
            events = [json.loads(line) for line in calls.read_text().splitlines()]
            assert len([e for e in events if e.get("method") == "query_history_k_data_plus"]) == 1
        os.kill(worker, signal.SIGKILL)
        worker = None
        env["SCENARIO"] = "success"
        for _ in range(4):
            result = invoke(cmd, env)
            if (result["status"] == "complete"
                    or result.get("reason") == "unknown_attempt_manual_review"):
                break
        if policy == "conservative":
            assert result["status"] == "partial"
            assert result["reason"] == "unknown_attempt_manual_review"
            queries = [json.loads(line) for line in calls.read_text().splitlines()]
            queries = [e for e in queries if e.get("method") == "query_history_k_data_plus"]
            assert len(queries) == (2 if session_pages == "1" else 1)
            assert result["campaign"]["unknown_attempts"] == 1
        else:
            assert result["status"] == "complete"
        before = calls.read_text()
        replay = invoke(cmd, env)
        assert replay["status"] == ("partial" if policy == "conservative" else "complete")
        assert calls.read_text() == before
    finally:
        if worker is not None:
            try:
                os.kill(worker, signal.SIGKILL)
            except ProcessLookupError:
                pass
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait()


def test_conservative_reference_reservation_is_unknown(tmp_path):
    cmd, env, calls, day = setup(tmp_path)
    config_path = tmp_path / "config"
    config = json.loads(config_path.read_text())
    config["unknown_attempt_policy"] = "conservative"
    config_path.write_text(json.dumps(config))
    # A durable reservation can precede process start; no source dispatch is proven.
    refs = tmp_path / "work" / "references" / day
    refs.mkdir(parents=True)
    (refs / "attempt-reserved.json").write_text(json.dumps({
        "operation": "BaostockTradeDates", "params": {},
        "status": "reserved", "call_journal": 1,
    }))
    result = invoke(cmd, env)
    assert result["reason"] == "unknown_attempt_manual_review"
    # Even a visible pointer cannot turn the unterminated reservation into success.
    (refs / "calendar.json").write_text("{}")
    assert invoke(cmd, env)["reason"] == "unknown_attempt_manual_review"
    assert calls.read_text() == ""
    assert (refs / "attempt-reserved.json").exists()


def test_worker_lock_descriptor_excludes_nested_owners_until_last_close(tmp_path):
    from openbb_collector_core import raw_acquisition as raw

    with raw._ownership(tmp_path, retain_for_workers=True) as fd:
        duplicate = os.dup(fd)
        with pytest.raises(raw.RawAcquisitionError, match="acquisition_busy"):
            with raw._ownership(tmp_path, retain_for_workers=True):
                pass
    try:
        with pytest.raises(raw.RawAcquisitionError, match="acquisition_busy"):
            with raw._ownership(tmp_path, retain_for_workers=True):
                pass
    finally:
        os.close(duplicate)
    with raw._ownership(tmp_path, retain_for_workers=True):
        pass

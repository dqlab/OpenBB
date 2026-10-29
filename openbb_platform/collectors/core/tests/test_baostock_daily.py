"""Exercise the actual launcher and spawned campaign offline."""

import json
import os
import subprocess
import sys
import time

import pytest
from pathlib import Path

from test_baostock_campaign import BOOTSTRAP
from test_baostock_daily_plan import inputs

from openbb_collector_core.baostock_daily_plan import shanghai_date


def test_launcher_real_process_resume(tmp_path):
    import openbb_baostock

    import openbb_collector_core

    day = shanghai_date()
    config, _, calendar, stock = inputs()
    config.update(start_date=day, end_date=day, adjustflags=["3", "2"])
    calendar["params"] = dict(start_date=day, end_date=day)
    calendar["pages"][0]["rows"] = [[day, "1"]]
    for name, value in [("config", config), ("calendar", calendar), ("universe", stock)]:
        (tmp_path / name).write_text(json.dumps(value))
    bootstrap = tmp_path / "bootstrap"
    bootstrap.mkdir()
    (bootstrap / "sitecustomize.py").write_text(BOOTSTRAP)
    calls = tmp_path / "calls"
    calls.touch()
    env = dict(
        os.environ,
        OPENBB_AUTO_BUILD="false",
        PYTHONDONTWRITEBYTECODE="1",
        CAMPAIGN_OFFLINE="1",
        CALLS=str(calls),
        OPENBB_CAMPAIGN_PACE_SECONDS="0",
        SCENARIO="fail_first",
        PYTHONPATH=os.pathsep.join(
            [
                str(bootstrap),
                str(Path(openbb_baostock.__file__).parents[1]),
                str(Path(openbb_collector_core.__file__).parents[1]),
            ]
        ),
    )
    command = [sys.executable, "-m", "openbb_collector_core.baostock_daily"]
    for name in ("config", "calendar", "universe", "work", "root"):
        command += ["--" + name, str(tmp_path / name)]
    command += ["--request-budget", "10", "--seconds", "60", "--backoff-seconds", "0"]
    first = subprocess.run(command, env=env, capture_output=True, text=True, timeout=90)
    assert first.returncode == 0, first.stdout + first.stderr
    assert json.loads(first.stdout)["status"] == "complete"
    before = calls.read_text()
    queries = [
        json.loads(line) for line in before.splitlines() if json.loads(line)["event"] == "query"
    ]
    assert [q["params"]["adjustflag"] for q in queries].count("3") == 1
    assert [q["params"]["adjustflag"] for q in queries].count("2") == 2
    second = subprocess.run(command, env=env, capture_output=True, text=True, timeout=90)
    assert second.returncode == 0, second.stdout + second.stderr
    assert json.loads(second.stdout)["status"] == "complete"
    assert calls.read_text() == before


def test_source_timeout_keeps_existing_evidence(tmp_path):
    import openbb_baostock

    import openbb_collector_core

    bootstrap = tmp_path / "bootstrap"
    bootstrap.mkdir()
    (bootstrap / "sitecustomize.py").write_text(BOOTSTRAP)
    calls = tmp_path / "calls"
    calls.touch()
    existing = tmp_path / "source" / shanghai_date() / "existing.json"
    existing.parent.mkdir(parents=True)
    existing.write_text('{"previous":true}')
    env = dict(
        os.environ,
        OPENBB_AUTO_BUILD="false",
        PYTHONDONTWRITEBYTECODE="1",
        CAMPAIGN_OFFLINE="1",
        CALLS=str(calls),
        SCENARIO="delay",
        PYTHONPATH=os.pathsep.join(
            [
                str(bootstrap),
                str(Path(openbb_baostock.__file__).parents[1]),
                str(Path(openbb_collector_core.__file__).parents[1]),
            ]
        ),
    )
    child = subprocess.run(
        [
            sys.executable,
            "-m",
            "openbb_collector_core.baostock_daily_source",
            "--output",
            str(tmp_path / "source"),
            "--timeout",
            "1",
        ],
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert child.returncode == 1, child.stdout + child.stderr
    sources = json.loads(child.stdout)["sources"]
    assert all(source["status"] == "timeout" for source in sources.values())
    assert all(
        json.loads(Path(source["path"]).read_text())["status"] == "timeout"
        for source in sources.values()
    )
    assert existing.read_text() == '{"previous":true}'


@pytest.mark.parametrize("session_pages", ["0", "1"])
@pytest.mark.parametrize("boundary", ["time", "midnight"])
def test_soft_deadline_real_process_and_resume(tmp_path, session_pages, boundary):
    import openbb_baostock
    import openbb_collector_core

    day = shanghai_date()
    config, _, calendar, stock = inputs()
    config.update(start_date=day, end_date=day,
                  fields="date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,peTTM,pbMRQ,psTTM,pcfNcfTTM,isST")
    calendar["params"] = dict(start_date=day, end_date=day)
    calendar["pages"][0]["rows"] = [[day, "1"]]
    for name, value in [("config", config), ("calendar", calendar), ("universe", stock)]:
        (tmp_path / name).write_text(json.dumps(value))
    bootstrap = tmp_path / "bootstrap"
    bootstrap.mkdir()
    (bootstrap / "sitecustomize.py").write_text(BOOTSTRAP + '''\nif os.environ.get("CAMPAIGN_OFFLINE") == "1":\n    old_resolve = helpers.resolve_method\n    def delayed_resolve(method):\n        original = old_resolve(method)\n        def query(**params):\n            result = original(**params)\n            if os.environ.get("SLOW_QUERY") == "1":\n                time.sleep(5)\n                if os.environ.get("CROSS_DAY") == "1":\n                    Path(os.environ["DAY_MARKER"]).touch()\n            return result\n        return query\n    helpers.resolve_method = delayed_resolve\n''')
    calls = tmp_path / "calls"
    calls.touch()
    env = dict(os.environ, OPENBB_AUTO_BUILD="false", PYTHONDONTWRITEBYTECODE="1",
               CAMPAIGN_OFFLINE="1", CALLS=str(calls), SLOW_QUERY="1", SCENARIO="pagination",
               OPENBB_CAMPAIGN_PACE_SECONDS="0", OPENBB_CAMPAIGN_SESSION_PAGES=session_pages,
               OPENBB_CAMPAIGN_DAILY_CALL_BUDGET="3",
               PYTHONPATH=os.pathsep.join([str(bootstrap), str(Path(openbb_baostock.__file__).parents[1]),
                                           str(Path(openbb_collector_core.__file__).parents[1])]))
    marker = tmp_path / "day-changed"
    env["DAY_MARKER"] = str(marker)
    env["CROSS_DAY"] = "1" if boundary == "midnight" else "0"
    if boundary == "midnight":
        from datetime import date, timedelta
        tomorrow = (date.fromisoformat(day) + timedelta(days=1)).isoformat()
        runner = ("import os; from pathlib import Path; "
                  "from openbb_collector_core import baostock_daily as d; "
                  f"d.shanghai_date=lambda: {tomorrow!r} if Path(os.environ['DAY_MARKER']).exists() else {day!r}; "
                  "raise SystemExit(d.main())")
        command = [sys.executable, "-c", runner]
    else:
        command = [sys.executable, "-m", "openbb_collector_core.baostock_daily"]
    for name in ("config", "calendar", "universe", "work", "root"):
        command += ["--" + name, str(tmp_path / name)]
    command += ["--request-budget", "10", "--seconds", "3" if boundary == "time" else "60", "--backoff-seconds", "0", "--max-attempts", "1"]
    first = subprocess.Popen(command, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    until = time.monotonic() + 20
    while time.monotonic() < until and '"event": "query"' not in calls.read_text():
        time.sleep(0.05)
    assert '"event": "query"' in calls.read_text()
    parallel = subprocess.run(command, env=env, capture_output=True, text=True, timeout=20)
    assert parallel.returncode == 2, parallel.stdout + parallel.stderr
    stdout, stderr = first.communicate(timeout=40)
    assert first.returncode == 1, stdout + stderr
    assert json.loads(stdout)["reason"] == ("time_budget" if boundary == "time" else "date_changed")
    events = [json.loads(line) for line in calls.read_text().splitlines()]
    assert len([e for e in events if e["event"] == "query"]) == 1
    assert not list((tmp_path / "root").rglob("cancel.json"))
    budget_file = next((tmp_path / "root").rglob("budget.json"))
    from openbb_collector_core import baostock_campaign as campaign
    assert campaign.Control().state(budget_file)["calls"] == 3
    assert not list((tmp_path / "root").rglob("terminal.json"))
    marker.unlink(missing_ok=True)
    env["CROSS_DAY"] = "0"
    env["SLOW_QUERY"] = "0"
    before_blocked = calls.read_text()
    blocked = subprocess.run(command, env=env, capture_output=True, text=True, timeout=30)
    assert blocked.returncode == 1, blocked.stdout + blocked.stderr
    assert calls.read_text() == before_blocked
    marker.unlink(missing_ok=True)
    env["CROSS_DAY"] = "0"
    env["SLOW_QUERY"] = "0"
    env["SCENARIO"] = "success"
    env["OPENBB_CAMPAIGN_DAILY_CALL_BUDGET"] = "30"
    command[command.index("--seconds") + 1] = "60"
    second = subprocess.run(command, env=env, capture_output=True, text=True, timeout=90)
    assert second.returncode == 0, second.stdout + second.stderr
    assert json.loads(second.stdout)["status"] == "complete"
    before = calls.read_text()
    third = subprocess.run(command, env=env, capture_output=True, text=True, timeout=90)
    assert third.returncode == 0, third.stdout + third.stderr
    assert calls.read_text() == before


def test_closed_correction_is_explicit_and_retains_original(tmp_path, monkeypatch):
    from argparse import Namespace
    from openbb_collector_core import baostock_daily as daily

    day = shanghai_date()
    config, _, calendar, stock = inputs()
    config.update(start_date=day, end_date=day)
    calendar["params"] = dict(start_date=day, end_date=day)
    calendar["pages"][0]["rows"] = [[day, "0"]]
    for name, value in [("config", config), ("calendar", calendar), ("universe", stock)]:
        (tmp_path / name).write_text(json.dumps(value))
    args = Namespace(**{name: tmp_path / name for name in ("config", "calendar", "universe", "work", "root")},
                     max_attempts=3, request_budget=10, backoff_seconds=0, seconds=1,
                     correct_closed=False, reason=None, date=None)
    assert daily.launch(args)["status"] == "closed"
    daydir = args.work / "days" / day
    original = (daydir / "identity.json").read_bytes()
    retained_calendar = (daydir / "calendar.json").read_bytes()
    calendar["pages"][0]["rows"] = [[day, "1"]]
    args.calendar.write_text(json.dumps(calendar))
    import pytest
    with pytest.raises(ValueError, match="daily_identity_conflict"):
        daily.launch(args)
    args.correct_closed, args.reason = True, "corrected exchange calendar"
    from openbb_collector_core import baostock_campaign as campaign
    existing = args.root / campaign.VERSION / campaign.digest("daily:" + day)
    existing.mkdir(parents=True)
    with pytest.raises(ValueError, match="daily_correction_existing_campaign"):
        daily.launch(args)
    existing.rmdir()
    monkeypatch.setattr(daily.campaign, "run", lambda *a, **k: dict(status="partial", daily_budget_exhausted=True, source_calls=0))
    assert daily.launch(args)["status"] == "partial"
    assert (daydir / "identity.json").read_bytes() == original
    assert (daydir / "calendar.json").read_bytes() == retained_calendar
    audit = json.loads((daydir / "closed-correction" / "audit.json").read_text())
    assert audit["original_identity"]["status"] == "closed"
    assert audit["corrected_identity"]["status"] == "planned"
    assert audit["reason"] == args.reason
    args.correct_closed, args.reason = False, None
    assert daily.launch(args)["status"] == "partial"


def test_budget_migrates_without_refunding_and_uses_shanghai(tmp_path, monkeypatch):
    from openbb_collector_core import baostock_campaign as campaign
    from datetime import datetime, timezone

    class FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 10, 30, 16, 30, tzinfo=timezone.utc).astimezone(tz)

    monkeypatch.setattr(campaign, "datetime", FrozenDateTime)
    io = campaign.Control()
    path = campaign.budget_path(tmp_path)
    io.write(path, {"day": "2026-10-30", "calls": 7})
    assert campaign.day_string() == "2026-10-31"
    assert campaign.budget_state(io, tmp_path) == {"day": "2026-10-31", "calls": 7}
    assert campaign.budget_state(io, tmp_path) == {"day": "2026-10-31", "calls": 7}


def test_correction_refuses_changed_capture_root(tmp_path, monkeypatch):
    # Root binding is installed with a new daily identity, before source work.
    from argparse import Namespace
    from openbb_collector_core import baostock_daily as daily

    day = shanghai_date()
    config, _, calendar, stock = inputs()
    config.update(start_date=day, end_date=day)
    calendar["params"] = dict(start_date=day, end_date=day)
    calendar["pages"][0]["rows"] = [[day, "1"]]
    for name, value in [("config", config), ("calendar", calendar), ("universe", stock)]:
        (tmp_path / name).write_text(json.dumps(value))
    args = Namespace(**{name: tmp_path / name for name in ("config", "calendar", "universe", "work", "root")},
                     max_attempts=3, request_budget=10, backoff_seconds=0, seconds=1,
                     correct_closed=False, reason=None, date=None)
    monkeypatch.setattr(daily.campaign, "run", lambda *a, **k: dict(status="partial", daily_budget_exhausted=True, source_calls=0))
    assert daily.launch(args)["status"] == "partial"
    args.root = tmp_path / "different-root"
    with pytest.raises(ValueError, match="daily_capture_root_conflict"):
        daily.launch(args)


def test_historical_correction_requires_explicit_date_and_authorization(tmp_path, monkeypatch):
    from argparse import Namespace
    from datetime import date, timedelta
    from openbb_collector_core import baostock_daily as daily

    day = shanghai_date()
    tomorrow = (date.fromisoformat(day) + timedelta(days=1)).isoformat()
    config, _, calendar, stock = inputs()
    config.update(start_date=day, end_date=tomorrow)
    calendar["params"] = dict(start_date=day, end_date=day)
    calendar["pages"][0]["rows"] = [[day, "0"]]
    for name, value in [("config", config), ("calendar", calendar), ("universe", stock)]:
        (tmp_path / name).write_text(json.dumps(value))
    args = Namespace(**{name: tmp_path / name for name in ("config", "calendar", "universe", "work", "root")},
                     max_attempts=3, request_budget=10, backoff_seconds=0, seconds=1,
                     correct_closed=False, reason=None, date=None)
    monkeypatch.setattr(daily, "shanghai_date", lambda: day)
    assert daily.launch(args)["status"] == "closed"
    monkeypatch.setattr(daily, "shanghai_date", lambda: tomorrow)
    args.date = day
    with pytest.raises(ValueError, match="historical_correction_requires_authorization"):
        daily.launch(args)
    calendar["pages"][0]["rows"] = [[day, "1"]]
    args.calendar.write_text(json.dumps(calendar))
    args.correct_closed, args.reason = True, "revised calendar"
    monkeypatch.setattr(daily.campaign, "run", lambda *a, **k: dict(status="partial", daily_budget_exhausted=True, source_calls=0))
    assert daily.launch(args)["status"] == "partial"

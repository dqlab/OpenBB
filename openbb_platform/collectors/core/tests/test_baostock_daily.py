"""Exercise the actual launcher and spawned campaign offline."""

import json
import os
import subprocess
import sys
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

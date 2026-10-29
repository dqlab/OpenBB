"""Daily raw behavior with synthetic provider responses, no network."""
import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from threading import Event
from zoneinfo import ZoneInfo

import pytest

from openbb_collector_core.daily_raw import acquire, make_task, report, schedule
from openbb_collector_core.models import ProviderFailure, Response


class Client:
    def __init__(self, fail=False):
        self.stop = Event()
        self.calls = 0
        self.fail = fail

    def fetch(self, identity, source, params):
        self.calls += 1
        if self.fail:
            raise ProviderFailure("synthetic_failure")
        return Response([{"date": params["date"], "extra": "", "price": "1.000"}])


def config(tmp_path):
    calendar = tmp_path / "calendar.json"
    calendar.write_text(json.dumps({"coverage_start": "2024-01-01", "coverage_end": "2024-12-31",
                                    "trading_dates": ["2024-01-02"]}))
    return {"root": str(tmp_path / "raw"), "source": {"provider": "synthetic", "model": "Daily"},
            "params": {}, "date_path": ["date"], "schedule": {
                "timezone": "Asia/Shanghai", "time": "16:00", "calendar": str(calendar),
                "max_attempts": 2}}


def test_capture_replay_and_report(tmp_path):
    cfg = config(tmp_path)
    client = Client()
    first = acquire(cfg, "2024-01-02", client=client)
    assert first["status"] == "captured"
    assert json.loads(first["responses"][0]["body"])["rows"][0]["extra"] == ""
    acquire(cfg, "2024-01-02", client=client)
    assert client.calls == 1
    assert report(cfg, "2024-01-02", 1)["row_counts"] == [1]
    assert make_task(cfg, "2024-01-03")["task_id"] != first["task"]["task_id"]


def test_scheduler_failure_retry_and_bound(tmp_path):
    cfg = config(tmp_path)
    client = Client(fail=True)
    now = datetime(2024, 1, 2, 17, tzinfo=ZoneInfo("Asia/Shanghai"))
    assert schedule(cfg, now, client=client)["status"] == "failed"
    assert schedule(cfg, now, client=client)["status"] == "failed"
    assert schedule(cfg, now, client=client)["status"] == "exhausted"
    assert client.calls == 2
    assert report(cfg, "2024-01-02", 1)["status"] == "failed"
    assert report(cfg, "2024-01-02", 2)["status"] == "failed"


def test_scheduler_success_retry_and_calendar(tmp_path):
    cfg = config(tmp_path)
    client = Client(fail=True)
    now = datetime(2024, 1, 2, 17, tzinfo=ZoneInfo("Asia/Shanghai"))
    assert schedule(cfg, now, client=client)["status"] == "failed"
    client.fail = False
    assert schedule(cfg, now, client=client)["status"] == "captured"
    assert schedule(cfg, now, client=client)["status"] == "captured"
    assert client.calls == 2
    assert schedule(cfg, now.replace(hour=15), client=client)["status"] == "not_due"
    with pytest.raises(ValueError, match="coverage"):
        schedule(cfg, now.replace(year=2025), client=client)


def test_subprocess_cli_with_synthetic_source(tmp_path):
    cfg = config(tmp_path)
    cfg_path = tmp_path / "config.json"
    cfg_path.write_text(json.dumps(cfg))
    # sitecustomize is loaded in the CLI and its spawned ProcessClient worker.
    (tmp_path / "sitecustomize.py").write_text('''\
import json, os
from openbb_collector_core import process
from openbb_collector_core.models import Response, ProviderFailure
class Synthetic:
    def __init__(self, source): pass
    def fetch(self, params):
        with open(os.environ["SYNTH_CALLS"], "a") as f: f.write("call\\n")
        if os.environ.get("SYNTH_FAIL") == "1": raise ProviderFailure("synthetic")
        return Response([{"date": params["date"], "unknown_column": "kept"}])
    def close(self): pass
process.ProviderDispatcher = Synthetic
''')
    env = os.environ.copy()
    env["PYTHONPATH"] = os.pathsep.join([str(tmp_path), str(Path(__file__).parents[1] / "src"),
                                        env.get("PYTHONPATH", "")])
    env["SYNTH_CALLS"] = str(tmp_path / "calls")
    cmd = [sys.executable, "-m", "openbb_collector_core.daily_raw", "acquire", "--config",
           str(cfg_path), "--date", "2024-01-02"]
    for _ in range(2):
        proc = subprocess.run(cmd, env=env, capture_output=True, text=True, check=True)
        assert json.loads(proc.stdout)["status"] == "captured"
    assert (tmp_path / "calls").read_text().splitlines() == ["call"]
    assert json.loads(acquire(cfg, "2024-01-02", client=Client())["responses"][0]["body"])[
        "rows"][0]["unknown_column"] == "kept"
    env["SYNTH_FAIL"] = "1"
    failed = subprocess.run(cmd + ["--attempt", "2"], env=env, capture_output=True, text=True)
    assert failed.returncode == 1 and json.loads(failed.stdout)["status"] == "failed"
    subprocess.run(cmd + ["--attempt", "2"], env=env, capture_output=True, check=False)
    assert len((tmp_path / "calls").read_text().splitlines()) == 2


def test_source_completeness_is_reported_after_archiving(tmp_path):
    cfg = config(tmp_path)
    client = Client()
    client.fetch = lambda *args: Response([{"evidence": {
        "status": "partial", "reason": "source_error", "pages": [
            {"fields": ["date", "extra"], "rows": [["2024-01-02", ""]]}]}}])
    assert acquire(cfg, "2024-01-02", client=client)["status"] == "captured"
    item = report(cfg, "2024-01-02", 1)["source_evidence"][0]
    assert item == {"status": "partial", "reason": "source_error", "page_count": 1,
                    "source_row_count": 1}


def test_cli_registered_baostock_source_offline(tmp_path):
    pytest.importorskip("openbb_baostock")
    cfg = config(tmp_path)
    cfg.update(source={"provider": "baostock", "model": "BaostockDailyHistoryKETFSource"},
               params={"literal": {}}, date_path=["literal", "date"])
    cfg_path = tmp_path / "config.json"
    cfg_path.write_text(json.dumps(cfg))
    (tmp_path / "sitecustomize.py").write_text('''\
from openbb_baostock.utils import helpers
class Result:
    error_code = "0"
    fields = ["date", "code", "extra", "extra"]
    data = [["2024-01-02", "synthetic", "", "1.000"]]
helpers.bs.login = lambda: Result()
helpers.bs.logout = lambda: Result()
def resolve(method):
    assert method == "query_daily_history_k_ETF"
    def query(**params):
        assert params == {"date": "2024-01-02"}
        return Result()
    return query
helpers.resolve_method = resolve
''')
    env = os.environ.copy()
    env["PYTHONPATH"] = os.pathsep.join([str(tmp_path), str(Path(__file__).parents[1] / "src"),
                                        env.get("PYTHONPATH", "")])
    cmd = [sys.executable, "-m", "openbb_collector_core.daily_raw", "acquire", "--config",
           str(cfg_path), "--date", "2024-01-02"]
    result = subprocess.run(cmd, env=env, capture_output=True, text=True, check=True)
    assert json.loads(result.stdout)["status"] == "captured"
    from openbb_collector_core.raw_acquisition import read_capture
    task = make_task(cfg, "2024-01-02")
    state = read_capture(cfg["root"], task["task_id"], task["generation"])
    evidence = json.loads(state["responses"][0]["body"])["rows"][0]["evidence"]
    assert evidence["status"] == "complete"
    assert evidence["pages"][0]["fields"] == ["date", "code", "extra", "extra"]
    assert evidence["pages"][0]["rows"] == [["2024-01-02", "synthetic", "", "1.000"]]

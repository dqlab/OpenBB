"""Offline tests of the NAS runner adapter, with explicitly mocked subprocesses."""

import json
from pathlib import Path
from subprocess import CompletedProcess

import pytest

from dq_historical_market_data_collector.cffex import scheduled

CLEAN = {
    "command": "backfill",
    "status": "partial",
    "deferred_days": 3,
    "failed_days": 0,
    "unprocessed_days": 0,
    "errors": [],
}


@pytest.mark.parametrize(
    "change",
    [
        {"failed_days": 1},
        {"unprocessed_days": 1},
        {"errors": [{"code": "provider_error"}]},
        {"deferred_days": 0},
        {"deferred_days": True},
        {"command": "update"},
        {"failed_days": False},
        {"status": "failed"},
    ],
)
def test_partial_failure_is_never_scheduler_success(change):
    assert not scheduled.deferred_success(3, {**CLEAN, **change})


def test_only_clean_deferred_backfill_is_scheduler_success():
    assert scheduled.deferred_success(3, CLEAN)
    assert not scheduled.deferred_success(1, CLEAN)


@pytest.mark.parametrize("code", [1, 2, 4, 5])
def test_calendar_failure_aborts_without_collecting(monkeypatch, capsys, code):
    calls = []

    def invoke(command, *_):
        calls.append(command)
        return code, {"status": "error", "code": "test_failure"}

    monkeypatch.setattr(scheduled, "invoke", invoke)
    assert scheduled.main(["update", "--config", "fixture.yaml"]) == code
    assert calls == ["calendar-refresh"]
    assert json.loads(capsys.readouterr().out)["exit_code"] == code


def test_clean_backfill_defers_without_hiding_work(monkeypatch, capsys):
    results = iter([(0, {"status": "complete"}), (3, CLEAN)])
    monkeypatch.setattr(scheduled, "invoke", lambda *_: next(results))
    assert scheduled.main(["backfill", "--config", "fixture.yaml"]) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["status"] == "deferred"
    assert report["collection"] == CLEAN
    assert report["collector_exit_code"] == 3


@pytest.mark.parametrize("code", [1, 3, 4, 5])
def test_real_collection_failures_preserve_nonzero(monkeypatch, capsys, code):
    results = iter([(0, {"status": "complete"}), (code, {**CLEAN, "failed_days": 1})])
    monkeypatch.setattr(scheduled, "invoke", lambda *_: next(results))
    assert scheduled.main(["backfill", "--config", "fixture.yaml"]) == code
    assert json.loads(capsys.readouterr().out)["status"] == "failed"


@pytest.mark.parametrize(
    "stdout",
    ["unexpected logs", "[]", '{"status":"partial"}', '{"status":"complete","command":"wrong"}'],
)
def test_bad_child_contract_is_rejected(monkeypatch, stdout):
    monkeypatch.setattr(
        scheduled.subprocess,
        "run",
        lambda *_, **__: CompletedProcess([], 0, stdout, ""),
    )
    with pytest.raises(scheduled.AdapterError):
        scheduled.invoke("update", Path("fixture.yaml"), [])


def test_invocation_reuses_interpreter_and_process_group(monkeypatch):
    captured = {}

    def run(args, **kwargs):
        captured.update(args=args, kwargs=kwargs)
        return CompletedProcess(args, 5, '{"status":"error","code":"writer_locked"}', "")

    monkeypatch.setattr(scheduled.subprocess, "run", run)
    assert scheduled.invoke("backfill", Path("fixture.yaml"), ["--max-days", "100"])[0] == 5
    assert captured["args"][0] == scheduled.sys.executable
    assert "start_new_session" not in captured["kwargs"]
    assert captured["args"][-2:] == ["--max-days", "100"]


def test_until_complete_releases_lock_between_batches(monkeypatch, capsys):
    results = iter(
        [
            (0, {"status": "complete"}),
            (3, CLEAN),
            (0, {"status": "complete"}),
            (0, {"command": "backfill", "status": "complete", "deferred_days": 0}),
        ]
    )
    pauses = []
    monkeypatch.setattr(scheduled, "invoke", lambda *_: next(results))
    monkeypatch.setattr(scheduled.time, "sleep", pauses.append)
    assert (
        scheduled.main(
            ["backfill", "--config", "fixture.yaml", "--until-complete", "--pause-seconds", "30"]
        )
        == 0
    )
    lines = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert [line["status"] for line in lines] == ["deferred", "complete"]
    assert pauses == [30]


def test_until_complete_does_not_loop_over_failures(monkeypatch, capsys):
    results = iter([(0, {"status": "complete"}), (3, {**CLEAN, "failed_days": 1})])
    monkeypatch.setattr(scheduled, "invoke", lambda *_: next(results))
    monkeypatch.setattr(scheduled.time, "sleep", lambda *_: pytest.fail("failure must propagate"))
    assert scheduled.main(["backfill", "--config", "fixture.yaml", "--until-complete"]) == 3
    assert json.loads(capsys.readouterr().out)["status"] == "failed"

"""Offline BaoStock raw collector; source fixtures are not vendor observations."""

import copy
import json
from threading import Event

import pytest

from openbb_collector_core.models import ProviderFailure, Response
from openbb_collector_core.raw_acquisition import read_capture


def task(model="BaostockHistoryKDataPlus", generation="g1"):
    return dict(
        version="raw-acquisition-v1",
        task_id="job:history",
        generation=generation,
        source=dict(provider="baostock", model=model),
        requests=[
            dict(
                request_id=mode,
                params=dict(
                    code="sh.600000",
                    start_date="2024-01-01",
                    end_date="2024-01-03",
                    fields="date,code,close",
                    frequency="d",
                    adjustflag=flag,
                ),
            )
            for mode, flag in [("unadjusted", "3"), ("forward", "2")]
        ],
    )


class OfflineClient:
    def __init__(self, fail=None):
        self.stop = Event()
        self.calls = []
        self.fail = fail

    def close(self):
        pass

    def fetch(self, identity, source, params):
        self.calls.append(copy.deepcopy(params))
        if params["adjustflag"] == self.fail:
            raise ProviderFailure("offline_failure")
        return Response([dict(code="WRONG BUSINESS ID", close="not-a-price", source_clock="")])


@pytest.mark.parametrize("central", ["missing", "locked", "inaccessible"])
def test_raw_entrypoint_never_imports_or_opens_central(tmp_path, monkeypatch, central):
    import builtins

    from openbb_collector_core.baostock_acquisition import acquire

    original = builtins.__import__

    def guard(name, *args, **kwargs):
        if name.startswith(("dq_quant", "duckdb", "sqlite3")):
            pytest.fail("central import in collector: " + name)
        return original(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", guard)
    client = OfflineClient()
    result = acquire(tmp_path / "private", [task()], client=client)
    assert result[0]["status"] == "captured" and len(client.calls) == 2
    body = json.loads(result[0]["responses"][0]["body"])
    assert body["rows"][0]["close"] == "not-a-price"


def test_successful_first_adjustment_survives_second_failure(tmp_path):
    from openbb_collector_core.baostock_acquisition import acquire

    client = OfflineClient(fail="2")
    result = acquire(tmp_path, [task()], client=client)[0]
    assert result["status"] == "partial"
    assert result["responses"][0]["body"] is not None
    assert result["responses"][1]["receipt"]["status"] == "failed"
    acquire(tmp_path, [task()], client=client)
    assert len(client.calls) == 2


def test_exact_replay_and_fresh_generation(tmp_path):
    from openbb_collector_core.baostock_acquisition import acquire

    client = OfflineClient()
    acquire(tmp_path, [task()], client=client)
    acquire(tmp_path, [task()], client=client)
    assert len(client.calls) == 2
    acquire(tmp_path, [task(generation="g2")], client=client)
    assert len(client.calls) == 4


def test_whole_bundle_preflight_before_first_provider_call(tmp_path):
    from openbb_collector_core.baostock_acquisition import acquire

    client = OfflineClient()
    wrong = task("EquityHistorical")
    with pytest.raises(ValueError):
        acquire(tmp_path, [task(), wrong], client=client)
    assert not client.calls and not list(tmp_path.iterdir())


def test_crash_after_durable_first_response_replays_without_refetch(tmp_path, monkeypatch):
    from openbb_collector_core import raw_acquisition as raw
    from openbb_collector_core.baostock_acquisition import acquire

    original = raw._install

    def crash(path, content):
        if path.name == "terminal.json":
            raise OSError("offline crash before terminal")
        return original(path, content)

    client = OfflineClient()
    monkeypatch.setattr(raw, "_install", crash)
    with pytest.raises(OSError):
        acquire(tmp_path, [task()], client=client)
    assert len(read_capture(tmp_path, "job:history", "g1")["responses"]) == 2
    monkeypatch.setattr(raw, "_install", original)
    assert acquire(tmp_path, [task()], client=client)[0]["status"] == "captured"
    assert len(client.calls) == 2


def test_real_processclient_selected_native_queryexecutor_offline(tmp_path, monkeypatch):
    import multiprocessing

    from openbb_baostock.models import native

    from openbb_collector_core.baostock_acquisition import acquire
    from openbb_collector_core.process import ProcessClient

    calls_file = tmp_path / "provider_calls.jsonl"

    async def offline(method, queries):
        assert method == "query_history_k_data_plus"
        with calls_file.open("a") as stream:
            stream.write(json.dumps(queries) + "\n")
        return [
            [
                dict(
                    code="sh.600000",
                    date="2024-01-02",
                    close="1.234567890123456789",
                    publication_clock="",
                    adjustflag=q["adjustflag"],
                )
            ]
            for q in queries
        ]

    monkeypatch.setattr(native, "query_batch", offline)
    client = ProcessClient()
    client._context = multiprocessing.get_context("fork")
    try:
        captured = acquire(tmp_path / "raw", [task()], client=client)[0]
        assert captured["status"] == "captured"
        source = json.loads(captured["responses"][0]["body"])
        assert source["rows"][0]["close"] == "1.234567890123456789"
        assert source["rows"][0]["publication_clock"] == ""
        assert source["metadata"]["provider"] == "baostock"
        assert source["metadata"]["model"] == "BaostockHistoryKDataPlus"
        acquire(tmp_path / "raw", [task()], client=client)
        assert len(calls_file.read_text().splitlines()) == 2
    finally:
        client.close()


def test_actual_raw_module_entrypoint(tmp_path, monkeypatch, capsys):
    from openbb_collector_core import process
    from openbb_collector_core.baostock_acquisition import main

    plan_file = tmp_path / "plan.json"
    plan_file.write_text(json.dumps(dict(tasks=[task()])))
    client = OfflineClient()
    monkeypatch.setattr(process, "ProcessClient", lambda **kwargs: client)
    assert main(["--plan", str(plan_file), "--root", str(tmp_path / "raw")]) == 0
    assert json.loads(capsys.readouterr().out)[0]["status"] == "captured"
    assert len(client.calls) == 2

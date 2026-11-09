"""Offline operational fault injection and CLI/scheduler contracts."""

from __future__ import annotations

import json
import multiprocessing as mp
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from threading import Event

import openbb_collector_core.models as core_models
import pytest
from openbb_collector_core.models import ProviderFailure
from openbb_collector_core.process import ProcessClient

import dq_historical_market_data_collector.etf_options.config as lane_config
from dq_historical_market_data_collector.etf_options import cli, scheduled
from dq_historical_market_data_collector.etf_options.config import (
    TradingCalendar,
    load_config,
)
from dq_historical_market_data_collector.etf_options.quality import DataError, contracts, quotes
from dq_historical_market_data_collector.etf_options.runtime import Collector
from dq_historical_market_data_collector.etf_options.storage import Journal, export_csv, status

from .test_etf_options import (
    DAY,
    NOW,
    FakeClient,
    TransportResponse,
    TransportStand,
    calendar,
    config,
    contract,
    literal_lane,
    quote,
    quote_body,
    run,
    sse_master_body,
)


def test_resume_only_same_date_configuration_and_exact_metadata(tmp_path):
    cfg = config(tmp_path, block_size=1)
    metadata = [contract(), contract("10000002")]
    with Journal(cfg.root) as journal:
        result = run(cfg, journal, FakeClient(metadata, [[quote()], []]))
        assert result["status"] == "failed"
        second = FakeClient(metadata, [[quote("10000002")]])
        result = run(cfg, journal, second)
        assert result["status"] == "complete" and result["resumed_rows"] == 1
        assert second.calls[-1][1] == {"symbols": "10000002"}
        assert journal.db.execute("SELECT count(*) FROM checkpoints").fetchone()[0] == 0
        assert journal.db.execute("SELECT count(*) FROM current_quotes").fetchone()[0] == 2


def test_configuration_change_cannot_reuse_checkpoint(tmp_path):
    cfg = config(tmp_path, block_size=1)
    metadata = [contract(), contract("10000002")]
    with Journal(cfg.root) as journal:
        assert run(cfg, journal, FakeClient(metadata, [[quote()], []]))["status"] == "failed"
        changed = cfg.model_copy(update={"block_size": 2})
        second = FakeClient(metadata, [[quote(), quote("10000002")]])
        report = run(changed, journal, second)
        assert report["status"] == "complete" and report["resumed_rows"] == 0
        assert second.calls[-1][1] == {"symbols": "10000001,10000002"}


def test_fetched_at_does_not_create_revision(tmp_path):
    cfg = config(tmp_path)
    with Journal(cfg.root) as journal:
        run(cfg, journal, FakeClient(baskets=[[quote(fetched_at="2026-10-14T15:20:00+08:00")]]))
        result = run(
            cfg, journal, FakeClient(baskets=[[quote(fetched_at="2026-10-14T18:30:00+08:00")]])
        )
        assert result["changed_snapshots"] == 0
        assert journal.db.execute("SELECT count(*) FROM revisions").fetchone()[0] == 1
        captures = list(journal.db.execute("SELECT quotes_json FROM captures"))
        assert len(captures) == 2 and captures[0][0] != captures[1][0]


def test_rollback_and_original_failure_if_cleanup_fails(tmp_path, monkeypatch):
    cfg = config(tmp_path)
    with Journal(cfg.root) as journal:
        run(cfg, journal, FakeClient())
        previous = journal.db.execute("SELECT revision_id FROM current_snapshots").fetchone()[0]
        original = journal.commit_capture

        def fatal(*args, **kwargs):
            original(*args, **kwargs)
            raise OSError("synthetic primary storage failure")

        def cleanup(*args):
            raise RuntimeError("synthetic secondary cleanup failure")

        monkeypatch.setattr(journal, "commit_capture", fatal)
        monkeypatch.setattr(journal, "abort_run", cleanup)
        with pytest.raises(OSError, match="primary storage") as failure:
            run(cfg, journal, FakeClient(baskets=[[quote(last=0.18)]]))
        assert any("cleanup" in note for note in failure.value.__notes__)
        assert (
            journal.db.execute("SELECT revision_id FROM current_snapshots").fetchone()[0]
            == previous
        )
        assert journal.db.execute("SELECT count(*) FROM revisions").fetchone()[0] == 1
    with Journal(cfg.root) as journal:
        assert (
            journal.db.execute("SELECT count(*) FROM runs WHERE status='running'").fetchone()[0]
            == 0
        )
        assert (
            journal.db.execute("SELECT count(*) FROM runs WHERE status='interrupted'").fetchone()[0]
            == 1
        )


def test_cancel_during_atomic_finish_rolls_back(tmp_path, monkeypatch):
    cfg, stop = config(tmp_path), Event()
    with Journal(cfg.root) as journal:
        original = journal.finish_run

        def finish(*args):
            original(*args)
            stop.set()

        monkeypatch.setattr(journal, "finish_run", finish)
        result = run(cfg, journal, FakeClient(), stop=stop)
        assert result["status"] == "interrupted"
        assert journal.db.execute("SELECT count(*) FROM revisions").fetchone()[0] == 0
        assert journal.db.execute("SELECT count(*) FROM current_quotes").fetchone()[0] == 0
        assert journal.db.execute("SELECT status FROM runs").fetchone()[0] == "interrupted"


def test_request_budget_is_nonzero(tmp_path):
    cfg = config(tmp_path, max_requests=1)
    with Journal(cfg.root) as journal:
        report = run(cfg, journal, FakeClient())
        assert report["status"] == "failed" and report["provider_calls"] == 1
        assert "request_budget_exceeded" in str(report["errors"])


def test_empty_metadata_is_not_clean_zero_work(tmp_path):
    cfg = config(tmp_path)
    client = FakeClient()
    client.metadata = []
    with Journal(cfg.root) as journal:
        report = run(cfg, journal, client)
        assert report["status"] == "failed" and not report["response_complete"]


def test_validated_holiday_skip_and_no_provider_requests(tmp_path):
    cfg = config(tmp_path)
    closed = TradingCalendar(
        coverage_start="2026-10-13",
        coverage_end="2026-10-15",
        trading_dates=["2026-10-13", "2026-10-15"],
        source="synthetic",
    )
    client = FakeClient()
    with Journal(cfg.root) as journal:
        report = Collector(cfg, journal, closed, client, clock=lambda: NOW).collect()
        assert report["status"] == "skipped" and client.calls == []
        assert not report["response_complete"]


def test_calendar_refresh_rejects_interior_removal_and_retains_file(tmp_path):
    cfg = config(tmp_path)
    cfg.calendar_file.write_text(calendar().model_dump_json())
    previous = cfg.calendar_file.read_bytes()

    class Removed(FakeClient):
        def fetch(self, source_id, source, request):
            response = super().fetch(source_id, source, request)
            response.rows.pop(1)
            return response

    with Journal(cfg.root) as journal:
        report = Collector(cfg, journal, None, Removed(), clock=lambda: NOW).refresh_calendar()
        assert report["status"] == "failed"
        assert "calendar_sessions_removed" in str(report["errors"])
        assert cfg.calendar_file.read_bytes() == previous
        assert (
            journal.db.execute(
                "SELECT count(*) FROM attempts WHERE raw_path IS NOT NULL"
            ).fetchone()[0]
            == 1
        )


def test_calendar_empty_fails_closed(tmp_path):
    cfg = config(tmp_path)

    class Empty(FakeClient):
        def fetch(self, *args):
            response = super().fetch(*args)
            response.rows = []
            return response

    with Journal(cfg.root) as journal:
        report = Collector(cfg, journal, None, Empty(), clock=lambda: NOW).refresh_calendar()
        assert report["status"] == "failed" and not cfg.calendar_file.exists()


def test_block_max_strict_paths_and_duplicate_yaml(tmp_path):
    with pytest.raises(ValueError):
        config(tmp_path, block_size=51)
    with pytest.raises(ValueError):
        config(tmp_path, root="s3://bucket/state")
    with pytest.raises(ValueError):
        config(tmp_path, exchanges={})
    path = tmp_path / "config.yaml"
    path.write_text("calendar_file: calendar.json\nblock_size: 50\nblock_size: 5\n")
    with pytest.raises(ValueError, match="duplicate"):
        load_config(path)
    path.write_text("root: state\ncalendar_file: calendar.json\n")
    assert load_config(path).root == tmp_path / "state"


def test_source_models_are_custom_and_process_compatible(tmp_path):
    cfg = config(tmp_path)
    for model in (
        "AkshareEtfOptionContracts",
        "AkshareEtfOptionQuotes",
        "AkshareToolTradeDateHistSina",
    ):
        source = cfg.source(model)
        assert source.model_dump()["model"] == model
        assert source.provider == "akshare"
        assert source.max_response_bytes > 0 and source.timeout_seconds > 0


def test_export_and_status_protect_journal(tmp_path):
    cfg = config(tmp_path)
    with Journal(cfg.root) as journal:
        run(cfg, journal, FakeClient())
    assert status(cfg.root)["row_count"] == 1
    output = tmp_path / "chain.csv"
    assert export_csv(cfg.root, output, DAY.isoformat()) == 1
    assert "10000001" in output.read_text()
    for output in (
        cfg.root / "writer.lock",
        cfg.root / "etf_options.sqlite3-wal",
        cfg.root / "raw" / "new.json",
    ):
        with pytest.raises(ValueError, match="protected"):
            export_csv(cfg.root, output)


@pytest.mark.parametrize(
    "extra,code",
    [
        ({"expiration": "2026-11-28"}, "expiration_mismatch"),
        ({"option_type": "put"}, "option_type_mismatch"),
    ],
)
def test_extended_quote_identity(extra, code):
    with pytest.raises(DataError, match=code):
        quotes([quote(**extra)], contracts([contract()], "SSE", "sina", DAY), DAY, NOW, NOW)


def test_pricing_eligible_requires_known_unit():
    metadata = contracts([contract(contract_unit=10000)], "SSE", "sina", DAY)
    assert quotes([quote()], metadata, DAY, NOW, NOW)[0]["pricing_eligible"]


def test_scheduler_calendar_first_preserves_failures(monkeypatch, tmp_path):
    calls = []

    def invoke(command, config_path, extra):
        calls.append(command)
        return (
            (0, {"status": "complete", "command": command})
            if command == "calendar-refresh"
            else (1, {"status": "failed", "command": command, "errors": ["missing_quotes"]})
        )

    monkeypatch.setattr(scheduled, "invoke", invoke)
    code, report = scheduled.one_pass(tmp_path / "config.yaml")
    assert code == 1 and report["status"] == "failed" and calls == ["calendar-refresh", "collect"]
    calls.clear()
    monkeypatch.setattr(scheduled, "invoke", lambda *args: (4, {"status": "interrupted"}))
    assert scheduled.one_pass(tmp_path / "config.yaml")[0] == 4


def test_scheduler_invalid_child_json_is_nonzero(monkeypatch, tmp_path):
    class Child:
        returncode = 0

        def communicate(self):
            return '{"status":"failed","command":"collect"}', ""

    monkeypatch.setattr(scheduled.subprocess, "Popen", lambda *args, **kwargs: Child())
    monkeypatch.setattr(scheduled.threading, "current_thread", lambda: None)
    assert scheduled.one_pass(tmp_path / "config.yaml")[0] == 2


def hanging_worker(connection, source):
    """Synthetic worker; test the real ProcessClient timeout/termination boundary."""
    connection.recv()
    time.sleep(60)


def test_real_process_timeout_kills_worker(tmp_path, monkeypatch):
    import openbb_collector_core.process as process

    monkeypatch.setattr(process, "_worker", hanging_worker)
    client = ProcessClient()
    client._context = mp.get_context("fork")  # Offline injection only; production remains spawn.
    source = (
        config(tmp_path)
        .source("AkshareEtfOptionQuotes")
        .model_copy(update={"timeout_seconds": 0.1})
    )
    started = time.monotonic()
    try:
        with pytest.raises(ProviderFailure, match="timeout"):
            client.fetch("synthetic", source, {"symbols": "10000001"})
        assert time.monotonic() - started < 4
        assert not client._workers
    finally:
        client.close()


def test_real_sigterm_cli_nonzero_and_no_complete(tmp_path):
    """Real OS signal, offline provider substitute, real journal and CLI handler."""
    cfg = config(tmp_path)
    path = tmp_path / "config.yaml"
    settings = cfg.model_dump(mode="json")
    settings["not_before"] = cfg.not_before.strftime("%H:%M")
    settings["quote_not_before"] = cfg.quote_not_before.strftime("%H:%M")
    path.write_text(json.dumps(settings))
    cfg.calendar_file.write_text(calendar().model_dump_json())
    marker = tmp_path / "waiting"
    script = tmp_path / "sigterm_fixture.py"
    script.write_text(f"""
from pathlib import Path
from datetime import datetime
from openbb_collector_core.models import ProviderFailure, Response
from dq_historical_market_data_collector.etf_options import runtime, cli
class Client:
    def __init__(self, stop): self.stop = stop
    def fetch(self, source_id, source, request):
        if source.model == "AkshareEtfOptionContracts":
            return Response([{contract()!r}])
        Path({str(marker)!r}).write_text("waiting")
        self.stop.wait(20)
        raise ProviderFailure("cancelled")
    def close(self): pass
class Collector(runtime.Collector):
    def __init__(self, *args, **kwargs):
        kwargs["clock"] = lambda: datetime.fromisoformat({NOW.isoformat()!r})
        super().__init__(*args, **kwargs)
runtime.ProcessClient = Client
cli.Collector = Collector
raise SystemExit(cli.main(["collect", "--config", {str(path)!r}]))
""")
    environment = os.environ.copy()
    environment["PYTHONPATH"] = os.pathsep.join(
        [
            str(Path(cli.__file__).parents[2]),
            str(Path(core_models.__file__).parents[1]),
            environment.get("PYTHONPATH", ""),
        ]
    )
    child = subprocess.Popen(
        [sys.executable, str(script)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=environment,
    )
    try:
        deadline = time.monotonic() + 10
        while not marker.exists() and child.poll() is None and time.monotonic() < deadline:
            time.sleep(0.02)
        assert marker.exists()
        child.send_signal(signal.SIGTERM)
        stdout, stderr = child.communicate(timeout=10)
        assert child.returncode == 4, (stdout, stderr)
        assert json.loads(stdout)["status"] == "interrupted"
        with Journal(cfg.root) as journal:
            assert journal.db.execute("SELECT count(*) FROM current_quotes").fetchone()[0] == 0
            assert (
                journal.db.execute("SELECT count(*) FROM runs WHERE status='complete'").fetchone()[
                    0
                ]
                == 0
            )
    finally:
        if child.poll() is None:
            child.kill()
            child.wait()


def test_cli_configuration_failure_nonzero(tmp_path, capsys):
    path = tmp_path / "invalid.yaml"
    path.write_text("calendar_file: absent.json\nblock_size: 51\n")
    assert cli.main(["collect", "--config", str(path)]) == 2
    assert json.loads(capsys.readouterr().out)["status"] == "failed"


def test_run_deadline_bounds_retries(tmp_path):
    cfg = config(
        tmp_path,
        max_run_seconds=1.0,
        limits={"retries": 1, "retry_delay_seconds": 2.0, "min_interval_seconds": 0.1},
    )
    with Journal(cfg.root) as journal:
        started = time.monotonic()
        report = run(cfg, journal, FakeClient(baskets=[ProviderFailure("timeout")]))
        assert report["status"] == "failed" and report["provider_calls"] == 2
        assert "run_budget_exceeded" in str(report["errors"])
        assert time.monotonic() - started < 2.5
        assert journal.db.execute("SELECT count(*) FROM current_quotes").fetchone()[0] == 0


def test_diagnostic_chain_response_complete_not_pricing_eligible(tmp_path):
    cfg = config(tmp_path)
    with Journal(cfg.root) as journal:
        report = run(cfg, journal, FakeClient(baskets=[[quote(bid=0, ask=None, last=0)]]))
        assert report["status"] == "complete" and report["response_complete"]
        assert report["row_count"] == 1 and report["pricing_eligible_count"] == 0
        row = journal.db.execute("SELECT quote_json FROM current_quotes").fetchone()[0]
        assert json.loads(row)["bid"] == 0 and json.loads(row)["ask"] is None
        assert "zero_bid" in json.loads(row)["quality_flags"]


def test_postclose_mixed_source_times_complete_with_diagnostic_rows(tmp_path):
    cfg = config(tmp_path)
    metadata = [contract(f"1000000{index}", contract_unit=10000) for index in range(1, 4)]
    stamps = [
        "2026-10-14T14:56:00+08:00",
        "2026-10-14T00:00:00+08:00",
        "2026-10-14T15:00:00+08:00",
    ]
    basket = [
        quote(f"1000000{index}", source_time=stamp, raw_fields={"quote_time": stamp})
        for index, stamp in enumerate(stamps, start=1)
    ]
    with Journal(cfg.root) as journal:
        report = run(cfg, journal, FakeClient(metadata, [basket]))
        assert report["status"] == "complete" and report["response_complete"]
        assert report["row_count"] == 3 and report["pricing_eligible_count"] == 1
        rows = [
            json.loads(row[0])
            for row in journal.db.execute("SELECT quote_json FROM current_quotes ORDER BY symbol")
        ]
        assert [row["source_time"] for row in rows] == stamps
        assert [row["pricing_eligible"] for row in rows] == [False, False, True]
        assert {"preclose_quote", "stale_quote"}.issubset(rows[0]["quality_flags"])
        assert "unknown_quote_time" in rows[1]["quality_flags"]
        raw_path = journal.db.execute(
            "SELECT raw_path FROM attempts WHERE model='AkshareEtfOptionQuotes'"
        ).fetchone()[0]
        assert json.loads((cfg.root / raw_path).read_text())["rows"] == basket
        capture = journal.db.execute(
            "SELECT response_complete,pricing_eligible_count FROM captures"
        ).fetchone()
        assert tuple(capture) == (1, 1)
        assert run(cfg, journal, FakeClient(metadata, [basket]))["changed_snapshots"] == 0
        assert journal.db.execute("SELECT count(*) FROM revisions").fetchone()[0] == 1


def test_nonfinite_runtime_failed_raw_preserved(tmp_path):
    cfg = config(tmp_path)
    with Journal(cfg.root) as journal:
        report = run(cfg, journal, FakeClient(baskets=[[quote(bid=float("nan"))]]))
        assert report["status"] == "failed"
        assert "nonfinite_numeric" in str(report["errors"])
        attempt = journal.db.execute(
            "SELECT raw_path FROM attempts WHERE model='AkshareEtfOptionQuotes'"
        ).fetchone()[0]
        assert "NaN" in (cfg.root / attempt).read_text()


def test_full_two_exchange_chain_and_explicit_policy(tmp_path):
    cfg = config(tmp_path, exchanges={"SSE": {"source": "sina"}, "SZSE": {"source": "official"}})

    class TwoExchanges(FakeClient):
        def fetch(self, source_id, source, request):
            if source.model == "AkshareEtfOptionContracts" and request["exchange"] == "SZSE":
                self.calls.append((source.model, request))
                assert request["source"] == "official"
                return core_models.Response(
                    [
                        contract(
                            "90000001",
                            exchange="SZSE",
                            underlying="159919",
                            metadata_source="official",
                            source_date=DAY.isoformat(),
                            contract_unit=10000,
                        )
                    ]
                )
            return super().fetch(source_id, source, request)

    client = TwoExchanges(baskets=[[quote()], [quote("90000001", underlying="159919")]])
    with Journal(cfg.root) as journal:
        report = run(cfg, journal, client)
        assert report["status"] == "complete" and report["row_count"] == 2
        assert report["pricing_eligible_count"] == 1
        assert journal.db.execute("SELECT count(*) FROM current_snapshots").fetchone()[0] == 2


def test_manual_universe_change_is_audited_and_retains_history(tmp_path):
    cfg = config(tmp_path)
    with Journal(cfg.root) as journal:
        run(
            cfg,
            journal,
            FakeClient([contract(), contract("10000002")], [[quote(), quote("10000002")]]),
        )
        collector = Collector(cfg, journal, calendar(), FakeClient(), clock=lambda: NOW)
        report = collector.collect(approve_universe_change=True)
        assert report["status"] == "complete" and report["approve_universe_change"]
        assert journal.db.execute("SELECT count(*) FROM current_quotes").fetchone()[0] == 1
        assert journal.db.execute("SELECT count(*) FROM revisions").fetchone()[0] == 2
        previous = journal.db.execute(
            "SELECT max(json_array_length(quotes_json)) FROM revisions"
        ).fetchone()[0]
        assert previous == 2


def test_raw_archive_cannot_overwrite_attempt(tmp_path):
    cfg = config(tmp_path)
    with Journal(cfg.root) as journal:
        run_id = journal.start_run({"command": "collect", "status": "running"})
        attempt = journal.begin_attempt(run_id, None, "synthetic", {})
        payload = {"observed_at": NOW.isoformat(), "rows": [quote()]}
        raw = journal.archive(attempt, payload)
        before = (cfg.root / raw).read_bytes()
        with pytest.raises(FileExistsError):
            journal.archive(attempt, {**payload, "rows": []})
        assert (cfg.root / raw).read_bytes() == before
        assert (cfg.root / raw).stat().st_mode & 0o222 == 0


def echo_worker(connection, source):
    request = connection.recv()
    connection.send_bytes(
        json.dumps(
            {
                "rows": [{"model": source["model"], "request": request}],
                "metadata": {},
                "warning_count": 0,
            }
        ).encode()
    )
    connection.recv()


def test_real_worker_boundary_accepts_custom_sources(tmp_path, monkeypatch):
    import openbb_collector_core.process as process

    monkeypatch.setattr(process, "_worker", echo_worker)
    client = ProcessClient()
    client._context = mp.get_context("fork")
    try:
        for model in ("AkshareEtfOptionContracts", "AkshareEtfOptionQuotes"):
            response = client.fetch(model, config(tmp_path).source(model), {"symbols": "10000001"})
            assert response.rows == [{"model": model, "request": {"symbols": "10000001"}}]
    finally:
        client.close()


# ---------------------------------------------------------------------------
# Task 5B-4 -- offline transport guard, zero-central children, concurrent writers
#
# Every layer that can leave the host on this lane's path is trapped, each trap is
# attribute-asserted before every row, and the falsifiability matrix proves that
# disarming one entry point removes exactly that label.
# ---------------------------------------------------------------------------

import contextlib  # noqa: E402
import fcntl  # noqa: E402
import socket  # noqa: E402
import threading  # noqa: E402

import requests  # noqa: E402

TRANSPORT_TRAPS = (
    ("socket", socket, "socket", "connect"),
    ("socket", socket, "socket", "connect_ex"),
    ("socket", socket, "create_connection", None),
    ("socket", socket, "getaddrinfo", None),
    ("requests", requests.sessions, "Session", "request"),
    ("requests", requests.sessions, "Session", "send"),
    ("provider", "etf_option_quotes", "make_quote_request", None),
    ("provider", "etf_option_contracts", "make_contract_request", None),
)
TRAFFIC_LABELS = tuple(
    (
        f"transport_trap:{layer}:{owner}.make_request"
        if layer == "provider"
        else f"transport_trap:{layer}:{name if attribute is None else name + '.' + attribute}"
    )
    for layer, owner, name, attribute in TRANSPORT_TRAPS
)


def _provider_module(name):
    import importlib

    return importlib.import_module("openbb_akshare.models.etf_options")


def _trap_targets():
    """(owner, attribute, label) for the layers that actually carry a request."""
    for layer, owner, name, attribute in TRANSPORT_TRAPS:
        if layer == "provider":
            yield (
                _provider_module(owner),
                name,
                f"transport_trap:{layer}:{owner}.make_request",
            )
        elif attribute is None:
            yield owner, name, f"transport_trap:{layer}:{name}"
        else:
            yield getattr(owner, name), attribute, f"transport_trap:{layer}:{name}.{attribute}"


class TransportTrap:
    def __init__(self, label):
        self.label = label

    def __call__(self, *_args, **_kwargs):
        raise AssertionError(f"network access attempted in an offline test [{self.label}]")


def install_transport_traps():
    installed = []
    for owner, attribute, label in _trap_targets():
        if not hasattr(owner, attribute):
            raise AssertionError(f"offline_guard_target_missing:{label}")
        setattr(owner, attribute, TransportTrap(label))
        if getattr(getattr(owner, attribute), "label", None) != label:
            raise AssertionError(f"offline_guard_target_not_trapped:{label}")
        installed.append(label)
    if tuple(installed) != TRAFFIC_LABELS or len(set(installed)) != len(installed):
        raise AssertionError("offline_guard_label_conflict")
    return installed


def assert_all_traps_armed() -> list[str]:
    """Every trap target, checked by its own attribute -- not just one helper."""
    armed = []
    for owner, attribute, label in _trap_targets():
        if getattr(owner, attribute, None) is None or (
            getattr(getattr(owner, attribute), "label", None) != label
        ):
            raise AssertionError(f"offline_guard_target_not_trapped:{label}")
        armed.append(label)
    if tuple(armed) != TRAFFIC_LABELS:
        raise AssertionError("offline_guard_label_conflict")
    return armed


_ORIGINALS = {}


def disarm_transport_trap(label, originals=None):
    originals = _ORIGINALS if originals is None else originals
    for owner, attribute, candidate in _trap_targets():
        if candidate == label:
            setattr(owner, attribute, originals[label])
            return originals[label]
    raise AssertionError(f"offline_guard_label_unknown:{label}")


@pytest.fixture(autouse=True)
def transport_guard():
    for owner, attribute, label in _trap_targets():
        if hasattr(owner, attribute):
            _ORIGINALS.setdefault(label, getattr(owner, attribute))
    installed = install_transport_traps()
    assert set(installed) == set(TRAFFIC_LABELS)
    try:
        yield installed
    finally:
        for owner, attribute, label in _trap_targets():
            setattr(owner, attribute, _ORIGINALS[label])


def traffic_call_shapes():
    quotes = _provider_module("etf_option_quotes")
    contracts = _provider_module("etf_option_contracts")

    def socket_connect():
        with socket.socket() as sock:
            sock.connect(("127.0.0.1", 1))

    def socket_connect_ex():
        with socket.socket() as sock:
            sock.connect_ex(("127.0.0.1", 1))

    def session_request():
        requests.Session().request("GET", "http://127.0.0.1:1/")

    def session_send():
        prepared = requests.Request("GET", "http://127.0.0.1:1/").prepare()
        requests.Session().send(prepared)

    return {
        "transport_trap:socket:socket.connect": socket_connect,
        "transport_trap:socket:socket.connect_ex": socket_connect_ex,
        "transport_trap:socket:create_connection": lambda: socket.create_connection(
            ("127.0.0.1", 1), timeout=0.1
        ),
        "transport_trap:socket:getaddrinfo": lambda: socket.getaddrinfo("127.0.0.1", 1),
        "transport_trap:requests:Session.request": session_request,
        "transport_trap:requests:Session.send": session_send,
        "transport_trap:provider:etf_option_quotes.make_request": lambda: quotes.make_quote_request(
            "http://127.0.0.1:1/"
        ),
        "transport_trap:provider:etf_option_contracts.make_request": lambda: (
            contracts.make_contract_request("http://127.0.0.1:1/")
        ),
    }


def test_guard_every_transport_layer_is_trapped_before_a_request(transport_guard):
    shapes = traffic_call_shapes()
    assert set(shapes) == set(TRAFFIC_LABELS) == set(transport_guard)
    for label, shape in shapes.items():
        with pytest.raises(AssertionError, match=f"\\[{label}\\]"):
            shape()


@pytest.mark.parametrize("label", sorted(TRAFFIC_LABELS))
def test_guard_each_transport_trap_label_is_falsifiable(label):
    shape = traffic_call_shapes()[label]
    with pytest.raises(AssertionError, match=f"\\[{label}\\]"):
        shape()
    disarm_transport_trap(label)
    observed = None
    try:
        shape()
    except AssertionError as exc:
        text = str(exc)
        start = text.find("[transport_trap:")
        observed = text[start + 1 : text.index("]", start)] if start != -1 else None
    except OSError:
        # A disarmed socket trap really dials the loopback port and is refused.
        observed = None
    assert observed != label
    if observed is not None:
        assert observed in TRAFFIC_LABELS


def test_guard_all_traps_are_attribute_asserted(transport_guard):
    assert assert_all_traps_armed() == list(TRAFFIC_LABELS)
    for label in TRAFFIC_LABELS:
        disarm_transport_trap(label)
        with pytest.raises(AssertionError, match="offline_guard_target_not_trapped"):
            assert_all_traps_armed()
        install_transport_traps()
    assert assert_all_traps_armed() == list(TRAFFIC_LABELS)


CENTRAL_CHILD = r"""
import json, os, sys, sqlite3

if os.environ.get("OPENBB_AUTO_BUILD") != "false":
    raise SystemExit("OPENBB_AUTO_BUILD must be false before Python starts")

opened = []


def audit(event, args):
    if event == "open" and args and isinstance(args[0], (str, bytes, os.PathLike)):
        opened.append(os.path.abspath(os.fspath(args[0])))


sys.addaudithook(audit)

root = os.path.abspath(sys.argv[1])
central = os.path.abspath(sys.argv[2])
private = root + os.sep
real_connect = sqlite3.connect


def poisoned(*args, **kwargs):
    target = str(args[0]) if args else ""
    if target.startswith("file:"):
        target = target[5:].split("?", 1)[0]
    target = os.path.abspath(target)
    if not (target == root or target.startswith(private)):
        raise AssertionError("database access outside the private root: " + target)
    return real_connect(*args, **kwargs)


sqlite3.connect = poisoned


class Blocker:
    def find_spec(self, name, path=None, target=None):
        if name.split(".")[0] in {"duckdb", "_duckdb"} or name.startswith("dq_quant_invest_data"):
            raise AssertionError("central module imported: " + name)
        return None


sys.meta_path.insert(0, Blocker())

from datetime import UTC, datetime
from openbb_collector_core.models import Response
from dq_historical_market_data_collector.etf_options.config import Config, TradingCalendar
from dq_historical_market_data_collector.etf_options.runtime import Collector
from dq_historical_market_data_collector.etf_options.storage import Journal

NOW = datetime(2026, 10, 14, 7, 20, tzinfo=UTC)
CONTRACT = {
    "symbol": "10000001", "exchange": "SSE", "underlying": "510050", "option_type": "call",
    "strike": 2.5, "expiration": "2026-10-28", "contract_unit": 10000, "contract_trade_code": None,
    "metadata_source": "sina", "source_date": "2026-10-14", "source_row": {},
}
QUOTE = {
    "symbol": "10000001", "source_time": "2026-10-14T15:00:00+08:00", "bid": 0.1, "ask": 0.2,
    "bid_size": 10, "ask_size": 20, "last": 0.15, "volume": 100, "turnover": 1000,
    "open_interest": 300, "underlying": "510050", "strike": 2.5, "contract_name": "synthetic",
    "source_status": "ok", "raw_fields": {}, "quote_error": None,
}
CALLS = {"value": 0}


class Client:
    def fetch(self, source_id, source, request):
        CALLS["value"] += 1
        if source.model == "AkshareEtfOptionContracts":
            return Response([dict(CONTRACT)])
        if source.model == "AkshareToolTradeDateHistSina":
            return Response([{"trade_date": "2026-10-13"}, {"trade_date": "2026-10-14"}])
        return Response([dict(QUOTE)])

    def close(self):
        return None


config = Config(
    root=os.path.join(root, "private"),
    calendar_file=os.path.join(root, "private", "calendar.json"),
    exchanges={"SSE": {"source": "sina"}},
    units={
        "price_unit": "CNY_per_contract_share",
        "size_unit": "contract",
        "volume_unit": "contract",
        "turnover_unit": "CNY",
    },
    limits={"retries": 0, "retry_delay_seconds": 0},
)
calendar = TradingCalendar(
    coverage_start="2026-10-13", coverage_end="2026-10-15",
    trading_dates=["2026-10-13", "2026-10-14", "2026-10-15"],
    source="offline child fixture",
)
with Journal(config.root) as journal:
    report = Collector(config, journal, calendar, Client(), clock=lambda: NOW).collect()

# Falsifiability: the same hook must observe a central path when one is touched.
try:
    with open(os.path.join(central, "probe"), "w", encoding="utf-8") as stream:
        stream.write("x")
except OSError:
    pass
falsification = [path for path in opened if path.startswith(central + os.sep)]
blocked = None
try:
    sqlite3.connect(os.path.join(central, "journal.sqlite3"))
except AssertionError:
    blocked = "poisoned"

print(json.dumps({
    "config_module": sys.modules["dq_historical_market_data_collector.etf_options.config"].__file__,
    "status": report["status"],
    "rows": report["row_count"],
    "calls": CALLS["value"],
    "unit_source": report["unit_source"],
    "central_opens": [
        path for path in opened if path == central
        or (path.startswith(central + os.sep) and path not in falsification)
    ],
    "falsification_opens": falsification,
    "central_database_blocked": blocked,
    "duckdb_loaded": "duckdb" in sys.modules,
    "auto_build_effective": os.environ.get("OPENBB_AUTO_BUILD"),
}))
"""


@contextlib.contextmanager
def hold_exclusive(path):
    """A real exclusive ``flock``, held for the caller's whole block."""
    primary = path.open("a+b")
    second = path.open("a+b")
    try:
        fcntl.flock(primary.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            fcntl.flock(second.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            pass
        else:
            raise AssertionError("central journal lock is not exclusive")
        yield primary
    finally:
        second.close()
        fcntl.flock(primary.fileno(), fcntl.LOCK_UN)
        primary.close()


def lock_is_held(path):
    with path.open("a+b") as stream:
        try:
            fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return True
        fcntl.flock(stream.fileno(), fcntl.LOCK_UN)
        return False


def run_central_child(tmp_path, central):
    from dq_historical_market_data_collector.etf_options import runtime as lane_runtime

    script = tmp_path / "central_child.py"
    script.write_text(CENTRAL_CHILD)
    env = os.environ.copy()
    source = Path(lane_runtime.__file__).resolve().parents[2]
    repo = Path(lane_runtime.__file__).resolve().parents[3]
    openbb = repo.parents[1]
    env["PYTHONPATH"] = os.pathsep.join(
        str(item)
        for item in (
            source,
            repo,
            openbb / "providers/akshare",
            openbb / "core",
            openbb / "collectors/core/src",
        )
    )
    env["OPENBB_AUTO_BUILD"] = "false"
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    child = subprocess.run(
        [sys.executable, str(script), str(tmp_path / "private"), str(central)],
        cwd=str(tmp_path),
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    return child, (json.loads(child.stdout) if child.returncode == 0 else None)


@pytest.mark.parametrize("state", ["absent", "locked", "inaccessible"])
def test_guard_private_run_touches_no_central_store(tmp_path, state):
    (tmp_path / "private").mkdir()
    central = tmp_path / f"central-{state}"
    if state != "absent":
        central.mkdir()
        (central / "journal.sqlite3").write_bytes(b"")
        (central / "catalog.duckdb").write_bytes(b"")
    if state == "inaccessible":
        central.chmod(0)
    held = hold_exclusive(central / "journal.sqlite3") if state == "locked" else None
    observed = {}
    try:
        with held if held is not None else contextlib.nullcontext():
            child, payload = run_central_child(tmp_path, central)
            if state == "locked":
                observed["lock_still_held_after_child"] = lock_is_held(central / "journal.sqlite3")
    finally:
        if state == "inaccessible":
            central.chmod(0o700)
    assert child.returncode == 0, child.stderr
    assert payload["status"] == "complete" and payload["rows"] == 1
    assert payload["calls"] == 2  # one master request, one quote block
    assert payload["central_opens"] == []
    assert payload["duckdb_loaded"] is False
    assert payload["central_database_blocked"] == "poisoned"
    assert payload["falsification_opens"], "the audit hook must be shown to be live"
    assert payload["auto_build_effective"] == "false"
    # The child ran the working-tree module, not an installed copy.
    assert Path(payload["config_module"]).resolve() == Path(lane_config.__file__).resolve()
    if state == "locked":
        assert observed["lock_still_held_after_child"] is True


def test_guard_two_concurrent_writers_one_generation(tmp_path, monkeypatch):
    """Two writers on one capture generation: the second is refused, not merged."""
    from dq_historical_market_data_collector.etf_options import runtime as lane

    entered, release = threading.Event(), threading.Event()
    bodies = [b'var hq_str_CON_OP_10000001="' + (",".join(["0"] * 51)).encode() + b'";\n']
    calls = []

    class Blocking:
        def __init__(self):
            self.first = True

        def __call__(self, url, **kwargs):
            calls.append(url)
            if self.first:
                self.first = False
                entered.set()
                assert release.wait(20), "the second writer never arrived"
            return TransportResponse(bodies[0], 200, "text/plain; charset=gb18030")

    stand = Blocking()
    import openbb_akshare.models.etf_options as quotes_module

    monkeypatch.setattr(quotes_module, "make_quote_request", stand)
    cfg = config(tmp_path)
    with Journal(cfg.root) as journal:
        collector = lane.Collector(cfg, journal, calendar(), clock=lambda: NOW)
        collector.token = "writers"
        entry = collector._entry(DAY, "SSE", "AkshareEtfOptionQuotes", {"symbols": "10000001"}, 0)
        results = {}

        def first():
            results["first"] = lane.acquire(cfg.root / "evidence", [entry], client=stand)

        worker = threading.Thread(target=first)
        worker.start()
        assert entered.wait(10)
        second = lane.acquire(cfg.root / "evidence", [entry], client=stand)
        release.set()
        worker.join(20)
        assert results["first"][0]["status"] in {"captured", "partial", "failed"}
    assert second[0]["status"] == "error"
    assert second[0]["reason"] == "acquisition_busy"
    # Exactly one body on disk; the loser wrote nothing.
    assert len(list((cfg.root / "evidence").rglob("body.bin"))) == 1


def test_guard_calendar_and_daily_deliveries_do_not_cross_read(tmp_path, monkeypatch):
    from dq_historical_market_data_collector.etf_options.runtime import (
        EvidenceStateError,
        read_calendar_delivery,
        read_delivery,
    )

    cfg = config(tmp_path)
    with Journal(cfg.root) as journal:
        report = Collector(cfg, journal, None, FakeClient(), clock=lambda: NOW).refresh_calendar()
        assert report["status"] == "complete"
        calendar_documents = sorted((cfg.root / "raw").rglob("*.calendar.json"))
        assert len(calendar_documents) == 1
        document = json.loads(calendar_documents[0].read_text())
        assert document["version"] == "etf-raw-calendar-v1"
        assert document["provenance"]["date_source"] == "row"
        with pytest.raises(EvidenceStateError, match="invalid_delivery"):
            read_delivery(calendar_documents[0], cfg.root / "evidence")
        assert (
            read_calendar_delivery(calendar_documents[0], cfg.root / "evidence")["version"]
            == "etf-raw-calendar-v1"
        )

    # The reverse direction: the calendar reader refuses a daily v2 delivery.
    stand = TransportStand(sse_master_body(), quote_body("10000001"))
    with literal_lane(tmp_path / "daily", monkeypatch, stand) as (daily_cfg, _j, _r):
        ((daily, _document),) = [
            (path, json.loads(path.read_text()))
            for path in sorted((daily_cfg.root / "raw").rglob("*.json"))
            if json.loads(path.read_text()).get("kind") == "quote-batch"
        ]
        with pytest.raises(EvidenceStateError, match="unsupported_delivery_version"):
            read_calendar_delivery(daily, daily_cfg.root / "evidence")


# ---------------------------------------------------------------------------
# acquisition-only task: zero central access, proved in a real child process
# ---------------------------------------------------------------------------

ACQUIRE_CHILD = """
import json, os, sqlite3, sys

root = os.path.abspath(sys.argv[1])
central = os.path.abspath(sys.argv[2])
plan_path = os.path.abspath(sys.argv[3])
opened, writes = [], []


def audit(event, args):
    if event != "open":
        return
    path = args[0] if args else None
    mode = args[1] if len(args) > 1 else None
    if isinstance(path, bytes):
        path = path.decode("utf-8", "replace")
    if not isinstance(path, str):
        try:
            path = os.fspath(path)
        except TypeError:
            return
        if isinstance(path, bytes):
            path = path.decode("utf-8", "replace")
    opened.append(path)
    if isinstance(mode, str) and any(flag in mode for flag in "wax+"):
        writes.append(path)


sys.addaudithook(audit)

real_connect = sqlite3.connect


def poisoned(*args, **kwargs):
    target = str(args[0]) if args else ""
    if target.startswith("file:"):
        target = target[5:].split("?", 1)[0]
    target = os.path.abspath(target)
    if not (target == root or target.startswith(root + os.sep)):
        raise AssertionError("database access outside the private root: " + target)
    return real_connect(*args, **kwargs)


sqlite3.connect = poisoned

DOWNSTREAM = {
    "dq_historical_market_data_collector.etf_options.config",
    "dq_historical_market_data_collector.etf_options.quality",
    "dq_historical_market_data_collector.etf_options.storage",
    "dq_historical_market_data_collector.etf_options.delivery",
    "dq_historical_market_data_collector.etf_options.runtime",
    "dq_historical_market_data_collector.etf_options.scheduled",
}


class Blocker:
    def find_spec(self, name, path=None, target=None):
        if name.split(".")[0] in {"duckdb", "_duckdb"} or name.startswith("dq_quant_invest_data"):
            raise AssertionError("central module imported: " + name)
        if name in DOWNSTREAM:
            raise AssertionError("downstream module imported by the acquisition task: " + name)
        return None


sys.meta_path.insert(0, Blocker())

from dq_historical_market_data_collector.etf_options import acquire
from dq_historical_market_data_collector.etf_options import source as lane_source

BODY = b'var hq_str_CON_OP_10000001="' + (",".join(["0"] * 43)).encode() + b'";\\n'


class Response:
    def __init__(self, body):
        self.content = body
        self.status_code = 200
        self.headers = {"Content-Type": "text/plain; charset=gb18030"}


def stand(url, **kwargs):
    return Response(BODY)


lane_source.quote_transport = lambda: stand

plan = {
    "version": "etf-acquire-plan-v1",
    "source": {
        "provider": "akshare",
        "model": "AkshareEtfOptionQuotes",
        "max_response_bytes": 1000000,
        "timeout_seconds": 15,
    },
    "requests": [
        {"request_id": "q1", "kind": "quote-batch", "params": {"symbols": "10000001"}}
    ],
}
with open(plan_path, "w", encoding="utf-8") as stream:
    json.dump(plan, stream)

import contextlib, io

buffer = io.StringIO()
with contextlib.redirect_stdout(buffer):
    code = acquire.main(["--plan", plan_path, "--root", root])
emitted = json.loads(buffer.getvalue().strip().splitlines()[-1])
before_falsification = list(writes)
# Falsifiability: the same hook must observe a central path when one is touched.
try:
    with open(os.path.join(central, "probe"), "w", encoding="utf-8") as stream:
        stream.write("x")
except OSError:
    pass
falsification = [path for path in opened if path.startswith(central + os.sep)]
blocked = None
try:
    sqlite3.connect(os.path.join(central, "journal.sqlite3"))
except AssertionError:
    blocked = "poisoned"

import hashlib

bodies = []
for base, _dirs, names in os.walk(root):
    for name in names:
        if name == "body.bin":
            bodies.append(os.path.join(base, name))
receipts = []
for base, _dirs, names in os.walk(root):
    for name in names:
        if name == "receipt.json":
            with open(os.path.join(base, name), encoding="utf-8") as stream:
                receipts.append(json.load(stream))

print(json.dumps({
    "main_exit": code,
    "emitted": emitted,
    "module_file": acquire.__file__,
    "forbidden_modules": sorted(
        name for name in sys.modules
        if name in DOWNSTREAM or name.split(".")[0] in {"duckdb", "_duckdb"}
    ),
    "config_loaded": any(name.endswith(".config") for name in sys.modules),
    "duckdb_loaded": "duckdb" in sys.modules,
    "sqlite_poisoned": blocked,
    "central_opens": [
        path for path in opened
        if (path == central or path.startswith(central + os.sep)) and path not in falsification
    ],
    "falsification_opens": falsification,
    "writes": before_falsification,
    "writes_outside_root": [
        path for path in before_falsification
        if not (path == root or path.startswith(root + os.sep)) and not path.endswith("plan.json")
    ],
    "body_count": len(bodies),
    "body_matches": bool(bodies) and open(bodies[0], "rb").read() == BODY,
    "receipt_sha256": receipts[0]["sha256"] if receipts else None,
    "body_sha256": hashlib.sha256(BODY).hexdigest(),
    "receipt_kind": receipts[0]["evidence_kind"] if receipts else None,
    "auto_build_effective": os.environ.get("OPENBB_AUTO_BUILD"),
}, sort_keys=True))
"""


def run_acquire_child(tmp_path, root, central, plan):
    from dq_historical_market_data_collector.etf_options import acquire as lane_acquire

    script = tmp_path / "acquire_child.py"
    script.write_text(ACQUIRE_CHILD)
    plan_path = tmp_path / "acquire_plan.json"
    plan_path.write_text(json.dumps(plan))
    env = os.environ.copy()
    module = Path(lane_acquire.__file__).resolve()
    source_root = module.parents[2]
    repo = module.parents[3]
    openbb = repo.parents[1]
    env["PYTHONPATH"] = os.pathsep.join(
        str(item)
        for item in (
            source_root,
            repo,
            openbb / "providers/akshare",
            openbb / "core",
            openbb / "collectors/core/src",
        )
    )
    env["OPENBB_AUTO_BUILD"] = "false"
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    child = subprocess.run(
        [sys.executable, str(script), str(root), str(central), str(plan_path)],
        cwd=str(tmp_path),
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    return child, (json.loads(child.stdout) if child.returncode == 0 else None)


ACQUIRE_PLAN = {
    "version": "etf-acquire-plan-v1",
    "source": {
        "provider": "akshare",
        "model": "AkshareEtfOptionQuotes",
        "max_response_bytes": 1_000_000,
        "timeout_seconds": 15,
    },
    "requests": [{"request_id": "q1", "kind": "quote-batch", "params": {"symbols": "10000001"}}],
}


@pytest.mark.parametrize("state", ["absent", "locked", "inaccessible"])
def test_guard_acquire_only_child_touches_no_central_store(tmp_path, state):
    """The acquisition task runs with no config, no journal and no central store."""
    root = tmp_path / "evidence"
    central = tmp_path / f"central-{state}"
    if state != "absent":
        central.mkdir()
        (central / "journal.sqlite3").write_bytes(b"")
        (central / "catalog.duckdb").write_bytes(b"")
    if state == "inaccessible":
        central.chmod(0)
    held = hold_exclusive(central / "journal.sqlite3") if state == "locked" else None
    observed = {}
    try:
        with held if held is not None else contextlib.nullcontext():
            child, payload = run_acquire_child(tmp_path, root, central, ACQUIRE_PLAN)
            if state == "locked":
                observed["lock_still_held_after_child"] = lock_is_held(central / "journal.sqlite3")
    finally:
        if state == "inaccessible":
            central.chmod(0o700)
    assert child.returncode == 0, child.stderr
    assert payload["main_exit"] == 0
    assert payload["emitted"]["status"] == "complete" and payload["emitted"]["verified_all"] is True
    # Nothing downstream was imported: not config, not quality, not the journal.
    assert payload["forbidden_modules"] == []
    assert payload["config_loaded"] is False
    assert payload["duckdb_loaded"] is False
    assert payload["sqlite_poisoned"] == "poisoned"
    # The store was never opened, and the audit hook is proven live while it says so.
    assert payload["central_opens"] == []
    assert payload["falsification_opens"], "the audit hook must be shown to be live"
    # The only writes are the evidence root (the plan file is the operator's).
    assert payload["writes_outside_root"] == []
    # And a real document capture happened, byte-identically, labelled raw-bytes.
    assert payload["body_count"] == 1 and payload["body_matches"] is True
    assert payload["receipt_sha256"] == payload["body_sha256"]
    assert payload["receipt_kind"] == "raw-bytes"
    assert payload["auto_build_effective"] == "false"
    if state == "locked":
        assert observed["lock_still_held_after_child"] is True


def test_raw_preserved_red_freeze_evidence_is_immutable(tmp_path):
    """The preserved red freeze pass: reconstructed, immutable, and named as such.

    The round-1 red receipts were written over by the green run, so the original
    bytes are gone.  What the bundle preserves is a *reconstruction* of the same
    scenario -- and the artifact must say so, or it would be passing off a rerun
    as the original failure.
    """
    location = os.environ.get("ETFO_RED_EVIDENCE")
    if not location:
        pytest.skip("no preserved freeze-red bundle in this environment")
    root = Path(location)
    if (root / "manifest.json").exists():
        run = root
    else:
        runs = sorted(path for path in root.iterdir() if (path / "manifest.json").is_file())
        if not runs:
            pytest.skip("the freeze-red bundle has no manifest")
        run = runs[-1]
    manifest = json.loads((run / "manifest.json").read_text())
    # Never presented as the original.
    assert manifest["reconstructed"] is True
    assert manifest["original_receipts_preserved"] is False
    assert "gone" in manifest["why"], manifest["why"]
    receipts = [run / name for name in manifest["receipts"]]
    assert receipts, "a control with no receipts is prose, not evidence"
    red = []
    for path in receipts:
        assert path.stat().st_mode & 0o222 == 0, (path.name, "a receipt must be read-only")
        payload = json.loads(path.read_text())
        # The pytest child ran and collected; the harness verdict is what went red.
        assert payload["child_exit"] == 0, (path.name, payload["child_exit"])
        assert payload["harness_verdict"] == "red", path.name
        assert payload["closure_not_frozen"], path.name
        red.append(payload)
    named = {item for payload in red for item in payload["closure_not_frozen"]}
    assert len(named) == manifest["observed_closure_not_frozen"]
    core = {item for item in named if item.startswith("core/openbb_core/")}
    assert len(core) == manifest["core_count"] == 34
    assert len(named - core) == manifest["provider_or_repo_count"] == 5
    # Immutable in practice: a second write of the same receipt name is refused.
    target = receipts[0]
    with pytest.raises(FileExistsError):
        with target.open("xb") as stream:
            stream.write(b"{}")
    assert target.stat().st_mode & 0o222 == 0

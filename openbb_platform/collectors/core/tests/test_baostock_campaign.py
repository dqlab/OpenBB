"""Actual subprocess/default-spawn/QueryExecutor/raw storage acceptance.

Synthetic ONLY at BaoStock SDK seam. Faults affect process death/control writes,
not substitute captures. Every invocation retains stdout/stderr and exits.
"""

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

from openbb_collector_core import baostock_campaign as campaign
from openbb_collector_core import baostock_page_capture as page_capture
from openbb_collector_core import raw_acquisition as raw

BOOTSTRAP = r"""
import os
if os.environ.get("CAMPAIGN_OFFLINE") == "1":
    import json, time, socket, multiprocessing, hashlib, sys
    from pathlib import Path
    assert os.environ["OPENBB_AUTO_BUILD"] == "false"
    assert os.environ["PYTHONDONTWRITEBYTECODE"] == "1"
    def trap(*a, **k):
        raise RuntimeError("OFFLINE_BAOSTOCK_NETWORK_TRAP")
    for name in ("connect", "connect_ex", "send", "sendall", "sendto"):
        original_socket_call = getattr(socket.socket, name)
        def guarded(self, *args, _original=original_socket_call, **kwargs):
            if self.family in (socket.AF_INET, socket.AF_INET6):
                return trap(*args, **kwargs)
            return _original(self, *args, **kwargs)
        setattr(socket.socket, name, guarded)
    socket.getaddrinfo = trap
    def filesystem_guard(event, args):
        if event in {"os.listdir", "os.scandir"} and args and any(
                token in str(args[0])
                for token in ("raw-acquisition-v1", "baostock-page-capture-v1")):
            raise RuntimeError("CAPTURE_ARCHIVE_SCAN_TRAP")
        if (event == "open" and args and os.environ.get("CENTRAL")
                and str(args[0]) == os.environ["CENTRAL"]):
            raise RuntimeError("CENTRAL_OPEN_TRAP")
    sys.addaudithook(filesystem_guard)
    from openbb_baostock.utils import helpers
    def log(name, **kw):
        with Path(os.environ["CALLS"]).open("a") as f:
            f.write(json.dumps(dict(event=name, pid=os.getpid(), **kw)) + "\n")
            f.flush(); os.fsync(f.fileno())
    class Socket:
        def close(self):
            log("close")
    class Result:
        def __init__(self, code="0", rows=None):
            self.error_code = code
            self.error_msg = "SECRET_EXCEPTION_MUST_NOT_LEAK"
            self.fields = ["date", "close", "close"]
            self.data = rows if rows is not None else [
                ["2024-01-02", "1.2300", "", "EXTRA"], ["short"]]
            self.cur_row_num = 0
        def next(self):
            self.error_code = "503"
            self.data = [["PAGINATION_FAILED_BODY"]]
            return False
    def login():
        helpers.context.default_socket = Socket()
        log("login")
        if os.environ.get("SCENARIO") == "login_failure":
            return Result("401", [])
        return Result()
    def logout():
        log("logout")
        if os.environ.get("SCENARIO") == "logout_failure":
            raise OSError("SECRET_LOGOUT")
        return Result()
    def resolve(method):
        assert method in {"query_history_k_data_plus", "query_adjust_factor",
                          "query_stock_basic", "query_trade_dates"}
        def call(**params):
            scenario = os.environ.get("SCENARIO", "success")
            old = [json.loads(x) for x in Path(os.environ["CALLS"]).read_text().splitlines()]
            prior = sum(x.get("event") == "query" and x.get("method") == method
                        and x.get("params") == params for x in old)
            fail = (scenario == "always" or (scenario == "fail_first"
                    and params.get("adjustflag") == "2" and prior == 0)
                    or (scenario == "flag2" and params.get("adjustflag") == "2")
                    or (scenario == "factor_fail" and method == "query_adjust_factor"
                        and prior == 0))
            result = Result("503" if fail else "0", [["FAILED_BODY_SENTINEL"]] if fail else None)
            if scenario == "pagination":
                result.data = [["PREFIX", "1.2300", ""]] * helpers.BAOSTOCK_PER_PAGE_COUNT
            if scenario == "large":
                result.data = [["L" * 17_000_000]]
            origins = {name:dict(path=m.__file__,
                       sha256=hashlib.sha256(Path(m.__file__).read_bytes()).hexdigest())
                       for name,m in tuple(sys.modules.items()) if name in {
                           "openbb_baostock.models.history_source",
                           "openbb_baostock.utils.source_evidence", "openbb_collector_core.process",
                           "openbb_collector_core.providers",
                           "openbb_collector_core.baostock_page_capture",
                           "openbb_core.provider.query_executor"}}
            log("query", method=method, params=params,
                start_method=multiprocessing.get_start_method(),
                error_code=result.error_code, origins=origins)
            if scenario == "delay":
                time.sleep(15)
            return result
        return call
    helpers.bs.login = login
    helpers.bs.logout = logout
    helpers.resolve_method = resolve
    if os.environ.get("FAULT"):
        from openbb_collector_core import baostock_page_capture as r
        original = r.capture_process
        def capture(*args, **kwargs):
            fault = os.environ["FAULT"]
            if fault == "crash_reserved":
                os._exit(89)
            result = original(*args, **kwargs)
            if fault == "crash_finalized":
                os._exit(88)
            if fault == "term_finalized":
                import signal
                os.kill(os.getpid(), signal.SIGTERM)
            return result
        r.capture_process = capture
        if os.environ["FAULT"] == "handoff_installed":
            original_install = r.raw._install
            def handoff_install(path, content, **kwargs):
                result = original_install(path, content, **kwargs)
                if path.name.startswith("handoff-"):
                    os._exit(91)
                return result
            r.raw._install = handoff_install
"""


# Mutable per-invocation control documents: the progress cursor and the v2.2
# rate-policy counters (natural-day budget, observed latency). Retained evidence,
# sealed handoffs and admission decisions are never in this set.
MUTABLE_CONTROL = {'progress.json', 'budget.json', 'latency.json'}


def evidence_for_link(root, link):
    """Test-only versioned read adapter; never a product aggregate backend."""
    directory = root / link["locator"]
    authority = json.loads((directory / "authority.json").read_bytes())
    captured = page_capture.read_capture(root, authority["task"], expected=link["receipt_sha256"])
    observations = [
        page_capture.read_page(directory, p["page"], decode=False) for p in captured["pages"]
    ]
    env = dict(observations[-1]) if observations else {}
    env["pages"] = [p for observation in observations for p in observation["pages"]]
    if captured["terminal"] and captured["terminal"]["summary"]:
        summary = captured["terminal"]["summary"]
        env.update(status=summary["status"], reason=summary["reason"], cleanup=summary["cleanup"])
    return env


def task(code="sh.600000", flags=("3", "2"), model="BaostockHistoryKDataPlus", suffix=""):
    params = dict(code=code, start_date="2024-01-01", end_date="2024-01-03")
    reqs = []
    for flag in flags:
        p = dict(params)
        if model == "BaostockHistoryKDataPlus":
            p.update(fields="date,code,close", frequency="d", adjustflag=flag)
        reqs.append(dict(request_id=flag, params=p))
    return dict(
        version=raw.VERSION,
        task_id="literal:" + code + suffix,
        generation="original-g1",
        source=dict(provider="baostock", model=model),
        requests=reqs,
    )


class Harness:
    def __init__(self, root, tasks=None, budget=20, attempts=3, backoff=0):
        self.root = root
        root.mkdir(parents=True, exist_ok=True)
        bootstrap = root / "bootstrap"
        bootstrap.mkdir()
        (bootstrap / "sitecustomize.py").write_text(BOOTSTRAP)
        import openbb_baostock

        self.env = dict(
            os.environ,
            OPENBB_AUTO_BUILD="false",
            PYTHONDONTWRITEBYTECODE="1",
            CAMPAIGN_OFFLINE="1",
            CALLS=str(root / "calls.jsonl"),
            PYTHONPATH=os.pathsep.join(
                [
                    str(bootstrap),
                    str(Path(campaign.__file__).parents[1]),
                    str(Path(openbb_baostock.__file__).parents[1]),
                ]
            ),
        )
        self.private = root / "private"
        self.manifest = root / "manifest"
        self.n = 0
        tasks = tasks if tasks is not None else [task(), task("sz.000001", ("3",))]
        plan = root / "plan.json"
        plan.write_text(json.dumps(dict(tasks=tasks)))
        result = self.invoke(
            [
                "compile",
                "--plan",
                str(plan),
                "--manifest",
                str(self.manifest),
                "--request-budget",
                str(budget),
                "--max-attempts",
                str(attempts),
                "--backoff-seconds",
                str(backoff),
            ]
        )
        assert result.returncode == 0, result.stdout + result.stderr

    def command(self, run="run1", slot="slot1"):
        return [
            "run",
            "--manifest",
            str(self.manifest),
            "--root",
            str(self.private),
            "--run",
            run,
            "--slot",
            slot,
        ]

    def invoke(self, args, scenario="success", fault="", wait=True):
        self.n += 1
        cmd = [sys.executable, "-B", "-m", "openbb_collector_core.baostock_campaign", *args]
        env = dict(self.env, SCENARIO=scenario, FAULT=fault)
        if not wait:
            return subprocess.Popen(
                cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
            )
        child = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=90)
        stem = self.root / f"invocation-{self.n:03d}"
        stem.with_suffix(".stdout").write_text(child.stdout)
        stem.with_suffix(".stderr").write_text(child.stderr)
        stem.with_suffix(".json").write_text(
            json.dumps(dict(command=cmd, exit=child.returncode), indent=2)
        )
        return child

    def run(self, scenario="success", **kwargs):
        child = self.invoke(
            self.command(kwargs.pop("run", "run1"), kwargs.pop("slot", "slot1")),
            scenario=scenario,
            **kwargs,
        )
        return child, json.loads(child.stdout) if child.stdout.strip() else None

    def calls(self):
        p = self.root / "calls.jsonl"
        return [json.loads(x) for x in p.read_text().splitlines()] if p.exists() else []

    def queries(self):
        return [x for x in self.calls() if x["event"] == "query"]

    def state_path(self):
        return self.private / campaign.VERSION / campaign.digest("run1") / "progress.json"

    def state(self):
        return json.loads(self.state_path().read_bytes())["document"]


def test_history_retry_prefix_completed_and_fresh_explicit_cycle(tmp_path):
    h = Harness(tmp_path)
    child, first = h.run("fail_first")
    assert child.returncode == 1 and first["remaining_requests"] == 1
    assert first["source_calls"] == 3 and len(h.queries()) == 3
    assert {q["start_method"] for q in h.queries()} == {"spawn"}
    state = h.state()
    good, failed = state["entries"][0]["links"][0], state["entries"][1]["links"][0]
    good_body = h.private / good["locator"] / "page-000.body"
    original = good_body.read_bytes()
    failed_body = h.private / failed["locator"] / "page-000.body"
    failed_bytes = failed_body.read_bytes()
    assert b"FAILED_BODY_SENTINEL" in failed_bytes and b'"error_code":"503"' in failed_bytes
    assert b"SECRET" not in failed_bytes
    envelope = json.loads(original)
    assert envelope["pages"][0]["fields"] == ["date", "close", "close"]
    assert envelope["pages"][0]["rows"] == [["2024-01-02", "1.2300", "", "EXTRA"], ["short"]]
    child, second = h.run("fail_first")
    assert child.returncode == 0 and second["source_calls"] == 1 and len(h.queries()) == 4
    assert good_body.read_bytes() == original and failed_body.read_bytes() == failed_bytes
    child, replay = h.run("fail_first")
    assert child.returncode == 0 and replay["source_calls"] == 0 and len(h.queries()) == 4
    child, fresh = h.run(run="run2", slot="slot2")
    assert child.returncode == 0 and fresh["source_calls"] == 3 and len(h.queries()) == 7
    assert all(q["origins"] for q in h.queries())


def test_history_factor_sequence(tmp_path):
    h = Harness(
        tmp_path,
        [task(flags=("3",)), task(flags=("f",), model="BaostockAdjustFactor", suffix=":factor")],
    )
    first, summary = h.run("factor_fail")
    assert first.returncode == 1 and summary["remaining_requests"] == 1
    second, summary = h.run("factor_fail")
    assert second.returncode == 0
    assert [q["method"] for q in h.queries()] == [
        "query_history_k_data_plus",
        "query_adjust_factor",
        "query_adjust_factor",
    ]


def test_pagination_error_retains_received_prefix_and_failure(tmp_path):
    h = Harness(tmp_path, [task(flags=("3",))])
    child, summary = h.run("pagination")
    assert child.returncode == 1 and summary["remaining_requests"] == 1
    link = h.state()["entries"][0]["links"][0]
    env = evidence_for_link(h.private, link)
    assert env["status"] == "partial"
    assert env["pages"][0]["rows"][0] == ["PREFIX", "1.2300", ""]
    assert env["pages"][1]["rows"] == [["PAGINATION_FAILED_BODY"]]
    assert env["error_code"] == "503"


@pytest.mark.parametrize("scenario", ["login_failure", "logout_failure"])
def test_cleanup_and_no_false_source_success(tmp_path, scenario):
    h = Harness(tmp_path, [task(flags=("3",))])
    child, summary = h.run(scenario)
    assert child.returncode == 1 and summary["remaining_requests"] == 1
    events = [c["event"] for c in h.calls()]
    assert "close" in events
    assert ("logout" in events) == (scenario != "login_failure")
    link = h.state()["entries"][0]["links"][0]
    env = evidence_for_link(h.private, link)
    if scenario == "login_failure":
        assert env["pages"] == [] and env["error_code"] == "401"
    else:
        assert env["cleanup"] == "logout_failed"


def test_exact_finalized_locator_crash_recovery(tmp_path):
    h = Harness(tmp_path, [task(flags=("3",))])
    child, _ = h.run(fault="crash_finalized")
    assert child.returncode == 88 and len(h.queries()) == 1 and h.state()["active"]
    child, summary = h.run()
    assert child.returncode == 0 and summary["source_calls"] == 0 and len(h.queries()) == 1


def test_installed_handoff_crash_replays_without_refetch(tmp_path):
    h = Harness(tmp_path, [task(flags=("3",))])
    child, _ = h.run(fault="handoff_installed")
    assert child.returncode == 91 and len(h.queries()) == 1
    before = h.state_path().with_name("handoff-000000.json").read_bytes()
    recovered, summary = h.run()
    assert recovered.returncode == 0 and summary["source_calls"] == 0
    assert h.state_path().with_name("handoff-000000.json").read_bytes() == before
    assert len(h.queries()) == 1


def test_reservation_crash_consumes_budget_without_refund(tmp_path):
    h = Harness(tmp_path, [task(flags=("3",))], budget=1)
    child, _ = h.run(fault="crash_reserved")
    assert child.returncode == 89 and not h.queries()
    child, summary = h.run()
    assert (
        child.returncode == 1
        and summary["reserved_requests"] == 1
        and summary["remaining_requests"] == 1
    )
    child, summary = h.run()
    assert child.returncode == 1 and summary["source_calls"] == 0 and not h.queries()


@pytest.mark.parametrize("mode", ["cap", "budget", "backoff"])
def test_restart_limits_persist(tmp_path, mode):
    h = Harness(
        tmp_path,
        [task(flags=("3",))],
        attempts=1 if mode == "cap" else 3,
        budget=1 if mode == "budget" else 20,
        backoff=86400 if mode == "backoff" else 0,
    )
    child, first = h.run("always")
    # A spent attempt cap closes the page with a declared failure instead of
    # stalling it forever; the reservation is still never refunded or refetched.
    expected = 0 if mode == "cap" else 1
    assert child.returncode == expected
    child, second = h.run()
    assert child.returncode == expected and second["source_calls"] == 0
    assert len(h.queries()) == 1
    assert first["reserved_requests"] == second["reserved_requests"] == 1
    if mode == "cap":
        document = campaign.verify_handoff(
            h.private, h.state_path().with_name("handoff-000000.json"), first["handoff_sha256"]
        )
        assert document["outcome"] == "closed_with_failures"
        assert [failure["index"] for failure in document["failures"]] == [0]


def test_cancel_at_finalization_is_durable_veto(tmp_path):
    h = Harness(tmp_path, [task(flags=("3",))])
    child, summary = h.run(fault="term_finalized")
    assert child.returncode == 1 and summary["status"] == "cancelled"
    child, summary = h.run()
    assert child.returncode == 1 and summary["source_calls"] == 0 and len(h.queries()) == 1
    state = h.state()
    state["cancelled"] = False
    poisoned = campaign.encode(dict(document=state, sha256=campaign.digest(state)))
    h.state_path().write_bytes(poisoned)
    refused, _ = h.run()
    assert refused.returncode == 2 and len(h.queries()) == 1
    assert h.state_path().read_bytes() == poisoned


def test_real_root_busy_and_sigterm_during_source(tmp_path):
    h = Harness(tmp_path, [task(flags=("3",))])
    owner = h.invoke(h.command(), scenario="delay", wait=False)
    try:
        end = time.monotonic() + 25
        while not h.queries() and time.monotonic() < end:
            time.sleep(0.05)
        assert h.queries()
        loser, summary = h.run()
        assert loser.returncode == 75 and summary["error"] == "acquisition_busy"
        owner.send_signal(signal.SIGTERM)
        out, err = owner.communicate(timeout=15)
        (tmp_path / "owner.stdout").write_text(out)
        (tmp_path / "owner.stderr").write_text(err)
        assert owner.returncode == 1 and json.loads(out)["status"] == "cancelled"
        child, summary = h.run()
        assert child.returncode == 1 and summary["source_calls"] == 0 and len(h.queries()) == 1
        pid = h.queries()[0]["pid"]
        with pytest.raises(ProcessLookupError):
            os.kill(pid, 0)
    finally:
        if owner.poll() is None:
            owner.kill()
            owner.wait()


def test_changed_slot_and_manifest_refuse_before_mutation(tmp_path):
    h = Harness(tmp_path)
    h.run("fail_first")
    before = h.state_path().read_bytes()
    child, summary = h.run(slot="DIFFERENT")
    assert child.returncode == 2 and summary["error"] == "immutable_run_conflict"
    assert h.state_path().read_bytes() == before and len(h.queries()) == 3
    path = h.manifest / "manifest.json"
    header = json.loads(path.read_bytes())
    header["policy"]["backoff_seconds"] = False
    path.write_bytes(campaign.encode(header))
    child, summary = h.run()
    assert child.returncode == 2 and h.state_path().read_bytes() == before and len(h.queries()) == 3


@pytest.mark.parametrize("damage", ["progress", "body", "locator", "page", "missing_authority"])
def test_corruption_refuses_without_fetch_or_progress_repair(tmp_path, damage):
    h = Harness(tmp_path)
    h.run("fail_first")
    if damage == "progress":
        h.state_path().write_text("{}")
    elif damage == "body":
        link = h.state()["entries"][0]["links"][0]
        (h.private / link["locator"] / "page-000.body").write_bytes(b"corrupt")
    elif damage == "locator":
        state = h.state()
        state["entries"][0]["links"][0]["locator"] = "raw-acquisition-v1/" + "0" * 64
        h.state_path().write_bytes(
            campaign.encode(dict(document=state, sha256=campaign.digest(state)))
        )
    elif damage == "page":
        (h.manifest / "page-000000.json").write_text("{}")
    else:
        h.state_path().with_name("authority.json").unlink()
    before = h.state_path().read_bytes()
    child, summary = h.run()
    assert child.returncode == 2 and summary["status"] == "refused"
    assert len(h.queries()) == 3 and h.state_path().read_bytes() == before


def test_large_literal_manifest_bounded_selected_page_and_tail_count(tmp_path):
    tasks = [task(flags=("3",), suffix=f":{n}") for n in range(1000)]
    h = Harness(tmp_path, tasks, budget=2000)
    child, first = h.run()
    assert (
        child.returncode == 1 and first["source_calls"] == 8 and first["remaining_requests"] == 992
    )
    child, second = h.run()
    assert (
        child.returncode == 1
        and second["source_calls"] == 8
        and second["remaining_requests"] == 984
    )
    assert second["metrics"]["page_reads"] == 1 and second["metrics"]["archive_scans"] == 0
    assert h.state_path().stat().st_size < 4096


def test_large_body_above_old_16mb_bound_is_saved(tmp_path):
    h = Harness(tmp_path, [task(flags=("3",))])
    child, summary = h.run("large")
    assert child.returncode == 0 and summary["source_calls"] == 1
    path = h.state_path().with_name("handoff-000000.json")
    document = campaign.verify_handoff(h.private, path, summary["handoff_sha256"])
    link = document["entries"][0]["links"][0]
    body = (h.private / link["locator"] / "page-000.body").read_bytes()
    assert len(body) > 16 * 1024 * 1024
    assert len(json.loads(body)["pages"][0]["rows"][0][0]) == 17_000_000
    verified = h.invoke(
        [
            "verify",
            "--root",
            str(h.private),
            "--handoff",
            str(path),
            "--sha256",
            summary["handoff_sha256"],
        ]
    )
    assert verified.returncode == 0 and json.loads(verified.stdout)["verified"] is True
    assert len(h.queries()) == 1


@pytest.mark.parametrize("mode", ["missing", "locked", "inaccessible"])
def test_no_central_access_and_guard_positive_controls(tmp_path, mode):
    import fcntl

    h = Harness(tmp_path, [task(flags=("3",))])
    central = tmp_path / "central.duckdb"
    h.env["CENTRAL"] = str(central)
    lock = None
    if mode != "missing":
        central.write_bytes(b"not a database")
        if mode == "locked":
            lock = central.open("rb")
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        else:
            central.chmod(0)
    try:
        child, summary = h.run()
        assert child.returncode == 0 and summary["source_calls"] == 1
        for code, label in [
            (
                "import socket; socket.socket().connect(('127.0.0.1',9))",
                "OFFLINE_BAOSTOCK_NETWORK_TRAP",
            ),
            ("import os; open(os.environ['CENTRAL'],'rb')", "CENTRAL_OPEN_TRAP"),
            (
                "import os; os.scandir('/nonexistent/raw-acquisition-v1')",
                "CAPTURE_ARCHIVE_SCAN_TRAP",
            ),
        ]:
            probe = subprocess.run(
                [sys.executable, "-B", "-c", code], env=h.env, capture_output=True, text=True
            )
            assert probe.returncode != 0 and label in probe.stderr
    finally:
        if lock:
            lock.close()
        if central.exists():
            central.chmod(0o600)


def test_prior_failures_and_current_reservations_bind_to_last_handoff(tmp_path):
    """Counter/checksum restatement cannot refund either page's attempts."""
    import hashlib

    h = Harness(tmp_path, [task(flags=("3",), suffix=f":{i}") for i in range(9)], budget=18)
    first, _ = h.run("always")
    assert first.returncode == 1 and len(h.queries()) == 8
    second, summary = h.run()
    assert second.returncode == 1 and summary["reserved_requests"] == 16
    third, summary = h.run("always")
    assert third.returncode == 1 and summary["reserved_requests"] == 17
    state = h.state()
    assert state["completed_requests"] == 8 and state["entries"][0]["attempts"] == 1
    before = {
        str(p): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in (h.private / page_capture.VERSION).rglob("*")
        if p.is_file()
    }
    edits = [
        dict(reserved=9),
        dict(completed_requests=7),
        dict(page_sha256="a" * 64),
        dict(last_handoff=None),
        dict(page=2, page_sha256=None, entries=[], completed_requests=9),
    ]
    header = json.loads((h.manifest / "manifest.json").read_bytes())
    page_zero = json.loads((h.manifest / "page-000000.json").read_bytes())
    edits.append(
        dict(
            page=0,
            page_sha256=header["first_sha256"],
            completed_requests=0,
            reserved=0,
            entries=campaign.initial_entries(page_zero),
            last_handoff=None,
        )
    )
    for edit in edits:
        changed = dict(state, **edit)
        data = campaign.encode(dict(document=changed, sha256=campaign.digest(changed)))
        h.state_path().write_bytes(data)
        child, result = h.run()
        assert child.returncode == 2 and result["status"] == "refused"
        assert h.state_path().read_bytes() == data and len(h.queries()) == 17
        assert before == {
            str(p): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in (h.private / page_capture.VERSION).rglob("*")
            if p.is_file()
        }
    # Restore only the synthetic fault fixture, then exercise the remaining
    # single reservation and completed replay without refunding failed attempts.
    h.state_path().write_bytes(campaign.encode(dict(document=state, sha256=campaign.digest(state))))
    final, summary = h.run()
    assert final.returncode == 0 and summary["reserved_requests"] == 18
    replay, summary = h.run()
    assert replay.returncode == 0 and summary["source_calls"] == 0 and len(h.queries()) == 18


def test_page_zero_count_and_cursor_refuse_without_mutation(tmp_path):
    """The immutable manifest, not progress's own checksum, authorizes page zero."""
    h = Harness(tmp_path, [task(flags=("3",))])
    first, _ = h.run("always")
    assert first.returncode == 1
    state = h.state()
    for edit in [
        dict(completed_requests=1),
        dict(page_sha256="a" * 64),
        dict(last_handoff="b" * 64),
        dict(reserved=0),
    ]:
        changed = dict(state, **edit)
        data = campaign.encode(dict(document=changed, sha256=campaign.digest(changed)))
        h.state_path().write_bytes(data)
        child, _ = h.run()
        assert child.returncode == 2 and len(h.queries()) == 1
        assert h.state_path().read_bytes() == data


def test_compile_rejects_unsupported_and_nonliteral_before_writes(tmp_path):
    bad = task()
    bad["source"]["model"] = "EquityHistorical"
    with pytest.raises(ValueError):
        campaign.compile_manifest(
            dict(tasks=[bad]),
            tmp_path / "manifest",
            dict(max_attempts=3, request_budget=10, backoff_seconds=0),
        )
    assert not (tmp_path / "manifest").exists()
    for value in [False, 0.0, "0"]:
        with pytest.raises(ValueError):
            campaign.compile_manifest(
                dict(tasks=[task()]),
                tmp_path / "manifest",
                dict(max_attempts=3, request_budget=10, backoff_seconds=value),
            )
    bad = task()
    bad["requests"][0]["params"]["code"] = "SH.600000"
    with pytest.raises(ValueError):
        campaign.compile_manifest(
            dict(tasks=[bad]),
            tmp_path / "manifest",
            dict(max_attempts=3, request_budget=10, backoff_seconds=0),
        )


Q2_BOUNDARIES = r'''
if os.environ.get("CAMPAIGN_OFFLINE") == "1":
    import fcntl, signal, subprocess
    from openbb_collector_core import raw_acquisition as qraw
    q2_fired = False
    q2_stage = None
    def q2_frame():
        f = sys._getframe(1)
        while f is not None:
            if (f.f_code.co_name == "run" and f.f_code.co_filename.endswith("baostock_campaign.py")
                    and "state" in f.f_locals):
                return f
            f = f.f_back
    def q2_fire(boundary):
        global q2_fired
        if q2_fired or os.environ.get("Q2_BOUNDARY") != boundary:
            return
        frame = q2_frame()
        if frame is None:
            return
        q2_fired = True
        state = frame.f_locals['state']
        log('q2_boundary', boundary=boundary, accepting=frame.f_locals['accepting'],
            page=state['page'], handoff=state['last_handoff'])
        if os.environ.get('Q2_STYLE') == 'event':
            frame.f_locals['stop'].set()
        else:
            os.kill(os.getpid(), signal.SIGTERM)
    def q2_write_phase():
        f = sys._getframe(1)
        while f is not None:
            if (f.f_code.co_name == 'write'
                    and f.f_code.co_filename.endswith('baostock_campaign.py')):
                doc = f.f_locals['document']
                if f.f_locals['path'].name == 'progress.json':
                    if (doc['page'] == 0 and doc['active'] is None and doc['entries']
                            and all(e['links'] and e['links'][-1]['status']=='success'
                                    for e in doc['entries'])):
                        return 'success'
                    if doc['page'] == 1:
                        return 'advanced'
                elif f.f_locals['path'].name.startswith('admission-'):
                    return doc['status']
            f = f.f_back
        return None
    q2_replace = os.replace
    def q2_replace_call(src, dst, *a, **kw):
        phase = q2_write_phase()
        q2_fire(str(phase)+'_replace_before')
        result = q2_replace(src, dst, *a, **kw)
        q2_fire(str(phase)+'_replace_after')
        return result
    os.replace = q2_replace_call
    q2_fsync = os.fsync
    def q2_fsync_call(fd):
        phase = q2_write_phase()
        # File and directory fsync are independently addressed.
        is_dir = os.path.isdir('/proc/self/fd/'+str(fd))
        tag = str(phase) + ('_dirsync' if is_dir else '_fsync')
        q2_fire(tag+'_before')
        result = q2_fsync(fd)
        q2_fire(tag+'_after')
        return result
    os.fsync = q2_fsync_call
    q2_install = qraw._install
    def q2_install_call(path, *a, **kw):
        if path.name.startswith('handoff-'):
            q2_fire('handoff_before')
        result = q2_install(path, *a, **kw)
        if path.name.startswith('handoff-'):
            q2_fire('handoff_after')
        return result
    qraw._install = q2_install_call
    q2_close = os.close
    def q2_close_call(fd):
        phase = q2_write_phase()
        result = q2_close(fd)
        if phase == 'pending':
            q2_fire('pending_cleanup')
        return result
    os.close = q2_close_call
    q2_flock = fcntl.flock
    def q2_flock_call(fd, operation):
        if operation == fcntl.LOCK_UN:
            q2_fire('unlock_before')
        result = q2_flock(fd, operation)
        if operation == fcntl.LOCK_UN:
            q2_fire('unlock_after')
        return result
    fcntl.flock = q2_flock_call
    def q2_profile(frame, event, arg):
        if (frame.f_code.co_name == 'is_set' and frame.f_back is not None
                and frame.f_back.f_code.co_name == 'finalize'
                and frame.f_back.f_code.co_filename.endswith('baostock_campaign.py')):
            if event == 'call':
                q2_fire('final_sample_before')
            elif event == 'return':
                q2_fire('final_sample_after')
        if event != 'return' or not frame.f_code.co_filename.endswith('baostock_campaign.py'):
            return
        if frame.f_code.co_name == 'summary':
            q2_fire('snapshot_return')
        elif frame.f_code.co_name == 'finalize':
            q2_fire('finalize_return')
    sys.setprofile(q2_profile)
'''


Q2_PRE_CUTOFF = [
    'success_replace_before', 'success_replace_after',
    'success_fsync_before', 'success_fsync_after',
    'success_dirsync_before', 'success_dirsync_after',
    'handoff_before', 'handoff_after',
    'advanced_replace_after', 'advanced_dirsync_after',
    'pending_replace_after', 'pending_fsync_after', 'pending_dirsync_after',
    'pending_cleanup', 'snapshot_return', 'final_sample_before',
]
Q2_POST_CUTOFF = [
    'eligible_replace_before', 'eligible_replace_after', 'eligible_dirsync_after',
    'finalize_return', 'unlock_before', 'unlock_after', 'final_sample_after',
]


def q2_verify(h, path, expected):
    before = {str(p): p.read_bytes() for p in h.private.rglob('*') if p.is_file()}
    verified = h.invoke(['verify', '--root', str(h.private), '--handoff', str(path),
                         '--sha256', expected])
    assert before == {str(p): p.read_bytes() for p in h.private.rglob('*') if p.is_file()}
    return verified


@pytest.mark.parametrize('style', ['event', 'sigterm'])
@pytest.mark.parametrize('boundary', Q2_PRE_CUTOFF + Q2_POST_CUTOFF)
def test_q2_final_boundary_matrix(tmp_path, boundary, style):
    h = Harness(tmp_path, [task(flags=('3',))])
    (tmp_path/'bootstrap/sitecustomize.py').write_text(BOOTSTRAP + Q2_BOUNDARIES)
    h.env.update(Q2_BOUNDARY=boundary, Q2_STYLE=style)
    child, result = h.run()
    events = [e for e in h.calls() if e['event'] == 'q2_boundary']
    assert len(events) == 1, (boundary, child.stdout, child.stderr, events)
    accepted = boundary in Q2_PRE_CUTOFF
    assert result['status'] == ('cancelled' if accepted else 'complete')
    assert child.returncode == (1 if accepted else 0)
    assert h.state()['cancelled'] == accepted
    path = h.state_path().with_name('handoff-000000.json')
    expected = result['handoff_sha256']
    if expected is not None:
        verified = q2_verify(h, path, expected)
        assert (verified.returncode == 0) == (not accepted), verified.stdout
    elif path.exists():
        # Also prove a retained candidate cannot be consumed by hiding its hash.
        verified = q2_verify(h, path, campaign.digest(json.loads(path.read_bytes())))
        assert verified.returncode != 0
    h.env['Q2_BOUNDARY'] = ''
    replay_child, replay = h.run()
    assert replay_child.returncode == child.returncode and replay['status'] == result['status']
    assert replay['source_calls'] == 0 and len(h.queries()) == 1
    assert replay['handoff_sha256'] == expected
    (tmp_path/'Q2_RESULT.json').write_text(json.dumps(dict(result=result, replay=replay,
                                                         events=events), indent=2))


Q2_BUSY = r'''
if os.environ.get('CAMPAIGN_OFFLINE') == '1':
    import subprocess, signal
    from openbb_collector_core import raw_acquisition as qraw
    q2_replace = os.replace
    q2_checked = False
    def q2_check_replace(src, dst, *a, **kw):
        global q2_checked
        result = q2_replace(src, dst, *a, **kw)
        if Path(dst).name == 'admission-000000.json' and not q2_checked:
            doc = json.loads(Path(dst).read_bytes())['document']
            if doc['status'] != 'pending':
                return result
            q2_checked = True
            directory = Path(dst).parent
            root = directory.parent.parent
            command = [sys.executable, '-B', '-m', 'openbb_collector_core.baostock_campaign',
                       'verify', '--root', str(root),
                       '--handoff', str(directory/'handoff-000000.json'),
                       '--sha256', doc['handoff_sha256']]
            child = subprocess.run(command, env=os.environ, capture_output=True,
                                   text=True, timeout=30)
            log('q2_locked_reader', exit=child.returncode, stdout=child.stdout, stderr=child.stderr)
            if os.environ.get('Q2_BUSY_CANCEL') == '1':
                os.kill(os.getpid(), signal.SIGTERM)
        return result
    os.replace = q2_check_replace
'''


@pytest.mark.parametrize('cancel', [False, True])
def test_q2_independent_reader_busy_until_cutoff(tmp_path, cancel):
    h = Harness(tmp_path, [task(flags=('3',))])
    (tmp_path/'bootstrap/sitecustomize.py').write_text(BOOTSTRAP + Q2_BUSY)
    h.env['Q2_BUSY_CANCEL'] = '1' if cancel else '0'
    child, result = h.run()
    event, = [e for e in h.calls() if e['event']=='q2_locked_reader']
    assert event['exit'] == 75 and json.loads(event['stdout'])['error'] == 'acquisition_busy'
    assert child.returncode == (1 if cancel else 0)
    verified = q2_verify(
        h, h.state_path().with_name('handoff-000000.json'), result['handoff_sha256']
    )
    assert (verified.returncode == 0) == (not cancel)


def test_q2_prior_page_and_other_run_keep_eligibility(tmp_path):
    h = Harness(tmp_path, [task(flags=('3',), suffix=str(i)) for i in range(9)])
    first, first_result = h.run()
    assert first.returncode == 1 and first_result['status'] == 'partial'
    old = h.state_path().with_name('handoff-000000.json')
    preserved = {str(p): p.read_bytes() for p in h.private.rglob('*')
                 if p.is_file() and p.name not in MUTABLE_CONTROL}
    (tmp_path/'bootstrap/sitecustomize.py').write_text(BOOTSTRAP + Q2_BOUNDARIES)
    h.env.update(Q2_BOUNDARY='handoff_after', Q2_STYLE='sigterm')
    child, result = h.run()
    assert child.returncode == 1 and result['status'] == 'cancelled'
    assert q2_verify(h, old, first_result['handoff_sha256']).returncode == 0
    assert q2_verify(
        h, old.with_name('handoff-000001.json'), result['handoff_sha256']
    ).returncode != 0
    assert all(Path(p).read_bytes() == b for p, b in preserved.items())
    h.env['Q2_BOUNDARY'] = ''
    replay_child, replay = h.run()
    assert replay_child.returncode == 1 and replay['source_calls'] == 0
    fresh, fresh_result = h.run(run='another-run', slot='another-slot')
    assert fresh_result['status'] == 'partial' and fresh_result['source_calls'] == 8
    fresh_path = h.private/campaign.VERSION/campaign.digest('another-run')/'handoff-000000.json'
    assert q2_verify(h, fresh_path, fresh_result['handoff_sha256']).returncode == 0
    assert q2_verify(h, old, first_result['handoff_sha256']).returncode == 0


@pytest.mark.parametrize('fault', [
    'page_veto', 'run_veto', 'progress', 'pending_cleanup', 'pending_fsync_cleanup'
])
def test_q2_persistence_fault_preserves_exact_primary_and_denies(tmp_path, monkeypatch, fault):
    h = Harness(tmp_path, [task(flags=('3',))])
    child, _ = h.run(fault='crash_finalized')
    assert child.returncode == 88
    stop = __import__('threading').Event()
    primary = OSError('primary-private-text')
    cause = RuntimeError('cause-private-text')
    primary.__cause__ = cause
    secondary = OSError('secondary-private-text')
    install = raw._install
    write = campaign.Control.write
    close = os.close
    fsync = os.fsync
    armed = False

    def patched_install(path, *args, **kwargs):
        if path.name.startswith('handoff-'):
            result = install(path, *args, **kwargs)
            if not fault.startswith('pending_'):
                stop.set()
                raise primary
            return result
        if ((fault == 'page_veto' and path.name.startswith('veto-'))
                or (fault == 'run_veto' and path.name == 'cancel.json')):
            raise secondary
        return install(path, *args, **kwargs)

    def patched_write(self, path, document):
        nonlocal armed
        if fault == 'progress' and path.name == 'progress.json' and document['cancelled']:
            raise secondary
        if fault.startswith('pending_') and path.name.startswith('admission-'):
            armed = True
            try:
                return write(self, path, document)
            finally:
                armed = False
        return write(self, path, document)

    def patched_close(fd):
        result = close(fd)
        if armed:
            stop.set()
            raise secondary if fault == 'pending_fsync_cleanup' else primary
        return result

    def patched_fsync(fd):
        result = fsync(fd)
        if armed and fault == 'pending_fsync_cleanup' and os.path.isdir(f'/proc/self/fd/{fd}'):
            raise primary
        return result

    with monkeypatch.context() as m:
        for key, value in h.env.items():
            m.setenv(key, value)
        m.setattr(raw, '_install', patched_install)
        m.setattr(campaign.Control, 'write', patched_write)
        m.setattr(os, 'close', patched_close)
        m.setattr(os, 'fsync', patched_fsync)
        with pytest.raises(OSError) as raised:
            campaign.run(h.manifest, h.private, 'run1', 'slot1', stop=stop)
        assert raised.value is primary and primary.__cause__ is cause
        path = h.state_path().with_name('handoff-000000.json')
        expected = campaign.digest(json.loads(path.read_bytes()))
        assert q2_verify(h, path, expected).returncode != 0
        # Repeat the real producer under continuing storage/cancellation failure.
        try:
            replay = campaign.run(h.manifest, h.private, 'run1', 'slot1', stop=stop)
        except (OSError, campaign.CampaignError):
            pass
        else:
            assert replay['status'] == 'cancelled' and replay['source_calls'] == 0
        assert q2_verify(h, path, expected).returncode != 0
    assert len(h.queries()) == 1
    # Real independent verifier has no repair authority even after faults clear.
    assert q2_verify(h, path, expected).returncode != 0


@pytest.mark.parametrize('veto_name', ['veto-000000.json', 'cancel.json'])
def test_q2_failed_veto_remains_denied_with_fresh_stop(tmp_path, monkeypatch, veto_name):
    from threading import Event

    h = Harness(tmp_path, [task(flags=('3',))])
    child, _ = h.run(fault='crash_finalized')
    assert child.returncode == 88
    stop = Event()
    install = raw._install
    primary = OSError('private-veto-failure')

    def injection(path, *args, **kwargs):
        if path.name == veto_name:
            raise primary
        result = install(path, *args, **kwargs)
        if path.name.startswith('handoff-'):
            stop.set()
        return result

    with monkeypatch.context() as m:
        for key, value in h.env.items():
            m.setenv(key, value)
        m.setattr(raw, '_install', injection)
        with pytest.raises(OSError) as caught:
            campaign.run(h.manifest, h.private, 'run1', 'slot1', stop=stop)
        assert caught.value is primary
        path = h.state_path().with_name('handoff-000000.json')
        expected = campaign.digest(json.loads(path.read_bytes()))
        assert q2_verify(h, path, expected).returncode != 0
        # A fresh Event must not allow a producer to forget the pending decision,
        # even when ONLY veto writes keep failing and source capture was valid.
        with pytest.raises(OSError) as caught:
            campaign.run(h.manifest, h.private, 'run1', 'slot1', stop=Event())
        assert caught.value is primary
        assert q2_verify(h, path, expected).returncode != 0
    child, replay = h.run()
    assert child.returncode == 1 and replay['status'] == 'cancelled'
    assert replay['source_calls'] == 0 and len(h.queries()) == 1
    assert q2_verify(h, path, expected).returncode != 0


D1_BARRIERS = r'''
if os.environ.get('CAMPAIGN_OFFLINE') == '1':
    import stat
    d1_fsync = os.fsync
    def d1_sync(fd):
        target = Path(os.readlink('/proc/self/fd/'+str(fd)))
        directory = Path(os.environ['D1_DIRECTORY'])
        admission = directory/'admission-000000.json'
        if (admission.exists()
                and json.loads(admission.read_bytes())['document']['status']=='eligible'):
            targets = [admission, directory, directory.parent, directory.parent.parent,
                       directory.parent.parent.parent]
            if target in targets:
                kind = 'directory' if stat.S_ISDIR(os.fstat(fd).st_mode) else 'file'
                fail = os.environ.get('D1_FAIL') == str(target)
                log('d1_barrier', target=str(target), kind=kind, failing=fail)
                if fail:
                    raise OSError('D1 exact descriptor barrier failed')
        return d1_fsync(fd)
    os.fsync = d1_sync
'''


def d1_setup(h):
    directory = h.private/campaign.VERSION/campaign.digest('run1')
    (h.root/'bootstrap/sitecustomize.py').write_text(BOOTSTRAP + D1_BARRIERS)
    h.env['D1_DIRECTORY'] = str(directory)
    return directory


@pytest.mark.parametrize('failure', [None, 0, 1, 2, 3, 4])
def test_d1_cli_continuing_directory_failure_and_clear_recovery(tmp_path, failure):
    h = Harness(tmp_path, [task(flags=('3',))])
    directory = d1_setup(h)
    targets = [directory/'admission-000000.json', directory, directory.parent,
               h.private, h.private.parent]
    h.env['D1_FAIL'] = str(targets[failure]) if failure is not None else ''
    first_child, first = h.run()
    preserved = {str(p): p.read_bytes() for p in h.private.rglob('*') if p.is_file()}
    second_child, second = h.run()
    assert first_child.returncode == second_child.returncode == (2 if failure is not None else 0)
    if failure is not None:
        assert first['error'] == second['error'] == 'campaign_operational_error'
        assert 'handoff_sha256' not in second
    else:
        assert second['status'] == 'complete' and second['source_calls'] == 0
        assert second['metrics']['control_writes'] == 0
    assert preserved == {str(p): p.read_bytes() for p in h.private.rglob('*') if p.is_file()}
    events = [e for e in h.calls() if e['event']=='d1_barrier']
    assert sum(e['failing'] for e in events) == (2 if failure is not None else 0)
    h.env['D1_FAIL'] = ''
    recovered_child, recovered = h.run()
    assert recovered_child.returncode == 0 and recovered['status'] == 'complete'
    assert recovered['source_calls'] == 0 and len(h.queries()) == 1
    assert recovered['metrics']['control_writes'] == 0
    assert recovered['metrics']['control_fsyncs'] == 5
    assert preserved == {str(p): p.read_bytes() for p in h.private.rglob('*') if p.is_file()}
    verified = q2_verify(h, directory/'handoff-000000.json', recovered['handoff_sha256'])
    assert verified.returncode == 0
    (tmp_path/'D1_RESULT.json').write_text(json.dumps(dict(first=first, second=second,
        recovered=recovered, events=events), indent=2))


@pytest.mark.parametrize('target_index', range(5))
@pytest.mark.parametrize('secondary_close', [False, True])
def test_d1_exact_recovery_barriers_primary_and_read_only(tmp_path, monkeypatch,
                                                       target_index, secondary_close):
    from threading import Event

    h = Harness(tmp_path, [task(flags=('3',))])
    child, completed = h.run()
    assert child.returncode == 0
    directory = h.state_path().parent
    admission = directory/'admission-000000.json'
    targets = [admission, directory, directory.parent, h.private, h.private.parent]
    target = targets[target_index]
    before = {str(p): p.read_bytes() for p in h.private.rglob('*') if p.is_file()}
    primary = OSError('D1 exact primary')
    cause = RuntimeError('D1 exact cause')
    primary.__cause__ = cause
    secondary = OSError('D1 secondary native close')
    fsync, close, opened = os.fsync, os.close, Path.open
    hits, closed = [], []
    failed_fd = None

    def inject_sync(fd):
        nonlocal failed_fd
        if Path(os.readlink(f'/proc/self/fd/{fd}')) == target:
            failed_fd = fd
            hits.append(str(target))
            raise primary
        return fsync(fd)

    def inject_close(fd):
        nonlocal failed_fd
        result = close(fd)
        if secondary_close and fd == failed_fd:
            with pytest.raises(OSError):
                os.fstat(fd)
            failed_fd = None
            closed.append(fd)
            raise secondary
        return result

    class Stream:
        def __init__(self, stream):
            self.stream = stream
        def __enter__(self):
            return self.stream.__enter__()
        def __exit__(self, *args):
            nonlocal failed_fd
            fd = self.stream.fileno()
            result = self.stream.__exit__(*args)
            if secondary_close and fd == failed_fd:
                assert self.stream.closed
                failed_fd = None
                closed.append(fd)
                raise secondary
            return result

    def inject_open(path, *args, **kwargs):
        stream = opened(path, *args, **kwargs)
        return Stream(stream) if path == target else stream

    with monkeypatch.context() as m:
        m.setattr(os, 'fsync', inject_sync)
        m.setattr(os, 'close', inject_close)
        m.setattr(Path, 'open', inject_open)
        for _ in range(2):
            with pytest.raises(OSError) as caught:
                campaign.run(h.manifest, h.private, 'run1', 'slot1', stop=Event())
            assert caught.value is primary and caught.value.__cause__ is cause
        assert len(hits) == 2
        if secondary_close:
            assert len(closed) == 2
        # An already successful returned expected remains valid: reader cannot
        # infer a later producer's storage failure and must never repair it.
        sync_count = len(hits)
        campaign.verify_handoff(h.private, directory/'handoff-000000.json',
                                completed['handoff_sha256'])
        assert len(hits) == sync_count
    recovered = campaign.run(h.manifest, h.private, 'run1', 'slot1', stop=Event())
    assert recovered['status'] == 'complete' and recovered['source_calls'] == 0
    assert recovered['metrics']['control_fsyncs'] == 5
    assert before == {str(p): p.read_bytes() for p in h.private.rglob('*') if p.is_file()}
    assert len(h.queries()) == 1


@pytest.mark.parametrize('file_barrier', [False, True])
def test_d1_initial_decision_primary_and_fresh_event(tmp_path, monkeypatch, file_barrier):
    from threading import Event

    h = Harness(tmp_path, [task(flags=('3',))])
    child, _ = h.run(fault='crash_finalized')
    assert child.returncode == 88
    primary = OSError('D1 initial decision sync')
    cause = RuntimeError('D1 initial cause')
    primary.__cause__ = cause
    fsync = os.fsync
    hits = []

    def injection(fd):
        frame = sys._getframe(1)
        while frame is not None:
            if frame.f_code.co_name == 'write' and frame.f_code.co_filename == campaign.__file__:
                path, document = frame.f_locals['path'], frame.f_locals['document']
                if (path.name.startswith('admission-') and document['status']=='eligible'
                        and os.path.isdir(f'/proc/self/fd/{fd}') != file_barrier):
                    hits.append(str(path))
                    raise primary
            frame = frame.f_back
        return fsync(fd)

    with monkeypatch.context() as m:
        for key, value in h.env.items():
            m.setenv(key, value)
        m.setattr(os, 'fsync', injection)
        with pytest.raises(OSError) as caught:
            campaign.run(h.manifest, h.private, 'run1', 'slot1', stop=Event())
        assert caught.value is primary and caught.value.__cause__ is cause
        assert len(hits) == 1
    # A file failure before replacement retains pending and therefore the
    # pre-existing conservative denial. A replaced eligible keeps its decision.
    child, replay = h.run()
    assert child.returncode == (1 if file_barrier else 0)
    assert replay['status'] == ('cancelled' if file_barrier else 'complete')
    assert replay['source_calls'] == 0 and len(h.queries()) == 1
    verified = q2_verify(h, h.state_path().with_name('handoff-000000.json'),
                         replay['handoff_sha256'])
    assert (verified.returncode == 0) == (not file_barrier)


@pytest.mark.parametrize('field,value', [('page', False), ('status', 'pending'),
                                        ('handoff_sha256', '0'*64)])
def test_d1_conflicting_decision_refuses_before_sync_and_restores(tmp_path, monkeypatch,
                                                              field, value):
    h = Harness(tmp_path, [task(flags=('3',))])
    child, completed = h.run()
    assert child.returncode == 0
    directory = h.state_path().parent
    path = directory/'admission-000000.json'
    original = path.read_bytes()
    document = json.loads(original)['document']
    document[field] = value
    path.write_bytes(campaign.encode(dict(document=document, sha256=campaign.digest(document))))
    handoff = json.loads((directory/'handoff-000000.json').read_bytes())
    def forbidden(*args, **kwargs):
        pytest.fail('conflicting decision reached a durability barrier')
    with monkeypatch.context() as m:
        m.setattr(os, 'fsync', forbidden)
        with pytest.raises(campaign.CampaignError, match='campaign_admission_conflict'):
            with raw._ownership(h.private):
                campaign._confirm_admission(campaign.Control(), h.private, directory, handoff)
    path.write_bytes(original)
    child, replay = h.run()
    assert child.returncode == 0 and replay['source_calls'] == 0
    assert replay['handoff_sha256'] == completed['handoff_sha256']
    assert len(h.queries()) == 1


def test_d1_new_completion_barrier_failure_remains_operational(tmp_path, monkeypatch):
    from threading import Event

    h = Harness(tmp_path, [task(flags=('3',))])
    child, _ = h.run(fault='crash_finalized')
    assert child.returncode == 88
    directory = h.state_path().parent
    fsync = os.fsync
    primary = OSError('D1 added completion directory barrier')
    cause = RuntimeError('D1 added completion cause')
    primary.__cause__ = cause
    stop = Event()
    hits = []
    def injection(fd):
        frame = sys._getframe(1)
        in_confirmation = False
        while frame is not None:
            in_confirmation |= (frame.f_code.co_name == '_confirm_admission'
                                and frame.f_code.co_filename == campaign.__file__)
            frame = frame.f_back
        if in_confirmation and Path(os.readlink(f'/proc/self/fd/{fd}')) == directory:
            hits.append(str(directory))
            stop.set()  # after cutoff on first invocation: storage still fails
            raise primary
        return fsync(fd)
    with monkeypatch.context() as m:
        for key, value in h.env.items():
            m.setenv(key, value)
        m.setattr(os, 'fsync', injection)
        for _ in range(2):
            stop = Event()
            with pytest.raises(OSError) as caught:
                campaign.run(h.manifest, h.private, 'run1', 'slot1', stop=stop)
            assert caught.value is primary and caught.value.__cause__ is cause
        assert len(hits) == 2
    assert not h.state()['cancelled']
    before = {str(p): p.read_bytes() for p in h.private.rglob('*') if p.is_file()}
    child, recovered = h.run()
    assert child.returncode == 0 and recovered['source_calls'] == 0
    assert before == {str(p): p.read_bytes() for p in h.private.rglob('*') if p.is_file()}
    assert len(h.queries()) == 1


def test_d1_recovery_holds_producer_lock_through_barriers(tmp_path, monkeypatch):
    h = Harness(tmp_path, [task(flags=('3',))])
    child, completed = h.run()
    assert child.returncode == 0
    path = h.state_path().with_name('handoff-000000.json')
    admission = path.with_name('admission-000000.json')
    fsync = os.fsync
    exits = []
    def injection(fd):
        if Path(os.readlink(f'/proc/self/fd/{fd}')) == admission:
            verified = q2_verify(h, path, completed['handoff_sha256'])
            exits.append(verified.returncode)
        return fsync(fd)
    with monkeypatch.context() as m:
        m.setattr(os, 'fsync', injection)
        result = campaign.run(h.manifest, h.private, 'run1', 'slot1')
    assert exits == [75] and result['source_calls'] == 0
    assert q2_verify(h, path, result['handoff_sha256']).returncode == 0


def test_d1_later_page_storage_failure_preserves_prior_success(tmp_path):
    h = Harness(tmp_path, [task(flags=('3',), suffix=str(i)) for i in range(9)])
    first_child, first = h.run()
    assert first_child.returncode == 1 and first['status'] == 'partial'
    directory = h.state_path().parent
    before = {str(p): p.read_bytes() for p in h.private.rglob('*')
              if p.is_file() and p.name not in MUTABLE_CONTROL}
    # Same real descriptor fault, aimed only at page one's selected decision.
    extension = D1_BARRIERS.replace('admission-000000.json', 'admission-000001.json')
    (h.root/'bootstrap/sitecustomize.py').write_text(BOOTSTRAP + extension)
    h.env.update(D1_DIRECTORY=str(directory), D1_FAIL=str(directory))
    child, failed = h.run()
    assert child.returncode == 2 and failed['error'] == 'campaign_operational_error'
    assert q2_verify(h, directory/'handoff-000000.json', first['handoff_sha256']).returncode == 0
    assert all(Path(p).read_bytes() == content for p, content in before.items())
    child, failed = h.run()
    assert child.returncode == 2 and len(h.queries()) == 9
    h.env['D1_FAIL'] = ''
    child, replay = h.run()
    assert child.returncode == 0 and replay['source_calls'] == 0
    assert q2_verify(h, directory/'handoff-000001.json', replay['handoff_sha256']).returncode == 0


def test_daily_verification_checks_failure_on_earlier_page(tmp_path):
    from openbb_collector_core.baostock_daily_plan import verify_campaign

    tasks = [task(f"sh.{i:06d}", ("2" if i == 0 else "3",)) for i in range(9)]
    h = Harness(tmp_path, tasks=tasks, attempts=1)
    _, first = h.run("flag2")
    assert first["page"] == 1 and first["outcome"] == "closed_with_failures"
    _, final = h.run()
    assert final["status"] == "complete" and final["outcome"] == "complete"
    result = verify_campaign(h.manifest, h.private, "run1", "slot1")
    assert result["status"] == "failed"
    assert [failure["page"] for failure in result["failures"]] == [0]
    before = len(h.queries())
    _, repeated = h.run()
    assert repeated["status"] == "complete" and len(h.queries()) == before
    assert verify_campaign(h.manifest, h.private, "run1", "slot1") == result

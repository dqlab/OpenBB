"""v3 session acceptance: one login per page, real subprocess harness.

Every campaign/CLI assertion below runs through the actual spawn worker and the
offline BaoStock SDK seam of ``test_baostock_campaign``. Only the two provider
unit tests at the end call the library directly; they are labelled as such.
"""

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from test_baostock_campaign import BOOTSTRAP, Harness, task  # noqa: E402

from openbb_collector_core import baostock_campaign as campaign  # noqa: E402
from openbb_collector_core import baostock_page_capture as page_capture  # noqa: E402

# A deterministic parent-side kill: the campaign process exits immediately after
# the Nth attempt's first page begin is durable, so the in-flight attempt keeps
# its authority (and that receipt prefix) with no terminal, exactly like a crash.
SESSION_FAULT = r'''
if os.environ.get("CAMPAIGN_OFFLINE") == "1":
    from openbb_collector_core import baostock_page_capture as s
    if os.environ.get("FAULT") == "session_link_kill":
        # Deterministic parent-side crash: the campaign process exits right after
        # the Nth attempt's eligibility marker is durable, so that attempt is
        # complete but its producer never recorded a link.
        s_install = s.raw._install
        s_seen = {"eligible": 0}
        def session_install(path, *args, **kwargs):
            result = s_install(path, *args, **kwargs)
            if path.name == "eligible.json":
                s_seen["eligible"] += 1
                if s_seen["eligible"] == int(os.environ["KILL_ELIGIBLE"]):
                    os._exit(87)
            return result
        s.raw._install = session_install
    if os.environ.get("FAULT") == "session_worker_kill":
        # The SDK seam kills only the capture worker, during one task's query,
        # so the campaign must observe a dead session instead of a terminal.
        from openbb_baostock.utils import helpers as s_helpers
        s_resolve = s_helpers.resolve_method
        def session_resolve(method):
            s_call = s_resolve(method)
            def guarded(**params):
                if params.get("code") == os.environ.get("KILL_TASK_CODE"):
                    os._exit(89)
                return s_call(**params)
            return guarded
        s_helpers.resolve_method = session_resolve
'''


def tasks(count, flag_at=None):
    return [
        task(
            code=f"sh.60000{index}",
            flags=("2",) if index == flag_at else ("3",),
            suffix=f":{index}",
        )
        for index in range(count)
    ]


def harness(tmp_path, count=4, flag_at=None, **kwargs):
    h = Harness(tmp_path, tasks(count, flag_at), **kwargs)
    (tmp_path / "bootstrap/sitecustomize.py").write_text(BOOTSTRAP + SESSION_FAULT)
    return h


def events(h, name):
    return [c for c in h.calls() if c["event"] == name]


def run_dir(h, run="run1"):
    return h.private / campaign.VERSION / campaign.digest(run)


def progress(h, run="run1"):
    return json.loads((run_dir(h, run) / "progress.json").read_bytes())["document"]


def links(h, run="run1"):
    """Links of the current (unsealed) page in the run's progress document."""
    return [entry["links"] for entry in progress(h, run)["entries"]]


def sealed(h, run="run1"):
    """Links of the sealed page: progress drops its entries when it advances."""
    path = run_dir(h, run) / "handoff-000000.json"
    return [entry["links"] for entry in json.loads(path.read_bytes())["entries"]]


def attempt_tasks(h, run="run1", attempt=1):
    """Rebuild the page's attempt tasks exactly as the campaign derives them."""
    page = json.loads((h.manifest / "page-000000.json").read_bytes())
    binding = campaign.digest(json.loads((run_dir(h, run) / "authority.json").read_bytes()))
    return [
        campaign.attempt_task(binding, 0, original, request, attempt)
        for original, _, request in campaign.requests(page)
    ]


def attempt_dir(h, link):
    return h.private / link["locator"]


def body(h, link):
    return (attempt_dir(h, link) / "page-000.body").read_bytes()


def receipt(h, link):
    document = json.loads((attempt_dir(h, link) / "page-000.receipt.json").read_bytes())
    for name in ("identity", "begin_sha256", "previous_page_root", "fragment_root"):
        document.pop(name)
    return document


def verify(h, result, run="run1"):
    path = run_dir(h, run) / "handoff-000000.json"
    child = h.invoke(
        [
            "verify",
            "--root",
            str(h.private),
            "--handoff",
            str(path),
            "--sha256",
            result["handoff_sha256"],
        ]
    )
    assert child.returncode == 0, child.stdout + child.stderr
    return campaign.verify_handoff(h.private, path, result["handoff_sha256"])


def test_session_one_login_per_page_installs_every_attempt(tmp_path):
    h = harness(tmp_path)
    child, result = h.run()
    assert child.returncode == 0, child.stdout + child.stderr
    assert result["status"] == "complete"
    # One login and one logout for all four tasks; the v2.2 cost was eight.
    assert len(events(h, "login")) == 1
    assert len(events(h, "logout")) == 1
    assert len(h.queries()) == 4
    assert result["source_calls"] == 4
    assert result["session"] == dict(
        tasks=4,
        reason="complete",
        error=None,
        cleanup="ok",
        logins=1,
        logouts=1,
        interrupted=None,
    )
    for entry in sealed(h):
        assert len(entry) == 1
        link = entry[0]
        assert link["status"] == "success" and link["attempt"] == 1
        directory = attempt_dir(h, link)
        for name in (
            "authority.json",
            "page-000.begin.json",
            "page-000.body",
            "page-000.fragments.jsonl",
            "page-000.receipt.json",
            "terminal.json",
            "eligible.json",
        ):
            assert (directory / name).is_file(), name
        assert not (directory / ".capacity").stat().st_size
    document = verify(h, result)
    assert document["version"] == campaign.HANDOFF_VERSION
    assert document["outcome"] == "complete" and document["failures"] == []
    assert [entry["attempts"] for entry in document["entries"]] == [1, 1, 1, 1]


def test_session_budget_is_login_logout_plus_observed_calls(tmp_path):
    h = harness(tmp_path)
    child, result = h.run()
    assert child.returncode == 0, child.stdout + child.stderr
    ledger = json.loads((run_dir(h) / "budget.json").read_bytes())["document"]
    assert len(h.queries()) == 4 and events(h, "next") == []
    # 1 login + 1 logout + four queries. The per-request path paid 2 x 4 + 4.
    assert ledger["calls"] == 2 + 4
    assert result["daily_calls"] == 6
    assert result["sdk_query_calls"] == 4 and result["sdk_next_calls"] == 0
    assert result["daily_budget_exhausted"] is False


def test_session_login_refusal_spends_no_attempt(tmp_path):
    h = harness(tmp_path)
    child, result = h.run("login_failure")
    assert child.returncode == 1
    assert result["status"] == "partial"
    assert len(events(h, "login")) == 1
    assert events(h, "logout") == []
    assert result["source_calls"] == 0 and h.queries() == []
    assert result["session"]["error"] == "401"
    assert result["session"]["logins"] == 1 and result["session"]["logouts"] == 0
    # No logout was needed, and the socket teardown itself succeeded.
    assert result["session"]["cleanup"] == "ok"
    # Nothing was attempted: no authority, no attempt, no reservation.
    capture_root = h.private / page_capture.VERSION
    assert not capture_root.exists() or not list(capture_root.rglob("authority.json"))
    state = progress(h)
    assert [entry["attempts"] for entry in state["entries"]] == [0, 0, 0, 0]
    assert [entry["links"] for entry in state["entries"]] == [[], [], [], []]
    assert state["reserved"] == 0 and state["active"] is None
    backoff = json.loads((run_dir(h) / "backoff.json").read_bytes())["document"]
    assert backoff["reason"] == "login_failed"
    # A refused login is charged once, and the next invocation asks nothing.
    assert json.loads((run_dir(h) / "budget.json").read_bytes())["document"]["calls"] == 1
    again, replay = h.run()
    assert again.returncode == 1 and replay["source_calls"] == 0
    assert len(events(h, "login")) == 1 and h.queries() == []


def test_session_mid_page_death_resumes_with_two_logins(tmp_path):
    h = harness(tmp_path, attempts=1)
    h.env["KILL_ELIGIBLE"] = "3"
    child, _ = h.run(fault="session_link_kill")
    assert child.returncode == 87, child.stdout + child.stderr
    third, fourth = attempt_tasks(h)[2:4]
    assert len(h.queries()) == 3
    state = progress(h)
    # The two completed attempts are durable links with no live reservation.
    assert [entry["links"][0]["status"] for entry in state["entries"][:2]] == [
        "success",
        "success",
    ]
    assert state["active"] == dict(entry=2, attempt=1)
    # The third attempt is proven on disk but its link was never recorded.
    assert state["entries"][2]["links"] == [] and state["entries"][2]["attempts"] == 1
    assert state["entries"][3]["attempts"] == 0
    third_dir = h.private / page_capture.capture_locator(third)
    assert (third_dir / "authority.json").is_file()
    assert (third_dir / "eligible.json").is_file()
    assert page_capture.read_capture(h.private, third)["complete"] is True
    assert not (h.private / page_capture.capture_locator(fourth)).exists()
    h.env["KILL_ELIGIBLE"] = ""
    resumed, result = h.run()
    assert resumed.returncode == 0, resumed.stdout + resumed.stderr
    assert result["status"] == "complete"
    # One session, then one recovered attempt plus one single request.
    assert len(events(h, "login")) == 2 and len(events(h, "logout")) == 2
    assert result["session"] is None and result["source_calls"] == 1
    document = verify(h, result)
    assert [entry["links"][-1]["status"] for entry in document["entries"]] == [
        "success"
    ] * 4
    # The recovered attempt reuses its own locator: nothing was refetched.
    assert document["entries"][2]["links"][0]["locator"] == page_capture.capture_locator(
        third
    )
    assert document["outcome"] == "complete" and document["failures"] == []


def test_session_in_flight_attempt_keeps_only_authority(tmp_path):
    h = harness(tmp_path, attempts=1)
    h.env["KILL_TASK_CODE"] = "sh.600002"
    child, result = h.run(fault="session_worker_kill")
    # The campaign survives the dead session and declares what it lost.
    assert child.returncode == 1, child.stdout + child.stderr
    assert result["status"] == "partial"
    assert result["session"]["reason"] == "worker_died"
    assert result["session"]["interrupted"] == 2
    assert result["session"]["cleanup"] is None
    assert len(h.queries()) == 2
    third, fourth = attempt_tasks(h)[2:4]
    state = progress(h)
    assert state["active"] == dict(entry=2, attempt=1)
    assert state["entries"][2]["links"] == [] and state["entries"][3]["attempts"] == 0
    third_dir = h.private / page_capture.capture_locator(third)
    assert (third_dir / "authority.json").is_file()
    for name in ("page-000.begin.json", "page-000.receipt.json", "terminal.json"):
        assert not (third_dir / name).exists(), name
    assert not (h.private / page_capture.capture_locator(fourth)).exists()
    # The retained authority alone is enough for the producer to prove it later.
    captured = page_capture.read_capture(h.private, third)
    assert captured["complete"] is False and captured["terminal"] is None
    h.env["KILL_TASK_CODE"] = ""
    resumed, result = h.run()
    assert resumed.returncode == 0, resumed.stdout + resumed.stderr
    document = verify(h, result)
    assert document["outcome"] == "closed_with_failures"
    assert document["failures"] == [
        dict(
            index=2,
            attempt=1,
            status="failed",
            locator=page_capture.capture_locator(third),
            terminal_sha256=None,
        )
    ]
    assert document["entries"][3]["links"][0]["status"] == "success"
    assert len(events(h, "login")) == 2


def test_session_pages_zero_restores_single_request_path(tmp_path):
    h = harness(tmp_path)
    h.env["OPENBB_CAMPAIGN_SESSION_PAGES"] = "0"
    child, result = h.run()
    assert child.returncode == 0, child.stdout + child.stderr
    assert result["session"] is None
    # The v2.2 behaviour: one login and one logout per request.
    assert len(events(h, "login")) == 4 and len(events(h, "logout")) == 4
    assert len(h.queries()) == 4 and result["source_calls"] == 4
    assert result["daily_calls"] == 2 * 4 + 4
    single = sorted(body(h, link) for entry in sealed(h) for link in entry)
    single_receipts = sorted(
        json.dumps(receipt(h, link), sort_keys=True)
        for entry in sealed(h)
        for link in entry
    )
    verify(h, result)
    # The same tasks through one session must produce the same source evidence.
    h.env["OPENBB_CAMPAIGN_SESSION_PAGES"] = "1"
    other, session_result = h.run(run="run2", slot="slot2")
    assert other.returncode == 0, other.stdout + other.stderr
    assert session_result["session"]["logins"] == 1
    session = sorted(body(h, link) for entry in sealed(h, "run2") for link in entry)
    session_receipts = sorted(
        json.dumps(receipt(h, link), sort_keys=True)
        for entry in sealed(h, "run2")
        for link in entry
    )
    assert session == single
    assert session_receipts == single_receipts
    assert session_result["daily_calls"] == 2 + 4


def test_session_single_task_data_error_does_not_abort_session(tmp_path):
    h = harness(tmp_path, flag_at=2)
    child, result = h.run("flag2")
    assert child.returncode == 1
    assert result["status"] == "partial"
    # The refused task did not end the session: all four were admitted.
    assert result["session"]["reason"] == "complete"
    assert len(events(h, "login")) == 1 and len(h.queries()) == 4
    state = progress(h)
    assert [entry["links"][-1]["status"] for entry in state["entries"]] == [
        "success",
        "success",
        "failed",
        "success",
    ]
    assert [entry["attempts"] for entry in state["entries"]] == [1, 1, 1, 1]
    assert state["page"] == 0 and state["reserved"] == 4
    # The failed entry keeps the v2.2 data-attempt cap and is retried later.
    nxt, result = h.run()
    assert nxt.returncode == 0, nxt.stdout + nxt.stderr
    assert result["source_calls"] == 1 and result["session"] is None
    document = verify(h, result)
    assert document["entries"][2]["links"][-1]["status"] == "success"
    assert document["entries"][2]["attempts"] == 2
    assert document["outcome"] == "complete" and document["failures"] == []
    assert len(events(h, "login")) == 2


def test_session_and_single_request_evidence_are_identical(tmp_path):
    h = harness(tmp_path)
    child, session_result = h.run(run="session-run", slot="session-slot")
    assert child.returncode == 0, child.stdout + child.stderr
    session_bodies = sorted(
        body(h, link) for entry in sealed(h, "session-run") for link in entry
    )
    h.env["OPENBB_CAMPAIGN_SESSION_PAGES"] = "0"
    child, single_result = h.run(run="single-run", slot="single-slot")
    assert child.returncode == 0, child.stdout + child.stderr
    single_bodies = sorted(
        body(h, link) for entry in sealed(h, "single-run") for link in entry
    )
    # Byte equality of the source envelope: only transport facts may differ.
    assert session_bodies == single_bodies
    for payload in single_bodies:
        document = json.loads(payload)
        assert document["version"] in (
            "baostock-source-evidence-v1",
            "baostock-source-evidence-v2",
        )
        # One durable page observation; the terminal summary owns completion.
        assert document["status"] == "partial"
        assert document["reason"] == "page_observation"
        assert document["cleanup"] == "ok" and document["error_code"] == "0"
        assert document["pages"][0]["rows"] == [
            ["2024-01-02", "1.2300", "", "EXTRA"],
            ["short"],
        ]
    assert session_result["session"]["cleanup"] == "ok"
    assert single_result["session"] is None


def test_session_logout_failure_keeps_evidence_complete(tmp_path):
    h = harness(tmp_path)
    child, result = h.run("logout_failure")
    assert child.returncode == 0, child.stdout + child.stderr
    assert result["status"] == "complete"
    # The session teardown is recorded honestly; no attempt is downgraded.
    assert result["session"]["cleanup"] == "logout_failed"
    assert result["session"]["logouts"] == 1
    assert len(events(h, "login")) == 1 and len(events(h, "logout")) == 1
    document = verify(h, result)
    assert [entry["links"][0]["status"] for entry in document["entries"]] == ["success"] * 4
    assert document["outcome"] == "complete" and document["failures"] == []
    assert [entry["attempts"] for entry in document["entries"]] == [1, 1, 1, 1]
    for entry in document["entries"]:
        terminal = json.loads((attempt_dir(h, entry["links"][0]) / "terminal.json").read_bytes())
        assert terminal["status"] == "complete"
        assert terminal["summary"]["cleanup"] == "ok"


def test_invalid_session_pages_knob_refuses_before_writes(tmp_path):
    h = harness(tmp_path)
    h.env["OPENBB_CAMPAIGN_SESSION_PAGES"] = "2"
    before = {str(p): p.read_bytes() for p in h.private.rglob("*") if p.is_file()}
    child, result = h.run()
    assert child.returncode == 2 and result["error"] == "invalid_session_pages"
    assert before == {str(p): p.read_bytes() for p in h.private.rglob("*") if p.is_file()}
    assert h.queries() == []


# --- Direct-call provider contract (no subprocess; labelled as such) ---------


def test_source_session_reuses_one_connection_directly(monkeypatch):
    from openbb_baostock.utils import helpers, source_evidence

    calls = []

    class Result:
        error_code = "0"
        fields = ["date"]
        data = [["2024-01-02"]]
        cur_row_num = 0

        def next(self):
            return False

    class Socket:
        def close(self):
            calls.append("close")

    def login():
        helpers.context.default_socket = Socket()
        calls.append("login")
        return Result()

    def logout():
        calls.append("logout")
        return Result()

    monkeypatch.setattr(helpers.bs, "login", login)
    monkeypatch.setattr(helpers.bs, "logout", logout)
    monkeypatch.setattr(helpers, "resolve_method", lambda method: lambda **params: Result())
    assert source_evidence.active_session() is None
    first = source_evidence._query_values("BaostockStockBasic", {"code": "sh.600000"})
    assert calls == ["login", "logout", "close"]
    assert source_evidence.active_session() is None
    calls.clear()
    with source_evidence.source_session() as (ok, code):
        assert ok is True and code is None
        session = source_evidence.active_session()
        assert session is not None and session.ok is True
        inside = source_evidence._query_values("BaostockStockBasic", {"code": "sh.600000"})
        assert calls == ["login"]
        assert source_evidence.active_session() is session
        with pytest.raises(ValueError, match="nested_source_session"):
            with source_evidence.source_session():
                pass
    # One login for the scope, then exactly one logout and one socket close.
    assert calls == ["login", "logout", "close"]
    assert session.login_calls == 1 and session.logout_calls == 1
    assert inside["status"] == "complete" and inside["cleanup"] == "ok"
    assert first["status"] == "complete"
    assert source_evidence.active_session() is None


def test_source_session_spans_the_sdk_thread_directly(monkeypatch):
    """The session capability must cross ``asyncio.to_thread``, not the TSD.

    The collector opens the session on the thread that then runs the event loop,
    and `query_evidence` hops to a helper thread for the SDK itself. Losing the
    capability on that hop would make the helper re-acquire the non-reentrant
    session lock, so this exact call order must not hang.
    """
    import asyncio
    import threading

    from openbb_baostock.utils import helpers, source_evidence

    calls = []

    class Result:
        error_code = "0"
        fields = ["date"]
        data = [["2024-01-02"]]
        cur_row_num = 0

        def next(self):
            return False

    monkeypatch.setattr(helpers.bs, "login", lambda: (calls.append("login"), Result())[1])
    monkeypatch.setattr(helpers.bs, "logout", lambda: (calls.append("logout"), Result())[1])
    monkeypatch.setattr(helpers, "resolve_method", lambda method: lambda **params: Result())
    seen = {}

    def session_on_one_thread():
        """Exactly the collector's topology: session and event loop, one thread."""
        try:
            with source_evidence.source_session() as (ok, code):
                seen["ok"], seen["code"] = ok, code
                seen["wire"] = asyncio.run(
                    source_evidence.query_evidence("BaostockStockBasic", {"code": "sh.600000"})
                )
        except Exception as exc:  # noqa: BLE001 - surfaced as a test failure below
            seen["error"] = exc

    worker = threading.Thread(target=session_on_one_thread)
    worker.start()
    worker.join(30)
    assert not worker.is_alive(), "the in-session SDK query deadlocked"
    assert "error" not in seen, seen.get("error")
    assert seen["ok"] is True and seen["code"] is None
    assert seen["wire"]["status"] == "complete"
    assert seen["wire"]["reason"] == "exhausted"
    # One login for the scope, one logout, and nothing in between.
    assert calls == ["login", "logout"]


def test_source_session_refusal_and_logout_failure_directly(monkeypatch):
    from openbb_baostock.utils import helpers, source_evidence

    class Result:
        def __init__(self, code):
            self.error_code = code
            self.fields = ["date"]
            self.data = []
            self.cur_row_num = 0

        def next(self):
            return False

    calls = []
    monkeypatch.setattr(
        helpers.bs, "login", lambda: (calls.append("login"), Result("401"))[1]
    )
    monkeypatch.setattr(
        helpers.bs, "logout", lambda: (calls.append("logout"), Result("0"))[1]
    )
    with source_evidence.source_session() as (ok, code):
        assert ok is False and code == "401"
        envelope = source_evidence._query_values("BaostockStockBasic", {"code": "sh.600000"})
        assert envelope["reason"] == "login_failed" and envelope["error_code"] == "401"
        assert envelope["pages"] == []
    # A refused session never logs out and never asks the source.
    assert calls == ["login"]

    def broken_logout():
        raise OSError("private-text-must-not-leak")

    monkeypatch.setattr(helpers.bs, "login", lambda: Result("0"))
    monkeypatch.setattr(helpers.bs, "logout", broken_logout)
    with source_evidence.source_session() as (ok, code):
        assert ok is True
        session = source_evidence.active_session()
    assert session.cleanup == "logout_failed" and session.logout_calls == 1
    assert source_evidence.active_session() is None

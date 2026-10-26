"""Campaign v2.2 rate policy: error-code grading, retry ledger, budget, latency.

Synthetic ONLY at the BaoStock SDK seam, through the same real subprocess
harness as the campaign acceptance suite (an appended sitecustomize wrapper that
stamps one authored `error_code` on the fake SDK result). Every claim below is
asserted against retained state: the sealed handoff, the retry ledger, the
control documents, or the recorded source-call counts.
"""

import json
import time

import pytest
from test_baostock_campaign import BOOTSTRAP, Harness, evidence_for_link, task

from openbb_collector_core import baostock_campaign as campaign

# The fake BaoStock SDK is already wired by BOOTSTRAP. This wrapper only chooses
# which authored error code the fake returns, so the campaign sees a real
# envelope at the real seam.
CODE_PATCH = r"""
import os as _os
from openbb_baostock.utils import helpers as _helpers

_original_login = _helpers.bs.login
def _login():
    result = _original_login()
    forced = _os.environ.get("FORCED_LOGIN_CODE", "")
    if forced:
        result.error_code = forced
    return result
_helpers.bs.login = _login

_original_resolve = _helpers.resolve_method
def _resolve(method):
    inner = _original_resolve(method)
    def call(**params):
        result = inner(**params)
        forced = _os.environ.get("FORCED_CODE", "")
        if forced and params.get("adjustflag") == _os.environ.get("FORCED_FLAG", "2"):
            result.error_code = forced
            result.data = [["FAILED_BODY_SENTINEL"]]
        return result
    return call
_helpers.resolve_method = _resolve
"""


class RateHarness(Harness):
    """The v2 acceptance harness with one forced source error code per test."""

    def __init__(self, root, tasks=None, **kwargs):
        super().__init__(root, tasks, **kwargs)
        (self.root / "bootstrap/sitecustomize.py").write_text(BOOTSTRAP + CODE_PATCH)


def run_directory(h):
    return h.private / campaign.VERSION / campaign.digest("run1")


def control_file(h, name):
    return run_directory(h) / name


def progress_path(h):
    return control_file(h, "progress.json")


def relax_due(h):
    """Clear a policy delay so the next invocation may retry immediately."""
    state = h.state()
    for entry in state["entries"]:
        entry["due"] = 0
    campaign.Control().write(progress_path(h), state)


def ledger(h):
    path = control_file(h, "transport.jsonl")
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines()]


def backoff_document(h):
    return campaign.Control().state(control_file(h, "backoff.json"))


def budget_document(h):
    return campaign.Control().state(control_file(h, "budget.json"))


def latency_document(h):
    return campaign.Control().state(control_file(h, "latency.json"))


def logins(h):
    return [call for call in h.calls() if call["event"] == "login"]


# 1 ---------------------------------------------------------------------------
def test_blacklist_lockout_stops_the_run_and_holds_the_source(tmp_path):
    # The blacklisted request is first in the page, so one refusal is enough to
    # prove the invocation stops instead of spending the second request.
    h = RateHarness(tmp_path, [task(flags=("2", "3"))])
    h.env["FORCED_CODE"] = "10001011"
    child, summary = h.run()
    assert child.returncode == 1 and summary["status"] == "partial"
    assert summary["source_calls"] == 1 and len(h.queries()) == 1
    assert [entry["attempts"] for entry in h.state()["entries"]] == [1, 0]
    document = backoff_document(h)
    assert document["reason"] == "blacklist"
    assert document["until_ns"] - time.time_ns() >= 6 * 3600 * 10**9 - 60 * 10**9
    assert campaign.Control().state(control_file(h, "blacklist-count.json")) == dict(
        year=time.localtime().tm_year, count=1
    )
    # While the lockout holds, this runs asks the source nothing at all.
    child, throttled = h.run()
    assert child.returncode == 1 and throttled["source_calls"] == 0
    assert len(h.queries()) == 1 and len(logins(h)) == 1
    # A second hit in the same year doubles the authored duration.
    campaign.Control().write(
        control_file(h, "backoff.json"),
        campaign.backoff_document(document["binding"], time.time_ns() - 1, "blacklist"),
    )
    child, second = h.run()
    assert second["source_calls"] == 1 and second["status"] == "partial"
    document = backoff_document(h)
    assert document["reason"] == "blacklist"
    assert document["until_ns"] - time.time_ns() >= 12 * 3600 * 10**9 - 60 * 10**9
    assert campaign.Control().state(control_file(h, "blacklist-count.json"))["count"] == 2


def test_blacklist_duration_grows_per_hit_is_bounded_and_resets_with_the_year(tmp_path):
    directory = tmp_path / "run"
    directory.mkdir()
    io = campaign.Control()
    for hit in range(1, 10):
        seconds = campaign.blacklist_hit(io, directory, campaign.DEFAULT_BLACKLIST_SECONDS)
        assert seconds == min(6 * 3600 * hit, 48 * 3600)
    assert io.state(campaign.blacklist_path(directory)) == dict(
        year=time.localtime().tm_year, count=9
    )
    # A stale count from another year never inflates this year's first hit.
    io.write(campaign.blacklist_path(directory), dict(year=time.localtime().tm_year - 1, count=9))
    assert campaign.blacklist_hit(io, directory, campaign.DEFAULT_BLACKLIST_SECONDS) == 6 * 3600


# 2 ---------------------------------------------------------------------------
def test_transport_failure_stays_retryable_without_spending_data_attempts(tmp_path):
    h = RateHarness(tmp_path, [task(flags=("3", "2"))])
    h.env["FORCED_CODE"] = "10002007"
    summaries = []
    for number in range(5):
        child, summary = h.run()
        summaries.append((child, summary))
        assert child.returncode == 1 and summary["status"] == "partial"
        if number < 4:
            relax_due(h)
    assert [summary["source_calls"] for _, summary in summaries] == [2, 1, 1, 1, 1]
    # Five network failures are five real attempts, and still no dead letter.
    state = h.state()
    assert state["page"] == 0 and not control_file(h, "handoff-000000.json").exists()
    entry = state["entries"][1]
    assert entry["attempts"] == len(entry["links"]) == 5
    assert [link["status"] for link in entry["links"]] == ["failed"] * 5
    assert entry["due"] > time.time_ns()  # delayed, never dropped
    records = ledger(h)
    assert len(records) == 5 and {record["code"] for record in records} == {"10002007"}
    assert [record["index"] for record in records] == [1] * 5
    assert [record["attempt"] for record in records] == [1, 2, 3, 4, 5]
    # The ledger grew; the authored data attempt budget did not move.
    retries = campaign.transport_read(run_directory(h))
    assert campaign.data_attempts(entry, retries[(0, 1)]) == 0
    assert campaign.entry_terminal(entry, 3, retries[(0, 1)]) is False
    # The authored 30/60/120s exponential delay is capped, never unbounded.
    assert [campaign.retry_delay(count) for count in (1, 2, 3, 4, 5)] == [30, 60, 120, 120, 120]


# 3 ---------------------------------------------------------------------------
def test_transport_cap_seals_a_dead_letter_at_the_real_link(tmp_path):
    h = RateHarness(tmp_path, [task(flags=("3", "2"))])
    h.env["FORCED_CODE"] = "10002007"
    h.env["OPENBB_CAMPAIGN_TRANSPORT_CAP"] = "3"
    summaries = []
    for _ in range(3):
        child, summary = h.run()
        summaries.append(summary)
        if summary["status"] != "partial":
            break
        relax_due(h)
    assert [summary["source_calls"] for summary in summaries] == [2, 1, 1]
    final = summaries[-1]
    assert final["status"] == "complete" and final["outcome"] == "closed_with_failures"
    document = json.loads(control_file(h, "handoff-000000.json").read_bytes())
    assert document["version"] == campaign.HANDOFF_VERSION
    entry = document["entries"][1]
    link = entry["links"][-1]
    assert entry["attempts"] == len(entry["links"]) == 3
    assert document["failures"] == [
        dict(
            index=1,
            attempt=link["attempt"],
            status="failed",
            locator=link["locator"],
            terminal_sha256=link["terminal_sha256"],
        )
    ]
    verified = campaign.verify_handoff(
        h.private, control_file(h, "handoff-000000.json"), final["handoff_sha256"]
    )
    assert verified["outcome"] == "closed_with_failures"
    # The declared failure points at retained evidence that really is a failure.
    envelope = evidence_for_link(h.private, link)
    assert envelope["error_code"] == "10002007" and envelope["status"] == "failed"
    records = ledger(h)
    assert [record["attempt"] for record in records] == [1, 2, 3]
    assert {record["code"] for record in records} == {"10002007"}


# 4 ---------------------------------------------------------------------------
def test_data_error_consumes_the_authored_attempt_cap(tmp_path):
    h = RateHarness(tmp_path, [task(flags=("3", "2"))], attempts=3)
    h.env["FORCED_CODE"] = "10004001"
    child, first = h.run()
    assert child.returncode == 1 and first["status"] == "partial"
    # One data failure is one attempt, immediately, exactly as in v2.1.
    assert first["source_calls"] == 2 and h.state()["entries"][1]["attempts"] == 1
    child, second = h.run()
    assert second["source_calls"] == 1 and second["status"] == "partial"
    assert h.state()["entries"][1]["attempts"] == 2
    child, third = h.run()
    assert child.returncode == 0 and third["status"] == "complete"
    assert third["source_calls"] == 1 and third["outcome"] == "closed_with_failures"
    document = json.loads(control_file(h, "handoff-000000.json").read_bytes())
    assert document["entries"][1]["attempts"] == 3
    assert [link["status"] for link in document["entries"][1]["links"]] == ["failed"] * 3
    assert not control_file(h, "transport.jsonl").exists()


# 5 ---------------------------------------------------------------------------
def test_daily_budget_returns_partial_then_a_new_day_resumes(tmp_path):
    h = RateHarness(tmp_path, [task(flags=("3", "2"))])
    # One capture costs one login, one cleanup and one observed query call.
    h.env["OPENBB_CAMPAIGN_DAILY_CALL_BUDGET"] = "3"
    child, summary = h.run()
    assert child.returncode == 1 and summary["status"] == "partial"
    assert summary["daily_budget_exhausted"] is True and summary["source_calls"] == 1
    today = time.strftime("%Y-%m-%d")
    assert budget_document(h) == dict(day=today, calls=3)
    child, blocked = h.run()
    assert child.returncode == 1 and blocked["status"] == "partial"
    assert blocked["source_calls"] == 0 and blocked["daily_budget_exhausted"] is True
    assert len(h.queries()) == 1
    # A previous natural day starts the count again without touching the source.
    campaign.Control().write(control_file(h, "budget.json"), dict(day="1970-01-01", calls=3))
    child, resumed = h.run()
    assert child.returncode == 0 and resumed["status"] == "complete"
    assert resumed["source_calls"] == 1
    assert budget_document(h) == dict(day=today, calls=3)


# 6 ---------------------------------------------------------------------------
def test_slow_captures_raise_the_pace_and_throttle_the_run(tmp_path):
    h = RateHarness(tmp_path, [task(flags=("3", "2"))])
    h.env["FORCED_CODE"] = "10002007"
    child, summary = h.run()
    assert child.returncode == 1 and summary["status"] == "partial"
    assert latency_document(h)["pace_seconds"] == campaign.DEFAULT_PACE_SECONDS
    # The last three observed captures are slow: two retained samples plus the
    # capture this invocation makes. Median 40s exceeds both authored bounds.
    campaign.Control().write(
        control_file(h, "latency.json"), dict(samples=[40.0, 41.0], pace_seconds=0)
    )
    relax_due(h)
    child, slow = h.run()
    assert child.returncode == 1 and slow["status"] == "partial"
    document = backoff_document(h)
    assert document["reason"] == "latency"
    remaining = document["until_ns"] - time.time_ns()
    assert 0 < remaining <= campaign.LATENCY_BACKOFF_SECONDS * 10**9
    assert latency_document(h)["pace_seconds"] == campaign.ELEVATED_PACE_SECONDS
    # No page is sealed by a throttle: the entry is still retryable evidence.
    assert h.state()["page"] == 0 and not control_file(h, "handoff-000000.json").exists()
    child, throttled = h.run()
    assert throttled["source_calls"] == 0 and throttled["status"] == "partial"
    # Recovery: a fast window returns the pace to the authored base value.
    campaign.Control().write(
        control_file(h, "latency.json"),
        dict(samples=[0.1] * campaign.FAST_SAMPLES, pace_seconds=campaign.ELEVATED_PACE_SECONDS),
    )
    campaign.Control().write(
        control_file(h, "backoff.json"),
        campaign.backoff_document(document["binding"], 0, "latency"),
    )
    relax_due(h)
    child, resumed = h.run()
    assert child.returncode == 1 and resumed["source_calls"] == 1
    document = latency_document(h)
    assert document["pace_seconds"] == campaign.DEFAULT_PACE_SECONDS
    assert len(document["samples"]) == campaign.FAST_SAMPLES + 1
    assert all(sample < campaign.FAST_SECONDS for sample in document["samples"][-5:])


# 7 ---------------------------------------------------------------------------
def test_login_limit_throttles_for_ten_minutes(tmp_path):
    h = RateHarness(tmp_path, [task(flags=("3",))])
    h.env["FORCED_LOGIN_CODE"] = "10001005"
    child, summary = h.run()
    assert child.returncode == 1 and summary["status"] == "partial"
    assert summary["source_calls"] == 1 and not h.queries() and len(logins(h)) == 1
    document = backoff_document(h)
    assert document["reason"] == "login_limit"
    remaining = document["until_ns"] - time.time_ns()
    assert 0 < remaining <= campaign.LOGIN_LIMIT_SECONDS * 10**9
    child, throttled = h.run()
    assert throttled["source_calls"] == 0 and len(logins(h)) == 1 and not h.queries()


# 8 ---------------------------------------------------------------------------
def test_rate_grading_never_changes_the_published_document_shapes(tmp_path):
    h = RateHarness(tmp_path, [task(flags=("3", "2"))])
    child, summary = h.run()
    assert child.returncode == 0 and summary["status"] == "complete"
    header = json.loads((h.manifest / "manifest.json").read_bytes())
    assert set(header) == {"version", "policy", "page_count", "request_count", "first_sha256"}
    assert set(header["policy"]) == {"max_attempts", "request_budget", "backoff_seconds"}
    page = json.loads((h.manifest / "page-000000.json").read_bytes())
    assert set(page) == {"version", "index", "tasks", "next_sha256"}
    state = h.state()
    assert set(state) == {
        "binding",
        "page",
        "page_sha256",
        "completed_requests",
        "reserved",
        "entries",
        "active",
        "cancelled",
        "last_handoff",
    }
    document = json.loads(control_file(h, "handoff-000000.json").read_bytes())
    assert set(document) == {
        "version",
        "binding",
        "page",
        "tasks",
        "entries",
        "page_sha256",
        "next_sha256",
        "prior_completed",
        "prior_reserved",
        "completed_requests",
        "reserved",
        "outcome",
        "failures",
    }
    assert set(document["entries"][0]) == {"attempts", "due", "links"}
    assert set(document["entries"][0]["links"][0]) == {
        "attempt",
        "locator",
        "status",
        "kind",
        "receipt_sha256",
        "terminal_sha256",
        "last_page_root",
    }
    # The progress key set is enforced, not merely observed.
    for tampered in (dict(state, extra=1), {k: v for k, v in state.items() if k != "reserved"}):
        with pytest.raises(campaign.CampaignError, match="invalid_campaign_shape"):
            campaign.state_check(tampered, state["binding"], header)
    # v3 and v4 handoffs of the same page still verify, unchanged.
    legacy = dict(document)
    legacy["version"] = campaign.HANDOFF_V3_VERSION
    legacy.pop("outcome")
    legacy.pop("failures")
    legacy_path = control_file(h, "handoff-000000-v3.json")
    legacy_path.write_bytes(campaign.encode(legacy))
    assert (
        campaign._verify_handoff(h.private, legacy_path, campaign.digest(legacy))["version"]
        == campaign.HANDOFF_V3_VERSION
    )
    assert (
        campaign.verify_handoff(
            h.private, control_file(h, "handoff-000000.json"), summary["handoff_sha256"]
        )["outcome"]
        == "complete"
    )
    # A clean run authors no retry ledger and no buffer document beside it.
    assert not control_file(h, "transport.jsonl").exists()


# 9 ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    "name, value, reason",
    [
        ("OPENBB_CAMPAIGN_BLACKLIST_SECONDS", "six", "invalid_blacklist_seconds"),
        ("OPENBB_CAMPAIGN_BLACKLIST_SECONDS", "172801", "invalid_blacklist_seconds"),
        ("OPENBB_CAMPAIGN_DAILY_CALL_BUDGET", "0", "invalid_daily_budget"),
        ("OPENBB_CAMPAIGN_TRANSPORT_CAP", "11", "invalid_transport_cap"),
    ],
)
def test_invalid_rate_environment_refuses_before_any_write(tmp_path, name, value, reason):
    h = RateHarness(tmp_path, [task(flags=("3",))])
    h.env[name] = value
    child, result = h.run()
    assert child.returncode == 2 and result["status"] == "refused"
    assert result["error"] == reason
    # Nothing was created: validation precedes every authority and ledger write.
    assert not h.private.exists()


# 10 --------------------------------------------------------------------------
def test_unclassified_source_code_uses_its_own_cap_not_the_data_cap(tmp_path):
    # max_attempts is 1, so a data-class code closes the page on the first
    # failure. An unclassified BaoStock code is retried like a transport fault,
    # bounded by its own authored cap of three.
    h = RateHarness(tmp_path, [task(flags=("3", "2"))], attempts=1)
    h.env["FORCED_CODE"] = "10003001"
    summaries = []
    for number in range(3):
        child, summary = h.run()
        summaries.append(summary)
        if number < 2:
            relax_due(h)
    assert [summary["source_calls"] for summary in summaries] == [2, 1, 1]
    assert [summary["status"] for summary in summaries] == ["partial", "partial", "complete"]
    document = json.loads(control_file(h, "handoff-000000.json").read_bytes())
    assert document["outcome"] == "closed_with_failures"
    assert document["entries"][1]["attempts"] == 3
    assert [record["code"] for record in ledger(h)] == ["10003001"] * 3


# 10 --------------------------------------------------------------------------
def test_transport_class_login_refusal_still_stops_the_invocation(tmp_path):
    """One refused login is run-wide even when its code grades as transport.

    Without this, a source outage walks every entry of the page and issues one
    login per entry instead of stopping at the first refusal.
    """
    h = RateHarness(
        tmp_path,
        [
            task(code="sh.600000", flags=("3",)),
            task(code="sh.600001", flags=("3",)),
            task(code="sh.600002", flags=("3",)),
        ],
    )
    h.env["FORCED_LOGIN_CODE"] = "10002007"
    child, summary = h.run()
    assert child.returncode == 1 and summary["status"] == "partial"
    # v3: one login covers the page, and its refusal is proven before any
    # attempt exists. Nothing is spent and no request reaches the source.
    assert summary["source_calls"] == 0 and summary["session"]["error"] == "10002007"
    assert len(logins(h)) == 1 and not h.queries()
    document = backoff_document(h)
    assert document["reason"] == "login_failed"
    remaining = document["until_ns"] - time.time_ns()
    assert 0 < remaining <= campaign.DEFAULT_LOGIN_BACKOFF_SECONDS * 10**9
    # No attempt, no ledger entry and no data attempt are consumed by a refusal.
    state = h.state()
    assert [entry["attempts"] for entry in state["entries"]] == [0, 0, 0]
    assert [entry["links"] for entry in state["entries"]] == [[], [], []]
    assert ledger(h) == []
    assert campaign.data_attempts(state["entries"][0], {}) == 0
    # While the throttle holds, the source is not touched again.
    child, throttled = h.run()
    assert throttled["source_calls"] == 0 and len(logins(h)) == 1

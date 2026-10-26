"""Dead-letter page closure, login throttle and pacing for campaign v2.1.

Synthetic ONLY at the BaoStock SDK seam, through the same real subprocess
harness as the campaign acceptance suite. Every claim below is asserted against
retained evidence: the sealed handoff, its admission decision, the exact
terminal record of a failed attempt, or the recorded source-call count.
"""

import hashlib
import json
import time

import pytest
from test_baostock_campaign import Harness, task

from openbb_collector_core import baostock_campaign as campaign


def run_directory(h):
    return h.private / campaign.VERSION / campaign.digest("run1")


def handoff_path(h, page=0):
    return run_directory(h) / f"handoff-{page:06d}.json"


def backoff_file(h):
    return run_directory(h) / "backoff.json"


def logins(h):
    return [call for call in h.calls() if call["event"] == "login"]


def capped_page(tmp_path, tasks=None, attempts=3):
    """Drive one page until a request exhausts its attempts and the page closes."""
    h = Harness(
        tmp_path,
        tasks if tasks is not None else [task(flags=("3", "2"))],
        attempts=attempts,
    )
    summaries = []
    for _ in range(attempts + 1):
        child, summary = h.run("flag2")
        summaries.append(summary)
        if summary["status"] != "partial":
            return h, summaries, child
    raise AssertionError("page never closed on a terminal failure")


# 1 ---------------------------------------------------------------------------
def test_dead_letter_closes_page_after_attempt_cap(tmp_path):
    h, summaries, child = capped_page(tmp_path)
    assert [summary["source_calls"] for summary in summaries] == [2, 1, 1]
    assert child.returncode == 0 and summaries[-1]["status"] == "complete"
    assert summaries[-1]["outcome"] == "closed_with_failures"
    state = h.state()
    # The whole page counts as accounted for, and the cursor advanced past it.
    assert state["page"] == 1 and state["completed_requests"] == 2 and state["entries"] == []
    document = json.loads(handoff_path(h).read_bytes())
    assert document["version"] == "baostock-campaign-handoff-v4"
    assert document["outcome"] == "closed_with_failures"
    success, failed = document["entries"]
    assert success["links"][-1]["status"] == "success"
    assert failed["attempts"] == len(failed["links"]) == 3
    assert [link["status"] for link in failed["links"]] == ["failed"] * 3
    link = failed["links"][-1]
    assert document["failures"] == [
        dict(
            index=1,
            attempt=3,
            status="failed",
            locator=link["locator"],
            terminal_sha256=link["terminal_sha256"],
        )
    ]


# 2 ---------------------------------------------------------------------------
def test_verified_dead_letter_handoff_locates_the_real_failure(tmp_path):
    h, summaries, _ = capped_page(tmp_path)
    expected = summaries[-1]["handoff_sha256"]
    document = campaign.verify_handoff(h.private, handoff_path(h), expected)
    failure, = document["failures"]
    terminal = (h.private / failure["locator"] / "terminal.json").read_bytes()
    assert hashlib.sha256(terminal).hexdigest() == failure["terminal_sha256"]
    assert json.loads(terminal)["status"] == "partial"
    verified = h.invoke(
        [
            "verify",
            "--root",
            str(h.private),
            "--handoff",
            str(handoff_path(h)),
            "--sha256",
            expected,
        ]
    )
    assert verified.returncode == 0 and json.loads(verified.stdout)["verified"] is True


# 3 ---------------------------------------------------------------------------
@pytest.mark.parametrize("tamper", ["attempt", "locator", "status", "outcome", "hide"])
def test_tampered_failures_are_refused(tmp_path, tamper):
    h, _, _ = capped_page(tmp_path)
    document = json.loads(handoff_path(h).read_bytes())
    if tamper == "attempt":
        document["failures"][0]["attempt"] = 2
    elif tamper == "locator":
        document["failures"][0]["locator"] = "baostock-page-capture-v1/" + "0" * 64
    elif tamper == "status":
        document["failures"][0]["status"] = "success"
    elif tamper == "outcome":
        document["outcome"] = "complete"
    else:
        # Suppressing the failure while keeping a self-consistent outcome: the
        # entries still derive it, so the claim is refused rather than repaired.
        document["failures"] = []
        document["outcome"] = "complete"
    path = handoff_path(h).with_name("handoff-000000-tampered.json")
    path.write_bytes(campaign.encode(document))
    with pytest.raises(campaign.CampaignError, match="handoff_failure_binding"):
        campaign._verify_handoff(h.private, path, campaign.digest(document))
    refused = h.invoke(
        [
            "verify",
            "--root",
            str(h.private),
            "--handoff",
            str(path),
            "--sha256",
            campaign.digest(document),
        ]
    )
    assert refused.returncode == 2
    assert json.loads(refused.stdout)["error"] == "handoff_failure_binding"


# 4 ---------------------------------------------------------------------------
def test_v3_handoff_still_verifies_unchanged(tmp_path):
    h = Harness(tmp_path, [task(flags=("3",))])
    child, summary = h.run()
    assert child.returncode == 0 and summary["status"] == "complete"
    document = json.loads(handoff_path(h).read_bytes())
    assert document["version"] == campaign.HANDOFF_VERSION
    legacy = dict(document)
    legacy["version"] = campaign.HANDOFF_V3_VERSION
    legacy.pop("outcome")
    legacy.pop("failures")
    path = handoff_path(h).with_name("handoff-000000-v3.json")
    path.write_bytes(campaign.encode(legacy))
    verified = campaign._verify_handoff(h.private, path, campaign.digest(legacy))
    assert verified["version"] == campaign.HANDOFF_V3_VERSION
    assert verified["entries"] == legacy["entries"]


def test_v3_handoff_refuses_terminal_failure(tmp_path):
    h, _, _ = capped_page(tmp_path)
    document = json.loads(handoff_path(h).read_bytes())
    document["version"] = campaign.HANDOFF_V3_VERSION
    document.pop("outcome")
    document.pop("failures")
    path = handoff_path(h).with_name("handoff-000000-v3.json")
    path.write_bytes(campaign.encode(document))
    with pytest.raises(campaign.CampaignError, match="incomplete_handoff"):
        campaign._verify_handoff(h.private, path, campaign.digest(document))


def test_v4_handoff_refuses_unfinished_reservation(tmp_path):
    h, _, _ = capped_page(tmp_path)
    document = json.loads(handoff_path(h).read_bytes())
    document["entries"][1]["attempts"] = len(document["entries"][1]["links"]) + 1
    path = handoff_path(h).with_name("handoff-000000-reserved.json")
    path.write_bytes(campaign.encode(document))
    with pytest.raises(campaign.CampaignError, match="incomplete_handoff"):
        campaign._verify_handoff(h.private, path, campaign.digest(document))


# 5 ---------------------------------------------------------------------------
def test_login_failure_writes_run_backoff_and_stops_the_invocation(tmp_path):
    h = Harness(tmp_path, [task(flags=("3", "2"))])
    child, summary = h.run("login_failure")
    assert child.returncode == 1 and summary["status"] == "partial"
    # v3: the page's two requests share one login, so the refusal is proven
    # before any attempt exists. No attempt is spent and no request is sent.
    assert summary["source_calls"] == 0 and len(logins(h)) == 1 and not h.queries()
    document = campaign.Control().state(backoff_file(h))
    authority = json.loads((run_directory(h) / "authority.json").read_bytes())
    assert document["version"] == campaign.BACKOFF_VERSION
    assert document["reason"] == "login_failed"
    assert document["binding"] == campaign.digest(authority)
    remaining = document["until_ns"] - time.time_ns()
    assert 0 < remaining <= campaign.DEFAULT_LOGIN_BACKOFF_SECONDS * 1_000_000_000
    entries = h.state()["entries"]
    assert [entry["attempts"] for entry in entries] == [0, 0]
    assert [entry["links"] for entry in entries] == [[], []]


# 6 ---------------------------------------------------------------------------
def test_active_backoff_skips_the_source_then_expires(tmp_path):
    h = Harness(tmp_path, [task(flags=("3", "2"))])
    h.run("login_failure")
    child, throttled = h.run()
    assert child.returncode == 1 and throttled["status"] == "partial"
    assert throttled["source_calls"] == 0 and len(logins(h)) == 1 and not h.queries()
    # v3 proved the refusal with the page's single login, so nothing was spent.
    assert [entry["attempts"] for entry in h.state()["entries"]] == [0, 0]
    binding = campaign.Control().state(backoff_file(h))["binding"]
    campaign.Control().write(
        backoff_file(h), campaign.backoff_document(binding, time.time_ns() - 1)
    )
    child, resumed = h.run()
    assert child.returncode == 0 and resumed["status"] == "complete"
    assert resumed["source_calls"] == 2 and len(h.queries()) == 2


# 7 ---------------------------------------------------------------------------
def test_pace_seconds_separates_source_calls(tmp_path):
    h = Harness(tmp_path, [task(flags=("3", "2")), task("sz.000001", ("3",))])
    h.env["OPENBB_CAMPAIGN_PACE_SECONDS"] = "2"
    start = time.monotonic()
    child, summary = h.run()
    elapsed = time.monotonic() - start
    assert child.returncode == 0 and summary["source_calls"] == 3
    assert elapsed >= 4


# 8 ---------------------------------------------------------------------------
def test_retryable_failure_does_not_close_the_page(tmp_path):
    h = Harness(tmp_path, [task(flags=("3", "2"))])
    child, summary = h.run("flag2")
    assert child.returncode == 1 and summary["status"] == "partial"
    state = h.state()
    assert state["page"] == 0 and state["completed_requests"] == 0
    assert not handoff_path(h).exists()


# 9 ---------------------------------------------------------------------------
def test_dead_letter_page_no_longer_blocks_later_pages(tmp_path):
    # Nine tasks compile into two pages: the first one holds the dead request.
    tasks = [task(flags=("3", "2"))] + [task(suffix=f":{i}", flags=("3",)) for i in range(8)]
    h = Harness(tmp_path, tasks, attempts=1)
    first, opening = h.run("flag2")
    assert first.returncode == 1 and opening["status"] == "partial"
    assert opening["outcome"] == "closed_with_failures" and opening["source_calls"] == 9
    assert h.state()["page"] == 1
    second, closing = h.run()
    assert second.returncode == 0 and closing["status"] == "complete"
    assert closing["source_calls"] == 1
    assert closing["outcome"] == "complete"
    assert h.state()["completed_requests"] == 10
    document = campaign.verify_handoff(h.private, handoff_path(h, 0), opening["handoff_sha256"])
    assert document["outcome"] == "closed_with_failures"
    assert [failure["index"] for failure in document["failures"]] == [1]

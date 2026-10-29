"""Build one day's literal campaign plan from retained BaoStock SDK evidence.

This is a planning primitive, not a scheduler or source client. Evidence must
be acquired and durably retained by the caller; unknown calendar is a refusal.
"""

from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from . import baostock_campaign as campaign
from .baostock_plan import make_plan


def shanghai_date(now=None):
    """Return the current civil date in Shanghai, independent of host timezone."""
    now = now or datetime.now(ZoneInfo("Asia/Shanghai"))
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("naive_datetime")
    return now.astimezone(ZoneInfo("Asia/Shanghai")).date().isoformat()


def _rows(evidence, operation, params, fields):
    if (
        type(evidence) is not dict
        or evidence.get("version") != "baostock-source-evidence-v1"
        or evidence.get("operation") != operation
        or evidence.get("params") != params
        or evidence.get("status") != "complete"
        or evidence.get("reason") != "exhausted"
        or evidence.get("error_code") != "0"
        or evidence.get("cleanup") != "ok"
    ):
        raise ValueError("incomplete_sdk_evidence")
    pages = evidence.get("pages")
    if type(pages) is not list or not pages:
        raise ValueError("missing_sdk_pages")
    rows = []
    for page in pages:
        if (
            type(page) is not dict
            or page.get("error_code") != "0"
            or page.get("fields") != fields
            or type(page.get("rows")) is not list
        ):
            raise ValueError("invalid_sdk_page")
        rows.extend(page["rows"])
    return rows


def daily_plan(config, day, calendar, stock_basic):
    """Return closed or a literal single-day plan; never fill historical gaps."""
    if type(day) is not str or date.fromisoformat(day).isoformat() != day:
        raise ValueError("invalid_date")
    rows = _rows(
        calendar,
        "BaostockTradeDates",
        {"start_date": day, "end_date": day},
        ["calendar_date", "is_trading_day"],
    )
    if len(rows) != 1 or type(rows[0]) is not list or len(rows[0]) != 2 or rows[0][0] != day:
        raise ValueError("unknown_trading_day")
    if rows[0][1] not in ("0", "1"):
        raise ValueError("unknown_trading_day")
    if rows[0][1] == "0":
        return {"status": "closed", "date": day}
    fields = ["code", "code_name", "ipoDate", "outDate", "type", "status"]
    rows = _rows(stock_basic, "BaostockStockBasic", {}, fields)
    snapshot = dict(
        operation="query_stock_basic", params={}, error_code="0", fields=fields, rows=rows
    )
    plan_config = {k: v for k, v in config.items() if k != "unknown_attempt_policy"}
    return {"status": "planned", "date": day,
            "plan": make_plan(plan_config, snapshot, daily_date=day)}


def verify_campaign(manifest, root, run_key, slot):
    """Verify every admitted page and report all terminal failures.

    The completed progress and last handoff pin the run; the manifest chain
    binds every page. A terminal dead letter is never a successful campaign.
    """
    manifest, root = Path(manifest), Path(root)
    io = campaign.Control()
    header = io.read(manifest / "manifest.json")
    campaign.header_check(header)
    directory = root / campaign.VERSION / campaign.digest(run_key)
    authority = io.read(directory / "authority.json")
    campaign.require(
        authority.get("run") == run_key
        and authority.get("slot") == slot
        and authority.get("manifest") == header,
        "daily_authority_binding",
    )
    binding = campaign.digest(authority)
    state = io.state(directory / "progress.json")
    campaign.state_check(state, binding, header)
    campaign.require(
        state["page"] == header["page_count"] and not state["cancelled"],
        "daily_campaign_incomplete",
    )
    expected = header["first_sha256"]
    failures = []
    previous_completed = 0
    previous_reserved = 0
    last_hash = None
    for index in range(header["page_count"]):
        page = campaign.page_read(io, manifest, index, expected, header)
        path = directory / f"handoff-{index:06d}.json"
        candidate = io.read(path)
        last_hash = campaign.digest(candidate)
        handoff = campaign.verify_handoff(root, path, last_hash)
        campaign.require(
            handoff["binding"] == binding
            and handoff["page"] == index
            and handoff["page_sha256"] == expected
            and handoff["next_sha256"] == page["next_sha256"]
            and campaign.encode(handoff["tasks"]) == campaign.encode(page["tasks"])
            and handoff["prior_completed"] == previous_completed
            and handoff["prior_reserved"] == previous_reserved,
            "daily_handoff_chain",
        )
        previous_completed = handoff["completed_requests"]
        previous_reserved = handoff["reserved"]
        failures.extend({"page": index, **failure} for failure in handoff.get("failures", []))
        expected = page["next_sha256"]
    campaign.require(
        expected is None
        and last_hash == state["last_handoff"]
        and previous_completed == header["request_count"]
        and previous_completed == state["completed_requests"]
        and previous_reserved == state["reserved"],
        "daily_terminal_binding",
    )
    return {
        "status": "failed" if failures else "complete",
        "failures": failures,
        "pages": header["page_count"],
        "requests": previous_completed,
    }

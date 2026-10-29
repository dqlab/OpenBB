"""Opt-in, standalone BaoStock campaign v1. Private evidence, never publication.

Compile literal plans once, then run with explicit run/slot identity. A call
selects at most one page (eight tasks, two requests each). Same-run recovery is
not a new polling cycle. A new explicit run is the only way to refresh.

A page advances when every entry is terminal: either its last attempt succeeded,
or it failed and its policy attempt cap is spent. Such a page is sealed as a
dead letter (`closed_with_failures`) with the failed attempts listed by index, so
one exhausted request can never block the pages behind it. Only a success counts
as captured data; a failure is always declared, never inferred away.

A source login refusal is treated as a run-wide throttle: the invocation stops
immediately and records `backoff.json` beside the authority, so the next call
returns without touching the source until it expires.

v2.2 grades the authored source error code of a failed attempt instead of
treating every failure alike. A blacklist lockout stops the whole run and
throttles it for 6h x this year's hit count (bounded); a login-count limit stops
it for 10 minutes; a transport error is retried on a later invocation with a
30/60/120s delay and is counted in `transport.jsonl`, an append-only ledger that
never enters progress, a handoff or a verify, so network faults cannot consume
the authored data-attempt budget or seal a page as a dead letter. A data/parameter
error keeps the v2.1 attempt cap. A natural-day `budget.json` bounds source calls
(login + cleanup + observed query/next calls) and returns `partial` when spent,
and `latency.json` widens the request interval (or throttles the run) as captures
get slower.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import signal
import time
import uuid
from contextlib import contextmanager
from datetime import datetime
from zoneinfo import ZoneInfo
from pathlib import Path
from threading import Event
from typing import Any

from . import baostock_page_capture as page_capture
from . import raw_acquisition as raw
from .baostock_acquisition import MODELS

LEGACY_VERSION = "baostock-campaign-v1"
VERSION = "baostock-campaign-v2"
OLD_HANDOFF_VERSION = "baostock-campaign-handoff-v2"
HANDOFF_V3_VERSION = "baostock-campaign-handoff-v3"
HANDOFF_VERSION = "baostock-campaign-handoff-v4"
# Handoff generations a reader can still prove read-only, oldest readable first.
HANDOFF_VERSIONS = (OLD_HANDOFF_VERSION, HANDOFF_V3_VERSION, HANDOFF_VERSION)
# Sealed pages of these generations also require an independent admission.
ADMISSION_VERSIONS = (HANDOFF_V3_VERSION, HANDOFF_VERSION)
BACKOFF_VERSION = "baostock-campaign-backoff-v1"
DEFAULT_LOGIN_BACKOFF_SECONDS = 300
DEFAULT_PACE_SECONDS = 0
# v3: one login for a whole page. 0 restores the v2.2 single-request path
# byte for byte; every other value would silently change the rate contract.
DEFAULT_SESSION_PAGES = 1
ADMISSION_VERSION = "baostock-campaign-page-admission-v1"
MAX_CONTROL = 262144
MAX_PAGE_BYTES = 65536  # reserve the rest for bounded attempt/handoff metadata
MAX_PAGES = 100000

# v2.2 rate policy. Every knob is validated from the environment before any
# authority, evidence or ledger byte is written; an unparsable value refuses.
DEFAULT_BLACKLIST_SECONDS = 21600
MAX_BLACKLIST_SECONDS = 172800
LOGIN_LIMIT_SECONDS = 600
LATENCY_BACKOFF_SECONDS = 300
DEFAULT_DAILY_CALL_BUDGET = 45000
DEFAULT_TRANSPORT_CAP = 10
UNKNOWN_RETRY_CAP = 3
# One entry may therefore retain max_attempts data attempts plus both retry
# classes. The bound is fixed so progress/handoff validation never depends on
# the current environment of a reader.
RETRY_ALLOWANCE = DEFAULT_TRANSPORT_CAP + UNKNOWN_RETRY_CAP
BLACKLIST_CODE = "10001011"
LOGIN_LIMIT_CODE = "10001005"
BAOSTOCK_CODE_PREFIX = "1000"
TRANSPORT_CODES = frozenset({"1000200" + str(i) for i in range(1, 9)})
DATA_CODE_PREFIX = "100040"
BACKOFF_REASONS = ("login_failed", "login_limit", "blacklist", "latency")
SLOW_SECONDS = 10
VERY_SLOW_SECONDS = 30
ELEVATED_PACE_SECONDS = 3
FAST_SECONDS = 3
FAST_SAMPLES = 5
LATENCY_SAMPLES = 8


class CampaignError(ValueError):
    """Authored diagnostic; no source exception text."""


def require(ok, reason):
    if not ok:
        raise CampaignError(reason)


def encode(value):
    return raw._encode(value)


def digest(value):
    return hashlib.sha256(encode(value)).hexdigest()


def keys(value, names):
    require(type(value) is dict and set(value) == set(names.split()), "invalid_campaign_shape")


def integer(value, low, high):
    return type(value) is int and low <= value <= high


def hash_value(value):
    return type(value) is str and len(value) == 64 and all(c in "0123456789abcdef" for c in value)


def policy_check(value):
    keys(value, "max_attempts request_budget backoff_seconds")
    require(integer(value["max_attempts"], 1, 10), "invalid_max_attempts")
    require(integer(value["request_budget"], 1, 16000000), "invalid_request_budget")
    require(integer(value["backoff_seconds"], 0, 86400), "invalid_backoff")


def tasks_check(tasks):
    require(type(tasks) is list and 1 <= len(tasks) <= 8, "invalid_page_tasks")
    from openbb_baostock.models.history_source import validate_literal

    result = []
    for task in tasks:
        task = raw.validate_task(task)
        require(
            task["source"]["provider"] == "baostock" and task["source"]["model"] in MODELS,
            "unsupported_campaign_operation",
        )
        require(1 <= len(task["requests"]) <= 2, "partition_campaign_task")
        for request in task["requests"]:
            validate_literal(task["source"]["model"], request["params"])
        result.append(task)
    return result


def load_json(path, maximum=MAX_CONTROL):
    content = raw._read(Path(path), maximum)

    # Source plans may be formatted JSON, but duplicate keys are never authority.
    def pairs(items):
        obj = {}
        for k, v in items:
            require(k not in obj, "duplicate_json_key")
            obj[k] = v
        return obj

    value = json.loads(content, object_pairs_hook=pairs)
    encode(value)
    return value


def compile_manifest(plan, destination, policy):
    """Pure literal enumeration, then immutable hash-linked pages. No discovery."""
    policy_check(policy)
    keys(plan, "tasks")
    require(
        type(plan["tasks"]) is list and 1 <= len(plan["tasks"]) <= MAX_PAGES * 8,
        "manifest_task_limit",
    )
    pages, seen, count = [], set(), 0
    for offset in range(0, len(plan["tasks"]), 8):
        tasks = tasks_check(plan["tasks"][offset : offset + 8])
        for task in tasks:
            identity = (task["task_id"], task["generation"])
            require(identity not in seen, "duplicate_campaign_task")
            seen.add(identity)
            count += len(task["requests"])
        pages.append(tasks)
    documents = []
    following = None
    for index in reversed(range(len(pages))):
        doc = dict(version=VERSION, index=index, tasks=pages[index], next_sha256=following)
        require(len(encode(doc)) <= MAX_PAGE_BYTES, "manifest_page_limit")
        documents.append((index, doc))
        following = digest(doc)
    header = dict(
        version=VERSION,
        policy=policy,
        page_count=len(pages),
        request_count=count,
        first_sha256=following,
    )
    destination = raw._path(Path(destination))
    require(not destination.exists(), "manifest_destination_exists")
    raw._mkdir(destination)
    for index, doc in documents:
        raw._install(destination / f"page-{index:06d}.json", encode(doc))
    raw._install(destination / "manifest.json", encode(header))
    return header


class Control:
    """Measured bounded control IO. Raw response IO is separately owned by raw."""

    def __init__(self):
        self.metrics = dict(
            control_read_bytes=0,
            control_write_bytes=0,
            control_reads=0,
            control_writes=0,
            control_fsyncs=0,
            page_reads=0,
            archive_scans=0,
        )

    def read(self, path):
        content = raw._read(path, MAX_CONTROL)
        self.metrics["control_read_bytes"] += len(content)
        self.metrics["control_reads"] += 1
        return raw._decode(content)

    def write(self, path, document):
        content = encode(dict(document=document, sha256=digest(document)))
        require(len(content) <= MAX_CONTROL, "control_limit")
        tmp = path.with_name("." + path.name + "." + uuid.uuid4().hex)
        with raw._opened(tmp, "xb") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(tmp, path)
        raw._sync(path.parent)
        self.metrics["control_write_bytes"] += len(content)
        self.metrics["control_writes"] += 1
        self.metrics["control_fsyncs"] += 2

    def state(self, path):
        wrapped = self.read(path)
        keys(wrapped, "document sha256")
        require(wrapped["sha256"] == digest(wrapped["document"]), "progress_checksum")
        return wrapped["document"]


def header_check(header):
    keys(header, "version policy page_count request_count first_sha256")
    require(header["version"] != LEGACY_VERSION, "legacy_capture_requires_new_run")
    require(header["version"] == VERSION, "unsupported_campaign_version")
    policy_check(header["policy"])
    require(integer(header["page_count"], 1, MAX_PAGES), "invalid_page_count")
    require(
        integer(header["request_count"], header["page_count"], header["page_count"] * 16),
        "invalid_request_count",
    )
    require(hash_value(header["first_sha256"]), "invalid_manifest_hash")


def page_read(io, manifest, index, expected, header):
    page = io.read(raw._path(manifest, f"page-{index:06d}.json"))
    io.metrics["page_reads"] += 1
    keys(page, "version index tasks next_sha256")
    require(len(encode(page)) <= MAX_PAGE_BYTES, "manifest_page_limit")
    require(
        digest(page) == expected
        and type(page["index"]) is int
        and page["index"] == index
        and page["version"] == VERSION,
        "page_binding",
    )
    require((page["next_sha256"] is None) == (index == header["page_count"] - 1), "page_chain")
    require(page["next_sha256"] is None or hash_value(page["next_sha256"]), "page_chain")
    tasks_check(page["tasks"])
    return page


def requests(page):
    return [
        (task, index, request)
        for task in page["tasks"]
        for index, request in enumerate(task["requests"])
    ]


def attempt_task(binding, page_index, task, request, attempt, *, version=VERSION) -> dict[str, Any]:
    identity = digest(
        [version, binding, page_index, task["task_id"], task["generation"], request["request_id"]]
    )
    return dict(
        version=raw.VERSION,
        task_id="campaign:" + identity,
        generation=f"attempt-{attempt}",
        source=dict(provider="baostock", model=task["source"]["model"] + "Source"),
        requests=[dict(request_id=request["request_id"], params={"literal": request["params"]})],
    )


def classify(captured, task, request):
    require(encode(captured["task"]) == encode(task), "attempt_task_binding")
    if captured["reason"] == "cancelled":
        return "cancelled"
    if not captured["responses"]:
        return "unknown"
    item = captured["responses"][0]
    if item["body"] is None:
        return "failed"
    body = json.loads(item["body"])
    require(type(body.get("rows")) is list and len(body["rows"]) == 1, "source_envelope_shape")
    from openbb_baostock.utils.source_evidence import decode_evidence

    encoded_env = body["rows"][0].get("evidence")
    require(type(encoded_env) is dict, "source_envelope_shape")
    env = decode_evidence(encoded_env)
    keys(env, "version operation params representation status error_code reason pages cleanup")
    require(
        env["version"] in ("baostock-source-evidence-v1", "baostock-source-evidence-v2")
        and env["operation"] + "Source" == task["source"]["model"]
        and encode(env["params"]) == encode(request["params"]),
        "source_envelope_binding",
    )
    require(
        env["representation"] == "baostock-sdk-pages-not-wire"
        and env["status"] in ("complete", "partial", "failed")
        and type(env["pages"]) is list,
        "source_envelope_shape",
    )
    if env["status"] == "complete":
        require(
            type(env["error_code"]) is str
            and env["error_code"] == "0"
            and env["reason"] == "exhausted"
            and env["pages"]
            and all(type(p["error_code"]) is str and p["error_code"] == "0" for p in env["pages"]),
            "false_source_success",
        )
    return (
        "success"
        if env["status"] == "complete" and env["cleanup"] == "ok" and captured["complete"]
        else "failed"
    )


def initial_entries(page):
    return [dict(attempts=0, due=0, links=[]) for _ in requests(page)]


def code_class(code):
    """Policy class of one authored source error code.

    Only the documented eight-digit BaoStock namespace is graded. A code outside
    it (an SDK-level status such as `503`, a bare login refusal, or a missing
    code because the attempt never received anything) is not a BaoStock access
    verdict and keeps the v2.1 attempt accounting unchanged.
    """
    if type(code) is not str or not code.startswith(BAOSTOCK_CODE_PREFIX):
        return None
    if len(code) != len(BAOSTOCK_CODE_PREFIX) + 4 or not code.isdigit():
        return None
    if code == BLACKLIST_CODE:
        return "blacklist"
    if code == LOGIN_LIMIT_CODE:
        return "login_limit"
    if code in TRANSPORT_CODES:
        return "transport"
    if code.startswith(DATA_CODE_PREFIX):
        return "data"
    return "unknown"


def attempt_error_code(root, locator):
    """The failed attempt's own evidence code; a missing body authors no code.

    Capture writes one observation body per observed source page. An attempt
    that ended on the source deadline holds no body at all, and this reader must
    never invent a code for it.
    """
    directory = raw._path(Path(root), locator)
    if not raw._path(directory, "page-000.body").is_file():
        return None
    return page_capture.read_page(directory, 0).get("error_code")


def transport_path(directory):
    return raw._path(directory, "transport.jsonl")


def transport_read(directory):
    """Append-only retry ledger, counted per page entry. A bad line fails closed.

    The ledger is deliberately outside the progress/handoff schema: it decides
    only whether another attempt is allowed, and never rewrites an attempt count.
    """
    counts: dict[tuple[int, int], dict[str, Any]] = {}
    path = transport_path(directory)
    if not path.exists():
        return counts
    content = raw._read(path, MAX_CONTROL)
    for line in content.splitlines(keepends=True):
        require(line.endswith(b"\n"), "invalid_transport_ledger")
        try:
            record = json.loads(line)
        except (UnicodeError, ValueError) as exc:
            raise CampaignError("invalid_transport_ledger") from exc
        require(encode(record) == line, "invalid_transport_ledger")
        keys(record, "page index attempt code at_ns")
        require(
            integer(record["page"], 0, MAX_PAGES)
            and integer(record["index"], 0, 15)
            and integer(record["attempt"], 1, 2**31 - 1)
            and integer(record["at_ns"], 0, 2**63 - 1),
            "invalid_transport_ledger",
        )
        found = code_class(record["code"])
        require(found in ("transport", "unknown"), "invalid_transport_ledger")
        kind = "transport" if found == "transport" else "unknown"
        view = counts.setdefault(
            (record["page"], record["index"]), dict(transport=0, unknown=0, last=None)
        )
        view[kind] += 1
        view["last"] = kind
    return counts


def transport_append(directory, page, index, attempt, code):
    """Append one durable retry record; never part of a handoff or a verify."""
    record = dict(page=page, index=index, attempt=attempt, code=code, at_ns=time.time_ns())
    with raw._opened(transport_path(directory), "ab") as stream:
        stream.write(encode(record))
        stream.flush()
        os.fsync(stream.fileno())
    return record


def retry_delay(count):
    """30/60/120 seconds: a transport failure slows the next attempt, never skips it."""
    return min(30 * 2 ** max(0, count - 1), 120)


def data_attempts(entry, retries=None):
    """Attempts that count against the authored data-retry budget."""
    view = retries or {}
    return max(0, entry["attempts"] - view.get("transport", 0) - view.get("unknown", 0)
               - sum(link["status"] == "unknown" for link in entry["links"]))


def entry_terminal(entry, max_attempts, retries=None, transport_cap=DEFAULT_TRANSPORT_CAP):
    """A page may advance when no further attempt can change an entry's outcome.

    Success is terminal immediately. A failure is terminal only once the policy
    attempt cap is spent, so a retryable page never closes on a transient error.
    A transport failure is not a data failure: it closes the page only after the
    transport (or unknown) retry cap is spent, and it never consumes a data
    attempt, so a network fault cannot burn the authored budget or manufacture a
    dead letter.
    """
    if not entry["links"]:
        return False
    last = entry["links"][-1]["status"]
    if last == "success":
        return True
    if last != "failed":
        return False
    view = retries or {}
    if data_attempts(entry, view) >= max_attempts:
        return True
    if view.get("last") == "transport":
        return view.get("transport", 0) >= transport_cap
    if view.get("last") == "unknown":
        return view.get("unknown", 0) >= UNKNOWN_RETRY_CAP
    return False


def entry_failures(entries):
    """Declared failures are derived from entries, never authored separately.

    Each record points at the real retained attempt: same locator and terminal
    hash as the verified link, so a closed failure cannot be invented.
    """
    failures = []
    for index, entry in enumerate(entries):
        if not entry["links"] or entry["links"][-1]["status"] == "success":
            continue
        link = entry["links"][-1]
        failures.append(
            dict(
                index=index,
                attempt=link["attempt"],
                status=link["status"],
                locator=link["locator"],
                terminal_sha256=link["terminal_sha256"],
            )
        )
    return failures


def env_integer(name, default, reason, low, high):
    """Bounded whole-number knob; an unparsable value refuses instead of guessing."""
    value = os.environ.get(name, "")
    if not value:
        return default
    require(value.isdigit() and low <= int(value) <= high, reason)
    return int(value)


def env_seconds(name, default, reason, high=86400):
    return env_integer(name, default, reason, 0, high)


def backoff_path(directory):
    return raw._path(directory, "backoff.json")


def backoff_document(binding, until_ns, reason="login_failed"):
    return dict(
        version=BACKOFF_VERSION,
        binding=binding,
        until_ns=until_ns,
        reason=reason,
    )


def backoff_active(io, directory, binding, now_ns):
    """Any recorded source throttle holds this run, not only the page it hit."""
    path = backoff_path(directory)
    if not path.exists():
        return False
    document = io.state(path)
    keys(document, "version binding until_ns reason")
    require(
        document["version"] == BACKOFF_VERSION
        and document["reason"] in BACKOFF_REASONS
        and integer(document["until_ns"], 0, 2**63 - 1),
        "invalid_campaign_backoff",
    )
    return document["binding"] == binding and document["until_ns"] > now_ns


def budget_path(directory):
    return raw._path(directory, "budget.json")


def day_string():
    return datetime.now(ZoneInfo("Asia/Shanghai")).date().isoformat()


def budget_state(io, directory):
    """Natural-day source-call ledger; a new local day starts again at zero."""
    current = dict(day=day_string(), calls=0)
    path = budget_path(directory)
    if not path.exists():
        io.write(raw._path(directory, "budget-timezone.json"), {"timezone": "Asia/Shanghai"})
        return current
    stored = io.state(path)
    keys(stored, "day calls")
    require(
        type(stored["day"]) is str
        and len(stored["day"]) == 10
        and integer(stored["calls"], 0, 16000000),
        "invalid_campaign_budget",
    )
    marker = raw._path(directory, "budget-timezone.json")
    if not marker.exists():
        # A legacy ledger used host localtime. Preserve a possible adjacent-day
        # charge once during migration; never grant extra budget on upgrade.
        if abs((datetime.fromisoformat(stored["day"]).date() -
                datetime.fromisoformat(current["day"]).date()).days) <= 1:
            current["calls"] = stored["calls"]
        io.write(path, current)
        io.write(marker, {"timezone": "Asia/Shanghai"})
        return current
    require(io.state(marker) == {"timezone": "Asia/Shanghai"}, "invalid_budget_timezone")
    return stored if stored["day"] == current["day"] else current


def budget_charge(io, directory, budget, calls):
    """One capture costs its login, its cleanup and every observed source call."""
    budget["calls"] += calls
    io.write(budget_path(directory), budget)
    return budget["calls"]


def latency_path(directory):
    return raw._path(directory, "latency.json")


def latency_state(io, directory, base_pace):
    current = dict(samples=[], pace_seconds=base_pace)
    path = latency_path(directory)
    if not path.exists():
        return current
    stored = io.state(path)
    keys(stored, "samples pace_seconds")
    require(
        type(stored["samples"]) is list
        and len(stored["samples"]) <= LATENCY_SAMPLES
        and all(
            type(sample) in (int, float) and 0 <= sample <= 86400
            for sample in stored["samples"]
        )
        and integer(stored["pace_seconds"], 0, 86400),
        "invalid_campaign_latency",
    )
    return stored


def latency_observe(io, directory, latency, elapsed, base_pace):
    """Slow captures widen the request interval; very slow ones throttle the run.

    Only observed captures enter the window, so a paced restart that asks the
    source nothing cannot move the interval in either direction.
    """
    samples = (list(latency["samples"]) + [round(float(elapsed), 3)])[-LATENCY_SAMPLES:]
    pace, reason = latency["pace_seconds"], None
    if len(samples) >= 3:
        window = sorted(samples[-3:])
        median = window[1]
        if median > VERY_SLOW_SECONDS:
            pace, reason = max(base_pace, ELEVATED_PACE_SECONDS), "latency"
        elif median > SLOW_SECONDS:
            pace = max(base_pace, ELEVATED_PACE_SECONDS)
    if len(samples) >= FAST_SAMPLES and all(s < FAST_SECONDS for s in samples[-FAST_SAMPLES:]):
        pace = base_pace
    document = dict(samples=samples, pace_seconds=pace)
    io.write(latency_path(directory), document)
    latency.update(document)
    return reason


def blacklist_path(directory):
    return raw._path(directory, "blacklist-count.json")


def blacklist_hit(io, directory, base_seconds):
    """Authored duration is 6h x this year's hit count, bounded and never guessed.

    The official counter is not observable here, so the count is this run's own
    record of hits this calendar year; a new year starts again at one.
    """
    year = time.localtime().tm_year
    count = 0
    path = blacklist_path(directory)
    if path.exists():
        stored = io.state(path)
        keys(stored, "year count")
        require(
            integer(stored["year"], 1970, 9999) and integer(stored["count"], 0, 1000),
            "invalid_blacklist_count",
        )
        count = stored["count"] if stored["year"] == year else 0
    count += 1
    io.write(path, dict(year=year, count=count))
    return min(base_seconds * count, MAX_BLACKLIST_SECONDS)


def login_reason(captured):
    """Only the terminal summary's authored reason may throttle the run."""
    terminal = captured.get("terminal") if captured is not None else None
    summary = terminal.get("summary") if terminal is not None else None
    return summary.get("reason") if type(summary) is dict else None


def state_check(state, binding, header):
    keys(
        state,
        "binding page page_sha256 completed_requests reserved "
        "entries active cancelled last_handoff",
    )
    require(state["binding"] == binding, "campaign_binding")
    require(integer(state["page"], 0, header["page_count"]), "invalid_progress_page")
    require(
        integer(state["completed_requests"], 0, header["request_count"]), "invalid_completed_count"
    )
    require(
        integer(state["reserved"], 0, header["policy"]["request_budget"]), "invalid_reserved_count"
    )
    require(
        type(state["cancelled"]) is bool
        and type(state["entries"]) is list
        and len(state["entries"]) <= 16,
        "invalid_progress",
    )
    require(
        state["last_handoff"] is None or hash_value(state["last_handoff"]), "invalid_handoff_hash"
    )
    require(
        state["page_sha256"] is None
        if state["page"] == header["page_count"]
        else hash_value(state["page_sha256"]),
        "invalid_progress_hash",
    )
    for entry in state["entries"]:
        keys(entry, "attempts due links")
        # The key set is unchanged. The bound widens by the fixed retry
        # allowance because transport-class retries are real retained attempts
        # that never consume the authored data budget.
        require(
            integer(entry["attempts"], 0, header["policy"]["max_attempts"] + RETRY_ALLOWANCE),
            "invalid_attempt_count",
        )
        require(
            integer(entry["due"], 0, 2**63 - 1) and type(entry["links"]) is list,
            "invalid_attempt_due",
        )
        require(len(entry["links"]) <= entry["attempts"], "invalid_attempt_links")
        for i, link in enumerate(entry["links"]):
            keys(link, "attempt locator status receipt_sha256 kind terminal_sha256 last_page_root")
            require(link["kind"] == page_capture.VERSION, "capture_kind_binding")
            require(link["status"] != "cancelled" or state["cancelled"], "cancelled_attempt_veto")
            require(
                type(link["attempt"]) is int
                and link["attempt"] == i + 1
                and link["status"] in ("success", "failed", "unknown", "cancelled"),
                "invalid_attempt_link",
            )
            require(
                link["receipt_sha256"] is None or hash_value(link["receipt_sha256"]),
                "invalid_receipt_hash",
            )
    if state["active"] is not None:
        keys(state["active"], "entry attempt")
        require(
            integer(state["active"]["entry"], 0, len(state["entries"]) - 1), "invalid_reservation"
        )
        entry = state["entries"][state["active"]["entry"]]
        require(
            type(state["active"]["attempt"]) is int
            and state["active"]["attempt"] == entry["attempts"] == len(entry["links"]) + 1,
            "invalid_reservation",
        )
    for index, entry in enumerate(state["entries"]):
        active = state["active"] is not None and state["active"]["entry"] == index
        require(entry["attempts"] == len(entry["links"]) + int(active), "missing_reservation")
        require(
            not any(link["status"] == "success" for link in entry["links"][:-1]),
            "retry_after_success",
        )
    consumed = sum(e["attempts"] for e in state["entries"])
    require(
        state["completed_requests"] + consumed <= state["reserved"],
        "reservation_accounting",
    )
    if state["page"] == 0:
        require(
            state["completed_requests"] == 0
            and state["last_handoff"] is None
            and state["page_sha256"] == header["first_sha256"]
            and state["reserved"] == consumed,
            "initial_page_authority",
        )
    else:
        require(state["last_handoff"] is not None, "missing_prior_handoff")
    if state["page"] < header["page_count"]:
        require(state["completed_requests"] < header["request_count"], "premature_completion")
    if state["page"] == header["page_count"]:
        require(
            not state["entries"]
            and state["active"] is None
            and state["completed_requests"] == header["request_count"]
            and state["last_handoff"] is not None,
            "incomplete_campaign",
        )


def linked_capture(root, binding, page_index, original, request, link):
    if link.get("kind") == page_capture.VERSION:
        task = attempt_task(binding, page_index, original, request, link["attempt"])
        locator = page_capture.capture_locator(task)
        require(link["locator"] == locator, "locator_binding")
        if link["status"] == "unknown" and not raw._path(root, locator).exists():
            require(
                link["receipt_sha256"] is None
                and link["terminal_sha256"] is None
                and link["last_page_root"] is None,
                "unknown_receipt",
            )
            return
        captured = page_capture.read_capture(root, task, expected=link["receipt_sha256"])
        require(
            (captured["status"] == link["status"]
             or (link["status"] == "failed" and captured["status"] == "unknown"
                 and captured["terminal"] is None))
            and captured["terminal_sha256"] == link["terminal_sha256"]
            and captured["last_page_root"] == link["last_page_root"],
            "page_link_binding",
        )
        return
    task = attempt_task(
        binding, page_index, original, request, link["attempt"], version=LEGACY_VERSION
    )
    locator = raw.capture_locator(task["task_id"], task["generation"])
    require(link["locator"] == locator, "locator_binding")
    path = raw._path(root, locator)
    if link["status"] == "unknown" and not path.exists():
        require(link["receipt_sha256"] is None, "unknown_receipt")
        return
    captured = raw.read_capture(root, task["task_id"], task["generation"])
    actual = classify(captured, task, request)
    require(actual == link["status"], "source_status_conflict")
    receipt_hash = digest(captured["responses"][0]["receipt"]) if captured["responses"] else None
    require(receipt_hash == link["receipt_sha256"], "linked_receipt_conflict")


@contextmanager
def _read_ownership(root):
    """No creation/repair: synchronize readers with the existing producer lock."""
    import fcntl

    with raw._opened(raw._path(Path(root), ".lock"), "rb") as stream:
        try:
            fcntl.flock(stream.fileno(), fcntl.LOCK_SH | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise raw.RawAcquisitionError("acquisition_busy") from exc
        # Closing the descriptor releases the shared lock, including on errors.
        yield


def admission_document(handoff, status):
    return dict(
        version=ADMISSION_VERSION,
        binding=handoff["binding"],
        page=handoff["page"],
        handoff_sha256=digest(handoff),
        status=status,
    )


def admission_path(directory, page):
    return raw._path(directory, f"admission-{page:06d}.json")


def page_veto_path(directory, page):
    return raw._path(directory, f"veto-{page:06d}.json")


def _admission_check(root, path, handoff):
    directory = Path(path).parent
    authority = raw._decode(raw._read(raw._path(directory, "authority.json"), MAX_CONTROL))
    require(digest(authority) == handoff["binding"], "admission_authority_binding")
    require(
        directory.absolute() == raw._path(
            Path(root), VERSION + "/" + digest(authority["run"])
        ).absolute()
        and Path(path).name == f"handoff-{handoff['page']:06d}.json",
        "admission_path_binding",
    )
    require(not page_veto_path(directory, handoff["page"]).exists(), "campaign_page_cancelled")
    decision = Control().state(admission_path(directory, handoff["page"]))
    require(
        encode(decision) == encode(admission_document(handoff, "eligible")),
        "campaign_page_not_admitted",
    )


def _confirm_admission(io, root, directory, handoff):
    """Producer-only completion barrier; visible eligible is not a sync receipt.

    Called with exclusive ownership, including on identical-existing recovery.
    Never rewrite the selected decision or turn a storage failure into a veto.
    """
    path = admission_path(directory, handoff["page"])
    require(
        encode(io.state(path)) == encode(admission_document(handoff, "eligible")),
        "campaign_admission_conflict",
    )
    raw._sync_file(path)
    io.metrics["control_fsyncs"] += 1
    # Bounded private-tree links, through the parent that contains the root.
    # External provisioning above that boundary is not campaign-owned.
    for parent in (directory, directory.parent, root, root.parent):
        raw._sync(parent)
        io.metrics["control_fsyncs"] += 1


def verify_handoff(root, path, expected):
    """Read-only proof against a successful producer's retained expected hash.

    A hash computed from a visible failed candidate is not a durable handoff.
    This reader checks evidence/admission, never establishes storage barriers.
    """
    # Historical handoffs are not silently upgraded or made dependent on new
    # metadata. The retained expected hash pins the version as well as bytes.
    document = raw._decode(raw._read(Path(path), MAX_CONTROL))
    require(digest(document) == expected, "handoff_checksum")
    if document.get("version") not in ADMISSION_VERSIONS:
        return _verify_handoff(root, path, expected)
    with _read_ownership(root):
        document = _verify_handoff(root, path, expected)
        _admission_check(root, path, document)
        return document


def _verify_handoff(root, path, expected):
    """Evidence proof only; producer calls this under its exclusive ownership."""
    document = raw._decode(raw._read(Path(path), MAX_CONTROL))
    require(digest(document) == expected, "handoff_checksum")
    version = document.get("version")
    if version == LEGACY_VERSION:
        # Exact historical page reading remains supported; old handoffs lack
        # cumulative accounting and cannot authorize a new cursor transition.
        keys(document, "version binding page tasks entries")
    else:
        names = (
            "version binding page tasks entries page_sha256 next_sha256 "
            "prior_completed prior_reserved completed_requests reserved"
        )
        if version == HANDOFF_VERSION:
            names += " outcome failures"
        keys(document, names)
        require(version in HANDOFF_VERSIONS, "handoff_version")
    tasks_check(document["tasks"])
    require(type(document["entries"]) is list, "handoff_entry_count")
    flat = requests(document)
    require(len(flat) == len(document["entries"]), "handoff_entry_count")
    if version == HANDOFF_VERSION:
        # A dead-letter page declares exactly the failures its own entries ended
        # in. Authoring that list differently is refused, never repaired.
        failures = entry_failures(document["entries"])
        require(
            encode(document["failures"]) == encode(failures)
            and document["outcome"]
            == ("complete" if not failures else "closed_with_failures"),
            "handoff_failure_binding",
        )
    for (task, _, request), entry in zip(flat, document["entries"], strict=True):
        require(entry["links"], "incomplete_handoff")
        if version == HANDOFF_VERSION:
            # A closed page may retain a terminal failure, but never an attempt
            # still reserved and unrecorded.
            require(entry["attempts"] == len(entry["links"]), "incomplete_handoff")
            require(entry["links"][-1]["status"] in ("success", "failed"), "incomplete_handoff")
        else:
            require(entry["links"][-1]["status"] == "success", "incomplete_handoff")
        for link in entry["links"]:
            linked_capture(Path(root), document["binding"], document["page"], task, request, link)
    return document


def progress_binding(state, binding, header, handoff):
    """Compare mutable counters/cursor to the retained immutable last handoff."""
    state_check(state, binding, header)
    if state["page"] == 0:
        require(handoff is None, "unexpected_prior_handoff")
        return
    require(
        handoff is not None and handoff["version"] in HANDOFF_VERSIONS,
        "handoff_accounting_unavailable",
    )
    require(
        handoff["binding"] == binding
        and type(handoff["page"]) is int
        and handoff["page"] == state["page"] - 1
        and digest(handoff) == state["last_handoff"],
        "prior_handoff_binding",
    )
    for name in ("prior_completed", "prior_reserved", "completed_requests", "reserved"):
        require(
            integer(
                handoff[name],
                0,
                header["policy"]["request_budget"]
                if "reserved" in name
                else header["request_count"],
            ),
            "handoff_accounting",
        )
    flat = requests(handoff)
    consumed = sum(e["attempts"] for e in handoff["entries"])
    require(
        handoff["completed_requests"] == handoff["prior_completed"] + len(flat)
        and handoff["reserved"] == handoff["prior_reserved"] + consumed
        and handoff["prior_reserved"] >= handoff["prior_completed"],
        "handoff_accounting",
    )
    # Reuse the same strict entry/reservation checks on the sealed page.
    sealed = dict(
        binding=binding,
        page=handoff["page"],
        page_sha256=handoff["page_sha256"],
        completed_requests=handoff["prior_completed"],
        reserved=handoff["reserved"],
        entries=handoff["entries"],
        active=None,
        cancelled=False,
        last_handoff=None if handoff["page"] == 0 else state["last_handoff"],
    )
    state_check(sealed, binding, header)
    page = dict(
        version=VERSION,
        index=handoff["page"],
        tasks=handoff["tasks"],
        next_sha256=handoff["next_sha256"],
    )
    require(digest(page) == handoff["page_sha256"], "handoff_page_binding")
    require(
        state["page_sha256"] == handoff["next_sha256"]
        and state["completed_requests"] == handoff["completed_requests"]
        and state["reserved"] == handoff["reserved"] + sum(e["attempts"] for e in state["entries"]),
        "progress_handoff_accounting",
    )
    require(
        (handoff["next_sha256"] is None) == (state["page"] == header["page_count"]),
        "handoff_terminal_binding",
    )


def handoff_document(state, page, *, version=HANDOFF_VERSION):
    """Seal counters together with the immutable page cursor before advancement."""
    document: dict[str, Any] = dict(
        version=version,
        binding=state["binding"],
        page=state["page"],
        tasks=page["tasks"],
        entries=state["entries"],
        page_sha256=state["page_sha256"],
        next_sha256=page["next_sha256"],
        prior_completed=state["completed_requests"],
        prior_reserved=state["reserved"] - sum(e["attempts"] for e in state["entries"]),
        completed_requests=state["completed_requests"] + len(requests(page)),
        reserved=state["reserved"],
    )
    if version == HANDOFF_VERSION:
        # Rebuildable from state alone: sealed_progress_conflict compares a
        # retained document against this exact derivation.
        failures = entry_failures(state["entries"])
        document["outcome"] = "complete" if not failures else "closed_with_failures"
        document["failures"] = failures
    return document


def retained_handoff(root, directory, state, binding, header):
    veto = raw._path(directory, "cancel.json")
    if veto.exists():
        require(
            raw._decode(raw._read(veto, MAX_CONTROL)) == dict(binding=binding, cancelled=True)
            and state["cancelled"],
            "campaign_cancellation_conflict",
        )
    else:
        require(not state["cancelled"], "missing_campaign_cancellation")
    handoff = None
    if state["page"]:
        handoff = _verify_handoff(
            root, directory / f"handoff-{state['page'] - 1:06d}.json", state["last_handoff"]
        )
    progress_binding(state, binding, header, handoff)
    # A sealed current page is possible only in the install-before-cursor crash
    # window. An exact lookup detects a rewound progress document without scans.
    current = raw._path(directory, f"handoff-{state['page']:06d}.json")
    if state["page"] < header["page_count"] and current.exists():
        sealed = raw._decode(raw._read(current, MAX_CONTROL))
        require(
            sealed.get("version") in HANDOFF_VERSIONS,
            "handoff_accounting_unavailable",
        )
        page = dict(
            version=VERSION,
            index=state["page"],
            tasks=sealed["tasks"],
            next_sha256=sealed["next_sha256"],
        )
        require(digest(page) == state["page_sha256"], "sealed_page_binding")
        require(
            encode(sealed) == encode(handoff_document(state, page, version=sealed["version"])),
            "sealed_progress_conflict",
        )
        _verify_handoff(root, current, digest(sealed))
    return handoff


def run(manifest, root, run_key, slot, *, stop=None, source_allowed=None):
    """One bounded page pass; retries happen on later explicit invocations."""
    stop = stop if stop is not None else Event()
    require(raw._text(run_key) and raw._text(slot), "explicit_run_and_slot_required")
    io = Control()
    manifest, root = raw._path(Path(manifest)), raw._path(Path(root))
    header = io.read(manifest / "manifest.json")
    header_check(header)
    # Environment knobs are validated before any authority or evidence is written.
    login_backoff = env_seconds(
        "OPENBB_CAMPAIGN_LOGIN_BACKOFF_SECONDS",
        DEFAULT_LOGIN_BACKOFF_SECONDS,
        "invalid_login_backoff",
    )
    pace = env_seconds(
        "OPENBB_CAMPAIGN_PACE_SECONDS", DEFAULT_PACE_SECONDS, "invalid_campaign_pace"
    )
    base_pace = pace
    blacklist_seconds = env_integer(
        "OPENBB_CAMPAIGN_BLACKLIST_SECONDS",
        DEFAULT_BLACKLIST_SECONDS,
        "invalid_blacklist_seconds",
        1,
        MAX_BLACKLIST_SECONDS,
    )
    daily_budget = env_integer(
        "OPENBB_CAMPAIGN_DAILY_CALL_BUDGET",
        DEFAULT_DAILY_CALL_BUDGET,
        "invalid_daily_budget",
        1,
        16000000,
    )
    transport_cap = env_integer(
        "OPENBB_CAMPAIGN_TRANSPORT_CAP",
        DEFAULT_TRANSPORT_CAP,
        "invalid_transport_cap",
        1,
        DEFAULT_TRANSPORT_CAP,
    )
    session_pages = env_integer(
        "OPENBB_CAMPAIGN_SESSION_PAGES",
        DEFAULT_SESSION_PAGES,
        "invalid_session_pages",
        0,
        1,
    )
    authority = dict(
        version=VERSION,
        run=run_key,
        slot=slot,
        manifest=header,
        capture_kind=page_capture.VERSION,
        capture_limits=page_capture.DEFAULT_LIMITS,
    )
    binding = digest(authority)
    require(
        not raw._path(root, LEGACY_VERSION + "/" + digest(run_key)).exists(),
        "legacy_capture_requires_new_run",
    )
    directory = raw._path(root, VERSION + "/" + digest(run_key))
    authority_path, progress_path = directory / "authority.json", directory / "progress.json"
    # Before ownership/mutation, refuse corrupt or mismatched existing authority.
    if directory.exists():
        require(raw._path(root, ".lock").is_file(), "missing_campaign_lock")
        require(encode(io.read(authority_path)) == encode(authority), "immutable_run_conflict")
        # Mutable progress/eligibility must only be sampled with ownership.
    raw._mkdir(root)
    with raw._ownership(root):
        if directory.exists():
            require(encode(io.read(authority_path)) == encode(authority), "immutable_run_conflict")
            state = io.state(progress_path)
            state_check(state, binding, header)
            prior_handoff = retained_handoff(root, directory, state, binding, header)
        else:
            page = page_read(io, manifest, 0, header["first_sha256"], header)
            raw._mkdir(directory)
            raw._install(authority_path, encode(authority))
            state: dict[str, Any] = dict(
                binding=binding,
                page=0,
                page_sha256=header["first_sha256"],
                completed_requests=0,
                reserved=0,
                entries=initial_entries(page),
                active=None,
                cancelled=False,
                last_handoff=None,
            )
            io.write(progress_path, state)
            prior_handoff = None
        # A recorded source throttle is a run-wide fact: read it under ownership,
        # before any page is selected, and never call the source while it holds.
        throttled = backoff_active(io, directory, binding, time.time_ns())
        # v2.2 ledgers: retry counts, the natural-day budget and the observed
        # latency window. None of them enters progress, a handoff or a verify.
        retries = transport_read(directory)
        budget = budget_state(io, directory)
        latency = latency_state(io, directory, base_pace)
        pace = max(base_pace, latency["pace_seconds"])
        daily_exhausted = int(budget["calls"]) >= daily_budget
        def admit_source(kind):
            nonlocal daily_exhausted
            if not source_allowed():
                return False
            charge = 2 if kind == "login" else 1  # Include eventual logout up front.
            if budget["calls"] + charge > daily_budget:
                daily_exhausted = True
                return False
            budget_charge(io, directory, budget, charge)
            if budget["calls"] >= daily_budget:
                daily_exhausted = True
            return source_allowed()

        source_calls = 0
        refetch_attempts = 0
        sdk_queries = 0
        sdk_next_calls = 0
        sdk_counts_known = True
        # v3 session facts for this invocation's summary: what the shared login
        # cost and how its teardown ended. Never part of progress or a handoff.
        session_hold: dict[str, Any] = dict(charged=False, record=None)
        candidate = None
        accepting = True
        # Only a page not already admitted on entry belongs to this invocation.
        # Prior successful pages are never vetoed by a later page's cancellation.
        owned_page = state["page"]
        if prior_handoff is not None and prior_handoff["version"] in ADMISSION_VERSIONS:
            decision_path = admission_path(directory, prior_handoff["page"])
            if decision_path.exists():
                decision = io.state(decision_path)
                require(
                    any(encode(decision) == encode(admission_document(prior_handoff, status))
                        for status in ("pending", "eligible")),
                    "campaign_admission_conflict",
                )
                admitted = decision["status"] == "eligible"
            else:
                admitted = False
            if not admitted:
                candidate = prior_handoff
                owned_page = candidate["page"]
                state["cancelled"] = True
            elif not page_veto_path(directory, prior_handoff["page"]).exists():
                _admission_check(
                    root, directory / f"handoff-{prior_handoff['page']:06d}.json", prior_handoff
                )
                _confirm_admission(io, root, directory, prior_handoff)
            else:
                require(state["cancelled"], "campaign_page_veto_conflict")

        def veto():
            # Page veto first, run state second. Neither success evidence nor
            # an older page's eligibility is rewritten.
            if owned_page < header["page_count"]:
                raw._install(
                    page_veto_path(directory, owned_page),
                    encode(dict(version=ADMISSION_VERSION, binding=binding, page=owned_page,
                                cancelled=True)),
                )
            raw._install(directory / "cancel.json", encode(dict(binding=binding, cancelled=True)))

        def save():
            if accepting and stop.is_set():
                state["cancelled"] = True
            if state["cancelled"]:
                veto()
            progress_binding(state, binding, header, prior_handoff)
            io.write(progress_path, state)

        def summary():
            successes = sum(
                bool(e["links"] and e["links"][-1]["status"] == "success") for e in state["entries"]
            )
            pending = header["request_count"] - state["completed_requests"] - successes
            progress_binding(state, binding, header, prior_handoff)
            terminal = state["page"] == header["page_count"] and prior_handoff is not None
            status = "cancelled" if state["cancelled"] else "complete" if terminal else "partial"
            # A closed page that kept terminal failures must never read as clean.
            outcome = (
                prior_handoff.get("outcome")
                if prior_handoff is not None and prior_handoff["version"] == HANDOFF_VERSION
                else None
            )
            return dict(
                version=VERSION,
                run=run_key,
                slot=slot,
                status=status,
                outcome=outcome,
                remaining_requests=pending,
                reserved_requests=state["reserved"],
                source_calls=source_calls,
                sdk_query_calls=sdk_queries if sdk_counts_known else None,
                sdk_next_calls=sdk_next_calls if sdk_counts_known else None,
                refetch=refetch_attempts > 0,
                refetch_attempts=refetch_attempts,
                page=state["page"],
                handoff_sha256=state["last_handoff"],
                capture_kind=page_capture.VERSION,
                retry_semantics="incomplete-request-new-session-may-refetch-prefix",
                daily_budget_exhausted=daily_exhausted,
                daily_calls=int(budget["calls"]),
                session=session_hold["record"],
                metrics=io.metrics,
            )

        def finalize():
            nonlocal accepting
            # Candidate/progress, final snapshot and their native cleanup are
            # all inside acceptance. Pending admission was persisted before
            # advancing the cursor or attempting any cancellation correction.
            result = summary()
            # The last Event sample is the cancellation cutoff. No subsequent
            # signal changes this invocation's decision, including commit/unlock.
            cancelled = stop.is_set() or state["cancelled"]
            accepting = False
            if cancelled:
                state["cancelled"] = True
                save()
                result["status"] = "cancelled"
            elif candidate is not None and candidate["version"] in ADMISSION_VERSIONS:
                require(not page_veto_path(directory, candidate["page"]).exists(),
                        "campaign_page_cancelled")
                io.write(admission_path(directory, candidate["page"]),
                         admission_document(candidate, "eligible"))
                _confirm_admission(io, root, directory, candidate)
            return result

        try:
            decision_path = admission_path(directory, owned_page)
            if decision_path.exists():
                decision = io.state(decision_path)
                sealed = candidate or raw._decode(raw._read(
                    directory / f"handoff-{owned_page:06d}.json", MAX_CONTROL
                ))
                # An interrupted final decision is denied, not silently
                # promoted on restart. This also survives failed veto writes.
                require(
                    encode(decision) == encode(admission_document(sealed, "pending")),
                    "campaign_admission_conflict",
                )
                state["cancelled"] = True
                save()
            if page_veto_path(directory, owned_page).exists():
                state["cancelled"] = True
                save()
            if stop.is_set():
                state["cancelled"] = True
                save()
            if state["cancelled"]:
                return finalize()
            if state["page"] == header["page_count"]:
                return finalize()
            if candidate is not None:
                # Complete the previous invocation's commit before advancing to
                # another page; never acquire another source during this replay.
                return finalize()
            if daily_exhausted:
                # The natural-day source-call budget is spent: this invocation may
                # not ask the source anything, and reports partial instead.
                return finalize()
            page = page_read(io, manifest, state["page"], state["page_sha256"], header)
            flat = requests(page)
            # An empty entry list means the prior page was atomically handed off.
            if not state["entries"]:
                state["entries"] = initial_entries(page)
            require(len(flat) == len(state["entries"]), "page_progress_shape")
            for (task, _, request), entry in zip(flat, state["entries"], strict=True):
                for link in entry["links"]:
                    linked_capture(root, binding, state["page"], task, request, link)

            def finish(index, attempt, captured):
                original, _, request = flat[index]
                task = attempt_task(binding, state["page"], original, request, attempt)
                status = captured["status"] if captured is not None else "unknown"
                if captured is not None:
                    require(encode(captured["task"]) == encode(task), "page_attempt_binding")
                link = dict(
                    attempt=attempt,
                    locator=page_capture.capture_locator(task),
                    status=status,
                    kind=page_capture.VERSION,
                    receipt_sha256=captured["capture_sha256"] if captured else None,
                    terminal_sha256=captured["terminal_sha256"] if captured else None,
                    last_page_root=captured["last_page_root"] if captured else None,
                )
                entry = state["entries"][index]
                entry["links"].append(link)
                state["active"] = None
                if status == "cancelled":
                    state["cancelled"] = True
                save()

            def observe_retry(index, attempt, code, kind):
                """Ledger a retryable failure, delay it, and never refund it.

                The attempt itself is real and already retained as a link; the
                ledger only decides whether another attempt may follow, so a
                network fault can neither burn the data budget nor seal a page.
                """
                transport_append(directory, state["page"], index, attempt, code)
                view = retries.setdefault(
                    (state["page"], index), dict(transport=0, unknown=0, last=None)
                )
                view[kind] += 1
                view["last"] = kind
                state["entries"][index]["due"] = (
                    time.time_ns() + retry_delay(view[kind]) * 1_000_000_000
                )
                save()

            def classify_retry(index, attempt):
                """Read the failed attempt's own code; ledger it when retryable."""
                original, _, request = flat[index]
                task = attempt_task(binding, state["page"], original, request, attempt)
                code = attempt_error_code(root, page_capture.capture_locator(task))
                found = code_class(code)
                if found == "transport":
                    observe_retry(index, attempt, code, "transport")
                elif found == "unknown":
                    observe_retry(index, attempt, code, "unknown")
                return found

            recovered = None
            if state["active"] is not None:
                index, attempt = state["active"]["entry"], state["active"]["attempt"]
                original, _, request = flat[index]
                task = attempt_task(binding, state["page"], original, request, attempt)
                locator = page_capture.capture_locator(task)
                captured = None
                if raw._path(root, locator).exists():
                    # Exact read-only recovery. A dead SDK session is never
                    # restarted under this attempt identity or fabricated cursor.
                    captured = page_capture.recover_capture(root, task)
                finish(index, attempt, captured)
                if captured is not None and captured["status"] != "unknown":
                    classify_retry(index, attempt)
                recovered = index
            settled: set[int] = set()

            def eligible(index):
                """Whether this invocation may still spend an attempt on an entry."""
                entry = state["entries"][index]
                if (
                    index in settled
                    or index == recovered
                    or (entry["links"] and entry["links"][-1]["status"] == "success")
                ):
                    return False
                view = retries.get((state["page"], index)) or {}
                return not (
                    data_attempts(entry, view) >= header["policy"]["max_attempts"]
                    or state["reserved"] >= header["policy"]["request_budget"]
                    or time.time_ns() < entry["due"]
                )

            def reserve(index, attempt):
                """Reserve exactly one attempt, durably, before the source is asked.

                Returns the attempt's task, or None when the reservation itself
                proves the entry may not be attempted any more. A reservation is
                never refunded, so a dead process still accounts for it.
                """
                nonlocal source_calls, refetch_attempts
                if source_allowed is not None and not source_allowed():
                    return None
                task = attempt_task(
                    binding, state["page"], flat[index][0], flat[index][2], attempt
                )
                page_capture.task_check(task)
                entry = state["entries"][index]
                entry["attempts"] = attempt
                entry["due"] = (
                    time.time_ns() + header["policy"]["backoff_seconds"] * 1_000_000_000
                )
                state["reserved"] += 1
                state["active"] = dict(entry=index, attempt=attempt)
                save()  # Reservation survives process death; never refunded.
                if state["cancelled"] or stop.is_set():
                    return None
                source_calls += 1
                refetch_attempts += int(bool(entry["links"]))
                settled.add(index)
                return task

            def settle(index, attempt, captured, elapsed, extra):
                """Record one captured attempt, then apply the v2.2 policy to it."""
                nonlocal sdk_queries, sdk_next_calls, sdk_counts_known, daily_exhausted, pace
                counts = captured["metrics"] if captured is not None else None
                known = bool(counts and counts["source_query_calls"] is not None)
                if known:
                    sdk_queries += counts["source_query_calls"]
                    sdk_next_calls += counts["source_next_calls"]
                else:
                    sdk_counts_known = False
                finish(index, attempt, captured)  # page eligibility precedes progress
                # Natural-day accounting: this attempt's own fixed cost (a whole
                # login and cleanup per request, nothing inside a shared session)
                # plus every source call the attempt actually made.
                if source_allowed is None:
                    budget_charge(
                        io, directory, budget,
                        extra + (counts["source_query_calls"] + counts["source_next_calls"]
                                 if known else 0),
                    )
                if int(budget["calls"]) >= daily_budget:
                    daily_exhausted = True
                found = (classify_retry(index, attempt)
                         if captured is not None and captured["status"] != "unknown" else None)
                if found == "blacklist":
                    # The source has locked this address out. Everything stops
                    # immediately and the next invocation is throttled for the
                    # authored duration (6h x this year's hit count, bounded).
                    seconds = blacklist_hit(io, directory, blacklist_seconds)
                    io.write(
                        backoff_path(directory),
                        backoff_document(
                            binding, time.time_ns() + seconds * 1_000_000_000, "blacklist"
                        ),
                    )
                    return "stop"
                if found == "login_limit":
                    io.write(
                        backoff_path(directory),
                        backoff_document(
                            binding,
                            time.time_ns() + LOGIN_LIMIT_SECONDS * 1_000_000_000,
                            "login_limit",
                        ),
                    )
                    return "stop"
                if login_reason(captured) == "login_failed":
                    # BaoStock refused this session's login. Every further attempt
                    # in this invocation meets the same refusal - including a
                    # refusal that carries a transport code - so record the
                    # run-wide throttle and stop before spending more budget.
                    # The attempt is already retained, and a transport-class
                    # refusal is already ledgered as retryable.
                    io.write(
                        backoff_path(directory),
                        backoff_document(
                            binding, time.time_ns() + login_backoff * 1_000_000_000
                        ),
                    )
                    return "stop"
                if found == "transport":
                    pass  # already ledgered with its own delay
                if latency_observe(io, directory, latency, elapsed, base_pace) == "latency":
                    # A very slow source is throttled for the rest of this run.
                    io.write(
                        backoff_path(directory),
                        backoff_document(
                            binding,
                            time.time_ns() + LATENCY_BACKOFF_SECONDS * 1_000_000_000,
                            "latency",
                        ),
                    )
                    return "stop"
                pace = max(base_pace, latency["pace_seconds"])
                return "stop" if daily_exhausted else "continue"

            # v3: one login for the whole page. A shared session is only worth it
            # when this invocation may really attempt two entries of this page;
            # otherwise the v2.2 single-request path below runs unchanged.
            session_ran = False
            if session_pages:
                candidates = [
                    index
                    for index in range(len(flat))
                    if not (throttled or state["cancelled"] or stop.is_set())
                    and (source_allowed is None or source_allowed())
                    and eligible(index)
                ]
                if len(candidates) >= 2:
                    session_ran = True

                    def admit(ordinal):
                        """Reserve one entry's attempt, only after a proven login."""
                        nonlocal daily_exhausted
                        if not session_hold["charged"]:
                            session_hold["charged"] = True
                            # The session's fixed cost: one login, one logout.
                            # Each task's own query/next calls are charged by
                            # `settle` as that attempt is recorded.
                            if source_allowed is None:
                                budget_charge(io, directory, budget, 2)
                            if int(budget["calls"]) >= daily_budget:
                                daily_exhausted = True
                                return False
                        index = candidates[ordinal]
                        if not eligible(index):
                            return False
                        return reserve(
                            index, state["entries"][index]["attempts"] + 1
                        ) is not None

                    def capture(ordinal, captured):
                        """Settle a durable attempt, then answer whether to go on."""
                        index = candidates[ordinal]
                        counts = captured["metrics"] if captured is not None else None
                        elapsed = 0.0
                        if counts and counts.get("elapsed_seconds"):
                            elapsed = float(counts["elapsed_seconds"])
                        attempt = state["entries"][index]["attempts"]
                        if settle(index, attempt, captured, elapsed, 0) == "stop":
                            return False
                        if pace and stop.wait(pace):
                            state["cancelled"] = True
                            save()
                            return False
                        return not (stop.is_set() or state["cancelled"])

                    tasks = [
                        attempt_task(
                            binding,
                            state["page"],
                            flat[index][0],
                            flat[index][2],
                            state["entries"][index]["attempts"] + 1,
                        )
                        for index in candidates
                    ]
                    for task in tasks:
                        page_capture.task_check(task)
                    if pace and source_calls and stop.wait(pace):
                        state["cancelled"] = True
                        save()
                    else:
                        session = page_capture.capture_session(
                            root, tasks, stop=stop, on_task=admit, on_capture=capture,
                            source_allowed=admit_source if source_allowed is not None else None
                        )
                        session_hold["record"] = dict(
                            tasks=len(candidates),
                            reason=session["reason"],
                            error=session["session_error"],
                            cleanup=session["session_cleanup"],
                            logins=session["session_logins"],
                            logouts=session["session_logouts"],
                            interrupted=session["interrupted"],
                        )
                        if session["session_error"] is not None:
                            # The session login was refused. No attempt exists and
                            # none was spent; exactly like a single refused
                            # request this throttles the whole run.
                            if source_allowed is None:
                                budget_charge(io, directory, budget, 1)
                            found = code_class(session["session_error"])
                            if found == "blacklist":
                                seconds = blacklist_hit(io, directory, blacklist_seconds)
                                io.write(
                                    backoff_path(directory),
                                    backoff_document(
                                        binding,
                                        time.time_ns() + seconds * 1_000_000_000,
                                        "blacklist",
                                    ),
                                )
                            elif found == "login_limit":
                                io.write(
                                    backoff_path(directory),
                                    backoff_document(
                                        binding,
                                        time.time_ns() + LOGIN_LIMIT_SECONDS * 1_000_000_000,
                                        "login_limit",
                                    ),
                                )
                            else:
                                io.write(
                                    backoff_path(directory),
                                    backoff_document(
                                        binding,
                                        time.time_ns() + login_backoff * 1_000_000_000,
                                    ),
                                )
                        elif not session_hold["charged"] and source_allowed is None:
                            # The session logged in but admitted no task at all.
                            budget_charge(io, directory, budget, 2)

            for index in range(len(flat)):
                if session_ran:
                    # The session owned this invocation's page pass: it either
                    # settled every candidate or stopped for a run-wide reason.
                    break
                if throttled:
                    # Recorded login refusal: this invocation must not call the
                    # source again, with or without a retryable entry.
                    break
                if state["cancelled"] or stop.is_set():
                    state["cancelled"] = True
                    save()
                    break
                if not eligible(index):
                    continue
                if pace and source_calls and stop.wait(pace):
                    # Pacing only separates source calls; a cancellation during
                    # the pause must not strand a fresh reservation.
                    state["cancelled"] = True
                    save()
                    break
                attempt = state["entries"][index]["attempts"] + 1
                task = reserve(index, attempt)
                if task is None:
                    break
                started = time.monotonic()
                captured = page_capture.capture_process(
                    root,
                    task,
                    stop=stop,
                    source_allowed=admit_source if source_allowed is not None else None,
                    restart_of=(
                        state["entries"][index]["links"][-1]["locator"]
                        if state["entries"][index]["links"]
                        else None
                    ),
                )
                if settle(index, attempt, captured, time.monotonic() - started, 2) == "stop":
                    break
            if not state["cancelled"] and all(
                entry_terminal(
                    e,
                    header["policy"]["max_attempts"],
                    retries.get((state["page"], i)),
                    transport_cap,
                )
                for i, e in enumerate(state["entries"])
            ):
                handoff_path = directory / f"handoff-{state['page']:06d}.json"
                version = (raw._decode(raw._read(handoff_path, MAX_CONTROL))["version"]
                           if handoff_path.exists() else HANDOFF_VERSION)
                handoff = handoff_document(state, page, version=version)
                candidate = handoff
                raw._install(handoff_path, encode(handoff))
                if handoff["version"] in ADMISSION_VERSIONS:
                    io.write(admission_path(directory, handoff["page"]),
                             admission_document(handoff, "pending"))
                state["last_handoff"] = digest(handoff)
                state["completed_requests"] += len(flat)
                state["page"] += 1
                state["page_sha256"] = page["next_sha256"]
                state["entries"] = []
                prior_handoff = handoff
                save()
            if stop.is_set() and not state["cancelled"]:
                save()
            state_check(state, binding, header)
            return finalize()
        except BaseException as primary:
            if accepting and (
                stop.is_set() or isinstance(primary, (KeyboardInterrupt, SystemExit))
            ):
                state["cancelled"] = True
                if candidate is not None and candidate["version"] in ADMISSION_VERSIONS:
                    try:
                        # Failure inside handoff installation may precede the
                        # normal pending write. Retain denial independently of
                        # a potentially failing immutable cancellation veto.
                        decision_path = admission_path(directory, candidate["page"])
                        if not decision_path.exists():
                            io.write(decision_path, admission_document(candidate, "pending"))
                    except BaseException as secondary:
                        primary.add_note(
                            "campaign_pending_persistence_failed:" + type(secondary).__name__
                        )
                try:
                    save()
                except BaseException as secondary:
                    primary.add_note("campaign_veto_persistence_failed:" + type(secondary).__name__)
            raise


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    build = sub.add_parser("compile")
    build.add_argument("--plan", type=Path, required=True)
    build.add_argument("--manifest", type=Path, required=True)
    build.add_argument("--max-attempts", type=int, default=3)
    build.add_argument("--request-budget", type=int, required=True)
    build.add_argument("--backoff-seconds", type=int, default=60)
    execute = sub.add_parser("run")
    execute.add_argument("--manifest", type=Path, required=True)
    execute.add_argument("--root", type=Path, required=True)
    execute.add_argument("--run", required=True)
    execute.add_argument("--slot", required=True)
    verify = sub.add_parser("verify")
    verify.add_argument("--root", type=Path, required=True)
    verify.add_argument("--handoff", type=Path, required=True)
    verify.add_argument("--sha256", required=True)
    args = parser.parse_args(argv)
    stop = Event()
    previous = {
        sig: signal.signal(sig, lambda *_: stop.set()) for sig in (signal.SIGTERM, signal.SIGINT)
    }
    try:
        if args.command == "compile":
            result = compile_manifest(
                load_json(args.plan, 128 * 1024 * 1024),
                args.manifest,
                dict(
                    max_attempts=args.max_attempts,
                    request_budget=args.request_budget,
                    backoff_seconds=args.backoff_seconds,
                ),
            )
        elif args.command == "verify":
            verified = verify_handoff(args.root, args.handoff, args.sha256)
            result = dict(status="complete", verified=True, page=verified["page"])
        else:
            result = run(args.manifest, args.root, args.run, args.slot, stop=stop)
        print(json.dumps(result, sort_keys=True))
        return 0 if args.command == "compile" or result["status"] == "complete" else 1
    except (ValueError, OSError) as exc:
        # Only authored protocol errors may enter diagnostics.
        reason = (
            str(exc)
            if type(exc) in (CampaignError, raw.RawAcquisitionError, page_capture.PageCaptureError)
            else "campaign_operational_error"
        )
        print(json.dumps(dict(status="refused", error=reason)))
        return 75 if reason == "acquisition_busy" else 2
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)


if __name__ == "__main__":
    raise SystemExit(main())

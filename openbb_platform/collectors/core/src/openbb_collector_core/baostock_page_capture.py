"""BaoStock-only spawn/page-ACK acquisition. Not raw-acquisition-v1.

The provider owns its SDK session; only this collector writes private evidence.
One fragment credit, one page in flight. No next() before durable page ACK.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import multiprocessing as mp
import os
import time
from pathlib import Path
from threading import Event

from . import raw_acquisition as raw
from .providers import executor_for, package_metadata

VERSION = "baostock-page-capture-v1"
SESSION_VERSION = "baostock-page-capture-session-v1"
FRAME = 65536
CONTROL = 4096
AUTHORITY_BYTES = 262144
BODY_BYTES = 134217728
OBSERVATIONS = 128
ZERO = "0" * 64
DEFAULT_LIMITS = dict(
    body_bytes=BODY_BYTES,
    observations=OBSERVATIONS,
    source_seconds=180,
    drain_seconds=120,
    attempt_seconds=900,
)


class PageCaptureError(ValueError):
    """Authored operational refusal, never arbitrary SDK exception text."""


def require(condition, reason):
    if not condition:
        raise PageCaptureError(reason)


def encode(value):
    return raw._encode(value)


def sha(content):
    return hashlib.sha256(content).hexdigest()


def digest(value):
    return sha(encode(value))


def integer(value, low, high):
    return type(value) is int and low <= value <= high


def limits_check(limits):
    require(type(limits) is dict and set(limits) == set(DEFAULT_LIMITS), "page_limits_shape")
    for name, maximum in DEFAULT_LIMITS.items():
        require(integer(limits[name], 1, maximum), "page_limits_range")
    return dict(limits)


def task_check(task):
    from openbb_baostock.models.history_source import validate_literal
    from openbb_baostock.utils.source_evidence import OPERATIONS

    task = raw.validate_task(task)
    model = task["source"]["model"]
    require(
        task["source"]["provider"] == "baostock"
        and model.endswith("Source")
        and model[:-6] in OPERATIONS
        and len(task["requests"]) == 1,
        "page_task_shape",
    )
    params = task["requests"][0]["params"]
    require(type(params) is dict and set(params) == {"literal"}, "page_literal_shape")
    validate_literal(model[:-6], params["literal"])
    return task


def capture_locator(task):
    task = task_check(task)
    return VERSION + "/" + digest(dict(version=VERSION, task=task))


def _read(path, maximum=CONTROL):
    return raw._decode(raw._read(path, maximum))


def _install(path, document, metrics=None, maximum=CONTROL):
    data = encode(document)
    require(len(data) <= maximum, "page_control_limit")
    raw._install(path, data)
    if metrics is not None:
        metrics["immutable_installs"] += 1
        metrics["metadata_bytes"] += len(data)
    return sha(data)


def _send(conn, document):
    data = encode(document)
    require(len(data) <= CONTROL, "page_ipc_control_limit")
    conn.send_bytes(b"C" + data)


def _ack(conn):
    data = conn.recv_bytes(CONTROL + 1)
    require(data[:1] == b"C", "page_ack_shape")
    return raw._decode(data[1:])


def _frames(envelope):
    from openbb_baostock.utils.source_evidence import encoded_chunks

    buffer = bytearray()
    for token in encoded_chunks(envelope):
        while token:
            take = min(FRAME - len(buffer), len(token))
            buffer.extend(token[:take])
            token = token[take:]
            if len(buffer) == FRAME:
                yield bytes(buffer)
                buffer.clear()
    if buffer:
        yield bytes(buffer)


def _worker(conn, task):
    from openbb_baostock.utils.source_evidence import EvidenceSinkV1

    class Sink(EvidenceSinkV1):
        def __init__(self):
            self.page = 0

        def source_call(self, kind):
            _send(conn, dict(kind="source", source=kind))
            reply = _ack(conn)
            require(reply.get("kind") == "source_ack", "source_ack_shape")
            return reply["continue"]

        def observe(self, envelope, metadata):
            page = self.page
            _send(conn, dict(kind="begin", page=page, metadata=metadata))
            reply = _ack(conn)
            require(reply == dict(kind="begin_ack", page=page), "begin_ack_shape")
            whole, size, count = hashlib.sha256(), 0, 0
            for fragment in _frames(envelope):
                conn.send_bytes(b"D" + fragment)
                reply = _ack(conn)
                require(
                    reply == dict(kind="fragment_ack", page=page, fragment=count),
                    "fragment_ack_shape",
                )
                whole.update(fragment)
                size += len(fragment)
                count += 1
            _send(
                conn,
                dict(kind="end", page=page, bytes=size, fragments=count, sha256=whole.hexdigest()),
            )
            reply = _ack(conn)
            require(
                reply.get("kind") == "page_ack"
                and reply.get("page") == page
                and reply.get("sha256") == whole.hexdigest(),
                "page_ack_shape",
            )
            self.page += 1
            return reply["continue"]

    try:
        executor = executor_for("baostock")
        result = asyncio.run(
            executor.execute(
                provider_name="baostock",
                model_name=task["source"]["model"],
                params=task["requests"][0]["params"],
                credentials={},
                evidence_sink_v1=Sink(),
            )
        )
        import resource

        _send(
            conn,
            dict(
                kind="terminal",
                summary=result[0].evidence,
                worker_maxrss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
            ),
        )
    except Exception:
        # The parent distinguishes this operational failure from SDK-authored
        # summary failures; opaque exception text never enters evidence.
        try:
            _send(conn, dict(kind="worker_error", reason="worker_operational_error"))
        except (OSError, EOFError):
            pass
        raise
    finally:
        conn.close()


def _prefix(directory, page, identity, budget):
    """Read exact contiguous journal records; never trim an uncertain suffix."""
    body_path = raw._path(directory, f"page-{page:03d}.body")
    journal_path = raw._path(directory, f"page-{page:03d}.fragments.jsonl")
    require(body_path.is_file() and journal_path.is_file(), "missing_page_fragments")
    require(body_path.stat().st_size <= budget, "page_body_limit")
    size, count, chain, whole = 0, 0, ZERO, hashlib.sha256()
    uncertain = False
    with body_path.open("rb") as body, journal_path.open("rb") as journal:
        while True:
            line = journal.readline(CONTROL + 2)
            if not line:
                break
            if len(line) > CONTROL + 1 or not line.endswith(b"\n"):
                uncertain = True
                break
            record = raw._decode(line)
            require(
                set(record)
                == {"identity", "page", "fragment", "offset", "bytes", "sha256", "previous"},
                "fragment_record_shape",
            )
            require(
                record["identity"] == identity
                and type(record["page"]) is int
                and record["page"] == page
                and type(record["fragment"]) is int
                and record["fragment"] == count
                and type(record["offset"]) is int
                and record["offset"] == size
                and record["previous"] == chain
                and integer(record["bytes"], 1, FRAME),
                "fragment_sequence",
            )
            fragment = body.read(record["bytes"])
            require(
                len(fragment) == record["bytes"] and sha(fragment) == record["sha256"],
                "fragment_digest",
            )
            whole.update(fragment)
            chain = sha(line)
            size += len(fragment)
            count += 1
            require(
                size <= budget and count <= budget // FRAME + OBSERVATIONS + 1, "fragment_budget"
            )
        uncertain = uncertain or bool(body.read(1))
    return dict(
        bytes=size,
        fragments=count,
        fragment_root=chain,
        sha256=whole.hexdigest(),
        uncertain=uncertain,
        body_path=str(body_path),
    )


def read_page(directory, page, *, decode=True):
    """Read a complete bounded observation; parent capture verification is separate."""
    from openbb_baostock.utils.source_evidence import decode_evidence

    envelope = json.loads(
        raw._read(raw._path(Path(directory), f"page-{page:03d}.body"), BODY_BYTES)
    )
    return decode_evidence(envelope) if decode else envelope


def read_capture(root, task, expected=None):
    """Read-only bounded attempt proof, including an interrupted durable prefix."""
    task = task_check(task)
    locator = capture_locator(task)
    directory = raw._path(Path(root), locator)
    authority = _read(directory / "authority.json", AUTHORITY_BYTES)
    require(
        authority.get("version") == VERSION and encode(authority.get("task")) == encode(task),
        "page_authority_binding",
    )
    limits = limits_check(authority["limits"])
    identity = digest(dict(version=VERSION, task=task))
    require(
        authority["identity"] == identity
        and type(authority["received_ns"]) is int
        and authority["codec"] == "baostock-source-evidence-v1-or-v2",
        "page_authority_shape",
    )
    soft_path = raw._path(directory, "soft-deadline.json")
    soft = _read(soft_path) if soft_path.exists() else None
    require(soft is None or soft == dict(identity=identity, reason="soft_deadline"),
            "soft_deadline_binding")
    previous = digest(authority)
    pages, pending, total, missing = [], None, 0, False
    for page in range(limits["observations"]):
        begin_path = raw._path(directory, f"page-{page:03d}.begin.json")
        receipt_path = raw._path(directory, f"page-{page:03d}.receipt.json")
        if not begin_path.exists():
            require(not receipt_path.exists(), "missing_page_begin")
            missing = True
            continue
        require(not missing and pending is None, "page_sequence_gap")
        begin = _read(begin_path)
        require(
            begin["identity"] == identity
            and type(begin["page"]) is int
            and begin["page"] == page
            and begin["previous_page_root"] == previous,
            "page_begin_binding",
        )
        require(
            begin["previous_state_sha256"] == (pages[-1]["receipt"]["sha256"] if pages else None),
            "page_state_chain",
        )
        prefix = _prefix(directory, page, identity, limits["body_bytes"] - total)
        total += prefix["bytes"]
        if not receipt_path.exists():
            pending = dict(
                page=page, begin_sha256=digest(begin), acknowledgement="unknown", **prefix
            )
            continue
        receipt = _read(receipt_path)
        require(
            not prefix["uncertain"]
            and receipt
            == dict(
                identity=identity,
                page=page,
                begin_sha256=digest(begin),
                previous_page_root=previous,
                bytes=prefix["bytes"],
                fragments=prefix["fragments"],
                fragment_root=prefix["fragment_root"],
                sha256=prefix["sha256"],
            ),
            "page_receipt_binding",
        )
        envelope = read_page(directory, page)
        require(
            envelope["operation"] + "Source" == task["source"]["model"]
            and encode(envelope["params"]) == encode(task["requests"][0]["params"]["literal"]),
            "page_source_binding",
        )
        previous = digest(receipt)
        pages.append(
            dict(
                page=page,
                receipt_sha256=previous,
                begin=begin,
                receipt=receipt,
                source_ok=type(envelope["error_code"]) is str
                and envelope["error_code"] == "0"
                and envelope["reason"] != "sdk_value_unrepresentable",
                exhausted=bool(
                    envelope["pages"]
                    and type(envelope["pages"][-1]["fields"]) is list
                    and type(envelope["pages"][-1]["rows"]) is list
                    and (len(envelope["pages"][-1]["rows"]) < 20000
                         if envelope["operation"] == "BaostockDailyHistoryKETF"
                         else len(envelope["pages"][-1]["rows"]) != 2000)
                ),
                body_path=prefix["body_path"],
            )
        )
    terminal_path = raw._path(directory, "terminal.json")
    terminal = _read(terminal_path) if terminal_path.exists() else None
    cancel_path = raw._path(directory, "cancel.json")
    cancel = _read(cancel_path) if cancel_path.exists() else None
    eligible_path = raw._path(directory, "eligible.json")
    eligible = _read(eligible_path) if eligible_path.exists() else None
    for record in (terminal, cancel, eligible):
        if record is not None:
            require(record["identity"] == identity, "attempt_record_binding")
    if terminal is not None:
        require(
            terminal["pages"] == len(pages)
            and terminal["last_page_root"] == previous
            and terminal["bytes"] == total,
            "terminal_page_binding",
        )
    if eligible is not None:
        require(
            terminal is not None
            and eligible
            == dict(identity=identity, terminal_sha256=digest(terminal), last_page_root=previous),
            "eligibility_binding",
        )
    complete = bool(eligible and not cancel and not pending and terminal["status"] == "complete")
    if terminal is not None and terminal["status"] == "complete":
        require(
            pages
            and pages[-1]["exhausted"]
            and pending is None
            and all(p["source_ok"] for p in pages)
            and terminal["summary"]["status"] == "complete"
            and terminal["summary"]["reason"] == "exhausted"
            and terminal["summary"]["cleanup"] == "ok"
            and terminal["summary"]["pages"] == len(pages),
            "false_page_completion",
        )
    proof = dict(
        authority=digest(authority),
        pages=[p["receipt_sha256"] for p in pages],
        pending=pending,
        terminal=digest(terminal) if terminal else None,
        cancel=digest(cancel) if cancel else None,
        eligible=digest(eligible) if eligible else None,
    )
    if soft is not None:
        proof["soft_deadline"] = digest(soft)
    anchor = digest(proof)
    require(expected is None or expected == anchor, "page_capture_anchor")
    return dict(
        kind=VERSION,
        task=task,
        locator=locator,
        authority=authority,
        pages=pages,
        pending=pending,
        terminal=terminal,
        terminal_sha256=proof["terminal"],
        last_page_root=previous,
        capture_sha256=anchor,
        complete=complete,
        status="cancelled" if cancel else "success" if complete else "unknown" if terminal is None and soft else "failed",
        restart_of=authority["restart_of"],
        refetch=authority["refetch"],
        metrics=terminal["metrics"] if terminal else None,
    )


def recover_capture(root, task):
    """Producer-only barrier recovery under the campaign lock; zero source calls.

    Re-sync an existing eligibility marker; visibility alone is not proof its
    install's final directory barrier returned. Never create missing evidence.
    """
    captured = read_capture(root, task)
    directory = raw._path(Path(root), captured["locator"])
    if captured["complete"]:
        with raw._path(directory, "eligible.json").open("rb") as stream:
            os.fsync(stream.fileno())
        raw._sync(directory)
    # Allocation credit is not source evidence; release a crashed producer's
    # unused reservation only after its exact attempt has been read and proved.
    capacity_path = raw._path(directory, ".capacity")
    if capacity_path.exists():
        with capacity_path.open("r+b") as capacity:
            capacity.truncate(0)
            capacity.flush()
            os.fsync(capacity.fileno())
    return read_capture(root, task, expected=captured["capture_sha256"])


def _capacity_bytes(limits):
    fragments = (limits["body_bytes"] + FRAME - 1) // FRAME + limits["observations"] - 1
    return (
        limits["body_bytes"]
        + fragments * (CONTROL + 1)
        + limits["observations"] * CONTROL * 3
        + AUTHORITY_BYTES
        + 16 * 1024 * 1024
    )


def capture_process(root, task, *, stop=None, restart_of=None, limits=None, source_allowed=None):
    """Caller holds the campaign root lock. Existing attempt means read, not refetch."""
    task = task_check(task)
    limits = limits_check(DEFAULT_LIMITS if limits is None else limits)
    require(
        restart_of is None or (type(restart_of) is str and restart_of.startswith(VERSION + "/")),
        "invalid_restart_locator",
    )
    stop = stop if stop is not None else Event()
    root = raw._path(Path(root))
    locator = capture_locator(task)
    directory = raw._path(root, locator)
    if directory.exists():
        return read_capture(root, task)
    metadata = package_metadata("baostock", task["source"]["model"])
    identity = digest(dict(version=VERSION, task=task))
    authority = dict(
        version=VERSION,
        identity=identity,
        task=task,
        limits=limits,
        codec="baostock-source-evidence-v1-or-v2",
        received_ns=time.time_ns(),
        source_time=None,
        packages=metadata,
        restart_of=restart_of,
        refetch=restart_of is not None,
    )
    require(len(encode(authority)) <= AUTHORITY_BYTES, "page_authority_limit")
    raw._mkdir(root)
    reserve_bytes = _capacity_bytes(limits)
    stat = os.statvfs(root)
    require(stat.f_bavail * stat.f_frsize >= reserve_bytes, "page_storage_headroom")
    raw._mkdir(directory)
    metrics = dict(
        fragment_writes=0,
        fragment_fsyncs=0,
        body_bytes=0,
        metadata_bytes=0,
        immutable_installs=0,
        max_frame_bytes=0,
        source_query_calls=0,
        source_next_calls=0,
        reserved_capacity_bytes=reserve_bytes,
    )
    # This is allocation credit, NOT source evidence. It shrinks only as new
    # durable data consumes credit; a crash-left remainder is retained, not deleted.
    capacity = (directory / ".capacity").open("xb")
    body = journal = parent = child = process = None
    primary = None
    page = 0
    total = 0
    previous = digest(authority)
    terminal_summary = None
    reason = "interrupted"
    source_kind = None
    start = time.monotonic()
    deadline = start + limits["source_seconds"]
    quota_remaining = reserve_bytes - 16 * 1024 * 1024
    size, count, chain, whole = 0, 0, ZERO, hashlib.sha256()
    try:
        os.posix_fallocate(capacity.fileno(), 0, quota_remaining)
        capacity.flush()
        os.fsync(capacity.fileno())
        _install(directory / "authority.json", authority, metrics, AUTHORITY_BYTES)
        if stop.is_set():
            _install(
                directory / "cancel.json", dict(identity=identity, reason="cancelled"), metrics
            )
        else:
            context = mp.get_context("spawn")
            parent, child = context.Pipe()
            process = context.Process(target=_worker, args=(child, task))
            process.start()
            child.close()
            child = None
            while True:
                if stop.is_set() and not (directory / "cancel.json").exists():
                    _install(
                        directory / "cancel.json",
                        dict(identity=identity, reason="cancelled"),
                        metrics,
                    )
                now = time.monotonic()
                if now >= deadline or now >= start + limits["attempt_seconds"]:
                    reason = "drain_deadline" if body is not None else "source_deadline"
                    break
                if not parent.poll(0.05):
                    if not process.is_alive():
                        reason = "worker_died"
                        break
                    continue
                try:
                    message = parent.recv_bytes(FRAME + 1)
                except EOFError:
                    reason = "worker_died"
                    break
                metrics["max_frame_bytes"] = max(metrics["max_frame_bytes"], len(message))
                if message[:1] == b"D":
                    require(body is not None, "fragment_without_begin")
                    fragment = message[1:]
                    require(1 <= len(fragment) <= FRAME, "page_frame_limit")
                    if total + size + len(fragment) > limits["body_bytes"]:
                        reason = "resource_exhausted"
                        break
                    # Release enough reserved allocation before adding the body
                    # and journal record. Physical storage failure remains possible.
                    quota_remaining = max(0, quota_remaining - len(fragment) - CONTROL - 1)
                    capacity.truncate(quota_remaining)
                    body.write(fragment)
                    body.flush()
                    os.fsync(body.fileno())
                    record = dict(
                        identity=identity,
                        page=page,
                        fragment=count,
                        offset=size,
                        bytes=len(fragment),
                        sha256=sha(fragment),
                        previous=chain,
                    )
                    line = encode(record)
                    require(len(line) <= CONTROL, "fragment_record_limit")
                    journal.write(line)
                    journal.flush()
                    os.fsync(journal.fileno())
                    metrics["fragment_writes"] += 2
                    metrics["fragment_fsyncs"] += 2
                    metrics["body_bytes"] += len(fragment)
                    metrics["metadata_bytes"] += len(line)
                    chain = sha(line)
                    whole.update(fragment)
                    size += len(fragment)
                    _send(parent, dict(kind="fragment_ack", page=page, fragment=count))
                    count += 1
                    continue
                require(message[:1] == b"C" and len(message) <= CONTROL + 1, "page_message_shape")
                item = raw._decode(message[1:])
                kind = item.get("kind")
                if kind == "source":
                    require(
                        body is None and item["source"] in ("login", "query", "next"),
                        "source_phase",
                    )
                    source_kind = item["source"]
                    if source_allowed is not None and not source_allowed(source_kind):
                        _install(directory / "soft-deadline.json", dict(identity=identity, reason="soft_deadline"))
                        reason = "soft_deadline"
                        break
                    proceed = (
                        not stop.is_set()
                        and page < limits["observations"]
                        and total < limits["body_bytes"]
                    )
                    if proceed:
                        if source_kind == "query":
                            metrics["source_query_calls"] += 1
                        elif source_kind == "next":
                            metrics["source_next_calls"] += 1
                    _send(parent, dict(kind="source_ack", **{"continue": proceed}))
                    deadline = time.monotonic() + limits["source_seconds"]
                elif kind == "begin":
                    require(
                        body is None
                        and type(item["page"]) is int
                        and item["page"] == page
                        and page < limits["observations"]
                        and source_kind is not None,
                        "page_begin_phase",
                    )
                    meta = item["metadata"]
                    require(
                        set(meta) == {"trigger", "previous_state_sha256", "received_ns"}
                        and meta["trigger"]
                        in ("login_return", "query_return", "next_return", "next_raise")
                        and integer(meta["received_ns"], 1, 2**63 - 1),
                        "page_observation_shape",
                    )
                    begin = dict(
                        identity=identity,
                        page=page,
                        previous_page_root=previous,
                        source_time=None,
                        **meta,
                    )
                    _install(directory / f"page-{page:03d}.begin.json", begin, metrics)
                    body = (directory / f"page-{page:03d}.body").open("xb")
                    journal = (directory / f"page-{page:03d}.fragments.jsonl").open("xb")
                    raw._sync(directory)
                    size, count, chain, whole = 0, 0, ZERO, hashlib.sha256()
                    deadline = time.monotonic() + limits["drain_seconds"]
                    _send(parent, dict(kind="begin_ack", page=page))
                elif kind == "end":
                    require(
                        body is not None
                        and item
                        == dict(
                            kind="end",
                            page=page,
                            bytes=size,
                            fragments=count,
                            sha256=whole.hexdigest(),
                        ),
                        "page_end_binding",
                    )
                    body.close()
                    body = None
                    journal.close()
                    journal = None
                    receipt = dict(
                        identity=identity,
                        page=page,
                        begin_sha256=digest(begin),
                        previous_page_root=previous,
                        bytes=size,
                        fragments=count,
                        fragment_root=chain,
                        sha256=whole.hexdigest(),
                    )
                    previous = _install(
                        directory / f"page-{page:03d}.receipt.json", receipt, metrics
                    )
                    # Actual bytes and codec must be readable before permitting next.
                    proved = read_capture(root, task)
                    require(
                        len(proved["pages"]) == page + 1 and proved["pending"] is None,
                        "page_ack_proof",
                    )
                    total += size
                    proceed = (
                        not stop.is_set()
                        and page + 1 < limits["observations"]
                        and total < limits["body_bytes"]
                    )
                    _send(
                        parent,
                        dict(
                            kind="page_ack",
                            page=page,
                            sha256=whole.hexdigest(),
                            **{"continue": proceed},
                        ),
                    )
                    page += 1
                    deadline = time.monotonic() + limits["source_seconds"]
                elif kind == "terminal":
                    require(body is None, "terminal_with_pending_page")
                    terminal_summary = item["summary"]
                    metrics["worker_maxrss_kib"] = item["worker_maxrss_kib"]
                    require(
                        terminal_summary["version"] == "baostock-page-summary-v1"
                        and terminal_summary["pages"] == page
                        and terminal_summary["query_calls"] == metrics["source_query_calls"]
                        and terminal_summary["next_calls"] == metrics["source_next_calls"],
                        "worker_summary_binding",
                    )
                    reason = terminal_summary["reason"]
                    break
                elif kind == "worker_error":
                    reason = "worker_operational_error"
                    break
                else:
                    raise PageCaptureError("unknown_page_message")
        if body is not None:
            body.close()
            body = None
        if journal is not None:
            journal.close()
            journal = None
        # A summary precedes multiprocessing finalizers, not process exit.
        # Share the original absolute attempt budget; drain bounds this final
        # phase independently. Polling is for cancellation, not a success gate.
        exited_normally = False
        if process is not None:
            exit_deadline = min(
                start + limits["attempt_seconds"],
                time.monotonic() + limits["drain_seconds"],
            )
            while terminal_summary is not None and process.is_alive():
                if stop.is_set():
                    reason = "cancelled"
                    break
                remaining = exit_deadline - time.monotonic()
                if remaining <= 0:
                    reason = "exit_deadline"
                    break
                process.join(min(0.05, remaining))
            exited_normally = (
                not process.is_alive()
                and time.monotonic() < exit_deadline
                and not stop.is_set()
            )
            if process.is_alive():
                # Transfer ownership into this scope: the outer finally must
                # not repeat signals or grant another pair of grace budgets.
                worker, process = process, None
                reap_failures = []
                for signal_worker in (worker.terminate, worker.kill):
                    try:
                        if worker.is_alive():
                            signal_worker()
                    except BaseException as exc:
                        reap_failures.append(exc)
                    try:
                        worker.join(2 if worker.is_alive() else 0)
                    except BaseException as exc:
                        reap_failures.append(exc)
                try:
                    require(not worker.is_alive(), "page_worker_not_reaped")
                    metrics["worker_exit"] = worker.exitcode
                    worker.close()
                except BaseException as exc:
                    reap_failures.append(exc)
                if reap_failures:
                    raise reap_failures[0]
            else:
                # join also reaps a child observed dead before the first poll.
                process.join(0)
                metrics["worker_exit"] = process.exitcode
        proved = read_capture(root, task)
        complete = bool(
            terminal_summary
            and terminal_summary["status"] == "complete"
            and terminal_summary["cleanup"] == "ok"
            and metrics.get("worker_exit") == 0
            and exited_normally
            and not stop.is_set()
            and not proved["pending"]
        )
        metrics["source_query_admissions"] = metrics["source_query_calls"]
        metrics["source_next_admissions"] = metrics["source_next_calls"]
        if terminal_summary is None:
            metrics["source_query_calls"] = None
            metrics["source_next_calls"] = None
        metrics["elapsed_seconds"] = time.monotonic() - start
        if reason == "soft_deadline":
            return read_capture(root, task)  # Durable prefix, without a terminal verdict.
        terminal = dict(
            identity=identity,
            status="complete" if complete else "partial",
            reason=reason,
            summary=terminal_summary,
            pages=len(proved["pages"]),
            last_page_root=proved["last_page_root"],
            bytes=sum(p["receipt"]["bytes"] for p in proved["pages"])
            + (proved["pending"]["bytes"] if proved["pending"] else 0),
            metrics=metrics,
        )
        terminal_hash = _install(directory / "terminal.json", terminal)
        if stop.is_set():
            _install(directory / "cancel.json", dict(identity=identity, reason="cancelled"))
        if complete and not stop.is_set():
            _install(
                directory / "eligible.json",
                dict(
                    identity=identity,
                    terminal_sha256=terminal_hash,
                    last_page_root=proved["last_page_root"],
                ),
            )
        if stop.is_set():
            _install(directory / "cancel.json", dict(identity=identity, reason="cancelled"))
        capacity.truncate(0)
        capacity.flush()
        os.fsync(capacity.fileno())
        return read_capture(root, task)
    except BaseException as exc:
        primary = exc
        raise
    finally:
        failures = []
        if process is not None:
            # This owns only pre-reap failures or a naturally exited worker.
            # Signal errors must not skip join/kill, and none may replace primary.
            for signal_worker in (process.terminate, process.kill):
                try:
                    if process.is_alive():
                        signal_worker()
                except BaseException as exc:
                    failures.append(exc)
                try:
                    process.join(2 if process.is_alive() else 0)
                except BaseException as exc:
                    failures.append(exc)
            try:
                require(not process.is_alive(), "page_worker_not_reaped")
                process.close()
            except BaseException as exc:
                failures.append(exc)
        try:
            capacity.truncate(0)
            capacity.flush()
            os.fsync(capacity.fileno())
        except BaseException as exc:
            failures.append(exc)
        for resource in (body, journal, parent, child, capacity):
            if resource is not None:
                try:
                    resource.close()
                except BaseException as exc:
                    failures.append(exc)
        if failures:
            if primary is not None:
                primary.add_note("page_capture_cleanup_failed")
            else:
                raise failures[0]


def _session_worker(conn, tasks):
    """One SDK session; one task at a time, each admitted by the parent.

    Every task still owns its attempt: a `task_begin` handshake proves the parent
    wrote `authority.json` before the first source call, and the `terminal` frame
    is answered only after that attempt is durable and installed. A task-level
    failure is reported and then the next task continues; a session-level failure
    ends the scope, whose logout is reported once in `session_end`.
    """
    from openbb_baostock.utils.source_evidence import (
        EvidenceSinkV1,
        active_session,
        source_session,
    )

    class Sink(EvidenceSinkV1):
        def __init__(self, index):
            self.index = index
            self.page = 0

        def source_call(self, kind):
            _send(conn, dict(kind="source", index=self.index, source=kind))
            reply = _ack(conn)
            require(
                reply.get("kind") == "source_ack" and reply.get("index") == self.index,
                "source_ack_shape",
            )
            return reply["continue"]

        def observe(self, envelope, metadata):
            index, page = self.index, self.page
            _send(conn, dict(kind="begin", index=index, page=page, metadata=metadata))
            reply = _ack(conn)
            require(reply == dict(kind="begin_ack", index=index, page=page), "begin_ack_shape")
            whole, size, count = hashlib.sha256(), 0, 0
            for fragment in _frames(envelope):
                conn.send_bytes(b"D" + fragment)
                reply = _ack(conn)
                require(
                    reply == dict(kind="fragment_ack", index=index, page=page, fragment=count),
                    "fragment_ack_shape",
                )
                whole.update(fragment)
                size += len(fragment)
                count += 1
            _send(
                conn,
                dict(
                    kind="end",
                    index=index,
                    page=page,
                    bytes=size,
                    fragments=count,
                    sha256=whole.hexdigest(),
                ),
            )
            reply = _ack(conn)
            require(
                reply.get("kind") == "page_ack"
                and reply.get("index") == index
                and reply.get("page") == page
                and reply.get("sha256") == whole.hexdigest(),
                "page_ack_shape",
            )
            self.page += 1
            return reply["continue"]

    import resource

    session = None
    try:
        _send(conn, dict(kind="session_login"))
        if not _ack(conn).get("continue"):
            return
        with source_session() as (ok, error_code):
            session = active_session()
            _send(conn, dict(kind="session", ok=ok, error_code=error_code))
            reply = _ack(conn)
            require(reply.get("kind") == "session_ack", "session_ack_shape")
            proceed = ok and reply["continue"] is True
            index = 0
            while proceed and index < len(tasks):
                _send(conn, dict(kind="task_begin", index=index))
                reply = _ack(conn)
                require(
                    reply.get("kind") == "task_ack" and reply.get("index") == index,
                    "task_ack_shape",
                )
                if not reply["continue"]:
                    break
                task = tasks[index]
                try:
                    result = asyncio.run(
                        executor_for("baostock").execute(
                            provider_name="baostock",
                            model_name=task["source"]["model"],
                            params=task["requests"][0]["params"],
                            credentials={},
                            evidence_sink_v1=Sink(index),
                        )
                    )
                except Exception:
                    # A task-level failure never ends the page's session: the
                    # parent installs the attempt's terminal and answers below.
                    _send(
                        conn,
                        dict(
                            kind="task_error",
                            index=index,
                            reason="worker_operational_error",
                            worker_maxrss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
                        ),
                    )
                else:
                    _send(
                        conn,
                        dict(
                            kind="terminal",
                            index=index,
                            summary=result[0].evidence,
                            worker_maxrss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
                        ),
                    )
                reply = _ack(conn)
                require(
                    reply.get("kind") == "continue_ack" and reply.get("index") == index,
                    "continue_ack_shape",
                )
                if not reply["continue"]:
                    break
                index += 1
        _send(
            conn,
            dict(
                kind="session_end",
                login_calls=session.login_calls,
                logout_calls=session.logout_calls,
                cleanup=session.cleanup,
            ),
        )
    except Exception:
        # Opaque exception text never enters evidence; the parent owns the
        # decision to keep or abandon the in-flight attempt.
        try:
            _send(conn, dict(kind="worker_error", reason="worker_operational_error"))
        except (OSError, EOFError):
            pass
        raise
    finally:
        conn.close()


def capture_session(root, tasks, *, stop=None, limits=None, on_capture=None, on_task=None,
                    source_allowed=None):
    """One SDK session, one attempt per task, each installed before the next.

    The login is paid once for the whole task list; every task still owns its
    own attempt directory, page receipts and terminal in the unchanged layout.
    `on_task(index)` is asked only after that login was proven and before its
    `authority.json` exists, so a refused session can never spend an attempt.
    `on_capture(index, captured)` runs after each attempt is durable and its
    return value decides whether the next task is admitted at all.
    """
    require(type(tasks) is list and 1 <= len(tasks) <= 16, "page_session_tasks")
    tasks = [task_check(task) for task in tasks]
    limits = limits_check(DEFAULT_LIMITS if limits is None else limits)
    require(on_capture is None or callable(on_capture), "page_session_callback")
    require(on_task is None or callable(on_task), "page_session_callback")
    stop = stop if stop is not None else Event()
    root = raw._path(Path(root))
    plan = []
    for index, task in enumerate(tasks):
        locator = capture_locator(task)
        directory = raw._path(root, locator)
        # An existing attempt is read by its caller, never refetched inside a
        # live session; refusing before any byte keeps both facts honest.
        require(not directory.exists(), "page_session_attempt_exists")
        plan.append(
            dict(
                index=index,
                task=task,
                locator=locator,
                directory=directory,
                packages=package_metadata("baostock", task["source"]["model"]),
                reserve_bytes=_capacity_bytes(limits),
            )
        )
    results = []
    live = None
    admitted = 0
    session_ok = None
    session_error = None
    session_end = None
    reason = "interrupted"
    started = time.monotonic()
    deadline = started + limits["attempt_seconds"]
    parent = child = process = None
    primary = None

    def open_task(index):
        """Durable authority before the source is asked anything for this task."""
        item = plan[index]
        directory = item["directory"]
        identity = digest(dict(version=VERSION, task=item["task"]))
        authority = dict(
            version=VERSION,
            identity=identity,
            task=item["task"],
            limits=limits,
            codec="baostock-source-evidence-v1-or-v2",
            received_ns=time.time_ns(),
            source_time=None,
            packages=item["packages"],
            restart_of=None,
            refetch=False,
        )
        require(len(encode(authority)) <= AUTHORITY_BYTES, "page_authority_limit")
        raw._mkdir(root)
        reserve_bytes = item["reserve_bytes"]
        stat = os.statvfs(root)
        require(stat.f_bavail * stat.f_frsize >= reserve_bytes, "page_storage_headroom")
        raw._mkdir(directory)
        metrics = dict(
            fragment_writes=0,
            fragment_fsyncs=0,
            body_bytes=0,
            metadata_bytes=0,
            immutable_installs=0,
            max_frame_bytes=0,
            source_query_calls=0,
            source_next_calls=0,
            reserved_capacity_bytes=reserve_bytes,
        )
        # Allocation credit, never source evidence; a crash-left remainder stays.
        capacity = (directory / ".capacity").open("xb")
        try:
            quota_remaining = reserve_bytes - 16 * 1024 * 1024
            os.posix_fallocate(capacity.fileno(), 0, quota_remaining)
            capacity.flush()
            os.fsync(capacity.fileno())
            _install(directory / "authority.json", authority, metrics, AUTHORITY_BYTES)
        except BaseException:
            capacity.close()
            raise
        return dict(
            index=index,
            task=item["task"],
            directory=directory,
            identity=identity,
            metrics=metrics,
            capacity=capacity,
            quota_remaining=quota_remaining,
            body=None,
            journal=None,
            page=0,
            total=0,
            previous=digest(authority),
            size=0,
            count=0,
            chain=ZERO,
            whole=hashlib.sha256(),
            source_kind=None,
            start=time.monotonic(),
        )

    def seal_task(task_state, terminal_summary, item_reason):
        """Install this attempt's terminal exactly once, then prove it readable."""
        directory = task_state["directory"]
        metrics = task_state["metrics"]
        for name in ("body", "journal"):
            stream = task_state[name]
            if stream is not None:
                stream.close()
                task_state[name] = None
        proved = read_capture(root, task_state["task"])
        metrics["source_query_admissions"] = metrics["source_query_calls"]
        metrics["source_next_admissions"] = metrics["source_next_calls"]
        if terminal_summary is None:
            metrics["source_query_calls"] = None
            metrics["source_next_calls"] = None
        metrics["elapsed_seconds"] = time.monotonic() - task_state["start"]
        metrics["session_index"] = task_state["index"]
        # The session's own liveness is proven by the frame protocol: a dead
        # worker never reports a terminal, so it can never read as complete.
        complete = bool(
            terminal_summary
            and terminal_summary["status"] == "complete"
            and terminal_summary["cleanup"] == "ok"
            and not stop.is_set()
            and not proved["pending"]
        )
        terminal = dict(
            identity=task_state["identity"],
            status="complete" if complete else "partial",
            reason=item_reason,
            summary=terminal_summary,
            pages=len(proved["pages"]),
            last_page_root=proved["last_page_root"],
            bytes=sum(p["receipt"]["bytes"] for p in proved["pages"])
            + (proved["pending"]["bytes"] if proved["pending"] else 0),
            metrics=metrics,
        )
        terminal_hash = _install(directory / "terminal.json", terminal)
        if stop.is_set():
            _install(
                directory / "cancel.json",
                dict(identity=task_state["identity"], reason="cancelled"),
            )
        if complete and not stop.is_set():
            _install(
                directory / "eligible.json",
                dict(
                    identity=task_state["identity"],
                    terminal_sha256=terminal_hash,
                    last_page_root=proved["last_page_root"],
                ),
            )
        capacity = task_state["capacity"]
        capacity.truncate(0)
        capacity.flush()
        os.fsync(capacity.fileno())
        capacity.close()
        task_state["capacity"] = None
        return read_capture(root, task_state["task"])

    try:
        context = mp.get_context("spawn")
        parent, child = context.Pipe()
        process = context.Process(target=_session_worker, args=(child, tasks))
        process.start()
        child.close()
        child = None
        while True:
            now = time.monotonic()
            if stop.is_set():
                if live is not None and not (live["directory"] / "cancel.json").exists():
                    _install(
                        live["directory"] / "cancel.json",
                        dict(identity=live["identity"], reason="cancelled"),
                        live["metrics"],
                    )
                reason = "cancelled"
                break
            if now >= deadline:
                reason = "attempt_deadline" if live is not None else "session_deadline"
                break
            if live is not None and now >= live["start"] + limits["attempt_seconds"]:
                # The v2.2 per-attempt bound still holds inside the session, so a
                # slow source cannot stretch one attempt past its own budget.
                reason = "attempt_deadline"
                break
            if not parent.poll(0.05):
                if not process.is_alive():
                    if reason == "interrupted":
                        reason = "worker_died"
                    break
                continue
            try:
                message = parent.recv_bytes(FRAME + 1)
            except EOFError:
                if reason == "interrupted":
                    reason = "worker_died"
                break
            metrics = live["metrics"] if live is not None else None
            if metrics is not None:
                metrics["max_frame_bytes"] = max(metrics["max_frame_bytes"], len(message))
            if message[:1] == b"D":
                require(live is not None and live["body"] is not None, "fragment_without_begin")
                fragment = message[1:]
                require(1 <= len(fragment) <= FRAME, "page_frame_limit")
                if live["total"] + live["size"] + len(fragment) > limits["body_bytes"]:
                    reason = "resource_exhausted"
                    break
                # Release enough reserved allocation before adding the body and
                # journal record. Physical storage failure remains possible.
                live["quota_remaining"] = max(
                    0, live["quota_remaining"] - len(fragment) - CONTROL - 1
                )
                live["capacity"].truncate(live["quota_remaining"])
                live["body"].write(fragment)
                live["body"].flush()
                os.fsync(live["body"].fileno())
                record = dict(
                    identity=live["identity"],
                    page=live["page"],
                    fragment=live["count"],
                    offset=live["size"],
                    bytes=len(fragment),
                    sha256=sha(fragment),
                    previous=live["chain"],
                )
                line = encode(record)
                require(len(line) <= CONTROL, "fragment_record_limit")
                live["journal"].write(line)
                live["journal"].flush()
                os.fsync(live["journal"].fileno())
                live["metrics"]["fragment_writes"] += 2
                live["metrics"]["fragment_fsyncs"] += 2
                live["metrics"]["body_bytes"] += len(fragment)
                live["metrics"]["metadata_bytes"] += len(line)
                live["chain"] = sha(line)
                live["whole"].update(fragment)
                live["size"] += len(fragment)
                _send(
                    parent,
                    dict(
                        kind="fragment_ack",
                        index=live["index"],
                        page=live["page"],
                        fragment=live["count"],
                    ),
                )
                live["count"] += 1
                continue
            require(message[:1] == b"C" and len(message) <= CONTROL + 1, "page_message_shape")
            item = raw._decode(message[1:])
            kind = item.get("kind")
            if kind == "session_login":
                allowed = not stop.is_set() and (source_allowed is None or source_allowed("login"))
                _send(parent, dict(kind="session_login_ack", **{"continue": allowed}))
                if not allowed:
                    reason = "soft_deadline"
                    break
                continue
            if kind == "session":
                require(session_ok is None and live is None and admitted == 0, "page_session_phase")
                session_ok = item["ok"] is True
                session_error = None if session_ok else item["error_code"]
                deadline = time.monotonic() + limits["source_seconds"]
                _send(
                    parent,
                    dict(kind="session_ack", **{"continue": session_ok and not stop.is_set()}),
                )
                if not session_ok:
                    reason = "login_refused"
                elif stop.is_set():
                    reason = "cancelled"
                continue
            if kind == "session_end":
                session_end = item
                if reason == "interrupted":
                    reason = "complete"
                deadline = time.monotonic() + limits["drain_seconds"]
                continue
            if kind == "worker_error":
                reason = "worker_operational_error"
                continue
            require(session_ok is True, "page_session_phase")
            index = item.get("index")
            require(integer(index, 0, len(tasks) - 1), "page_session_index")
            if kind == "task_begin":
                require(live is None and index == admitted, "page_session_sequence")
                proceed = not stop.is_set() and (on_task is None or on_task(index) is not False)
                if not proceed:
                    _send(parent, dict(kind="task_ack", index=index, **{"continue": False}))
                    reason = "caller_stopped"
                    deadline = time.monotonic() + limits["drain_seconds"]
                    continue
                live = open_task(index)
                _send(parent, dict(kind="task_ack", index=index, **{"continue": True}))
                deadline = time.monotonic() + limits["source_seconds"]
                continue
            require(live is not None and live["index"] == index, "page_session_sequence")
            if kind == "source":
                require(
                    live["body"] is None and item["source"] in ("login", "query", "next"),
                    "source_phase",
                )
                live["source_kind"] = item["source"]
                if source_allowed is not None and not source_allowed(live["source_kind"]):
                    _install(live["directory"] / "soft-deadline.json",
                             dict(identity=live["identity"], reason="soft_deadline"))
                    reason = "soft_deadline"
                    break
                proceed = (
                    not stop.is_set()
                    and live["page"] < limits["observations"]
                    and live["total"] < limits["body_bytes"]
                )
                if proceed:
                    if live["source_kind"] == "query":
                        live["metrics"]["source_query_calls"] += 1
                    elif live["source_kind"] == "next":
                        live["metrics"]["source_next_calls"] += 1
                _send(parent, dict(kind="source_ack", index=index, **{"continue": proceed}))
                deadline = time.monotonic() + limits["source_seconds"]
                continue
            if kind == "begin":
                require(
                    live["body"] is None
                    and type(item["page"]) is int
                    and item["page"] == live["page"]
                    and live["page"] < limits["observations"]
                    and live["source_kind"] is not None,
                    "page_begin_phase",
                )
                meta = item["metadata"]
                require(
                    set(meta) == {"trigger", "previous_state_sha256", "received_ns"}
                    and meta["trigger"]
                    in ("login_return", "query_return", "next_return", "next_raise")
                    and integer(meta["received_ns"], 1, 2**63 - 1),
                    "page_observation_shape",
                )
                begin = dict(
                    identity=live["identity"],
                    page=live["page"],
                    previous_page_root=live["previous"],
                    source_time=None,
                    **meta,
                )
                _install(
                    live["directory"] / f"page-{live['page']:03d}.begin.json",
                    begin,
                    live["metrics"],
                )
                live["body"] = (live["directory"] / f"page-{live['page']:03d}.body").open("xb")
                live["journal"] = (
                    live["directory"] / f"page-{live['page']:03d}.fragments.jsonl"
                ).open("xb")
                live["begin"] = begin
                raw._sync(live["directory"])
                live.update(size=0, count=0, chain=ZERO, whole=hashlib.sha256())
                deadline = time.monotonic() + limits["drain_seconds"]
                _send(parent, dict(kind="begin_ack", index=index, page=live["page"]))
                continue
            if kind == "end":
                require(
                    live["body"] is not None
                    and item
                    == dict(
                        kind="end",
                        index=index,
                        page=live["page"],
                        bytes=live["size"],
                        fragments=live["count"],
                        sha256=live["whole"].hexdigest(),
                    ),
                    "page_end_binding",
                )
                live["body"].close()
                live["body"] = None
                live["journal"].close()
                live["journal"] = None
                receipt = dict(
                    identity=live["identity"],
                    page=live["page"],
                    begin_sha256=digest(live["begin"]),
                    previous_page_root=live["previous"],
                    bytes=live["size"],
                    fragments=live["count"],
                    fragment_root=live["chain"],
                    sha256=live["whole"].hexdigest(),
                )
                live["previous"] = _install(
                    live["directory"] / f"page-{live['page']:03d}.receipt.json",
                    receipt,
                    live["metrics"],
                )
                # Actual bytes and codec must be readable before permitting next.
                proved = read_capture(root, live["task"])
                require(
                    len(proved["pages"]) == live["page"] + 1 and proved["pending"] is None,
                    "page_ack_proof",
                )
                live["total"] += live["size"]
                proceed = (
                    not stop.is_set()
                    and live["page"] + 1 < limits["observations"]
                    and live["total"] < limits["body_bytes"]
                )
                _send(
                    parent,
                    dict(
                        kind="page_ack",
                        index=index,
                        page=live["page"],
                        sha256=live["whole"].hexdigest(),
                        **{"continue": proceed},
                    ),
                )
                live["page"] += 1
                deadline = time.monotonic() + limits["source_seconds"]
                continue
            if kind in ("terminal", "task_error"):
                require(
                    live["body"] is None and live["journal"] is None,
                    "terminal_with_pending_page",
                )
                if kind == "terminal":
                    terminal_summary = item["summary"]
                    require(
                        terminal_summary["version"] == "baostock-page-summary-v1"
                        and terminal_summary["pages"] == live["page"]
                        and terminal_summary["query_calls"] == live["metrics"]["source_query_calls"]
                        and terminal_summary["next_calls"] == live["metrics"]["source_next_calls"],
                        "worker_summary_binding",
                    )
                    item_reason = terminal_summary["reason"]
                else:
                    terminal_summary = None
                    item_reason = item["reason"]
                live["metrics"]["worker_maxrss_kib"] = item["worker_maxrss_kib"]
                captured = seal_task(live, terminal_summary, item_reason)
                results.append(
                    dict(
                        index=index,
                        status=captured["status"],
                        captured=captured,
                        elapsed_seconds=live["metrics"]["elapsed_seconds"],
                    )
                )
                admitted += 1
                # The callback owns progress: a failure here must stop the
                # session before the source is asked for another task.
                proceed = on_capture is None or on_capture(index, captured) is not False
                live = None
                _send(parent, dict(kind="continue_ack", index=index, **{"continue": bool(proceed)}))
                if not proceed:
                    reason = "caller_stopped"
                    deadline = time.monotonic() + limits["drain_seconds"]
                else:
                    deadline = time.monotonic() + limits["source_seconds"]
                continue
            raise PageCaptureError("unknown_page_message")
    except BaseException as exc:
        primary = exc
        raise
    finally:
        failures = []
        if live is not None:
            # An in-flight attempt keeps its authority and any page receipts
            # only: no terminal is authored for an unproven capture.
            for name in ("body", "journal", "capacity"):
                stream = live.get(name)
                if stream is not None:
                    try:
                        stream.close()
                    except BaseException as exc:
                        failures.append(exc)
        if process is not None:
            for signal_worker in (process.terminate, process.kill):
                try:
                    if process.is_alive():
                        signal_worker()
                except BaseException as exc:
                    failures.append(exc)
                try:
                    process.join(2 if process.is_alive() else 0)
                except BaseException as exc:
                    failures.append(exc)
            try:
                require(not process.is_alive(), "page_worker_not_reaped")
                process.close()
            except BaseException as exc:
                failures.append(exc)
        for resource in (parent, child):
            if resource is not None:
                try:
                    resource.close()
                except BaseException as exc:
                    failures.append(exc)
        if failures:
            if primary is not None:
                primary.add_note("page_capture_cleanup_failed")
            else:
                raise failures[0]
    return dict(
        version=SESSION_VERSION,
        reason=reason,
        session_ok=session_ok is True,
        session_error=session_error,
        session_cleanup=(session_end or {}).get("cleanup"),
        session_logins=(session_end or {}).get("login_calls"),
        session_logouts=(session_end or {}).get("logout_calls"),
        completed=[dict(entry) for entry in results],
        interrupted=live["index"] if live is not None else None,
        elapsed_seconds=time.monotonic() - started,
    )

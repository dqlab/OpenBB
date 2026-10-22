"""Real opt-in campaign CLI/spawn/executor/page filesystem, SDK-only fixtures."""

import hashlib
import json

import pytest
import test_baostock_campaign as harness

from openbb_collector_core import baostock_page_capture as pages

EXTRA = r"""
if os.environ.get("CAMPAIGN_OFFLINE") == "1":
    import signal
    for name in ("connect", "connect_ex", "send", "sendall", "sendto"):
        original_socket_call = getattr(socket.socket, name)
        def internet_guard(self, *args, _old=original_socket_call, _name=name, **kwargs):
            if self.family in (socket.AF_INET, socket.AF_INET6):
                raise RuntimeError("PAGE_NET_" + _name)
            return _old(self, *args, **kwargs)
        setattr(socket.socket, name, internet_guard)
    from openbb_collector_core import baostock_page_capture as pc
    base_resolve = helpers.resolve_method
    acked = 0
    def resolve_pages(method):
        original_query = base_resolve(method)
        def query(**params):
            result = original_query(**params)
            case = os.environ.get("PAGE_SCENARIO", "")
            if case in ("aggregate", "failure", "changed_exception", "unchanged_exception",
                        "single25", "cancel", "death", "quota", "next_timeout"):
                size = 25_000_000 if case == "single25" else 7_000_000
                result.data = [["P" * size]] + [
                    [] for _ in range(helpers.BAOSTOCK_PER_PAGE_COUNT - 1)]
                result.cur_page_num = 1
                def next_page():
                    assert acked == 1, "NEXT_WITHOUT_PAGE_ACK"
                    log("sdk_next", acked=acked)
                    if case == "next_timeout":
                        time.sleep(6)
                    if case == "unchanged_exception":
                        raise OSError("SECRET_NEXT_EXCEPTION")
                    result.cur_page_num = 2
                    result.data = [] if case == "single25" else [["Q" * 44_000_000]]
                    if case in ("failure", "changed_exception"):
                        result.error_code = "503"
                    log("sdk_page_return", chars=0 if case == "single25" else 44_000_000)
                    if case == "changed_exception":
                        raise OSError("SECRET_NEXT_EXCEPTION")
                    return bool(result.data)
                result.next = next_page
            elif case == "codec":
                result.fields = ["dup", "dup", None, 7]
                result.data = [["\x00\\\n😀" * 20000, float("nan"), float("inf"), -0.0,
                                {"value_types": [{"type": "float64"}]}, "$float64", False]]
            return result
        return query
    helpers.resolve_method = resolve_pages
    old_ack = pc._ack
    def observed_ack(conn):
        global acked
        value = old_ack(conn)
        if value["kind"] == "fragment_ack":
            fault = os.environ.get("PAGE_FAULT", "")
            if fault == "death_fragment" and value["fragment"] == 2:
                log("death_fragment")
                os._exit(86)
            if fault == "term_fragment" and value["fragment"] == 2:
                os.kill(os.getppid(), signal.SIGTERM)
        if value["kind"] == "page_ack":
            acked += 1
            log("page_ack_received", page=value["page"])
            if os.environ.get("PAGE_FAULT") == "death_page":
                os._exit(87)
        return value
    pc._ack = observed_ack
    old_sync = os.fsync
    def sync(fd):
        target = os.readlink('/proc/self/fd/' + str(fd))
        if os.environ.get("PAGE_FAULT") == "fsync_body" and target.endswith("page-001.body"):
            raise OSError(5, "synthetic fsync failure")
        return old_sync(fd)
    os.fsync = sync
    old_install = pc._install
    def install(path, *args, **kwargs):
        if (os.environ.get("PAGE_FAULT") in ("receipt_enospc", "receipt_edquot")
                and path.name == "page-001.receipt.json"):
            code = 28 if os.environ["PAGE_FAULT"] == "receipt_enospc" else 122
            raise OSError(code, "synthetic storage exhaustion")
        return old_install(path, *args, **kwargs)
    pc._install = install
    old_link = os.link
    def link(src, dst, *args, **kwargs):
        if (os.environ.get("PAGE_FAULT") == "receipt_install"
                and str(dst).endswith("page-001.receipt.json")):
            raise OSError(5, "synthetic no-clobber install failure")
        return old_link(src, dst, *args, **kwargs)
    os.link = link
    old_replace = os.replace
    def replace(src, dst, *args, **kwargs):
        if (os.environ.get("PAGE_FAULT") == "progress_replace"
                and str(dst).endswith("progress.json")
                and Path(os.environ["CALLS"]).exists()
                and "page_ack_received" in Path(os.environ["CALLS"]).read_text()):
            raise OSError(5, "synthetic progress rename failure")
        return old_replace(src, dst, *args, **kwargs)
    os.replace = replace
    old_dir_sync = pc.raw._sync
    def dir_sync(path):
        if (os.environ.get("PAGE_FAULT") == "eligibility_directory"
                and (Path(path) / "eligible.json").exists()):
            raise OSError(5, "synthetic eligibility directory barrier")
        return old_dir_sync(path)
    pc.raw._sync = dir_sync
    old_open = Path.open
    class CloseFault:
        def __init__(self, stream):
            self.stream = stream
        def __getattr__(self, name):
            return getattr(self.stream, name)
        def close(self):
            self.stream.close()
            raise OSError(9, "synthetic close failure")
    def opened(path, *args, **kwargs):
        stream = old_open(path, *args, **kwargs)
        if (os.environ.get("PAGE_FAULT") == "close_body"
                and path.name == "page-001.body" and args == ("xb",)):
            return CloseFault(stream)
        return stream
    Path.open = opened
    if any(os.environ.get(key) for key in
           ("PAGE_BODY_LIMIT", "PAGE_SOURCE_SECONDS", "PAGE_OBSERVATIONS")):
        original_capture = pc.capture_process
        def limited(*args, **kwargs):
            kwargs["limits"] = dict(pc.DEFAULT_LIMITS)
            for env, key in [("PAGE_BODY_LIMIT", "body_bytes"),
                             ("PAGE_SOURCE_SECONDS", "source_seconds"),
                             ("PAGE_OBSERVATIONS", "observations")]:
                if os.environ.get(env):
                    kwargs["limits"][key] = int(os.environ[env])
            return original_capture(*args, **kwargs)
        pc.capture_process = limited
"""


def make(tmp_path, scenario, fault="", **kwargs):
    h = harness.Harness(tmp_path, [harness.task(flags=("3",))], **kwargs)
    (tmp_path / "bootstrap/sitecustomize.py").write_text(harness.BOOTSTRAP + EXTRA)
    h.env.update(PAGE_SCENARIO=scenario, PAGE_FAULT=fault)
    return h


def captured(h):
    state = h.state()
    if state["entries"]:
        entry = state["entries"][0]
    else:
        doc = json.loads(h.state_path().with_name("handoff-000000.json").read_bytes())
        entry = doc["entries"][0]
    if entry["links"]:
        locator = entry["links"][-1]["locator"]
    else:
        original = harness.task(flags=("3",))
        task = harness.campaign.attempt_task(
            state["binding"], 0, original, original["requests"][0], entry["attempts"]
        )
        locator = pages.capture_locator(task)
    directory = h.private / locator
    authority = json.loads((directory / "authority.json").read_bytes())
    return pages.read_capture(h.private, authority["task"]), directory


@pytest.mark.parametrize(
    "scenario,complete",
    [
        ("aggregate", True),
        ("single25", True),
        ("failure", False),
        ("changed_exception", False),
        ("unchanged_exception", False),
    ],
)
def test_large_received_pages_preserved_through_actual_cli(tmp_path, scenario, complete):
    h = make(tmp_path, scenario)
    child, result = h.run()
    assert child.returncode == (0 if complete else 1), child.stdout + child.stderr
    capture, directory = captured(h)
    assert capture["complete"] is complete
    first = pages.read_page(directory, 0)
    expected = 25_000_000 if scenario == "single25" else 7_000_000
    assert first["pages"][0]["rows"] == [["P" * expected]] + [[] for _ in range(1999)]
    if scenario != "unchanged_exception":
        second = pages.read_page(directory, 1)
        assert second["pages"][0]["rows"] == (
            [] if scenario == "single25" else [["Q" * 44_000_000]]
        )
        assert second["error_code"] == (
            "503" if scenario in ("failure", "changed_exception") else "0"
        )
    assert len(h.queries()) == 1
    assert {q["start_method"] for q in h.queries()} == {"spawn"}
    assert len([x for x in h.calls() if x["event"] == "sdk_next"]) == 1
    assert capture["metrics"]["max_frame_bytes"] <= pages.FRAME + 1
    saved = {
        str(p): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in directory.iterdir()
        if p.is_file()
    }
    assert (
        pages.read_capture(h.private, capture["task"], capture["capture_sha256"])["capture_sha256"]
        == capture["capture_sha256"]
    )
    assert saved == {
        str(p): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in directory.iterdir()
        if p.is_file()
    }
    (tmp_path / "MEASURED.json").write_text(
        json.dumps(
            dict(
                result=result,
                metrics=capture["metrics"],
                page_receipts=[p["receipt"] for p in capture["pages"]],
            ),
            indent=2,
        )
    )


def test_codec_in_actual_stream_preserves_nonfinite_and_marker_values(tmp_path):
    import math
    import struct

    h = make(tmp_path, "codec")
    child, _ = h.run()
    assert child.returncode == 0, child.stdout + child.stderr
    capture, directory = captured(h)
    env = pages.read_page(directory, 0)
    row = env["pages"][0]["rows"][0]
    assert row[0] == "\x00\\\n😀" * 20000
    assert math.isnan(row[1]) and math.isinf(row[2])
    assert struct.pack("!d", row[3]).hex() == "8000000000000000"
    assert row[4:] == [{"value_types": [{"type": "float64"}]}, "$float64", False]
    assert env["pages"][0]["fields"] == ["dup", "dup", None, 7]
    assert capture["complete"]


@pytest.mark.parametrize(
    "fault",
    [
        "death_page",
        "death_fragment",
        "term_fragment",
        "fsync_body",
        "receipt_enospc",
        "receipt_edquot",
        "receipt_install",
        "close_body",
    ],
)
def test_process_and_storage_faults_preserve_durable_prefix(tmp_path, fault):
    h = make(tmp_path, "aggregate", fault)
    child, result = h.run()
    assert child.returncode != 0, result
    capture, directory = captured(h)
    assert not capture["complete"]
    if fault == "death_fragment":
        assert capture["pending"]["bytes"] == 3 * pages.FRAME
        assert capture["pages"] == []
    else:
        assert pages.read_page(directory, 0)["pages"][0]["rows"][0][0] == "P" * 7_000_000
    if fault in ("death_page", "death_fragment", "term_fragment"):
        assert not any(x["event"] == "sdk_next" for x in h.calls())
    if fault == "term_fragment":
        assert result["status"] == "cancelled"
        replay, result = h.run()
        assert replay.returncode == 1 and result["source_calls"] == 0 and len(h.queries()) == 1
    else:
        before = len(h.queries())
        assert (
            pages.read_capture(h.private, capture["task"])["capture_sha256"]
            == capture["capture_sha256"]
        )
        assert len(h.queries()) == before


@pytest.mark.parametrize("scenario,complete", [("", True), ("aggregate", False)])
def test_observation_cap_stops_next_but_allows_already_proved_exhaustion(
    tmp_path, scenario, complete
):
    h = make(tmp_path, scenario)
    h.env["PAGE_OBSERVATIONS"] = "1"
    child, result = h.run()
    assert child.returncode == (0 if complete else 1)
    capture, _ = captured(h)
    assert capture["complete"] is complete and len(capture["pages"]) == 1
    assert not any(x["event"] == "sdk_next" for x in h.calls())
    assert result["source_calls"] == 1


def test_source_deadline_preserves_previous_page_and_unknown_actual_call_count(tmp_path):
    h = make(tmp_path, "next_timeout")
    h.env["PAGE_SOURCE_SECONDS"] = "3"
    child, result = h.run()
    assert child.returncode == 1
    capture, directory = captured(h)
    assert capture["terminal"]["reason"] == "source_deadline"
    assert capture["metrics"]["source_query_calls"] is None
    assert result["sdk_query_calls"] is None and not capture["complete"]
    assert pages.read_page(directory, 0)["pages"][0]["rows"][0][0] == "P" * 7_000_000
    assert len(h.queries()) == 1


def test_progress_rename_failure_recovers_exact_capture_without_source(tmp_path):
    h = make(tmp_path, "", "progress_replace")
    child, _ = h.run()
    assert child.returncode == 2 and h.state()["active"] and len(h.queries()) == 1
    h.env["PAGE_FAULT"] = ""
    recovered, result = h.run()
    assert recovered.returncode == 0 and result["source_calls"] == 0 and len(h.queries()) == 1


def test_eligibility_barrier_failure_requires_producer_resync_without_refetch(tmp_path):
    h = make(tmp_path, "", "eligibility_directory")
    child, _ = h.run()
    assert child.returncode == 2 and len(h.queries()) == 1 and h.state()["active"]
    again, _ = h.run()
    assert again.returncode == 2 and len(h.queries()) == 1 and h.state()["active"]
    h.env["PAGE_FAULT"] = ""
    recovered, result = h.run()
    assert recovered.returncode == 0 and result["source_calls"] == 0 and len(h.queries()) == 1


def test_dead_session_retry_reports_refetch_and_preserves_prior_bytes(tmp_path):
    h = make(tmp_path, "aggregate", "death_page")
    child, _ = h.run()
    assert child.returncode == 1
    first, directory = captured(h)
    before = {p.name: p.read_bytes() for p in directory.iterdir() if p.is_file()}
    h.env["PAGE_FAULT"] = ""
    child, result = h.run()
    assert child.returncode == 0 and len(h.queries()) == 2
    second, _ = captured(h)
    assert second["restart_of"] == first["locator"] and second["refetch"] is True
    assert second["authority"]["received_ns"] > first["authority"]["received_ns"]
    assert before == {p.name: p.read_bytes() for p in directory.iterdir() if p.is_file()}
    assert result["retry_semantics"] == "incomplete-request-new-session-may-refetch-prefix"


@pytest.mark.parametrize(
    "damage", ["body", "fragment_order", "fragment_duplicate", "begin", "terminal", "symlink"]
)
def test_page_chain_corruption_refuses_read_and_restart(tmp_path, damage):
    h = make(tmp_path, "codec")
    child, _ = h.run()
    assert child.returncode == 0
    capture, directory = captured(h)
    if damage == "body":
        target = directory / "page-000.body"
        target.write_bytes(b"corrupt")
    elif damage.startswith("fragment"):
        target = directory / "page-000.fragments.jsonl"
        lines = target.read_bytes().splitlines(keepends=True)
        assert len(lines) > 1
        if damage == "fragment_order":
            lines[0], lines[1] = lines[1], lines[0]
        else:
            lines.insert(1, lines[0])
        target.write_bytes(b"".join(lines))
    elif damage == "symlink":
        target = directory / "page-000.body"
        saved = directory / "fixture-original.body"
        target.rename(saved)
        target.symlink_to(saved)
    else:
        target = directory / ("page-000.begin.json" if damage == "begin" else "terminal.json")
        doc = json.loads(target.read_bytes())
        doc["identity"] = "0" * 64
        target.write_bytes(pages.encode(doc))
    with pytest.raises(ValueError):
        pages.read_capture(h.private, capture["task"], expected=capture["capture_sha256"])
    refused, result = h.run()
    assert refused.returncode == 2 and result["status"] == "refused" and len(h.queries()) == 1


def test_legacy_raw_handoff_remains_readable_and_old_manifest_refuses(tmp_path):
    import subprocess
    import sys

    h = make(tmp_path, "")
    original = harness.task(flags=("3",))
    binding = harness.campaign.digest({"legacy": "read-only-control"})
    task = harness.campaign.attempt_task(
        binding, 0, original, original["requests"][0], 1, version=harness.campaign.LEGACY_VERSION
    )
    script = (
        "import json,os; from pathlib import Path; "
        "from openbb_collector_core import raw_acquisition as r; "
        "from openbb_collector_core.baostock_acquisition import Source; "
        "t=json.loads(os.environ['LEGACY_TASK']); "
        "r.capture_process(Path(os.environ['LEGACY_ROOT']),t,Source(t['source']['model']))"
    )
    child = subprocess.run(
        [sys.executable, "-B", "-c", script],
        env=dict(h.env, LEGACY_TASK=json.dumps(task), LEGACY_ROOT=str(h.private)),
        capture_output=True,
        text=True,
        timeout=90,
    )
    assert child.returncode == 0, child.stdout + child.stderr
    old = harness.raw.read_capture(h.private, task["task_id"], task["generation"])
    doc = dict(
        version=harness.campaign.LEGACY_VERSION,
        binding=binding,
        page=0,
        tasks=[original],
        entries=[
            dict(
                attempts=1,
                due=0,
                links=[
                    dict(
                        attempt=1,
                        locator=harness.raw.capture_locator(task["task_id"], task["generation"]),
                        status="success",
                        receipt_sha256=harness.campaign.digest(old["responses"][0]["receipt"]),
                    )
                ],
            )
        ],
    )
    path = tmp_path / "legacy-handoff.json"
    path.write_bytes(harness.campaign.encode(doc))
    assert harness.campaign.verify_handoff(h.private, path, harness.campaign.digest(doc)) == doc
    before = {str(p): p.read_bytes() for p in h.private.rglob("*") if p.is_file()}
    manifest = h.manifest / "manifest.json"
    header = json.loads(manifest.read_bytes())
    header["version"] = harness.campaign.LEGACY_VERSION
    manifest.write_bytes(harness.campaign.encode(header))
    refused, result = h.run()
    assert refused.returncode == 2 and result["error"] == "legacy_capture_requires_new_run"
    assert before == {str(p): p.read_bytes() for p in h.private.rglob("*") if p.is_file()}
    assert len(h.queries()) == 1


@pytest.mark.parametrize(
    "operation,params",
    [
        ("BaostockStockBasic", {"code": "sh.600000"}),
        ("BaostockTradeDates", {"start_date": "2024-01-01", "end_date": "2024-01-03"}),
    ],
)
def test_reference_operations_use_real_streamed_dispatch(tmp_path, operation, params):
    task = harness.task(flags=("r",))
    task["source"]["model"] = operation
    task["requests"][0]["params"] = params
    h = harness.Harness(tmp_path, [task])
    child, result = h.run("always")
    assert child.returncode == 1 and result["remaining_requests"] == 1
    child, result = h.run()
    assert child.returncode == 0 and len(h.queries()) == 2
    child, result = h.run()
    assert child.returncode == 0 and result["source_calls"] == 0 and len(h.queries()) == 2


def test_true_encoded_quota_retains_old_page_and_current_fragment_prefix(tmp_path):
    h = make(tmp_path, "quota")
    h.env["PAGE_BODY_LIMIT"] = "9000000"
    child, _ = h.run()
    assert child.returncode == 1
    capture, directory = captured(h)
    assert not capture["complete"] and len(capture["pages"]) == 1 and capture["pending"]
    assert pages.read_page(directory, 0)["pages"][0]["rows"][0][0] == "P" * 7_000_000
    assert capture["terminal"]["reason"] == "resource_exhausted"
    assert capture["terminal"]["bytes"] <= 9_000_000


@pytest.mark.parametrize("shape", ["dict", "list"])
@pytest.mark.parametrize("persistent", [False, True])
def test_nested_source_codes_readable_and_retry_accounted(tmp_path, shape, persistent):
    """Real spawn/CLI must retain failure evidence and never fake completion."""
    import struct

    h = make(tmp_path, "", attempts=2)
    extension = r'''
if os.environ.get("CAMPAIGN_OFFLINE") == "1":
    import struct
    nested_original = helpers.resolve_method
    def nested_resolve(method):
        query = nested_original(method)
        def call(**params):
            result = query(**params)
            shape = os.environ.get("NESTED_CODE", "")
            if shape:
                value = struct.unpack("!d", bytes.fromhex("fff8000000001234"))[0]
                result.error_code = {"detail": [value]} if shape == "dict" else [float("inf")]
                result.data = [["RETAIN_NESTED_CODE", False, 0, 0.0]]
            return result
        return call
    helpers.resolve_method = nested_resolve
'''
    (tmp_path / "bootstrap/sitecustomize.py").write_text(harness.BOOTSTRAP + EXTRA + extension)
    h.env["NESTED_CODE"] = shape
    child, first = h.run()
    assert child.returncode == 1 and h.state()["completed_requests"] == 0
    assert first["source_calls"] == 1 and first["remaining_requests"] == 1
    assert h.state()["active"] is None
    capture, directory = captured(h)
    assert not capture["complete"] and capture["status"] == "failed"
    before = {p.name: p.read_bytes() for p in directory.iterdir() if p.is_file()}
    value = pages.read_page(directory, 0)
    # Independent stdlib decoding of actual saved bytes, not shared-code equality.
    independent = json.loads((directory / "page-000.body").read_bytes())
    for item in independent["value_types"]:
        target = independent
        for part in item["path"][:-1]:
            target = target[part]
        assert target[item["path"][-1]] == "$float64"
        target[item["path"][-1]] = struct.unpack("!d", bytes.fromhex(item["bits"]))[0]
    for env in (value, independent):
        for code in (env["error_code"], env["pages"][0]["error_code"]):
            assert type(code) is (dict if shape == "dict" else list)
            scalar = code["detail"][0] if shape == "dict" else code[0]
            assert type(scalar) is float
            assert struct.pack("!d", scalar).hex() == (
                "fff8000000001234" if shape == "dict" else "7ff0000000000000"
            )
        row = env["pages"][0]["rows"][0]
        assert row == ["RETAIN_NESTED_CODE", False, 0, 0.0]
        assert [type(x) for x in row] == [str, bool, int, float]
    h.env["NESTED_CODE"] = shape if persistent else ""
    child, second = h.run()
    assert child.returncode == (1 if persistent else 0)
    assert second["source_calls"] == 1 and len(h.queries()) == 2
    assert h.state()["completed_requests"] == (0 if persistent else 1)
    assert second["remaining_requests"] == (1 if persistent else 0)
    child, replay = h.run()
    assert child.returncode == (1 if persistent else 0)
    assert replay["source_calls"] == 0 and len(h.queries()) == 2
    assert {q["start_method"] for q in h.queries()} == {"spawn"}
    assert before == {p.name: p.read_bytes() for p in directory.iterdir() if p.is_file()}


LIFECYCLE = r'''
if os.environ.get("CAMPAIGN_OFFLINE") == "1":
    from multiprocessing.process import BaseProcess
    from multiprocessing.util import Finalize
    from openbb_collector_core import baostock_page_capture as pc
    import multiprocessing
    primary_exit = (KeyboardInterrupt() if os.environ.get("EXIT_PRIMARY_KIND") == "cancel"
                    else OSError(28, "test primary"))
    cause_exit = ValueError("test cause")
    primary_exit.__cause__ = cause_exit
    secondary_exit = OSError(9, "test process close")
    primary_injected = False
    capture_started = False
    original_allocate = os.posix_fallocate
    def allocate_exit(*args):
        frame = sys._getframe(1)
        if frame.f_code.co_name == "capture_process":
            log("attempt_clock", start=frame.f_locals["start"])
        return original_allocate(*args)
    os.posix_fallocate = allocate_exit
    original_capture_exit = pc.capture_process
    def capture_exit(*args, **kwargs):
        global capture_started
        capture_started = True
        kwargs["limits"] = dict(pc.DEFAULT_LIMITS, source_seconds=8,
            drain_seconds=int(os.environ.get("EXIT_DRAIN", "8")),
            attempt_seconds=int(os.environ.get("EXIT_ATTEMPT", "12")))
        try:
            return original_capture_exit(*args, **kwargs)
        except BaseException as exc:
            log("outward_error", exact_primary=exc is primary_exit,
                exact_cause=exc.__cause__ is cause_exit, exact_close=exc is secondary_exit,
                notes=getattr(exc, "__notes__", []))
            raise
        finally:
            log("children_after_capture",
                children=[p.pid for p in multiprocessing.active_children()])
    pc.capture_process = capture_exit
    original_resolve_exit = helpers.resolve_method
    def resolve_exit(method):
        original = original_resolve_exit(method)
        def query(**params):
            value = original(**params)
            time.sleep(float(os.environ.get("EXIT_QUERY_DELAY", "0")))
            def finish():
                log("finalizer_enter", mono=time.monotonic())
                mode = os.environ.get("EXIT_MODE", "")
                if mode == "ignore_term":
                    signal.signal(signal.SIGTERM, signal.SIG_IGN)
                if mode == "term_exit_zero":
                    signal.signal(signal.SIGTERM, lambda *a: os._exit(0))
                if mode == "exception":
                    raise SystemExit(23)  # multiprocessing swallows ordinary Exception.
                time.sleep(float(os.environ.get("EXIT_DELAY", "0.8")))
                log("finalizer_return", mono=time.monotonic())
            Finalize(None, finish, exitpriority=0)
            return value
        return query
    helpers.resolve_method = resolve_exit
    original_join_exit = BaseProcess.join
    def join_exit(self, timeout=None):
        global primary_injected
        log("join_enter", worker=self.pid, timeout=timeout, mono=time.monotonic())
        value = original_join_exit(self, timeout)
        log("join_leave", worker=self.pid, exitcode=self.exitcode, alive=self.is_alive(),
            mono=time.monotonic())
        if (os.environ.get("EXIT_PRIMARY") == "1" and capture_started
                and not primary_injected):
            primary_injected = True
            raise primary_exit
        return value
    BaseProcess.join = join_exit
    for method in ("terminate", "kill"):
        original_signal_exit = getattr(BaseProcess, method)
        def signal_exit(self, _name=method, _old=original_signal_exit):
            log("signal_" + _name, worker=self.pid, mono=time.monotonic())
            return _old(self)
        setattr(BaseProcess, method, signal_exit)
    original_close_exit = BaseProcess.close
    def close_exit(self):
        pid, code, alive = self.pid, self.exitcode, self.is_alive()
        value = original_close_exit(self)
        log("process_closed", worker=pid, exitcode=code, alive=alive)
        if os.environ.get("EXIT_CLOSE") == "1":
            raise secondary_exit
        return value
    BaseProcess.close = close_exit
'''


def lifecycle(tmp_path, **env):
    h = make(tmp_path, "", attempts=2, budget=2)
    (tmp_path / "bootstrap/sitecustomize.py").write_text(harness.BOOTSTRAP + EXTRA + LIFECYCLE)
    h.env.update({"EXIT_" + key: str(value) for key, value in env.items()})
    return h


def assert_reaped(h):
    import os

    closed = [x for x in h.calls() if x["event"] == "process_closed"]
    assert closed and all(not x["alive"] and x["exitcode"] is not None for x in closed)
    for item in closed:
        with pytest.raises(ProcessLookupError):
            os.kill(item["worker"], 0)
    assert all(not x["children"] for x in h.calls() if x["event"] == "children_after_capture")
    return closed


@pytest.mark.parametrize("delay", [0, 0.05, 0.8])
def test_worker_exit_wait_is_not_fixed_success_gate(tmp_path, delay):
    h = lifecycle(tmp_path, DELAY=delay)
    child, result = h.run()
    assert child.returncode == 0, child.stdout + child.stderr
    capture, _ = captured(h)
    assert capture["complete"] and capture["metrics"]["worker_exit"] == 0
    assert h.state()["reserved"] == 1
    for _ in range(2):
        child, result = h.run()
        assert child.returncode == 0 and result["source_calls"] == 0
    assert len(h.queries()) == 1 and h.state()["reserved"] == 1
    assert {q["start_method"] for q in h.queries()} == {"spawn"}
    assert not any(x["event"].startswith("signal_") for x in h.calls())
    assert_reaped(h)


@pytest.mark.parametrize(
    "env,budget,reason,kill",
    [
        ({"DELAY": 20, "ATTEMPT": 4}, 4, "exit_deadline", False),
        ({"DELAY": 20, "DRAIN": 1}, None, "exit_deadline", False),
        ({"DELAY": 3, "QUERY_DELAY": 1.5, "ATTEMPT": 4}, 4, "exit_deadline", False),
        ({"DELAY": 20, "ATTEMPT": 4, "MODE": "ignore_term"}, 4, "exit_deadline", True),
        ({"DELAY": 20, "ATTEMPT": 4, "MODE": "term_exit_zero"}, 4, "exit_deadline", False),
    ],
)
def test_worker_exit_uses_remaining_absolute_budget(tmp_path, env, budget, reason, kill):
    h = lifecycle(tmp_path, **env)
    child, _ = h.run()
    assert child.returncode == 1, child.stdout + child.stderr
    capture, _ = captured(h)
    assert not capture["complete"] and capture["terminal"]["reason"] == reason
    events = h.calls()
    start = next(x["start"] for x in events if x["event"] == "attempt_clock")
    finalizer = next(x["mono"] for x in events if x["event"] == "finalizer_enter")
    terms = [x for x in events if x["event"] == "signal_terminate"]
    kills = [x for x in events if x["event"] == "signal_kill"]
    assert len(terms) == 1 and len(kills) == int(kill)
    elapsed = terms[0]["mono"] - start
    if budget is not None:
        assert budget <= elapsed < budget + 0.6
    else:
        assert 0.8 <= terms[0]["mono"] - finalizer < 1.6
    assert capture["metrics"]["elapsed_seconds"] < (budget or (finalizer - start + 1)) + 4.6
    assert not h.state_path().with_name("handoff-000000.json").exists()
    closed = assert_reaped(h)
    if env.get("MODE") == "term_exit_zero":
        assert closed[0]["exitcode"] == 0  # Forced exit zero is NOT success.


def test_real_finalizer_exception_exit_cannot_reuse_success_summary(tmp_path):
    h = lifecycle(tmp_path, MODE="exception")
    child, _ = h.run()
    assert child.returncode == 1, child.stdout + child.stderr
    capture, _ = captured(h)
    assert capture["terminal"]["summary"]["status"] == "complete"
    assert capture["terminal"]["summary"]["cleanup"] == "ok"
    assert not capture["complete"] and capture["metrics"]["worker_exit"] != 0
    assert not h.state_path().with_name("handoff-000000.json").exists()
    assert_reaped(h)


@pytest.mark.parametrize("primary", [False, True, "cancel"])
def test_real_process_close_fault_preserves_exact_primary(tmp_path, primary):
    h = lifecycle(tmp_path, PRIMARY=int(bool(primary)), PRIMARY_KIND=primary, CLOSE=1)
    child = h.invoke(h.command())
    assert child.returncode != 0
    error = next(x for x in h.calls() if x["event"] == "outward_error")
    assert error["exact_primary"] is bool(primary)
    assert error["exact_cause"] is bool(primary)
    assert error["exact_close"] is not bool(primary)
    if primary:
        assert "page_capture_cleanup_failed" in error["notes"]
    assert not h.state_path().with_name("handoff-000000.json").exists()
    assert_reaped(h)

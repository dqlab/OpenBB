"""Bounded REST snapshot sampling; no historical or streaming claims."""

from __future__ import annotations

import argparse
import base64
import http.client
import json
import math
import multiprocessing
import signal
import subprocess
import sys
import threading
import time
import uuid
from contextlib import suppress
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

from openbb_futu.archive import Archive, recover_open_files
from openbb_futu.client import BASE, FutuError, _base, _raw_snapshot_reserved, _reserve
from openbb_futu.collector import TokenStore
from openbb_futu.coordination import Owner, coordination_directory
from openbb_futu.coverage import write_json
from openbb_futu.discovery import SYMBOL
from openbb_futu.offline import verify
from openbb_futu.read_budget import Budget
from openbb_futu.root_handle import RootHandle

_EVENT_PREFIX = "futu-sampled-event-"


def _event(handle, kind, request_id, data):
    """Publish an immutable, synced event directly under the held output root."""
    payload = {"version": 1, "event": kind, "request_id": request_id, **data}
    handle.publish(_EVENT_PREFIX + request_id + "-" + kind + ".json",
                   json.dumps(payload, separators=(",", ":"), ensure_ascii=True).encode())


def _unresolved(handle):
    """Bound all directory enumeration and event reads; fail closed on malformed evidence."""
    budget = Budget(bytes_limit=64 * 1024 * 1024, entries_limit=100_000)
    events = {}
    for name in budget.entries_at(handle):
        if not name.startswith(_EVENT_PREFIX):
            continue
        raw, _ = budget.read_identity_at(handle, name, max_bytes=2 * 1024 * 1024)
        obj = json.loads(raw)
        request_id = obj.get("request_id")
        kind = obj.get("event")
        if (not isinstance(request_id, str) or len(request_id) != 32
                or any(c not in "0123456789abcdef" for c in request_id)
                or kind not in {"attempt", "result"} or obj.get("version") != 1
                or name != _EVENT_PREFIX + request_id + "-" + kind + ".json"):
            raise ValueError("invalid sampled event")
        events.setdefault(request_id, set()).add(kind)
    unresolved = sum(kinds != {"attempt", "result"} for kinds in events.values())
    if unresolved:
        raise RuntimeError(f"sampled_unresolved:{unresolved}")



def _finish_failure(handle, exc, run_id):
    """Complete only attempts which reached durable publication."""
    name = _EVENT_PREFIX + exc.request_id + "-attempt.json"
    try:
        Budget().read_identity_at(handle, name, max_bytes=65536)
    except FileNotFoundError:
        return
    _event(handle, "result", exc.request_id, {"session_id": run_id,
           "status": "failure", "error": exc.category})


def _snapshot_child(connection, lock_fd, symbols, token, timeout, secrets, *, request):
    """Perform one reserved request; only sanitized results cross the pipe."""
    # The inherited descriptor keeps the flock until process exit.
    if lock_fd < 0:
        raise RuntimeError("missing owner lock")
    http.client.HTTPConnection.debuglevel = 0
    http.client.HTTPSConnection.debuglevel = 0
    try:
        endpoint = _base()
        parsed = urlparse(endpoint)
        if endpoint != BASE and (parsed.path not in {"", "/"} or parsed.query or parsed.fragment):
            raise FutuError("invalid_endpoint")
        result = _raw_snapshot_reserved(
            symbols,
            token,
            timeout=timeout,
            secrets=secrets,
            request=request,
        )
        connection.send(("ok", result))
    except FutuError as exc:
        connection.send(
            (
                "error",
                (
                    exc.category,
                    exc.http_status,
                    exc.received_bytes,
                    exc.encoded_bytes,
                    exc.decoded_bytes,
                ),
            )
        )
    except BaseException:
        connection.send(("error", ("transport", None, None, None, None)))
    finally:
        connection.close()
        # The kernel closes lock_fd only when this process exits.


def _controlled_snapshot(symbols, token, timeout, secrets, *, stop, archive, owner, journal, run_id, entries):
    """Reserve in the owner and keep its flock alive through worker exit."""
    if http.client.HTTPConnection.debuglevel or http.client.HTTPSConnection.debuglevel:
        raise FutuError("unsafe_http_debug")
    request_id = uuid.uuid4().hex
    started = datetime.now(timezone.utc).isoformat()
    def fail(category, details=(None, None, None, None)):
        raise FutuError(category, request_id=request_id, started_at_utc=started,
                        http_status=details[0], received_bytes=details[1],
                        encoded_bytes=details[2], decoded_bytes=details[3])

    with owner.borrow(coordination_directory()):
        # A fresh owner has no persisted history, so it waits a full window.
        delay = owner._acquired_at + owner.budget().window - time.monotonic()
        while delay > 0:
            if stop.wait(min(delay, 0.05)):
                fail("stopped")
            if archive.stop.is_set() or archive.error or not archive.thread.is_alive():
                fail("archive_failure")
            delay = owner._acquired_at + owner.budget().window - time.monotonic()
        try:
            _reserve(owner)
        except FutuError as exc:
            fail(exc.category)
        # Symbols are arbitrary caller input and may contain credentials.  The
        # journal needs the request shape, not the caller-supplied identifiers.
        scope = {asset: sum(entry["asset"] == asset for entry in entries)
                 for asset in ("equity", "etf", "option", "future")}
        _event(journal, "attempt", request_id, {"session_id": run_id, "scope": scope,
               "started_at_utc": started, "status": "unknown"})
        context = multiprocessing.get_context("spawn")
        receive, send = context.Pipe(duplex=False)
        command = [sys.executable, "-m", "openbb_futu.sampled_snapshot", "--worker",
                   str(owner._fd), str(send.fileno())]
        payload = json.dumps([symbols, token, timeout, list(secrets), request_id, started]).encode()
        result = []
        done = threading.Event()
        def read_result():
            try:
                result.append(receive.recv())
            except (EOFError, OSError):
                pass
            finally:
                done.set()
        started_process = False
        writer_started = False
        try:
            process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.DEVNULL,  # noqa: S603
                                       stderr=subprocess.DEVNULL, pass_fds=(owner._fd, send.fileno()))
            started_process = True
            send.close()
            def write_request():
                try:
                    process.stdin.write(payload)
                except OSError:
                    pass
                finally:
                    with suppress(OSError):
                        process.stdin.close()
            writer = threading.Thread(target=write_request, daemon=True)
            reader = threading.Thread(target=read_result, daemon=True)
            deadline = time.monotonic() + timeout
            writer.start()
            writer_started = True
            reader.start()
            failure = None
            while not done.wait(0.02):
                if stop.is_set():
                    failure = "stopped"
                    break
                if archive.stop.is_set() or archive.error or not archive.thread.is_alive():
                    failure = "archive_failure"
                    break
                if time.monotonic() >= deadline:
                    failure = "timeout"
                    break
            if failure:
                fail(failure)
            if not result:
                fail("transport")
            try:
                process.wait(timeout=max(0.001, deadline - time.monotonic()))
            except subprocess.TimeoutExpired:
                fail("timeout")
            if process.returncode != 0:
                fail("transport")
            kind, value = result[0]
            if kind != "ok":
                fail(value[0], value[1:])
            return value
        except OSError:
            fail("transport")
        finally:
            send.close()
            if started_process:
                if process.poll() is None:
                    process.kill()
                process.wait()
                if writer_started:
                    writer.join()
                # Count the attempt through actual worker exit, including a
                # delayed dispatch, so it cannot age out before the send.
                owner._requests[-1] = time.monotonic()
            receive.close()


def rebuild_rest_coverage(manifest_path, symbols):
    """Recompute REST coverage from verified sealed records and decoded payloads."""
    path = Path(manifest_path)
    coverage = dict.fromkeys(symbols, "unknown")

    def check_manifest(manifest):
        if manifest.get("transport") != "rest":
            raise ValueError("not a REST manifest")

    def consume(record):
        if (
            record.get("transport") != "rest"
            or record.get("data_kind") != "sampled_snapshot"
            or record.get("payload_encoding") != "base64"
            or record.get("payload_representation") != "http_content_decoded"
            or record.get("requested_symbols") != symbols
        ):
            raise ValueError("invalid REST record")
        payload = json.loads(base64.b64decode(record["raw_payload"], validate=True))
        if (
            not 200 <= record["http_status"] < 300
            or not isinstance(payload, dict)
            or payload.get("ret_code") != 0
        ):
            return
        data = payload.get("data")
        rows = data.get("snapshot_list") if isinstance(data, dict) else None
        if not isinstance(rows, list) or any(
            not isinstance(row, dict) or not isinstance(row.get("code"), str) for row in rows
        ):
            return
        codes = [row["code"] for row in rows]
        if not set(codes) <= set(symbols):
            return
        for symbol in symbols:
            if symbol in codes:
                coverage[symbol] = "returned_code"
            elif coverage[symbol] != "returned_code":
                coverage[symbol] = "missing"
    if verify(path, on_record=consume, on_manifest=check_manifest)["status"] != "verified":
        raise ValueError("invalid manifest")
    return coverage


def _valid_polling_bounds(polls, interval, timeout):
    if isinstance(polls, bool) or not isinstance(polls, int) or not 1 <= polls <= 1000:
        return False

    def bounded_number(value, low, high, *, inclusive_low):
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return False
        return math.isfinite(value) and (low <= value if inclusive_low else low < value) and value <= high

    return bounded_number(interval, 0.1, 86400, inclusive_low=True) and bounded_number(
        timeout, 0, 60, inclusive_low=False
    )


def run(config, token, output, *, stop=None, secrets=()):  # noqa: PLR0912
    """Collect explicit dimensions and report only synced response evidence."""
    entries = config["symbols"]
    if not isinstance(entries, list) or not 1 <= len(entries) <= 400:
        raise ValueError("expected 1 to 400 symbols")
    if any(
        not isinstance(e, dict)
        or set(e) != {"asset", "symbol"}
        or e["asset"] not in {"equity", "etf", "option", "future"}
        or not isinstance(e["symbol"], str)
        or not SYMBOL.fullmatch(e["symbol"])
        or len(e["symbol"]) > 32
        for e in entries
    ):
        raise ValueError("invalid symbol")
    symbols = [e["symbol"] for e in entries]
    # Reject credential/target overlap before creating any output or making a
    # request. These identifiers are intentionally retained in public metadata.
    secrets = tuple(secrets)
    credentials = (token, *secrets)
    if not isinstance(token, str) or not token or any(
        not isinstance(value, str) for value in secrets
    ):
        raise ValueError("invalid credentials")
    if any(
        secret and (secret == symbol or (len(secret) >= 8 and secret in symbol))
        for symbol in symbols for secret in credentials
    ):
        raise ValueError("target conflicts with credentials")
    if len(set(symbols)) != len(symbols):
        raise ValueError("duplicate symbol")
    polls = config.get("max_polls", 1)
    interval = config.get("interval_seconds", 60)
    timeout = config.get("timeout_seconds", 10)
    if not _valid_polling_bounds(polls, interval, timeout):
        raise ValueError("invalid polling bounds")
    stop = stop or threading.Event()
    root = Path(output)
    with RootHandle.create(root):
        pass
    run_id = uuid.uuid4().hex
    attempts = []
    failure = None
    with Owner(coordination_directory()) as owner, RootHandle(root) as journal:
        _unresolved(journal)
        if recover_open_files(root):
            raise RuntimeError("recovery_unresolved")
        archive = Archive(
            root,
            run_id=run_id,
            transport="rest",
            capacity=config.get("queue_capacity", 1000),
        )
        try:
            for index in range(polls):
                if stop.is_set():
                    break
                if archive.stop.is_set() or archive.error or not archive.thread.is_alive():
                    failure = "archive_failure"
                    break
                try:
                    response = _controlled_snapshot(
                        symbols, token, timeout, secrets, stop=stop, archive=archive, owner=owner,
                        journal=journal, run_id=run_id, entries=entries
                    )
                except FutuError as exc:
                    failure = exc.category
                    if exc.request_id:
                        _finish_failure(journal, exc, run_id)
                        attempts.append(
                            {
                                "request_id": exc.request_id,
                                "started_at_utc": exc.started_at_utc,
                                "error": exc.category,
                                "http_status": exc.http_status,
                                "received_bytes": exc.received_bytes,
                                "encoded_bytes": exc.encoded_bytes,
                                "decoded_bytes": exc.decoded_bytes,
                                "body": None,
                                "durability": "unknown",
                                "loss": "unknown",
                                "returned_codes": None,
                                "send_status": "unknown",
                            }
                        )
                    break
                _event(journal, "result", response.request_id, {
                    "session_id": run_id, "status": "response", "http_status": response.status,
                    "raw_payload": base64.b64encode(response.body).decode("ascii"),
                    "payload_encoding": "base64", "payload_representation": "http_content_decoded",
                    "http_content_encoding": response.content_encoding})
                now = datetime.now(timezone.utc).isoformat()
                record = {
                    "schema_version": "futu_raw_v1",
                    "provider": "futu",
                    "api_family": "futunn_http_ws",
                    "transport": "rest",
                    "data_kind": "sampled_snapshot",
                    "received_at_utc": now,
                    "request_id": response.request_id,
                    "started_at_utc": response.started_at_utc,
                    "http_status": response.status,
                    "requested_symbols": symbols,
                    "raw_payload": base64.b64encode(response.body).decode("ascii"),
                    "payload_encoding": "base64",
                    "payload_representation": "http_content_decoded",
                    "http_content_encoding": response.content_encoding,
                }
                if not archive.accept_record(record):
                    attempts.append(
                        {
                            "request_id": response.request_id,
                            "started_at_utc": response.started_at_utc,
                            "http_status": response.status,
                            "body": None,
                            "durability": "unknown",
                            "returned_codes": None,
                            "error": "archive_failure",
                        }
                    )
                    failure = "archive_failure"
                    break
                observed = None
                obj = response.parsed
                if (
                    200 <= response.status < 300
                    and isinstance(obj, dict)
                    and obj.get("ret_code") == 0
                    and isinstance(obj.get("data"), dict)
                ):
                    rows = obj["data"].get("snapshot_list")
                    if isinstance(rows, list) and all(
                        isinstance(row, dict) and isinstance(row.get("code"), str) for row in rows
                    ):
                        codes = [row["code"] for row in rows]
                        if set(codes) <= set(symbols):
                            observed = codes
                attempts.append(
                    {
                        "seq": archive.received,
                        "request_id": response.request_id,
                        "started_at_utc": response.started_at_utc,
                        "http_status": response.status,
                        "returned_codes": observed,
                    }
                )
                if observed is None:
                    failure = "provider_failure"
                    break
                if index + 1 < polls:
                    deadline = time.monotonic() + interval
                    while time.monotonic() < deadline:
                        if stop.wait(min(0.1, max(0, deadline - time.monotonic()))):
                            break
                        if archive.stop.is_set() or archive.error or not archive.thread.is_alive():
                            failure = "archive_failure"
                            break
                    if stop.is_set() or failure:
                        break
        finally:
            manifest = archive.close()
    if manifest["error"] or manifest["durable_seq"] != manifest["received_frames"]:
        failure = "archive_failure"
    coverage = dict.fromkeys(symbols, "unknown")
    if not manifest["error"] and manifest["durable_seq"] == manifest["received_frames"]:
        try:
            coverage = rebuild_rest_coverage(root / "futu-manifests" / (manifest["session_id"] + ".json"), symbols)
        except (OSError, ValueError, KeyError, TypeError):
            failure = "archive_failure"
    try:
        integrity_status = verify(Path(manifest["manifest"]))["status"] if manifest.get("manifest") else "missing"
    except (OSError, ValueError, KeyError, TypeError):
        integrity_status = "failed"
    if integrity_status != "verified":
        failure = "archive_failure"
        coverage = dict.fromkeys(symbols, "unknown")
    result = {
        "run_id": run_id,
        "transport": "rest",
        "data_kind": "sampled_snapshot",
        "requested": entries,
        "coverage": coverage,
        "integrity_status": integrity_status,
        "original_commit_status": "unknown",
        "original_commit_status_basis": "no_trustworthy_commit_confirmation",
        "quote_evidence": {
            symbol: {
                "response_item_present": (True if status == "returned_code" else False if status == "missing" else None),
                "quote_fields_present": "unknown",
                "quote_fields_valid": "unknown",
                "quote_observed": "unknown",
                "permission": "unknown",
            }
            for symbol, status in coverage.items()
        },
        "attempts": attempts,
        "failure": failure,
        "manifest": manifest,
    }
    (root / "futu-sampled-coverage").mkdir(mode=0o700, exist_ok=True)
    write_json(root / "futu-sampled-coverage" / (run_id + ".json"), result)
    return result


def main():
    """Run an explicitly bounded local sampler."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
    parser.add_argument("--token-file", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    try:
        config = json.loads(Path(args.config).read_text(encoding="utf-8"))
        credentials = TokenStore(Path(args.token_file)).read()
        result = run(
            config,
            credentials["access_token"],
            args.output,
            stop=stop,
            secrets=tuple(
                credentials[key]
                for key in ("refresh_token", "client_secret")
                if key in credentials
            ),
        )
        sys.stdout.write(json.dumps({"run_id": result["run_id"], "failure": result["failure"]}) + "\n")
        if result["failure"]:
            sys.stderr.write("sampled snapshot failed: " + result["failure"] + "\n")
        return 1 if result["failure"] else 0
    except Exception as exc:
        reason = exc.category if isinstance(exc, FutuError) else type(exc).__name__
        if type(exc) is RuntimeError and (
            str(exc) == "recovery_unresolved" or str(exc).startswith("sampled_unresolved:")
        ):
            reason = str(exc)
        sys.stderr.write("sampled snapshot failed: " + reason + "\n")
        return 1


if __name__ == "__main__":
    if len(sys.argv) == 4 and sys.argv[1] == "--worker":
        from multiprocessing.connection import Connection
        worker_lock = int(sys.argv[2])
        worker_connection = Connection(int(sys.argv[3]), readable=False)
        worker_symbols, worker_token, worker_timeout, worker_secrets, worker_id, worker_started = json.load(sys.stdin)
        _snapshot_child(worker_connection, worker_lock, worker_symbols, worker_token, worker_timeout,
                        worker_secrets, request=(worker_id, worker_started))
    else:
        raise SystemExit(main())

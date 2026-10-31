"""Bounded REST snapshot sampling; no historical or streaming claims."""

from __future__ import annotations

import argparse
import base64
import http.client
import json
import math
import multiprocessing
import signal
import sys
import threading
import time
import uuid
from contextlib import suppress
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

from openbb_futu.archive import Archive, recover_open_files
from openbb_futu.client import BASE, FutuError, _base, _raw_snapshot_reserved
from openbb_futu.collector import TokenStore
from openbb_futu.coordination import Owner, RequestBudget, coordination_directory
from openbb_futu.coverage import write_json
from openbb_futu.discovery import SYMBOL
from openbb_futu.offline import verify


def _snapshot_child(connection, symbols, token, timeout, secrets, *, request):
    """Perform one reserved request; only sanitized results cross the pipe."""
    http.client.HTTPConnection.debuglevel = 0
    http.client.HTTPSConnection.debuglevel = 0
    try:
        endpoint = _base()
        parsed = urlparse(endpoint)
        if endpoint != BASE and (
            parsed.path not in {"", "/"} or parsed.query or parsed.fragment
        ):
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


def _controlled_snapshot(symbols, token, timeout, secrets, *, stop, archive):
    """Reserve in the owner process and supervise one spawned HTTP request."""
    if http.client.HTTPConnection.debuglevel or http.client.HTTPSConnection.debuglevel:
        raise FutuError("unsafe_http_debug")
    request_id = uuid.uuid4().hex
    started = datetime.now(timezone.utc).isoformat()
    deadline = time.monotonic() + timeout
    if RequestBudget(coordination_directory()).acquire() > 0:
        raise FutuError("local_budget_exhausted")
    context = multiprocessing.get_context("spawn")
    receiver, sender = context.Pipe(duplex=False)
    process = context.Process(
        target=_snapshot_child,
        args=(sender, symbols, token, timeout, secrets),
        kwargs={"request": (request_id, started)},
    )
    outcome = []

    def read_result():
        with suppress(EOFError, OSError):
            outcome.append(receiver.recv())

    reader = None
    try:
        try:
            process.start()
        except Exception:
            raise FutuError(
                "transport", request_id=request_id, started_at_utc=started
            ) from None
        sender.close()
        reader = threading.Thread(target=read_result, daemon=True)
        reader.start()
        while reader.is_alive():
            if archive.stop.is_set() or archive.error or not archive.thread.is_alive():
                raise FutuError(
                    "archive_failure", request_id=request_id, started_at_utc=started
                )
            if stop.is_set():
                raise FutuError(
                    "stopped", request_id=request_id, started_at_utc=started
                )
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise FutuError(
                    "timeout", request_id=request_id, started_at_utc=started
                )
            reader.join(min(0.05, remaining))
        if archive.stop.is_set() or archive.error or not archive.thread.is_alive():
            raise FutuError(
                "archive_failure", request_id=request_id, started_at_utc=started
            )
        if stop.is_set():
            raise FutuError("stopped", request_id=request_id, started_at_utc=started)
        if time.monotonic() >= deadline:
            raise FutuError("timeout", request_id=request_id, started_at_utc=started)
        if not outcome:
            raise FutuError("transport", request_id=request_id, started_at_utc=started)
        kind, value = outcome[0]
        if kind == "error":
            raise FutuError(
                value[0],
                request_id=request_id,
                started_at_utc=started,
                http_status=value[1],
                received_bytes=value[2],
                encoded_bytes=value[3],
                decoded_bytes=value[4],
            )
        return value
    finally:
        sender.close()
        if process.pid is not None:
            if process.is_alive():
                process.terminate()
                process.join(0.3)
            if process.is_alive():
                process.kill()
            process.join(0.3)
            if not process.is_alive():
                process.close()
        if reader:
            reader.join(0.3)
        receiver.close()


def rebuild_rest_coverage(manifest_path, symbols):
    """Recompute REST coverage from verified sealed records and decoded payloads."""
    path = Path(manifest_path)
    if verify(path)["status"] != "verified":
        raise ValueError("invalid manifest")
    manifest = json.loads(path.read_text(encoding="utf-8"))
    if manifest.get("transport") != "rest":
        raise ValueError("not a REST manifest")
    coverage = dict.fromkeys(symbols, "unknown")
    for entry in manifest["files"]:
        with Path(entry["file"]).open(encoding="utf-8") as stream:
            for line in stream:
                record = json.loads(line)
                if (
                    record.get("transport") != "rest"
                    or record.get("data_kind") != "sampled_snapshot"
                    or record.get("payload_encoding") != "base64"
                    or record.get("payload_representation") != "http_content_decoded"
                    or record.get("requested_symbols") != symbols
                ):
                    raise ValueError("invalid REST record")
                payload = json.loads(
                    base64.b64decode(record["raw_payload"], validate=True)
                )
                if (
                    not 200 <= record["http_status"] < 300
                    or not isinstance(payload, dict)
                    or payload.get("ret_code") != 0
                ):
                    continue
                data = payload.get("data")
                rows = data.get("snapshot_list") if isinstance(data, dict) else None
                if not isinstance(rows, list) or any(
                    not isinstance(row, dict) or not isinstance(row.get("code"), str)
                    for row in rows
                ):
                    continue
                codes = [row["code"] for row in rows]
                if not set(codes) <= set(symbols):
                    continue
                for symbol in symbols:
                    if symbol in codes:
                        coverage[symbol] = "returned_code"
                    elif coverage[symbol] != "returned_code":
                        coverage[symbol] = "missing"
    return coverage


def _valid_polling_bounds(polls, interval, timeout):
    if isinstance(polls, bool) or not isinstance(polls, int) or not 1 <= polls <= 1000:
        return False

    def bounded_number(value, low, high, *, inclusive_low):
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return False
        return (
            math.isfinite(value)
            and (low <= value if inclusive_low else low < value)
            and value <= high
        )

    return bounded_number(interval, 0.1, 86400, inclusive_low=True) and bounded_number(
        timeout, 0, 60, inclusive_low=False
    )


def run(config, token, output, *, stop=None, secrets=()):
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
    if len(set(symbols)) != len(symbols):
        raise ValueError("duplicate symbol")
    polls = config.get("max_polls", 1)
    interval = config.get("interval_seconds", 60)
    timeout = config.get("timeout_seconds", 10)
    if not _valid_polling_bounds(polls, interval, timeout):
        raise ValueError("invalid polling bounds")
    stop = stop or threading.Event()
    root = Path(output)
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    run_id = uuid.uuid4().hex
    attempts = []
    failure = None
    with Owner(coordination_directory()):
        recover_open_files(root)
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
                if (
                    archive.stop.is_set()
                    or archive.error
                    or not archive.thread.is_alive()
                ):
                    failure = "archive_failure"
                    break
                try:
                    response = _controlled_snapshot(
                        symbols, token, timeout, secrets, stop=stop, archive=archive
                    )
                except FutuError as exc:
                    failure = exc.category
                    if exc.request_id:
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
                        isinstance(row, dict) and isinstance(row.get("code"), str)
                        for row in rows
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
                        if (
                            archive.stop.is_set()
                            or archive.error
                            or not archive.thread.is_alive()
                        ):
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
            coverage = rebuild_rest_coverage(
                root / "futu-manifests" / (manifest["session_id"] + ".json"), symbols
            )
        except (OSError, ValueError, KeyError, TypeError):
            failure = "archive_failure"
    result = {
        "run_id": run_id,
        "transport": "rest",
        "data_kind": "sampled_snapshot",
        "requested": entries,
        "coverage": coverage,
        "quote_evidence": {
            symbol: {
                "response_item_present": (
                    True
                    if status == "returned_code"
                    else False if status == "missing" else None
                ),
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
            secrets=(credentials["refresh_token"], credentials["client_id"]),
        )
        sys.stdout.write(
            json.dumps({"run_id": result["run_id"], "failure": result["failure"]})
            + "\n"
        )
        if result["failure"]:
            sys.stderr.write("sampled snapshot failed: " + result["failure"] + "\n")
        return 1 if result["failure"] else 0
    except Exception as exc:
        sys.stderr.write(
            "sampled snapshot failed: "
            + (exc.category if isinstance(exc, FutuError) else type(exc).__name__)
            + "\n"
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

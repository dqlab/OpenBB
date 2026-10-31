"""Reconstruct conservative coverage from immutable plan, events and sealed archive."""

import hashlib
import json
import os
from pathlib import Path

from openbb_futu.contracts import plan
from openbb_futu.discovery import SYMBOL
from openbb_futu.offline import verify


def prepare(config):
    """Create deterministic bounded dimensions from explicitly supplied seeds."""
    entries = config["dimensions"]
    if not isinstance(entries, list) or not entries:
        raise ValueError("dimensions must be a nonempty list")
    for item in entries:
        if not isinstance(item, dict) or set(item) - {
            "asset",
            "symbol",
            "channel",
            "priority",
        }:
            raise ValueError("invalid dimension")
        if (
            not isinstance(item.get("symbol"), str)
            or not SYMBOL.fullmatch(item["symbol"])
            or len(item["symbol"]) > 32
        ):
            raise ValueError("invalid symbol")
        if not isinstance(item.get("priority", 100), int) or isinstance(
            item.get("priority", 100), bool
        ):
            raise ValueError("invalid priority")
    budget = config.get("subscription_budget")
    if (
        not isinstance(budget, int)
        or isinstance(budget, bool)
        or not 1 <= budget <= 10000
    ):
        raise ValueError("subscription_budget must be a positive bounded integer")
    result = plan([dict(item) for item in entries], budget=budget)
    result["discovery"] = {
        "source": "configured_seeds",
        "universe": "unknown",
        "entitlement": "unknown",
    }
    evidence = config.get("discovery_report")
    if evidence is not None:
        path = Path(evidence)
        if path.stat().st_size > 8_388_608:
            raise ValueError("discovery report too large")
        raw = path.read_bytes()
        if len(raw) > 8_388_608:
            raise ValueError("discovery report too large")
        source = json.loads(raw)
        requests = source["requests"]
        if not isinstance(requests, list) or len(requests) > 100:
            raise ValueError("invalid discovery report")
        verified = []
        for item in requests:
            ref = {
                k: item[k]
                for k in (
                    "asset",
                    "symbol",
                    "status",
                    "error",
                    "http_status",
                    "sha256",
                    "bytes",
                )
                if k in item
            }
            ref["symbols"] = []
            if "raw_file" in item:
                response_path = Path(item["raw_file"])
                if response_path.resolve().parent != path.resolve().parent:
                    raise ValueError("discovery response outside evidence directory")
                if response_path.stat().st_size > 32 * 1024 * 1024:
                    raise ValueError("discovery response too large")
                response = response_path.read_bytes()
                if (
                    len(response) > 32 * 1024 * 1024
                    or len(response) != item["bytes"]
                    or hashlib.sha256(response).hexdigest() != item["sha256"]
                ):
                    raise ValueError("discovery response checksum mismatch")
                obj = json.loads(response)
                field = {"option": "option_chain", "future": "reference_list"}.get(
                    item["asset"]
                )
                if item.get("status") == "seed_scope_observed":
                    rows = obj.get("data", {}).get(field)
                    if (
                        not 200 <= item.get("http_status", 0) < 300
                        or obj.get("ret_code") != 0
                        or obj.get("pagination") is not None
                        or not isinstance(rows, list)
                    ):
                        raise ValueError("invalid discovery status")
                    ref["symbols"] = [
                        row["code"]
                        for row in rows
                        if isinstance(row, dict) and isinstance(row.get("code"), str)
                    ]
                    if len(ref["symbols"]) != len(rows) or ref["symbols"] != item.get(
                        "symbols"
                    ):
                        raise ValueError("discovery symbols mismatch")
                ref["raw_file"] = str(response_path.resolve())
            elif item.get("status") == "seed_scope_observed":
                raise ValueError("discovery response missing")
            verified.append(ref)
        result["discovery"] = {
            "source": "seed_discovery",
            "report_file": str(path.resolve()),
            "report_sha256": hashlib.sha256(raw).hexdigest(),
            "requests": verified,
            "universe": "unknown",
            "entitlement": "unknown",
        }
        for entry in result["selected"] + result["not_selected"]:
            entry["discovery_refs"] = [
                i
                for i, ref in enumerate(verified)
                if ref.get("asset") == entry["asset"]
                and entry["symbol"] in ref["symbols"]
            ]
    channels: dict = {}
    for item in result["selected"]:
        channel, symbol = item["channel"], item["symbol"]
        channels.setdefault(channel, []).append(
            {"symbol": symbol, "period": "1m", "adjust": "none"}
            if channel == "kline"
            else symbol
        )
    return result, channels


def write_json(path, obj):
    """Atomically publish and sync a report."""
    path = Path(path)
    temp = path.with_name(path.name + ".tmp")
    fd = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as stream:
        json.dump(obj, stream, ensure_ascii=False, indent=2)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temp, path)
    fd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _strict_integer(value):
    return isinstance(value, int) and value.__class__ is int


def _observations_from_manifest(manifest, dimension_keys, observations):
    """Collect bounded locators only from checksum-matched durable records."""
    for file in manifest["files"]:
        path = Path(file["file"])
        digest = hashlib.sha256()
        with path.open("rb") as source:
            while chunk := source.read(1024 * 1024):
                digest.update(chunk)
        if digest.hexdigest() != file["sha256"]:
            raise ValueError("archive checksum mismatch")
        with path.open("rb") as source:
            for line in source:
                record = json.loads(line)
                if record["local_seq"] > manifest["durable_seq"]:
                    continue
                obj = json.loads(record["raw_payload"])
                channel = {
                    "QUOTE": "quote",
                    "ORDER_BOOK": "order_book",
                    "TICKER": "ticker",
                    "KLINE": "kline",
                }.get(obj["type"])
                symbol = obj.get("symbol")
                if (
                    not channel
                    or not isinstance(symbol, str)
                    or (symbol, channel) not in dimension_keys
                ):
                    continue
                if channel == "kline" and (
                    obj["data"].get("period"),
                    obj["data"].get("adjust"),
                ) != ("1m", "none"):
                    continue
                observation = observations.setdefault(
                    (symbol, channel), {"refs": [], "count": 0}
                )
                refs = observation["refs"]
                if channel == "ticker":
                    rows = obj["data"].get("ticker_list")
                    if not isinstance(rows, list):
                        continue
                    for index, row in enumerate(rows):
                        if (
                            isinstance(row, dict)
                            and _strict_integer(row.get("time_ms"))
                            and _strict_integer(row.get("sequence"))
                        ):
                            observation["count"] += 1
                            if len(refs) < 100:
                                refs.append(
                                    {
                                        "local_seq": record["local_seq"],
                                        "ticker_index": index,
                                        "time_ms": row["time_ms"],
                                        "sequence": row["sequence"],
                                    }
                                )
                else:
                    observation["count"] += 1
                    if len(refs) < 100:
                        refs.append({"local_seq": record["local_seq"]})


def report(root, run_id, *, publish=True):  # noqa: PLR0912
    """Rebuild from sealed manifest only; interrupted or unverifiable data stays unknown."""
    root = Path(root)
    result = json.loads((root / f"futu-plan-{run_id}.json").read_text())
    events_path = root / f"futu-events-{run_id}.jsonl"
    events: list = []
    last_event = None
    event_count = 0
    gap_started = None
    if events_path.exists():
        with events_path.open(encoding="utf-8") as stream:
            for line in stream:
                try:
                    event = json.loads(line)
                    last_event = event
                    if event.get("event") == "gap_start":
                        gap_started = event.get("time")
                    elif (
                        event.get("event") == "gap_end"
                        and isinstance(gap_started, (int, float))
                        and isinstance(event.get("time"), (int, float))
                    ):
                        event["duration_seconds"] = max(0, event["time"] - gap_started)
                        gap_started = None
                    if event.get("event") in {
                        "gap_start",
                        "gap_end",
                        "gap_unresolved",
                        "interrupted_segment",
                        "mode_change",
                    }:
                        event_count += 1
                        if len(events) < 1000:
                            events.append(event)
                except ValueError:
                    break
    result["run_id"] = run_id
    result["events"] = [
        e
        for e in events
        if e["event"]
        in {
            "gap_start",
            "gap_end",
            "gap_unresolved",
            "interrupted_segment",
            "mode_change",
        }
    ]
    result["timeline_event_count"] = event_count
    result["timeline_truncated"] = event_count > len(events)
    result["timeline_file"] = str(events_path)
    result["run_status"] = "interrupted"
    result["authentication"] = "unknown"
    result["dimensions"] = []
    manifest = None
    manifest_path = None
    invalid_manifest = False
    for path in (root / "futu-manifests").glob("*.json"):
        try:
            candidate = json.loads(path.read_text())
            if candidate["run_id"] == run_id:
                if manifest is not None:
                    invalid_manifest = True
                manifest = candidate
                manifest_path = path
        except (OSError, ValueError, KeyError):
            continue
    observations: dict = {}
    dimension_keys = {
        (item["symbol"], item["channel"])
        for item in result["selected"] + result["not_selected"]
    }
    if manifest:
        try:
            invalid_manifest = (
                invalid_manifest or verify(manifest_path)["status"] != "verified"
            )
        except (OSError, ValueError, KeyError, TypeError):
            invalid_manifest = True
    if manifest and not invalid_manifest:
        _observations_from_manifest(manifest, dimension_keys, observations)
    for item in result["selected"] + result["not_selected"]:
        selected = item in result["selected"]
        observation = observations.get(
            (item["symbol"], item["channel"]), {"refs": [], "count": 0}
        )
        refs = observation["refs"]
        result["dimensions"].append(
            {
                **item,
                "selected": selected,
                "subscription_ack": "unknown" if selected else "not_requested",
                "durable_observations": refs,
                "durable_observation_count": observation["count"],
                "locators_truncated": observation["count"] > len(refs),
                "received_rows_at_least": observation["count"],
                "written_rows_at_least": observation["count"],
                "synced_rows": observation["count"],
                "entitlement": "unknown",
            }
        )
    if last_event and last_event["event"] == "stopped":
        status = last_event["status"]
        if status == "failed" or (
            status == "stopped" and manifest and not manifest.get("error")
        ):
            result["run_status"] = status
    if invalid_manifest:
        result["run_status"] = "failed"
    result["evidence_status"] = (
        "failed" if invalid_manifest else ("verified" if manifest else "missing")
    )
    for asset, channels in result["by_asset_channel"].items():
        for channel, summary in channels.items():
            dims = [
                d
                for d in result["dimensions"]
                if d["asset"] == asset and d["channel"] == channel
            ]
            summary["discovery_refs"] = sorted(
                {ref for d in dims for ref in d.get("discovery_refs", [])}
            )
            summary["synced_rows"] = sum(d["synced_rows"] for d in dims)
            summary["persistence_status"] = (
                "observed" if summary["synced_rows"] else "not_observed"
            )
            summary["entitlement"] = "unknown"
    result["archive_manifest"] = manifest
    if publish:
        write_json(root / f"futu-coverage-{run_id}.json", result)
    return result

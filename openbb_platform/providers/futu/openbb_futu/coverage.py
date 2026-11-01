"""Reconstruct conservative coverage from immutable plan, events and sealed archive."""

import hashlib
import json
import math
import re
from pathlib import Path

from openbb_futu.contracts import plan
from openbb_futu.discovery import SYMBOL
from openbb_futu.offline import verify
from openbb_futu.read_budget import Budget, BudgetExceeded, EvidenceChanged
from openbb_futu.root_handle import PublicationDurabilityUnknown, RootHandle


def prepare(config, *, budget=None, root_handle=None):
    """Create deterministic bounded dimensions from explicitly supplied seeds."""
    read_budget = budget if budget is not None else Budget()
    if root_handle is not None:
        root_handle.check()
    entries = config["dimensions"]
    if not isinstance(entries, list) or not entries:
        raise ValueError("dimensions must be a nonempty list")
    if len(entries) > 10000:
        raise ValueError("too many dimensions")
    for item in entries:
        if not isinstance(item, dict) or set(item) - {
            "asset",
            "symbol",
            "channel",
            "priority",
        }:
            raise ValueError("invalid dimension")
        if not isinstance(item.get("symbol"), str) or not SYMBOL.fullmatch(item["symbol"]) or len(item["symbol"]) > 32:
            raise ValueError("invalid symbol")
        if not isinstance(item.get("priority", 100), int) or isinstance(item.get("priority", 100), bool):
            raise ValueError("invalid priority")
    budget = config.get("subscription_budget")
    if not isinstance(budget, int) or isinstance(budget, bool) or not 1 <= budget <= 10000:
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
        raw = read_budget.read(path, max_bytes=8_388_608)
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
                response = read_budget.read(response_path, max_bytes=32 * 1024 * 1024)
                if (
                    len(response) > 32 * 1024 * 1024
                    or len(response) != item["bytes"]
                    or hashlib.sha256(response).hexdigest() != item["sha256"]
                ):
                    raise ValueError("discovery response checksum mismatch")
                obj = json.loads(response)
                field = {"option": "option_chain", "future": "reference_list"}.get(item["asset"])
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
                        row["code"] for row in rows if isinstance(row, dict) and isinstance(row.get("code"), str)
                    ]
                    if len(ref["symbols"]) != len(rows) or ref["symbols"] != item.get("symbols"):
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
                if ref.get("asset") == entry["asset"] and entry["symbol"] in ref["symbols"]
            ]
    channels: dict = {}
    for item in result["selected"]:
        channel, symbol = item["channel"], item["symbol"]
        channels.setdefault(channel, []).append(
            {"symbol": symbol, "period": "1m", "adjust": "none"} if channel == "kline" else symbol
        )
    if root_handle is not None:
        root_handle.check()
    return result, channels


def write_json(path, obj, *, root_handle=None, replace=False):
    """Publish JSON through a held root; replacement is for derived reports."""
    path = Path(path).absolute()
    content = (json.dumps(obj, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
    if root_handle is None:
        with RootHandle(path.parent) as handle:
            handle.publish(path.name, content, replace=replace)
    else:
        if path.parent != root_handle.path:
            raise ValueError("report outside held root")
        root_handle.publish(path.name, content, replace=replace)


def _strict_integer(value):
    return isinstance(value, int) and value.__class__ is int


def _observe_record(record, dimension_keys, observations):
    """Collect locators from the exact records being verified."""
    obj = json.loads(record["raw_payload"])
    channel = {
        "QUOTE": "quote",
        "ORDER_BOOK": "order_book",
        "TICKER": "ticker",
        "KLINE": "kline",
    }.get(obj["type"])
    symbol = obj.get("symbol")
    if not channel or not isinstance(symbol, str) or (symbol, channel) not in dimension_keys:
        return
    if channel == "kline" and (
        obj["data"].get("period"),
        obj["data"].get("adjust"),
    ) != ("1m", "none"):
        return
    observation = observations.setdefault((symbol, channel), {"refs": [], "count": 0})
    refs = observation["refs"]
    if channel == "ticker":
        rows = obj["data"].get("ticker_list")
        if not isinstance(rows, list):
            return
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


def _report(root, run_id, *, publish=True, budget, root_handle=None, plan_snapshot=None):  # noqa: PLR0912
    """Rebuild from sealed manifest only; interrupted or unverifiable data stays unknown."""
    root = Path(root)
    plan_path = root / f"futu-plan-{run_id}.json"
    if root_handle is None or root.absolute() != root_handle.path:
        raise ValueError("plan root is not held")
    if plan_snapshot is None:
        raw_plan, plan_identity = budget.read_identity_at(root_handle, plan_path.name, max_bytes=8_388_608)
    else:
        raw_plan, plan_identity = plan_snapshot
        budget.check_identity_at(root_handle, plan_path.name, plan_identity)
    result = json.loads(raw_plan)
    events_path = root / f"futu-events-{run_id}.jsonl"
    events: list = []
    last_event = None
    failed_stop = False
    commit_failures = {}
    malformed_commit_failure = False
    event_log_status = "missing"
    event_count = 0
    gap_started = None
    try:
        event_log_status = "complete"
        for line in budget.lines(events_path):
            event = None
            try:
                if not line.endswith(b"\n"):
                    raise ValueError("incomplete event")
                event = json.loads(line)
                if not isinstance(event, dict) or not isinstance(event.get("event"), str):
                    raise ValueError("invalid event")
                if "time" in event and (
                    type(event["time"]) not in (int, float) or not math.isfinite(event["time"])
                ):
                    raise ValueError("invalid event time")
                if event["event"] == "stopped" and (
                    not isinstance(event.get("status"), str)
                    or event["status"] not in {"failed", "stopped"}
                ):
                    raise ValueError("invalid stopped event")
                if event["event"] == "manifest_commit_failed" and (
                    not isinstance(event.get("run_id"), str)
                    or not isinstance(event.get("session_id"), str)
                    or not re.fullmatch(r"[0-9a-f]{32}", event["run_id"])
                    or not re.fullmatch(r"[0-9a-f]{32}", event["session_id"])
                    or type(event.get("publish_attempt")) is not int
                    or event["publish_attempt"] < 1
                    or type(event.get("original_commit_failure")) is not bool
                    or event.get("stage") not in (
                        "manifest_write", "manifest_file_fsync", "manifest_publish",
                        "manifest_directory_fsync", "manifest_parent_fsync", "manifest_temp_cleanup",
                    )
                    or (event.get("original_commit_failure") is True and (
                        event.get("publish_attempt") != 1 or event.get("stage") == "manifest_temp_cleanup"
                    ))
                ):
                    malformed_commit_failure = True
                    raise ValueError("invalid commit event")
                if event["event"] == "event_log_corrupt_tail":
                    event_log_status = "corrupt"
                last_event = event
                if event.get("event") == "stopped" and event.get("status") == "failed":
                    failed_stop = True
                if event.get("event") == "manifest_commit_failed":
                    key = (event["run_id"], event["session_id"], event["original_commit_failure"])
                    if key not in commit_failures and len(commit_failures) >= 1000:
                        raise BudgetExceeded("commit failure summary limit exceeded")
                    commit_failures.setdefault(key, event)
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
            except BudgetExceeded:
                raise
            except (ValueError, UnicodeError):
                if isinstance(event, dict) and event.get("event") == "manifest_commit_failed":
                    malformed_commit_failure = True
                event_log_status = "corrupt"
    except FileNotFoundError:
        event_log_status = "missing"
    except PermissionError:
        if publish:
            raise
        event_log_status = "unreadable"
    except (OSError, ValueError):
        raise
    result["event_log_status"] = event_log_status
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
    manifest_identity = None
    invalid_manifest = False
    manifest_dir = root / "futu-manifests"
    for name in budget.entries(manifest_dir):
        if not name.endswith(".json"):
            continue
        path = manifest_dir / name
        try:
            raw, candidate_identity = budget.read_identity(path, max_bytes=8 * 1024 * 1024)
            candidate = json.loads(raw)
            if candidate["run_id"] == run_id:
                if manifest is not None:
                    invalid_manifest = True
                manifest = candidate
                manifest_path = path
                manifest_identity = candidate_identity
        except BudgetExceeded:
            raise
        except (OSError, ValueError, KeyError, TypeError):
            raise ValueError("invalid manifest input") from None
    observations: dict = {}
    dimension_keys = {(item["symbol"], item["channel"]) for item in result["selected"] + result["not_selected"]}
    if manifest:
        try:
            invalid_manifest = invalid_manifest or verify(
                manifest_path,
                budget=budget,
                expected_manifest_identity=manifest_identity,
                on_record=lambda record: _observe_record(record, dimension_keys, observations),
                on_manifest=lambda checked: checked == manifest or (_ for _ in ()).throw(ValueError("manifest changed")),
            )["status"] != "verified"
        except (BudgetExceeded, EvidenceChanged):
            raise
        except (OSError, ValueError, KeyError, TypeError):
            invalid_manifest = True
    if invalid_manifest:
        observations.clear()
    for item in result["selected"] + result["not_selected"]:
        selected = item in result["selected"]
        observation = observations.get((item["symbol"], item["channel"]), {"refs": [], "count": 0})
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
        if status == "failed" or (status == "stopped" and manifest and not manifest.get("error")):
            result["run_status"] = status
    failure = next(
        (
            event
            for event in commit_failures.values()
            if event.get("run_id") == run_id
            and isinstance(event.get("session_id"), str)
            and re.fullmatch(r"[0-9a-f]{32}", event["session_id"])
        ),
        None,
    )
    unattributed_failure = malformed_commit_failure or any(
        manifest is None
        or event.get("session_id") != manifest.get("session_id")
        or event.get("run_id") != run_id
        or not isinstance(event.get("session_id"), str)
        or not re.fullmatch(r"[0-9a-f]{32}", event["session_id"])
        for event in commit_failures.values()
    )
    uncertain_events = (
        event_log_status in {"corrupt", "unreadable"}
        or (event_log_status == "missing" and manifest is not None)
        or unattributed_failure
    )
    if uncertain_events:
        result["run_status"] = "unknown"
    confirmed_failure = any(
        manifest is not None
        and event.get("session_id") == manifest.get("session_id")
        and event.get("run_id") == run_id
        and type(event.get("publish_attempt")) is int
        and event.get("original_commit_failure") is True
        and event.get("publish_attempt") == 1
        and isinstance(event.get("session_id"), str)
        and re.fullmatch(r"[0-9a-f]{32}", event["session_id"])
        for event in commit_failures.values()
    )
    if failure and not confirmed_failure:
        uncertain_events = True
        result["run_status"] = "unknown"
    if invalid_manifest or (failed_stop and not uncertain_events) or (confirmed_failure and not uncertain_events):
        result["run_status"] = "failed"
    original_failure = next(
        (event for event in commit_failures.values()
         if manifest is not None
         and event.get("session_id") == manifest.get("session_id")
         and event.get("run_id") == run_id
         and type(event.get("publish_attempt")) is int
         and event.get("original_commit_failure") is True
         and event.get("publish_attempt") == 1
         and isinstance(event.get("session_id"), str)
         and re.fullmatch(r"[0-9a-f]{32}", event["session_id"])
         and (manifest is None or event["session_id"] == manifest.get("session_id"))),
        None,
    )
    result["original_commit_status"] = "failed" if original_failure else "unknown"
    result["original_commit_status_basis"] = (
        "recorded_commit_failure" if original_failure else "no_trustworthy_commit_confirmation"
    )
    if original_failure:
        stages = {
            "manifest_write",
            "manifest_file_fsync",
            "manifest_publish",
            "manifest_directory_fsync",
            "manifest_parent_fsync",
        }
        stage = original_failure.get("stage")
        result["original_commit_failure_stage"] = stage if stage in stages else "unknown"
    result["integrity_status"] = "failed" if invalid_manifest else ("verified" if manifest else "missing")
    result["evidence_status"] = "unknown" if uncertain_events and not invalid_manifest else result["integrity_status"]
    result["unattributed_commit_failure"] = unattributed_failure
    for asset, channels in result["by_asset_channel"].items():
        for channel, summary in channels.items():
            dims = [d for d in result["dimensions"] if d["asset"] == asset and d["channel"] == channel]
            summary["discovery_refs"] = sorted({ref for d in dims for ref in d.get("discovery_refs", [])})
            summary["synced_rows"] = sum(d["synced_rows"] for d in dims)
            summary["persistence_status"] = "observed" if summary["synced_rows"] else "not_observed"
            summary["entitlement"] = "unknown"
    result["archive_manifest"] = manifest
    budget.check_identity_at(root_handle, plan_path.name, plan_identity)
    if publish:
        write_json(root / f"futu-coverage-{run_id}.json", result, root_handle=root_handle, replace=True)
    return result


def report(root, run_id, *, publish=True, budget=None, root_handle=None):
    """Reconstruct coverage with a shared input budget."""
    try:
        if root_handle is not None:
            return _report(root, run_id, publish=publish, budget=budget if budget is not None else Budget(),
                           root_handle=root_handle)
        with RootHandle(root) as handle:
            return _report(root, run_id, publish=publish, budget=budget if budget is not None else Budget(),
                           root_handle=handle)
    except PublicationDurabilityUnknown:
        return {"run_id": run_id, "run_status": "unknown", "evidence_status": "unknown",
                "integrity_status": "unknown", "budget_or_input_error": True,
                "publication_status": "visible_durability_unknown"}
    except (OSError, ValueError, KeyError, TypeError):
        return {"run_id": run_id, "run_status": "unknown", "evidence_status": "unknown",
                "integrity_status": "unknown", "budget_or_input_error": True}

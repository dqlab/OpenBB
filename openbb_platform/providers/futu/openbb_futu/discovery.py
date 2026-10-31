"""Bounded, seed-scoped official derivatives discovery; no universe claim."""

from __future__ import annotations

import hashlib
import json
import os
import re
from pathlib import Path
from uuid import uuid4

from openbb_core.provider.utils.helpers import make_request

from openbb_futu.client import FutuError, _base
from openbb_futu.coordination import Owner, RequestBudget, coordination_directory

SYMBOL = re.compile(r"^[A-Z0-9_]+\.[A-Za-z0-9_.-]+$")
ENDPOINTS = {
    "option": ("option-chain", "option_chain"),
    "future": ("reference-future", "reference_list"),
}


def discover(
    seeds: list[dict],
    token: str,
    output: Path,
    *,
    max_requests: int = 20,
    max_bytes: int = 8_388_608,
    secrets: tuple[str, ...] = (),
) -> dict:
    """Capture exact response bytes for bounded explicit seeds, including HTTP failures.

    The schema documents no pagination parameters for these endpoints. Unexpected
    pagination and malformed or oversized responses fail closed, never as complete.
    """
    if not token:
        raise FutuError("not_configured")
    if (
        not 1 <= max_requests <= 100
        or not 1 <= max_bytes <= 32 * 1024 * 1024
        or len(seeds) > max_requests
    ):
        raise ValueError("invalid discovery bounds")
    for seed in seeds:
        if (
            set(seed) != {"asset", "symbol"}
            or seed["asset"] not in ENDPOINTS
            or not isinstance(seed["symbol"], str)
            or not SYMBOL.fullmatch(seed["symbol"])
            or len(seed["symbol"]) > 32
        ):
            raise ValueError("invalid discovery seed")
    directory = Path(output) / uuid4().hex
    directory.mkdir(parents=True, mode=0o700)
    report: dict = {
        "evidence_dir": str(directory),
        "target_status": {x: "unknown" for x in ("equity", "etf", "option", "future")},
        "requests": [],
        "coverage_partial": True,
    }
    sensitive_keys = {
        "authorization",
        "access_token",
        "refresh_token",
        "client_id",
        "client_secret",
        "password",
        "secret",
        "credential",
        "credential_id",
        "signature",
        "private_key",
        "api_key",
        "token",
        "cookie",
        "set-cookie",
    }

    def sensitive(value):
        if isinstance(value, dict):
            return any(
                k.lower() in sensitive_keys or sensitive(v) for k, v in value.items()
            )
        if isinstance(value, list):
            return any(sensitive(v) for v in value)
        return isinstance(value, str) and (
            bool(re.search(r"(?i)\bbearer\s+\S+", value))
            or any(secret and secret in value for secret in (token, *secrets))
        )

    with Owner(coordination_directory()):
        for index, seed in enumerate(seeds):
            endpoint, field = ENDPOINTS[seed["asset"]]
            item = {**seed, "status": "unknown", "symbols": []}
            report["requests"].append(item)
            if RequestBudget(coordination_directory()).acquire() > 0:
                item["error"] = "local_budget_exhausted"
                break
            try:
                response = make_request(
                    _base() + "/api/v1.0/quote/" + seed["symbol"] + "/" + endpoint,
                    method="GET",
                    headers={"Authorization": "Bearer " + token},
                    timeout=10,
                    stream=True,
                    allow_redirects=False,
                )
                try:
                    chunks = bytearray()
                    for chunk in response.iter_content(65536):
                        chunks.extend(chunk)
                        if len(chunks) > max_bytes:
                            raise ValueError("response_too_large")
                    raw = bytes(chunks)
                    status = response.status_code
                finally:
                    response.close()

                # Persist only JSON that can be inspected recursively. Never write the
                # response before checking keys and every known credential value.
                def unique(pairs):
                    result = {}
                    for key, value in pairs:
                        if key in result:
                            raise ValueError("invalid_response")
                        result[key] = value
                    return result

                obj = json.loads(raw, object_pairs_hook=unique)
                if sensitive(obj):
                    raise ValueError("credential_in_response")
                path = directory / f"response-{index:04d}.bin"
                fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
                with os.fdopen(fd, "wb") as stream:
                    stream.write(raw)
                    stream.flush()
                    os.fsync(stream.fileno())
                item.update(
                    http_status=status,
                    raw_file=str(path),
                    sha256=hashlib.sha256(raw).hexdigest(),
                    bytes=len(raw),
                )
                if (
                    not 200 <= status < 300
                    or not isinstance(obj, dict)
                    or obj.get("ret_code") != 0
                ):
                    item["error"] = "provider_failure"
                    continue
                if obj.get("pagination") is not None:
                    item["error"] = "undocumented_pagination"
                    continue
                rows = obj.get("data", {}).get(field)
                if not isinstance(rows, list) or any(
                    not isinstance(row, dict) or not isinstance(row.get("code"), str)
                    for row in rows
                ):
                    item["error"] = "invalid_response"
                    continue
                item.update(
                    status="seed_scope_observed", symbols=[row["code"] for row in rows]
                )
            except Exception as exc:
                item["error"] = (
                    str(exc)
                    if isinstance(exc, ValueError)
                    and str(exc) in {"response_too_large", "credential_in_response"}
                    else "invalid_response_or_transport"
                )
    fd = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
    return report

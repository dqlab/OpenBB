"""Bounded local FUTUNN WebSocket collector. No network activity on import."""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import logging
import math
import os
import signal
import stat
import sys
import time
import uuid
from dataclasses import dataclass
from pathlib import Path

from websockets.asyncio import client as websocket_client

from openbb_futu.archive import Archive, recover_open_files
from openbb_futu.client import FutuError, refresh_tokens
from openbb_futu.coordination import Owner, coordination_directory
from openbb_futu.coverage import prepare, report, write_json
from openbb_futu.ws import URL


class TokenStore:
    """User-provided private token file; ambiguous interrupted rotation fails closed."""

    def __init__(self, path: Path):
        self.path = path
        self.marker = path.with_name(path.name + ".refresh-pending")

    def read(self):
        """Handle read."""
        parent = self.path.parent.stat()
        if parent.st_uid != os.getuid() or parent.st_mode & 0o022:
            raise FutuError("insecure_token_store")
        if self.marker.exists():
            raise FutuError("token_rotation_uncertain")
        fd = os.open(self.path, os.O_RDONLY | os.O_NOFOLLOW)
        try:
            info = os.fstat(fd)
            if info.st_size > 65536:
                raise FutuError("token_store_too_large")
            if (
                not stat.S_ISREG(info.st_mode)
                or info.st_uid != os.getuid()
                or info.st_mode & 0o077
            ):
                raise FutuError("insecure_token_store")
            with os.fdopen(fd, encoding="utf-8") as stream:
                fd = -1
                obj = json.load(stream)
            if not all(
                isinstance(obj.get(k), str) and obj[k]
                for k in ("client_id", "access_token", "refresh_token")
            ):
                raise FutuError("invalid_token_store")
            if any(
                len(obj[k].encode("utf-8")) > 8192
                for k in ("client_id", "access_token", "refresh_token")
            ):
                raise FutuError("token_store_too_large")
            if not isinstance(obj.get("expires_at"), (int, float)):
                raise FutuError("invalid_token_store")
            return obj
        finally:
            if fd >= 0:
                os.close(fd)

    def rotate(self, owner):
        """Handle rotate."""
        obj = self.read()
        fd = os.open(
            self.marker, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600
        )
        os.fsync(fd)
        os.close(fd)
        self._sync_dir()
        # Any failure leaves the marker: the server may have rotated the token.
        result = refresh_tokens(obj["client_id"], obj["refresh_token"], owner=owner)
        obj.update(
            access_token=result.access_token,
            refresh_token=result.refresh_token,
            expires_at=time.time() + result.expires_in,
        )
        temp = self.path.with_name(self.path.name + "." + uuid.uuid4().hex + ".tmp")
        fd = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(obj, stream)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp, self.path)
        self._sync_dir()
        self.marker.unlink()
        self._sync_dir()
        return obj

    def _sync_dir(self):
        fd = os.open(self.path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)


@dataclass
class CollectorLifecycle:
    """Mutable connection and shutdown state for a single collector run."""

    connection: str | None = None
    session_id: str | None = None
    finalized: bool = False
    mode: str | None = None
    fatal_errno: int | None = None
    failure: str | None = None


class Collector:
    """Handle Collector."""

    def __init__(self, config, store, root, *, connection_factory=None):
        self.config, self.store, self.root = config, store, Path(root)
        self.connection_factory = connection_factory
        self.stop = asyncio.Event()
        self.pending = {}
        self.run_id = uuid.uuid4().hex
        self.archive = None
        self.gap = False
        self.logfile = None
        self.lifecycle = CollectorLifecycle()

    def event(self, kind, **fields):
        """Handle event."""
        record = {"time": time.time(), "event": kind, **fields}
        if self.logfile is None:
            raise RuntimeError("event log is not open")
        self.logfile.write(json.dumps(record) + "\n")
        self.logfile.flush()
        os.fsync(self.logfile.fileno())

    def set_mode(self, mode):
        """Persist lifecycle transitions without recording credentials."""
        if self.lifecycle.mode != mode:
            self.event(
                "mode_change",
                previous=self.lifecycle.mode,
                mode=mode,
                connection=self.lifecycle.connection,
            )
            self.lifecycle.mode = mode

    async def command(self, socket, action, payload):
        """Handle command."""
        ident = uuid.uuid4().hex
        future = asyncio.get_running_loop().create_future()
        self.pending[ident] = future
        try:
            await socket.send(json.dumps({"id": ident, "action": action, **payload}))
            try:
                response = await asyncio.wait_for(
                    future, self.config.get("ack_timeout", 10)
                )
            except asyncio.TimeoutError:
                raise FutuError("invalid_or_missing_ack") from None
            if action in ("auth", "refresh"):
                session_id = response.get("session_id")
                if (
                    not isinstance(session_id, str)
                    or not session_id
                    or (action == "refresh" and session_id != self.lifecycle.session_id)
                ):
                    raise FutuError("authentication")
                self.lifecycle.session_id = session_id
            elif response.get("code") != 0:
                raise FutuError("subscription_rejected")
            else:
                for channel, items in payload.items():
                    for item in items:
                        self.event(
                            "subscription_ack",
                            connection=self.lifecycle.connection,
                            channel=channel,
                            dimension=item,
                            status="unknown",
                            code=0,
                        )
        finally:
            self.pending.pop(ident, None)

    async def reader(self, socket):
        """Handle reader."""
        if self.archive is None:
            raise RuntimeError("archive is not initialized")
        failure = FutuError("connection_lost")
        try:
            async for raw in socket:

                def unique(pairs):
                    """Handle unique."""
                    result = {}
                    for key, value in pairs:
                        if key in result:
                            raise ValueError("duplicate key")
                        result[key] = value
                    return result

                try:
                    obj = json.loads(raw, object_pairs_hook=unique)
                except (ValueError, TypeError):
                    raise FutuError("unclassifiable_frame") from None
                if not isinstance(obj, dict):
                    raise FutuError("unclassifiable_frame")
                if "id" in obj:
                    future = self.pending.get(obj["id"])
                    if future is None or future.done() or "type" in obj:
                        raise FutuError("unexpected_control")
                    future.set_result(obj)
                elif not self.archive.accept(raw, self.lifecycle.connection):
                    raise FutuError("archive_rejected")
        except FutuError as exc:
            failure = exc
            raise
        finally:
            for future in self.pending.values():
                if not future.done():
                    future.set_exception(failure)
        if not self.stop.is_set():
            raise FutuError("connection_lost")

    async def session(self, owner):
        """Handle session."""
        if self.archive is None:
            raise RuntimeError("archive is not initialized")

        self.set_mode("connecting")
        token = self.store.read()
        margin = self.config.get("token_margin", 60)
        if token["expires_at"] <= time.time() + margin:
            token = await asyncio.to_thread(self.store.rotate, owner)
        self.archive.add_secrets(
            (token["access_token"], token["refresh_token"], token["client_id"])
        )
        quiet_logger = logging.Logger(
            "futu.websocket.suppressed", level=logging.CRITICAL + 1
        )
        quiet_logger.disabled = True
        factory = self.connection_factory or websocket_client.connect
        async with factory(
            URL,
            logger=quiet_logger,
            open_timeout=10,
            ping_interval=self.config.get("ping_interval", 20),
            ping_timeout=self.config.get("ping_timeout", 20),
            max_size=2**20,
        ) as socket:
            self.lifecycle.connection = uuid.uuid4().hex
            self.lifecycle.session_id = None
            reader = asyncio.create_task(self.reader(socket))
            try:
                await self.command(
                    socket,
                    "auth",
                    {
                        "data": {
                            "auth_type": "oauth2",
                            "authorization": "Bearer " + token["access_token"],
                        }
                    },
                )
                await self.command(socket, "subscribe", self.config["channels"])
                self.event(
                    "subscription_response",
                    connection=self.lifecycle.connection,
                    status="unknown",
                )
                self.set_mode("streaming")
                if self.gap:
                    self.event(
                        "gap_end",
                        connection=self.lifecycle.connection,
                        recovery="resubscribed; missing data not recovered",
                    )
                    self.gap = False
                last_refresh = last_subscription = time.monotonic()
                while not self.stop.is_set():
                    delay = max(
                        0,
                        min(
                            last_refresh
                            + self.config.get("channel_refresh", 300)
                            - time.monotonic(),
                            last_subscription
                            + self.config.get("resubscribe_interval", 300)
                            - time.monotonic(),
                            token["expires_at"] - margin - time.time(),
                        ),
                    )
                    stop_task = asyncio.create_task(self.stop.wait())
                    done, _ = await asyncio.wait(
                        {reader, stop_task},
                        timeout=min(delay, 0.5),
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    if stop_task not in done:
                        stop_task.cancel()
                    if reader in done:
                        await reader
                    if self.stop.is_set():
                        break
                    if self.archive.stop.is_set():
                        raise FutuError("archive_failure")
                    if time.monotonic() >= last_subscription + self.config.get(
                        "resubscribe_interval", 300
                    ):
                        await self.command(socket, "subscribe", self.config["channels"])
                        last_subscription = time.monotonic()
                        self.event(
                            "subscription_response",
                            connection=self.lifecycle.connection,
                            status="unknown",
                        )
                    if (
                        time.monotonic()
                        >= last_refresh + self.config.get("channel_refresh", 300)
                        or time.time() >= token["expires_at"] - margin
                    ):
                        if token["expires_at"] <= time.time() + margin:
                            token = await asyncio.to_thread(self.store.rotate, owner)
                            self.archive.add_secrets(
                                (
                                    token["access_token"],
                                    token["refresh_token"],
                                    token["client_id"],
                                )
                            )
                        await self.command(
                            socket,
                            "refresh",
                            {
                                "data": {
                                    "authorization": "Bearer " + token["access_token"]
                                }
                            },
                        )
                        last_refresh = time.monotonic()
                        self.event(
                            "channel_refreshed", connection=self.lifecycle.connection
                        )
            finally:
                if self.stop.is_set() and not reader.done():
                    try:
                        await asyncio.wait_for(socket.close(), timeout=5)
                        await asyncio.wait_for(asyncio.shield(reader), timeout=5)
                    except Exception:
                        self.gap = True
                        self.lifecycle.failure = (
                            self.lifecycle.failure or "shutdown_receive_uncertain"
                        )
                        self.event("gap_start", reason="shutdown_receive_uncertain")
                if not reader.done():
                    reader.cancel()
                await asyncio.gather(reader, return_exceptions=True)

    async def run(self):
        """Handle run."""
        try:
            self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
            event_fd = os.open(
                self.root / ("futu-events-" + self.run_id + ".jsonl"),
                os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                0o600,
            )
            self.logfile = os.fdopen(event_fd, "w", encoding="utf-8")
            with Owner(coordination_directory()) as owner:
                if "dimensions" in self.config:
                    planned, self.config["channels"] = prepare(self.config)
                    write_json(self.root / f"futu-plan-{self.run_id}.json", planned)
                    for previous in self.root.glob("futu-plan-*.json"):
                        previous_id = previous.stem.removeprefix("futu-plan-")
                        if previous_id != self.run_id:
                            report(self.root, previous_id)
                    self.event("plan_created", selected=len(planned["selected"]))
                for recovery in recover_open_files(self.root):
                    self.event(
                        "interrupted_segment",
                        evidence_dir=recovery["evidence_dir"],
                        gap="unresolved",
                    )
                self.archive = Archive(
                    self.root,
                    run_id=self.run_id,
                    capacity=self.config.get("capacity", 1000),
                )
                retries = 0
                while not self.stop.is_set():
                    try:
                        await self.session(owner)
                        break
                    except Exception as exc:
                        category = (
                            exc.category if isinstance(exc, FutuError) else "transport"
                        )
                        self.event("session_failure", category=category)
                        if not self.gap:
                            self.gap = True
                            self.event("gap_start", reason=category)
                        if category not in (
                            "transport",
                            "connection_lost",
                            "invalid_or_missing_ack",
                        ) or retries >= self.config.get("max_reconnects", 5):
                            self.lifecycle.failure = category
                            break
                        delay = min(
                            self.config.get("backoff_max", 30),
                            self.config.get("backoff_base", 1) * 2**retries,
                        )
                        retries += 1
                        self.set_mode("reconnecting")
                        self.event("reconnect_wait", attempt=retries, seconds=delay)
                        with contextlib.suppress(asyncio.TimeoutError):
                            await asyncio.wait_for(self.stop.wait(), timeout=delay)
                if self.gap:
                    self.lifecycle.failure = self.lifecycle.failure or "gap_unresolved"
                    self.event("gap_unresolved")
                if self.lifecycle.failure or self.gap:
                    self.archive.error = self.archive.error or "incomplete_receive"
                result = await asyncio.to_thread(self.archive.close)
                self.event(
                    "archive_closed",
                    manifest=result.get("manifest"),
                    error=result["error"],
                    durable=result["durable_seq"],
                )
                if result["error"] or result["queue_depth"]:
                    self.lifecycle.failure = "archive_failure"
                self.set_mode("failed" if self.lifecycle.failure else "stopped")
                self.event(
                    "stopped", status="failed" if self.lifecycle.failure else "stopped"
                )
                if "dimensions" in self.config:
                    report(self.root, self.run_id)
                self.lifecycle.finalized = True
        except Exception as exc:
            self.lifecycle.failure = (
                exc.category if isinstance(exc, FutuError) else "collector_failure"
            )
            self.lifecycle.fatal_errno = exc.errno if isinstance(exc, OSError) else None
            with contextlib.suppress(Exception):
                self.event("fatal", category=self.lifecycle.failure)
        finally:
            try:
                if not self.lifecycle.finalized:
                    self.event(
                        "stopped",
                        status="failed" if self.lifecycle.failure else "stopped",
                    )
                if self.logfile is not None:
                    self.logfile.close()
            except Exception:
                self.lifecycle.failure = (
                    self.lifecycle.failure or "status_write_failure"
                )
            if self.lifecycle.failure:
                detail = ""
                if self.lifecycle.fatal_errno is not None:
                    detail += f"; fatal errno={self.lifecycle.fatal_errno}"
                if self.archive and self.archive.error:
                    detail += "; " + self.archive.error
                    if self.archive.error_errno is not None:
                        detail += f" errno={self.archive.error_errno}"
                print(  # noqa: T201
                    "collector failed: " + self.lifecycle.failure + detail,
                    file=sys.stderr,
                    flush=True,
                )  # noqa: T201
        return 1 if self.lifecycle.failure else 0


def main():
    """Handle main."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--token-file", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    with args.config.open("rb") as stream:
        config_bytes = stream.read(1024 * 1024 + 1)
    if len(config_bytes) > 1024 * 1024:
        parser.error("config exceeds byte budget")
    config = json.loads(config_bytes)
    if len(config.get("dimensions", [])) > 10000:
        parser.error("dimension budget exceeded")
    if "ws_url" in config and config["ws_url"] != URL:
        parser.error("ws_url must be the official endpoint")
    if "dimensions" in config:
        try:
            _, config["channels"] = prepare(config)
        except (ValueError, KeyError, TypeError) as exc:
            parser.error(str(exc))
    channels = config.get("channels")
    if (
        not isinstance(channels, dict)
        or not channels
        or set(channels) - {"quote", "order_book", "ticker", "kline"}
    ):
        parser.error("invalid channels")
    if any(not isinstance(items, list) or not items for items in channels.values()):
        parser.error("channels must contain nonempty lists")
    if sum(len(items) for items in channels.values()) > 10000:
        parser.error("subscription budget exceeded")
    if any(
        not isinstance(item, str) or not item.strip()
        for key, items in channels.items()
        if key != "kline"
        for item in items
    ):
        parser.error("invalid channel symbol")
    if any(
        not isinstance(item, dict)
        or not isinstance(item.get("symbol"), str)
        or not item["symbol"].strip()
        or item.get("period") != "1m"
        or item.get("adjust") != "none"
        for item in channels.get("kline", [])
    ):
        parser.error("invalid kline subscription")
    numeric_limits = {
        "ack_timeout": (0, 3600),
        "capacity": (0, 1000000),
        "backoff_base": (0, 3600),
        "backoff_max": (0, 86400),
        "ping_interval": (0, 3600),
        "ping_timeout": (0, 3600),
        "token_margin": (0, 86400),
    }
    for name, (lower, upper) in numeric_limits.items():
        value = config.get(
            name,
            {
                "ack_timeout": 10,
                "capacity": 1000,
                "backoff_base": 1,
                "backoff_max": 30,
                "ping_interval": 20,
                "ping_timeout": 20,
                "token_margin": 60,
            }[name],
        )
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(value)
            or not lower < value <= upper
        ):
            parser.error("invalid " + name)
        if name == "capacity" and not isinstance(value, int):
            parser.error("invalid capacity")
    if config.get("backoff_base", 1) > config.get("backoff_max", 30):
        parser.error("invalid backoff limits")
    if isinstance(config.get("max_reconnects", 5), bool) or not isinstance(
        config.get("max_reconnects", 5), int
    ):
        parser.error("invalid max_reconnects")
    for name, default in (("channel_refresh", 300), ("resubscribe_interval", 300)):
        value = config.get(name, default)
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(value)
        ):
            parser.error("invalid " + name)
    if (
        not 0 < config.get("channel_refresh", 300) < 600
        or not 0 <= config.get("max_reconnects", 5) <= 100
    ):
        parser.error("invalid lifecycle limits")
    if not 0 < config.get("resubscribe_interval", 300) <= 86400:
        parser.error("invalid resubscription interval")

    async def run():
        """Handle run."""
        collector = Collector(config, TokenStore(args.token_file), args.output)
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, collector.stop.set)
        return await collector.run()

    raise SystemExit(asyncio.run(run()))


if __name__ == "__main__":
    main()

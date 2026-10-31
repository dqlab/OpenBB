"""Explicit WebSocket source; no connection or credentials are created on import."""

import asyncio
import json
import logging
from collections import deque
from typing import Any

from websockets.asyncio import client as websocket_client

from openbb_futu.client import FutuError
from openbb_futu.coordination import Owner, coordination_directory

URL = "wss://webapi-quote.futunn.com/ws"


class WebSocketSource:
    """One authenticated connection. Caller owns refresh scheduling and recovery."""

    def __init__(
        self, access_token: str, *, timeout: float = 10, connection_factory=None
    ):
        if not access_token:
            raise FutuError("not_configured")
        self._token = access_token
        self._timeout = timeout
        self._connection_factory = connection_factory
        self._socket: Any = None
        self._owner = None
        self._authenticated = False
        self._session_id = None
        self._pending: deque = deque()

    async def _send(self, value):
        await self._socket.send(json.dumps(value, separators=(",", ":")))

    async def _ack(self, request_id):
        async def wait():
            """Handle wait."""
            while True:
                raw = await self._socket.recv()
                response = json.loads(raw)
                if not isinstance(response, dict):
                    raise ValueError
                if response.get("id") == request_id:
                    return response
                if (
                    not isinstance(response.get("type"), str)
                    or len(self._pending) >= 1000
                ):
                    raise ValueError
                self._pending.append(raw)

        try:
            return await asyncio.wait_for(wait(), self._timeout)
        except Exception:
            raise FutuError("invalid_or_missing_ack") from None

    async def connect(self):
        """Handle connect."""
        try:
            self._owner = Owner(coordination_directory())

            logger = logging.Logger(
                "futu.websocket.suppressed", level=logging.CRITICAL + 1
            )
            logger.disabled = True
            factory = self._connection_factory or websocket_client.connect
            self._socket = await factory(
                URL,
                open_timeout=self._timeout,
                ping_interval=20,
                ping_timeout=20,
                logger=logger,
            )
            await self._send(
                {
                    "id": "auth",
                    "action": "auth",
                    "data": {
                        "auth_type": "oauth2",
                        "authorization": "Bearer " + self._token,
                    },
                }
            )
            response = await self._ack("auth")
            if not response.get("session_id"):
                raise FutuError("authentication")
            self._session_id = response["session_id"]
            self._authenticated = True
        except Exception as exc:
            await self.close()
            if isinstance(exc, FutuError):
                raise
            raise FutuError("transport") from None

    async def subscribe(self, channels: dict):
        """Handle subscribe."""
        if not self._authenticated:
            raise FutuError("authentication")
        if not channels or set(channels) - {"quote", "order_book", "ticker", "kline"}:
            raise ValueError("invalid subscription channels")
        await self._send({"id": "subscribe", "action": "subscribe", **channels})
        response = await self._ack("subscribe")
        if response.get("code") != 0:
            raise FutuError("subscription_rejected")

    async def refresh(self, access_token: str):
        """Handle refresh."""
        if not self._authenticated or not access_token:
            raise FutuError("authentication")
        await self._send(
            {
                "id": "refresh",
                "action": "refresh",
                "data": {"authorization": "Bearer " + access_token},
            }
        )
        response = await self._ack("refresh")
        if response.get("session_id") != self._session_id:
            raise FutuError("authentication")
        self._token = access_token

    async def receive(self):
        """Handle receive."""
        if not self._authenticated:
            raise FutuError("authentication")
        if self._pending:
            return self._pending.popleft()
        try:
            return await self._socket.recv()
        except Exception:
            raise FutuError("transport") from None

    async def close(self):
        """Handle close."""
        self._authenticated = False
        self._pending.clear()
        try:
            if self._socket is not None:
                await self._socket.close()
                self._socket = None
        finally:
            if self._owner is not None:
                self._owner.close()
                self._owner = None

"""Offline contract checks with explicit network-negative controls."""

import asyncio
import json
from unittest.mock import patch

import pytest
from openbb_futu.client import FutuError, refresh_tokens, snapshot
from openbb_futu.quote import FutuEquityQuoteFetcher
from openbb_futu.ws import WebSocketSource


class Response:
    """Handle Response."""

    def __init__(self, status, body):
        self.status_code, self.body = status, body

    def json(self):
        """Handle json."""
        return self.body


def test_snapshot_and_partial_response():
    """Test snapshot and partial response."""
    rows = [{"code": "HK.00700", "last_price": 100}]
    with patch(
        "openbb_futu.client.make_request",
        return_value=Response(200, {"ret_code": 0, "data": {"snapshot_list": rows}}),
    ) as request:
        result = snapshot(["HK.00700"], "secret")
        assert request.call_args.kwargs["json"] == {"code_list": ["HK.00700"]}
        assert (
            FutuEquityQuoteFetcher.transform_data(None, result)[0].symbol == "HK.00700"
        )
        with pytest.raises(FutuError, match="partial_or_no_data"):
            snapshot(["HK.00700", "US.AAPL"], "secret")


@pytest.mark.parametrize(
    "status,category",
    [
        (401, "authentication"),
        (403, "permission"),
        (429, "rate_limit"),
        (500, "transport"),
    ],
)
def test_http_failure_classification(status, category):
    """Test http failure classification."""
    with patch("openbb_futu.client.make_request", return_value=Response(status, {})):
        with pytest.raises(FutuError, match=category) as exc:
            snapshot(["US.AAPL"], "secret")
        assert "secret" not in str(exc.value)


def test_no_credentials_and_transport_are_not_fake_success():
    """Test no credentials and transport are not fake success."""
    with patch(
        "openbb_futu.client.make_request", side_effect=RuntimeError("secret")
    ) as request:
        with pytest.raises(FutuError, match="not_configured"):
            snapshot(["US.AAPL"], "")
        request.assert_not_called()
        with pytest.raises(FutuError, match="transport") as exc:
            snapshot(["US.AAPL"], "secret")
        assert "secret" not in str(exc.value)


def test_refresh_rotation():
    """Test refresh rotation."""
    with patch(
        "openbb_futu.client.make_request",
        return_value=Response(
            200, {"access_token": "new", "refresh_token": "rotated", "expires_in": 60}
        ),
    ) as request:
        tokens = refresh_tokens("client", "old")
        assert (
            tokens.refresh_token == "rotated"  # noqa: S105 -- synthetic test credential
        )
        assert "rotated" not in repr(tokens)
        assert request.call_args.kwargs["data"]["grant_type"] == "refresh_token"


def test_ws_auth_subscription_and_refresh():
    """Test ws auth subscription and refresh."""

    class Socket:
        """Handle Socket."""

        def __init__(self):
            self.sent = []
            self.responses = iter(
                [
                    {"id": "auth", "session_id": "s"},
                    {"id": "subscribe", "code": 0},
                    {"id": "refresh", "session_id": "s"},
                ]
            )

        async def send(self, value):
            """Handle send."""
            self.sent.append(json.loads(value))

        async def recv(self):
            """Handle recv."""
            return json.dumps(next(self.responses))

        async def close(self):
            """Handle close."""
            pass

    async def run():
        """Handle run."""
        sock = Socket()

        async def connect(*args, **kwargs):
            """Handle connect."""
            return sock

        source = WebSocketSource("secret", connection_factory=connect)
        await source.connect()
        await source.subscribe({"ticker": ["US.AAPL"]})
        await source.refresh("new")
        await source.close()
        assert sock.sent[0]["data"]["authorization"] == "Bearer secret"
        assert sock.sent[1] == {
            "id": "subscribe",
            "action": "subscribe",
            "ticker": ["US.AAPL"],
        }
        assert sock.sent[2]["action"] == "refresh"

    asyncio.run(run())

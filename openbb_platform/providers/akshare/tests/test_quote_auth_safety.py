"""Requests authentication cannot consume an unbounded challenge body."""

# ruff: noqa: D101, D102, D103

import io
import socket

import pytest
import requests
from openbb_akshare.utils import etf_option_quotes as quotes
from openbb_core.provider.utils import helpers
from requests.adapters import HTTPAdapter
from requests.auth import AuthBase, HTTPBasicAuth, HTTPDigestAuth


class ReadingAuth(AuthBase):
    def __call__(self, request):
        request.register_hook("response", lambda response, **kwargs: response.content)
        return request


class BasicSubclass(HTTPBasicAuth):
    def __call__(self, request):
        request.register_hook("response", lambda response, **kwargs: response.content)
        return request


class TrackedBody(io.BytesIO):
    def __init__(self):
        super().__init__(b"x" * 2_000_000)
        self.bytes_read = 0

    def read(self, size=-1, **kwargs):
        data = super().read(size)
        self.bytes_read += len(data)
        return data


@pytest.fixture
def isolated(monkeypatch):
    body = TrackedBody()
    calls = []

    def no_network(*args, **kwargs):
        raise AssertionError("real network forbidden")

    def send(_adapter, request, **kwargs):
        calls.append(request)
        response = requests.Response()
        response.status_code = 401
        response.headers["WWW-Authenticate"] = 'Digest realm="test", nonce="synthetic"'
        response.url = request.url
        response.request = request
        response.raw = body
        response.connection = _adapter
        return response

    monkeypatch.setattr(socket.socket, "connect", no_network)
    monkeypatch.setattr(socket.socket, "connect_ex", no_network)
    monkeypatch.setattr(HTTPAdapter, "send", send)
    monkeypatch.setattr(helpers, "get_python_request_settings", lambda: {})
    return body, calls


def test_requests_digest_hook_precedes_session_hook(isolated):
    body, calls = isolated
    session = helpers.get_requests_session()
    session.auth = HTTPDigestAuth("synthetic", "synthetic")
    seen = []
    session.hooks["response"].append(lambda response, **kwargs: seen.append(body.bytes_read) or response)
    response = session.get("https://example.invalid/", stream=True)
    assert len(calls) == 2 and body.bytes_read == 2_000_000
    assert seen == [2_000_000]
    response.close()



@pytest.mark.parametrize(
    "auth",
    [HTTPDigestAuth("synthetic", "synthetic"), ReadingAuth(), BasicSubclass("synthetic", "synthetic")],
)
def test_unsafe_auth_rejected_before_request(isolated, monkeypatch, auth):
    body, calls = isolated
    monkeypatch.setattr(helpers, "get_python_request_settings", lambda: {"auth": auth})
    with pytest.raises(ValueError, match="quote_auth_unsupported"):
        quotes._read_quote_body("https://example.invalid/")
    assert not calls and body.bytes_read == 0


@pytest.mark.parametrize("auth", [("synthetic", "synthetic"), HTTPBasicAuth("synthetic", "synthetic")])
def test_basic_auth_does_not_read_challenge(isolated, monkeypatch, auth):
    body, calls = isolated
    monkeypatch.setattr(helpers, "get_python_request_settings", lambda: {"auth": auth})
    with pytest.raises(requests.HTTPError):
        quotes._read_quote_body("https://example.invalid/")
    assert len(calls) == 1 and calls[0].headers["Authorization"].startswith("Basic ")
    assert body.bytes_read == 0


def test_existing_response_hook_rejected(isolated, monkeypatch):
    body, calls = isolated
    get_session = quotes.get_requests_session

    def session_with_hook():
        session = get_session()
        session.hooks["response"].append(lambda response, **kwargs: response)
        return session

    monkeypatch.setattr(quotes, "get_requests_session", session_with_hook)
    with pytest.raises(ValueError, match="quote_response_hooks_unsupported"):
        quotes._read_quote_body("https://example.invalid/")
    assert not calls and body.bytes_read == 0

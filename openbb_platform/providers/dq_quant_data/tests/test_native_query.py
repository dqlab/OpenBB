"""Native reads preserve exact records, bounded responses and public rejections."""

import json
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from fastapi.testclient import TestClient
from openbb_core.app.model.abstract.error import OpenBBError
from requests import RequestException, Response

from openbb_dq_quant_data import native as client
from openbb_dq_quant_data import router


def response(body, status=200, retry_after=None):
    result = Response()
    result.status_code = status
    result._content = json.dumps(body).encode()  # pylint: disable=protected-access
    result._content_consumed = True  # pylint: disable=protected-access
    if retry_after is not None:
        result.headers["Retry-After"] = retry_after
    return result


def native_result():
    return {
        "data": [
            {"event_epoch_ns": "1791936000000000001", "close": "12.123456789", "volume": None}
        ],
        "meta": {"rows": 1, "provider": "ibkr", "quality": {"source_published_at": None}},
    }


def test_native_command_preserves_exact_data_metadata_credentials_and_one_post(monkeypatch):
    credentials = {
        "dq_quant_data_api_url": "https://dq.example",
        "dq_quant_data_api_key": "test-key",
        "dq_quant_data_api_ca_file": "/test/ca.crt",
    }
    request = Mock(return_value=response(native_result()))
    monkeypatch.setattr(client, "make_request", request)
    cc = SimpleNamespace(
        user_settings=SimpleNamespace(credentials=SimpleNamespace(model_dump=lambda: credentials))
    )
    data = {"limit": 2, "start_ns": "1791936000000000001", "knowledge_basis": "source_published"}
    result = router.native_query(cc, data)
    assert result.results == native_result()["data"]
    assert result.extra == native_result()["meta"]
    request.assert_called_once_with(
        "https://dq.example/v2/query",
        method="POST",
        json=data,
        headers={"X-API-Key": "test-key"},
        timeout=10,
        allow_redirects=False,
        verify="/test/ca.crt",
    )


def test_generated_sdk_keeps_native_request_dictionary(monkeypatch):
    from openbb import obb

    request = Mock(return_value=response(native_result()))
    monkeypatch.setattr(client, "make_request", request)
    result = obb.dq_data.native_query(request={"limit": 1})
    assert result.results == native_result()["data"]
    assert all(result.extra[key] == value for key, value in native_result()["meta"].items())
    request.assert_called_once()


@pytest.mark.parametrize("status", [422, 429])
def test_generated_sdk_retains_status_in_public_error_without_retry(monkeypatch, status):
    from openbb import obb

    request = Mock(return_value=response({}, status, "2"))
    monkeypatch.setattr(client, "make_request", request)
    with pytest.raises(OpenBBError, match=f"HTTP {status}"):
        obb.dq_data.native_query(request={"limit": 1})
    request.assert_called_once()


@pytest.mark.parametrize("limit", [0, 10001, True, "10"])
def test_invalid_native_limit_fails_before_http(monkeypatch, limit):
    request = Mock()
    monkeypatch.setattr(client, "make_request", request)
    with pytest.raises(OpenBBError, match="limit"):
        client.NativeAPIClient(None).native_query({"limit": limit})
    request.assert_not_called()


@pytest.mark.parametrize("data", [{"blob": "x" * 65536}, {"value": float("nan")}, []])
def test_invalid_or_large_native_request_fails_before_http(monkeypatch, data):
    request = Mock()
    monkeypatch.setattr(client, "make_request", request)
    with pytest.raises(OpenBBError):
        client.NativeAPIClient(None).native_query(data)
    request.assert_not_called()


@pytest.mark.parametrize(
    "body",
    [
        [],
        {"data": [1], "meta": {"rows": 1}},
        {"data": [{}, {}], "meta": {"rows": 2}},
        {"data": [{}], "meta": {"rows": 0}},
        {"data": [{}], "meta": {"rows": True}},
        {"data": [{}], "meta": []},
    ],
)
def test_native_envelope_and_requested_row_bound(monkeypatch, body):
    monkeypatch.setattr(client, "make_request", Mock(return_value=response(body)))
    with pytest.raises(OpenBBError):
        client.NativeAPIClient(None).native_query({"limit": 1})


def test_native_json_and_response_byte_bounds(monkeypatch):
    bad = response({})
    bad._content = b"not-json"  # pylint: disable=protected-access
    monkeypatch.setattr(client, "make_request", Mock(return_value=bad))
    with pytest.raises(OpenBBError, match="invalid JSON"):
        client.NativeAPIClient(None).native_query({})
    large = response({})
    large._content = b" " * (16 * 1024**2 + 1)  # pylint: disable=protected-access
    monkeypatch.setattr(client, "make_request", Mock(return_value=large))
    with pytest.raises(OpenBBError, match="16 MiB"):
        client.NativeAPIClient(None).native_query({})


@pytest.mark.parametrize("status", [401, 403, 422, 429, 503, 302])
def test_native_rejection_has_safe_status_and_no_retry(monkeypatch, status):
    request = Mock(
        return_value=response({"private": "/secret/path", "key": "private"}, status, "2")
    )
    monkeypatch.setattr(client, "make_request", request)
    with pytest.raises(client.NativeQueryError) as exc:
        client.NativeAPIClient(None).native_query({})
    assert exc.value.status_code == (502 if status == 302 else status)
    assert exc.value.retry_after == ("2" if status == 429 else None)
    assert "secret" not in str(exc.value) and "private" not in str(exc.value)
    request.assert_called_once()


@pytest.mark.parametrize("status", [422, 429])
def test_installed_core_rest_preserves_native_status_and_retry_after(monkeypatch, status):
    from openbb_core.api.rest_api import app

    request = Mock(return_value=response({"private": "/secret/path"}, status, "2"))
    monkeypatch.setattr(client, "make_request", request)
    with TestClient(app) as api:
        result = api.post("/api/v1/dq_data/native_query", json={"limit": 1})
    assert result.status_code == status
    assert result.json() == {"detail": f"DQ native API request failed (HTTP {status})."}
    assert result.headers.get("Retry-After") == ("2" if status == 429 else None)
    request.assert_called_once()


def test_native_retry_delay_never_forwards_untrusted_header():
    assert client.NativeQueryError(429, "2\r\nX-Key: private").retry_after is None


@pytest.mark.parametrize("error", [RequestException("/private/ca"), OSError("/private/ca")])
def test_native_transport_error_is_bounded_and_not_an_invented_upstream_status(monkeypatch, error):
    request = Mock(side_effect=error)
    monkeypatch.setattr(client, "make_request", request)
    with pytest.raises(client.NativeQueryError) as exc:
        client.NativeAPIClient(None).native_query({})
    assert exc.value.status_code == 502 and exc.value.upstream_status is None
    assert str(exc.value) == "DQ native API is unavailable."
    request.assert_called_once()

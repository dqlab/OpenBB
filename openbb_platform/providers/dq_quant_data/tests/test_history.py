"""Frozen history SDK/REST contracts and a command bridge that cannot forward."""

import json
import subprocess
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from fastapi.testclient import TestClient
from openbb_core.app.model.abstract.error import OpenBBError
from requests import Request, Response

from openbb_dq_quant_data import history, history_transport
from openbb_dq_quant_data.history_transport import HistoryConnection, HistorySSHAdapter
from openbb_dq_quant_data.native import NativeQueryError

RELEASE = "a" * 64
CONFIG = {
    "transport": "ssh-command",
    "ssh_host": "nas.example",
    "ssh_user": "owner",
    "identity_file": "/test/ssh-key",
    "docker_path": "/test/docker",
    "container_id": "b" * 64,
    "releases": {"cash": RELEASE},
}
BODY = {
    "instrument": "ibkr:conid:123",
    "day": "2026-09-10",
    "fields": ["event_epoch_ns", "close", "source_published_ns", "provider"],
    "limit": 2,
}


def page(cursor=None):
    return {
        "data": [
            {
                "event_epoch_ns": "1791936000000000001",
                "close": "12.123456789000",
                "source_published_ns": None,
                "provider": "ibkr",
            }
        ],
        "meta": {
            "release_id": RELEASE,
            "rows": 1,
            "next_cursor": cursor,
            "consistency": "fixed_reconciled_batch_membership",
            "permissions_checked_each_page": True,
            "quality": {"production_eligible": False},
        },
    }


def response(payload, status=200):
    value = Response()
    value.status_code = status
    value._content = json.dumps(payload).encode()  # pylint: disable=protected-access
    value._content_consumed = True  # pylint: disable=protected-access
    return value


@pytest.fixture
def configured(monkeypatch, tmp_path):
    path = tmp_path / "history.json"
    path.write_text(json.dumps(CONFIG))
    monkeypatch.setenv("OPENBB_DQ_HISTORY_CONFIG", str(path))
    request = Mock(return_value=response(page()))
    monkeypatch.setattr(history, "make_request", request)
    return request


def test_disabled_without_explicit_connection(monkeypatch):
    monkeypatch.delenv("OPENBB_DQ_HISTORY_CONFIG", raising=False)
    with pytest.raises(OpenBBError, match="Configure"):
        history.read_history({}, RELEASE, "pages", BODY)


def test_one_page_exact_native_envelope_with_no_old_connection(configured):
    result = history.read_history(
        {"api_url": "https://old.invalid", "api_key": "synthetic"},
        RELEASE,
        "pages",
        BODY,
    )
    assert result == page()
    args, kwargs = configured.call_args
    assert args == (f"http://dq-fixed-history.invalid/v2/history/{RELEASE}/pages",)
    assert kwargs["json"] == BODY and kwargs["allow_redirects"] is False
    assert kwargs["session"].trust_env is False
    assert "headers" not in kwargs
    configured.assert_called_once()


@pytest.mark.parametrize(
    "change",
    [
        {"day": "2026-02-30"},
        {"day": "2026-09-10T00:00:00Z"},
        {"day": None},
        {"limit": True},
        {"limit": 2049},
        {"limit": 0},
        {"as_of": "2026-10-01"},
        {"end": "2026-09-11"},
        {"fields": []},
        {"cursor": "x" * 8193},
    ],
)
def test_unbounded_or_unknown_time_rejected_before_transport(configured, change):
    with pytest.raises(OpenBBError, match="Invalid bounded"):
        history.read_history({}, RELEASE, "pages", {**BODY, **change})
    configured.assert_not_called()


@pytest.mark.parametrize("release", ["c" * 64, "../query", "a" * 63])
def test_release_allowlist_is_not_a_high_watermark(configured, release):
    with pytest.raises(history.HistoryError, match="HTTP 422"):
        history.read_history({}, release, "pages", BODY)
    configured.assert_not_called()


@pytest.mark.parametrize("status", [401, 403, 422, 429, 500, 503, 302])
def test_rejections_never_retry_fallback_or_leak(configured, status):
    configured.return_value = response({"detail": "secret /private/path"}, status)
    with pytest.raises(NativeQueryError) as exc:
        history.read_history({}, RELEASE, "pages", BODY)
    assert exc.value.upstream_status == status
    assert "secret" not in str(exc.value) and "/private" not in str(exc.value)
    configured.assert_called_once()


def test_all_excluded_contract_days_explicit_error(configured):
    configured.return_value = response(
        {"detail": "history_scope_has_excluded_rows_use_legacy"}, 422
    )
    with pytest.raises(
        history.HistoryError, match="history_scope_has_excluded_rows_use_legacy"
    ):
        history.read_history({}, RELEASE, "pages", BODY)
    configured.assert_called_once()


@pytest.mark.parametrize(
    "payload",
    [
        {"data": [], "meta": {**page()["meta"], "rows": 0}},
        {"data": page()["data"], "meta": {**page()["meta"], "release_id": "c" * 64}},
        {"data": page()["data"], "meta": {**page()["meta"], "rows": True}},
        {"data": page()["data"], "meta": {**page()["meta"], "rows": 0}},
        {"data": page()["data"], "meta": {**page()["meta"], "next_cursor": 123}},
        {"data": [{**page()["data"][0], "close": 12.123}], "meta": page()["meta"]},
        {
            "data": [{**page()["data"][0], "event_epoch_ns": 1791936000000000001}],
            "meta": page()["meta"],
        },
    ],
)
def test_false_empty_identity_numeric_projection_and_invalid_envelopes(
    configured, payload
):
    configured.return_value = response(payload)
    with pytest.raises(OpenBBError):
        history.read_history({}, RELEASE, "pages", BODY)


def test_terminal_cursor_page_may_be_empty_but_cannot_repeat_cursor(configured):
    configured.return_value = response(
        {"data": [], "meta": {**page()["meta"], "rows": 0}}
    )
    assert (
        history.read_history({}, RELEASE, "pages", {**BODY, "cursor": "previous"})[
            "data"
        ]
        == []
    )
    configured.return_value = response(page("previous"))
    with pytest.raises(OpenBBError):
        history.read_history({}, RELEASE, "pages", {**BODY, "cursor": "previous"})


def test_adapter_each_page_key_only_stdin_and_no_forwarding(monkeypatch):
    run = Mock(
        return_value=SimpleNamespace(
            returncode=0,
            stdout=json.dumps(
                {"status": 200, "body": json.dumps(page()), "retry_after": None}
            ).encode(),
            stderr=b"",
        )
    )
    monkeypatch.setattr(history_transport.subprocess, "run", run)
    adapter = HistorySSHAdapter(HistoryConnection(**CONFIG), "synthetic-key-only-stdin")
    for cursor in [None, "cursor"]:
        request = Request(
            "POST",
            f"http://dq-fixed-history.invalid/v2/history/{RELEASE}/pages",
            json={**BODY, "cursor": cursor},
        ).prepare()
        assert adapter.send(request).status_code == 200
    assert run.call_count == 2
    for args, kwargs in run.call_args_list:
        argv = args[0]
        assert "synthetic-key-only-stdin" not in " ".join(argv)
        assert not {"-L", "-R", "-D"}.intersection(argv)
        assert "ClearAllForwardings=yes" in argv and "StrictHostKeyChecking=yes" in argv
        assert json.loads(kwargs["input"])["key"] == "synthetic-key-only-stdin"
        assert kwargs["timeout"] == 30


@pytest.mark.parametrize(
    "path,method",
    [
        ("/v2/query", "POST"),
        ("/health/live", "GET"),
        (f"/v2/history/{RELEASE}/pages?target=x", "POST"),
        (f"/v2/history/{'c'*64}/pages", "POST"),
        (f"/v2/history/{RELEASE}/pages", "DELETE"),
    ],
)
def test_bridge_rejects_other_endpoints_without_ssh(monkeypatch, path, method):
    run = Mock()
    monkeypatch.setattr(history_transport.subprocess, "run", run)
    adapter = HistorySSHAdapter(HistoryConnection(**CONFIG), "synthetic")
    with pytest.raises(NativeQueryError):
        adapter.send(
            Request(method, "http://dq-fixed-history.invalid" + path).prepare()
        )
    run.assert_not_called()


@pytest.mark.parametrize(
    "failure",
    [
        subprocess.TimeoutExpired("ssh", 30),
        OSError("secret"),
        SimpleNamespace(
            returncode=1, stdout=b"", stderr=b"administratively prohibited"
        ),
    ],
)
def test_bridge_failure_never_retries_or_exposes_stderr(monkeypatch, failure):
    run = (
        Mock(side_effect=failure)
        if isinstance(failure, Exception)
        else Mock(return_value=failure)
    )
    monkeypatch.setattr(history_transport.subprocess, "run", run)
    adapter = HistorySSHAdapter(HistoryConnection(**CONFIG), "synthetic")
    with pytest.raises(NativeQueryError, match="unavailable"):
        adapter.send(
            Request(
                "GET", f"http://dq-fixed-history.invalid/v2/history/{RELEASE}"
            ).prepare()
        )
    run.assert_called_once()


def test_generated_obb_pages_cursor_and_source_null(configured):
    from openbb import obb

    configured.side_effect = [response(page("cursor")), response(page())]
    first = obb.dq_data.history.pages(release_id=RELEASE, request=BODY)
    second = obb.dq_data.history.pages(
        release_id=RELEASE,
        request={**BODY, "cursor": first.results["meta"]["next_cursor"]},
    )
    assert first.results == page("cursor") and second.results == page()
    assert configured.call_count == 2


@pytest.mark.parametrize("status", [403, 422, 429])
def test_installed_rest_retains_rejection_status(configured, status):
    from openbb_core.api.rest_api import app

    configured.return_value = response({"detail": "not public"}, status)
    configured.return_value.headers["Retry-After"] = "2"
    with TestClient(app) as api:
        result = api.post(
            f"/api/v1/dq_data/history/pages?release_id={RELEASE}", json=BODY
        )
    assert result.status_code == status
    assert result.headers.get("Retry-After") == ("2" if status == 429 else None)
    configured.assert_called_once()

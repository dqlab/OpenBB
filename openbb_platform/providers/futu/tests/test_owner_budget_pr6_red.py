"""Offline regression tests for the PR6 owner lifetime boundary."""

import os
from types import SimpleNamespace

import pytest
from openbb_futu import client
from openbb_futu.coordination import Owner


def test_new_owner_cools_before_first_rest_send(tmp_path, monkeypatch):
    """A new owner cannot send immediately, even without old budget files."""
    monkeypatch.setenv("FUTU_COORDINATION_DIR", str(tmp_path))
    sent = []
    monkeypatch.setattr(client, "make_request", lambda *a, **kw: (sent.append(1) or SimpleNamespace(
        status_code=200, json=lambda: {}
    )))
    with Owner(tmp_path) as owner, pytest.raises(client.FutuError):
        client._post("/offline-test", owner=owner)
    assert sent == []


@pytest.mark.skipif(not hasattr(os, "fork"), reason="requires fork")
def test_forked_child_cannot_borrow_parent_owner(tmp_path):
    """A copied in-memory budget must never authorize a child process."""
    with Owner(tmp_path) as owner:
        read_fd, write_fd = os.pipe()
        pid = os.fork()
        if pid == 0:
            os.close(read_fd)
            try:
                with owner.borrow(tmp_path):
                    result = b"allowed"
            except Exception:
                result = b"rejected"
            os.write(write_fd, result)
            os._exit(0)
        os.close(write_fd)
        result = os.read(read_fd, 32)
        os.close(read_fd)
        _, status = os.waitpid(pid, 0)
        assert os.waitstatus_to_exitcode(status) == 0
        assert result == b"rejected"

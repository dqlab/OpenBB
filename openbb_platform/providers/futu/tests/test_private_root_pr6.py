"""Private output roots are checked at the actual descriptor boundary."""

import os

import pytest
from openbb_futu.root_handle import RootHandle


def test_private_root_publish_preserves_evidence(tmp_path):
    """Existing evidence is not replaced."""
    with RootHandle(tmp_path) as root:
        root.publish("evidence", b"first")
        with pytest.raises(FileExistsError):
            root.publish("evidence", b"second")
    assert (tmp_path / "evidence").read_bytes() == b"first"


@pytest.mark.parametrize("mode", [0o720, 0o702])
def test_writable_root_rejected(tmp_path, mode):
    """Reject roots writable by another user."""
    tmp_path.chmod(mode)
    with pytest.raises(PermissionError):
        RootHandle(tmp_path)


def test_symlink_and_fifo_rejected(tmp_path):
    """Reject symlink and FIFO roots."""
    target = tmp_path / "target"
    target.mkdir()
    (tmp_path / "link").symlink_to(target, target_is_directory=True)
    os.mkfifo(tmp_path / "fifo")
    for path in (tmp_path / "link", tmp_path / "fifo"):
        with pytest.raises(OSError):
            RootHandle(path)


def test_discovery_rejects_insecure_root_before_request(tmp_path, monkeypatch):
    """Writable evidence roots never receive a request or response."""
    from openbb_futu.discovery import discover

    root = tmp_path / "discovery"
    root.mkdir(mode=0o777)
    root.chmod(0o777)
    monkeypatch.setattr("openbb_futu.discovery.make_request", lambda *a, **k: pytest.fail("request sent"))
    with pytest.raises(PermissionError):
        discover([{"asset": "option", "symbol": "US.AAPL"}], "synthetic", root)
    assert not list(root.iterdir())


def test_rotation_rejects_insecure_parent_before_refresh(tmp_path, monkeypatch):
    """A writable token parent is rejected before touching token state."""
    from openbb_futu.collector import TokenStore

    root = tmp_path / "tokens"
    root.mkdir()
    root.chmod(0o777)
    token = root / "token"
    token.write_bytes(b"synthetic")
    monkeypatch.setattr("openbb_futu.collector.refresh_tokens", lambda *a, **k: pytest.fail("refresh sent"))
    with pytest.raises(PermissionError):
        TokenStore(token).rotate(None)
    assert token.read_bytes() == b"synthetic"
    assert list(root.iterdir()) == [token]

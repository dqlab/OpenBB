"""Discovery must not write through an unprotected evidence pathname."""

from functools import partial

import pytest
from dq_live_market_data_collector.tick_file import TickWriter
from openbb_futu.archive import recover_open_files
from openbb_futu.collector import TokenStore
from openbb_futu.directory_chain import DirectoryChain
from openbb_futu.discovery import discover
from openbb_futu.root_handle import RootHandle


def test_discovery_does_not_create_through_symlink_ancestor(tmp_path, monkeypatch):
    """Creating a missing output must not follow an intermediate symlink."""
    outside = tmp_path / "outside"
    outside.mkdir()
    (tmp_path / "link").symlink_to(outside, target_is_directory=True)
    monkeypatch.setattr("openbb_futu.discovery.make_request", lambda *a, **k: pytest.fail("network request"))
    with pytest.raises(OSError):
        discover([{"asset": "option", "symbol": "US.AAPL"}], "synthetic", tmp_path / "link" / "new")
    assert list(outside.iterdir()) == []


def test_discovery_rejects_symlink_root_before_sending_or_writing(tmp_path):
    """A symlinked evidence root cannot receive data."""
    outside = tmp_path / "outside"
    outside.mkdir()
    output = tmp_path / "output"
    output.symlink_to(outside, target_is_directory=True)
    with pytest.raises(OSError):
        discover([{"asset": "option", "symbol": "US.AAPL"}], "synthetic", output)
    assert list(outside.iterdir()) == []


def test_recovery_and_report_preserve_evidence(tmp_path):
    """Recovery preserves raw evidence and publication does not overwrite."""
    source = tmp_path / "source_id=futu" / "recv_date=2026-01-01" / "session=example"
    source.mkdir(parents=True)
    segment = source / "part-000001.jsonl.open"
    segment.write_bytes(b"original evidence\n")
    recovered = recover_open_files(tmp_path)
    assert recovered[0]["error"] == "source_stability_unknown"
    assert segment.read_bytes() == b"original evidence\n"
    with RootHandle(tmp_path) as root:
        root.publish("report.json", b"data")
        with pytest.raises(FileExistsError):
            root.publish("report.json", b"replacement")
    assert (tmp_path / "report.json").read_bytes() == b"data"


def test_token_rotation_refuses_before_touching_token(tmp_path):
    """Unprotected rotation does not create or replace token evidence."""
    target = tmp_path / "target"
    target.write_bytes(b"original")
    target.chmod(0o600)
    token = tmp_path / "token"
    token.symlink_to(target)
    with pytest.raises(OSError):
        TokenStore(token).rotate(None)
    assert target.read_bytes() == b"original"
    assert not (tmp_path / "token.refresh-pending").exists()


def test_writer_move_after_check_reaches_displaced_file(tmp_path):
    """A real write can reach a moved inode before the next identity check."""
    root_path = tmp_path / "root"
    root_path.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    with RootHandle(root_path) as root:
        writer = TickWriter(root_path, "futu", directory_chain_factory=partial(DirectoryChain, root))
        first = {"local_seq": 1, "received_at_utc": "2026-01-01T00:00:00Z"}
        writer.append(first)
        original_check = writer._check_anchored_file
        moved = False

        def move_after_check():
            nonlocal moved
            original_check()
            if not moved:
                (root_path / "source_id=futu").rename(outside / "displaced")
                moved = True

        writer._check_anchored_file = move_after_check
        with pytest.raises(Exception):
            writer.append({**first, "local_seq": 2})
        writer.stream.close()
        writer._chain.close()
    raw = next(outside.rglob("*.jsonl.open")).read_bytes()
    assert b'"local_seq":2' in raw

"""Publication stays anchored and never replaces evidence."""
import os
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

import pytest
from openbb_futu.archive import recover_open_files
from openbb_futu.coverage import write_json
from openbb_futu.read_budget import EvidenceChanged


def test_recovery_root_swap(tmp_path):
    """Check anchored publication behavior."""
    root = tmp_path / "root"
    source = root / "source_id=futu/recv_date=2026-01-01/session=s"
    source.mkdir(parents=True)
    (source / "x.jsonl.open").write_bytes(b'{"local_seq":1,"collector_run_id":"r"}\n')
    original = os.mkdir
    def swap(name, *args, **kwargs):
        result = original(name, *args, **kwargs)
        if name == "futu-recovery":
            root.rename(tmp_path / "moved")
            root.mkdir()
        return result
    with patch("os.mkdir", side_effect=swap), pytest.raises(EvidenceChanged):
        recover_open_files(root)
    assert not list(root.iterdir())


def test_ancestor_swap_fails(tmp_path):
    """Check anchored publication behavior."""
    ancestor = tmp_path / "ancestor"
    root = ancestor / "root"
    root.mkdir(parents=True)
    from openbb_futu.root_handle import RootHandle
    with RootHandle(root) as handle:
        ancestor.rename(tmp_path / "moved")
        root.mkdir(parents=True)
        with pytest.raises(EvidenceChanged):
            write_json(root / "report.json", {}, root_handle=handle)
    assert not list(root.iterdir())


def test_temp_collision_and_concurrent_publish(tmp_path):
    """Check anchored publication behavior."""
    (tmp_path / "report.json.tmp").write_text("evidence")
    def publish(value):
        try:
            write_json(tmp_path / "report.json", {"value": value})
            return True
        except FileExistsError:
            return False
    with ThreadPoolExecutor(max_workers=2) as pool:
        assert sorted(pool.map(publish, range(2))) == [False, True]
    assert (tmp_path / "report.json.tmp").read_text() == "evidence"


def test_random_temp_collision_preserves_existing(tmp_path):
    """Check anchored publication behavior."""
    collision = tmp_path / (".tmp-" + "a" * 32)
    collision.write_text("preserve")
    with patch("openbb_futu.root_handle.secrets.token_hex", side_effect=["a" * 32, "b" * 32]):
        write_json(tmp_path / "report.json", {"ok": True})
    assert collision.read_text() == "preserve"


def test_recovery_directory_swap(tmp_path):
    """Check anchored publication behavior."""
    root = tmp_path / "root"
    source = root / "source_id=futu/recv_date=2026-01-01/session=s"
    source.mkdir(parents=True)
    (source / "x.jsonl.open").write_bytes(b'{"local_seq":1,"collector_run_id":"r"}\n')
    original = os.open
    def swap(name, *args, **kwargs):
        fd = original(name, *args, **kwargs)
        if name == "futu-recovery" and kwargs.get("dir_fd") is not None:
            (root / "futu-recovery").rename(root / "moved")
            (root / "futu-recovery").mkdir()
        return fd
    with patch("os.open", side_effect=swap), pytest.raises((ValueError, OSError)):
        recover_open_files(root)
    assert not list((root / "futu-recovery").iterdir())


def test_collision_limit_fails_closed(tmp_path):
    """Repeated random names do not loop or delete someone else's file."""
    from openbb_futu.root_handle import TEMP_ATTEMPTS
    collision = tmp_path / (".tmp-" + "a" * 32)
    collision.write_bytes(b"other evidence")
    with (patch("openbb_futu.root_handle.secrets.token_hex", return_value="a" * 32) as token,
          pytest.raises(FileExistsError, match="collision limit")):
        write_json(tmp_path / "report.json", {})
    assert token.call_count == TEMP_ATTEMPTS
    assert collision.read_bytes() == b"other evidence"
    assert not (tmp_path / "report.json").exists()

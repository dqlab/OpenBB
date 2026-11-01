"""Offline Futu provider tests."""

import json
import tempfile
import unittest
from pathlib import Path

from openbb_futu.archive import Archive


class ArchiveTests(unittest.TestCase):
    """Handle ArchiveTests."""

    def test_drain_rotation_and_manifest(self):
        """Test drain rotation and manifest."""
        with tempfile.TemporaryDirectory() as tmp:
            archive = Archive(Path(tmp), run_id="synthetic", max_bytes=1)
            raw = (
                '{"type":"TICKER","data":{"ticker_list":['
                '{"sequence":9223372036854775807},{"sequence":9223372036854775807}]}}'
            )
            self.assertTrue(archive.accept(raw, "connection-1"))
            self.assertTrue(archive.accept(raw, "connection-1"))
            result = archive.close()
            self.assertIsNone(result["error"])
            self.assertEqual((result["received_frames"], result["durable_seq"]), (2, 2))
            self.assertEqual(len(result["files"]), 2)
            self.assertEqual(
                [json.loads(Path(f["file"]).read_text())["raw_payload"] for f in result["files"]], [raw, raw]
            )
            self.assertTrue(Path(result["manifest"]).exists())

    def test_rejected_control_is_incomplete(self):
        """Test rejected control is incomplete."""
        with tempfile.TemporaryDirectory() as tmp:
            archive = Archive(Path(tmp), run_id="synthetic")
            self.assertFalse(archive.accept('{"action":"auth"}', "c"))
            result = archive.close()
            self.assertEqual(result["rejected_frames"], 1)
            self.assertIsNotNone(result["error"])


def test_recovery_preflight_budget_preserves_evidence(tmp_path):
    """An exhausted scan cannot alter sources or earlier recovery evidence."""
    import pytest
    from openbb_futu.archive import recover_open_files
    from openbb_futu.read_budget import Budget, BudgetExceeded

    session = tmp_path / "source_id=futu" / "recv_date=2026-01-01" / "session=abc"
    session.mkdir(parents=True)
    segment = session / "part.jsonl.open"
    segment.write_bytes(b'{"local_seq":1,"collector_run_id":"a"}\npartial')
    saved = tmp_path / "futu-recovery" / "old" / "tail.bin"
    saved.parent.mkdir(parents=True)
    saved.write_bytes(b"original")
    with pytest.raises(BudgetExceeded):
        recover_open_files(tmp_path, budget=Budget(bytes_limit=5))
    assert segment.read_bytes().endswith(b"partial")
    assert saved.read_bytes() == b"original"
    assert list(saved.parent.parent.iterdir()) == [saved.parent]
    reports = recover_open_files(tmp_path)
    assert len(reports) == 1
    evidence = Path(reports[0]["evidence_dir"])
    assert (evidence / "tail.bin").read_bytes() == b"partial"
    assert segment.read_bytes().endswith(b"partial")


def test_recovery_rejects_special_segment(tmp_path):
    """Special files are rejected without consuming or modifying them."""
    import os

    import pytest
    from openbb_futu.archive import recover_open_files

    directory = tmp_path / "source_id=futu" / "recv_date=2026-01-01" / "session=a"
    directory.mkdir(parents=True)
    os.mkfifo(directory / "part.jsonl.open")
    with pytest.raises(ValueError):
        recover_open_files(tmp_path)
    assert not (tmp_path / "futu-recovery").exists()


def test_recovery_rejects_replaced_shared_root(tmp_path):
    """A collector's held root must not silently switch to a replacement."""
    import pytest
    from openbb_futu.archive import recover_open_files
    from openbb_futu.read_budget import EvidenceChanged
    from openbb_futu.root_handle import RootHandle

    root = tmp_path / "root"
    root.mkdir()
    with RootHandle(root) as held:
        root.rename(tmp_path / "original")
        root.mkdir()
        with pytest.raises(EvidenceChanged):
            recover_open_files(root, root_handle=held)
    assert not list(root.iterdir())
    assert not list((tmp_path / "original").iterdir())


def test_recovery_repeated_and_complete_archive(tmp_path):
    """Complete historic sessions are skipped and open originals remain intact."""
    from openbb_futu.archive import recover_open_files

    archive = Archive(tmp_path, run_id="previous")
    assert archive.accept('{"type":"QUOTE","symbol":"US.A","data":{}}', "c")
    assert archive.close()["error"] is None
    assert recover_open_files(tmp_path) == []
    session = tmp_path / "source_id=futu" / "recv_date=2026-01-01" / "session=unfinished"
    session.mkdir(parents=True)
    source = session / "part.jsonl.open"
    data = b'{"local_seq":1,"collector_run_id":"new"}\npartial'
    source.write_bytes(data)
    first = recover_open_files(tmp_path)
    second = recover_open_files(tmp_path)
    assert len(first) == len(second) == 1
    assert first[0]["evidence_dir"] != second[0]["evidence_dir"]
    assert source.read_bytes() == data
    assert Path(first[0]["evidence_dir"], "tail.bin").read_bytes() == b"partial"
    assert Path(second[0]["evidence_dir"], "tail.bin").read_bytes() == b"partial"

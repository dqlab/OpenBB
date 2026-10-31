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

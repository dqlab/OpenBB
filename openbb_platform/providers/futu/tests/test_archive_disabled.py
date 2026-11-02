"""The WIP archive must refuse unsafe pathname-based storage."""

import asyncio
from unittest.mock import patch

from openbb_futu.archive import Archive
from openbb_futu.collector import Collector


def test_archive_refuses_symlink_without_writing(tmp_path):
    """Refuse an archive even when its source name points outside the root."""
    root, outside = tmp_path / "root", tmp_path / "outside"
    root.mkdir()
    outside.mkdir()
    (root / "source_id=futu").symlink_to(outside, target_is_directory=True)
    archive = Archive(root, run_id="a" * 32)
    assert archive.accept('{"type":"QUOTE","symbol":"US.A","data":{}}', "c")
    result = archive.close()
    assert result["error"].startswith("writer failure:")
    assert not list(outside.iterdir())


def test_collector_fails_before_connecting(tmp_path):
    """Do not start a network session through a symlink ancestor."""
    outside = tmp_path / "outside"
    outside.mkdir()
    link = tmp_path / "link"
    link.symlink_to(outside, target_is_directory=True)
    collector = Collector({}, None, link / "output")
    with patch("openbb_futu.collector.coordination_directory", return_value=tmp_path / "locks"):
        assert asyncio.run(collector.run()) == 1
    assert not list(tmp_path.rglob("*.jsonl.open"))
    assert not list(outside.iterdir())


def test_event_path_only_accepts_generated_root_relative_identifiers(tmp_path):
    """Only generated identifiers under the root are eligible for events."""
    collector = Collector({}, None, tmp_path)
    name = "a" * 32
    assert collector._event_path(tmp_path / "futu-recovery" / name, "futu-recovery", "") == f"futu-recovery/{name}"
    assert collector._event_path(
        tmp_path / "futu-manifests" / f"{name}.json", "futu-manifests", r"\.json"
    ) == f"futu-manifests/{name}.json"
    assert collector._event_path(tmp_path.parent / "private" / name, "futu-recovery", "") is None
    assert collector._event_path(tmp_path / "futu-recovery" / "private", "futu-recovery", "") is None


def test_public_collector_cli_fails_without_disclosing_secrets(tmp_path):
    """The public entry rejects a symlink ancestor without disclosing inputs."""
    import json
    import os
    import subprocess
    import sys

    config = tmp_path / "config.json"
    config.write_text(json.dumps({"channels": {"quote": ["US.AAPL"]}}))
    token = tmp_path / "private-token.json"
    secrets = ("synthetic-access-secret", "synthetic-refresh-secret", "synthetic-client-secret")
    token.write_text(json.dumps(dict(zip(("access_token", "refresh_token", "client_id"), secrets))))
    token.chmod(0o600)
    outside = tmp_path / "outside"
    outside.mkdir()
    link = tmp_path / "link"
    link.symlink_to(outside, target_is_directory=True)
    output = link / "private-output"
    result = subprocess.run(  # noqa: S603 -- fixed local CLI command
        [sys.executable, "-m", "openbb_futu.collector", "--config", str(config),
         "--token-file", str(token), "--output", str(output)],
        env=dict(os.environ, FUTU_COORDINATION_DIR=str(tmp_path / "coord")),
        capture_output=True, text=True, timeout=15, check=False,
    )
    assert result.returncode == 1
    combined = result.stdout + result.stderr
    assert "collector failed: collector_failure" in combined
    assert all(secret not in combined for secret in secrets)
    assert str(tmp_path) not in combined
    assert not list(outside.iterdir())

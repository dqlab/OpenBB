"""Installed command behavior and evidence integrity."""

import json
import os
import subprocess
import sys
from pathlib import Path


def invoke(*args):
    """Run the offline CLI in a subprocess."""
    return subprocess.run(  # noqa: S603
        [sys.executable, "-m", "openbb_futu.evidence_cli", *map(str, args)],
        capture_output=True,
        text=True,
        check=False,
        env=os.environ.copy(),
    )


def test_replay_verify_and_corruption(tmp_path):
    """Repeated frames, multiple ticks, unknown fields survive and tampering fails."""
    tick = '{"type":"TICKER","symbol":"US.A","data":{"ticker_list":[{"sequence":1},{"sequence":2}],"unknown":42}}'
    bar = '{"type":"KLINE","data":{"period":"1m","adjust":"none","unknown":true}}'
    frames = [tick, tick, bar, bar, '{"type":"FUTURE_TYPE","data":{"extra":1}}']
    source = tmp_path / "input"
    source.write_text("\n".join(frames) + "\n")
    result = invoke("replay", "--input", source, "--output", tmp_path / "archive")
    assert result.returncode == 0, result.stderr
    manifest = json.loads(result.stdout)
    path = Path(manifest["files"][0]["file"])
    assert [json.loads(line)["raw_payload"] for line in path.read_text().splitlines()] == frames
    verification = invoke("verify", "--manifest", manifest["manifest"])
    assert verification.returncode == 0
    assert json.loads(verification.stdout)["records"] == 5
    before = path.read_bytes()
    path.write_bytes(before.replace(b"unknown", b"UNknown", 1))
    assert invoke("verify", "--manifest", manifest["manifest"]).returncode != 0
    path.write_bytes(before)
    manifest_path = Path(manifest["manifest"])
    old = json.loads(manifest_path.read_text())
    old.pop("uncertain_loss")  # manifest written before this version
    manifest_path.write_text(json.dumps(old))
    assert invoke("verify", "--manifest", manifest_path).returncode == 0
    old["durable_seq"] -= 1
    manifest_path.write_text(json.dumps(old))
    assert invoke("verify", "--manifest", manifest_path).returncode != 0


def test_replay_oversize_fails(tmp_path):
    """An over-budget input is recorded as uncertain rather than truncated."""
    source = tmp_path / "large"
    source.write_bytes(b"x" * (1024 * 1024 + 2) + b"\n")
    result = invoke("replay", "--input", source, "--output", tmp_path / "archive")
    assert result.returncode != 0
    assert json.loads(result.stdout)["uncertain_loss"] is True


def test_archive_resource_budgets(tmp_path, monkeypatch):
    """Byte, secret and segment exhaustion fail without silent truncation."""
    import openbb_futu.archive as module

    raw = '{"type":"QUOTE","data":{"v":1}}'
    monkeypatch.setattr(module, "MAX_QUEUE_BYTES", 1)
    archive = module.Archive(tmp_path / "bytes", run_id="r")
    assert not archive.accept(raw, "c")
    assert archive.close()["uncertain_loss"]
    monkeypatch.setattr(module, "MAX_SECRETS", 1)
    archive = module.Archive(tmp_path / "secrets", run_id="r")
    archive.add_secrets(["first"])
    import pytest

    with pytest.raises(ValueError):
        archive.add_secrets(["second"])
    assert archive.close()["uncertain_loss"]
    monkeypatch.setattr(module, "MAX_QUEUE_BYTES", 1024 * 1024)
    monkeypatch.setattr(module, "MAX_SEGMENTS", 1)
    archive = module.Archive(tmp_path / "segments", run_id="r", max_bytes=1)
    assert archive.accept(raw, "c")
    assert archive.accept(raw, "c")
    result = archive.close()
    assert result["error"] and result["uncertain_loss"]


def test_verify_rejects_boolean_sequence_with_consistent_checksum(tmp_path):
    """JSON booleans must not pass as integer sequence numbers."""
    import hashlib

    source = tmp_path / "input"
    source.write_text('{"type":"QUOTE","data":{}}\n')
    replay = invoke("replay", "--input", source, "--output", tmp_path / "archive")
    assert replay.returncode == 0, replay.stderr
    manifest = json.loads(replay.stdout)
    manifest_path = Path(manifest["manifest"])
    segment = Path(manifest["files"][0]["file"])
    record = json.loads(segment.read_text())
    record["local_seq"] = True
    payload = (json.dumps(record) + "\n").encode()
    segment.write_bytes(payload)
    entry = manifest["files"][0]
    entry.update(sha256=hashlib.sha256(payload).hexdigest(), bytes=len(payload), valid_bytes=len(payload))
    manifest_path.write_text(json.dumps(manifest))
    assert invoke("verify", "--manifest", manifest_path).returncode != 0

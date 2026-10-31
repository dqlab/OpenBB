"""Offline evidence, bounded discovery and interrupted segment checks."""

import json
from pathlib import Path
from unittest.mock import patch

from openbb_futu.archive import recover_open_files
from openbb_futu.contracts import plan
from openbb_futu.discovery import discover


class Response:
    """Synthetic HTTP response with an exact error body."""

    status_code = 403

    def iter_content(self, _size):
        """Handle iter content."""
        yield b'{"ret_code":-9,"detail":"denied"}'

    def close(self):
        """Handle close."""
        pass


def test_discovery_captures_error_and_leaves_universe_unknown(tmp_path, monkeypatch):
    """Test discovery captures error and leaves universe unknown."""
    monkeypatch.setenv("FUTU_COORDINATION_DIR", str(tmp_path / "coord"))
    with patch("openbb_futu.discovery.make_request", return_value=Response()) as request:
        result = discover([{"asset": "option", "symbol": "HK.00700"}], "synthetic-secret", tmp_path / "raw")
    item = result["requests"][0]
    assert request.call_args.args[0].endswith("/HK.00700/option-chain")
    assert item["error"] == "provider_failure"
    assert Path(item["raw_file"]).read_bytes() == b'{"ret_code":-9,"detail":"denied"}'
    assert set(result["target_status"].values()) == {"unknown"}
    assert "synthetic-secret" not in json.dumps(result)


def test_interrupted_tail_preserves_original_and_gap(tmp_path):
    """Test interrupted tail preserves original and gap."""
    path = tmp_path / "source_id=futu/recv_date=2026-01-01/session=dead/part-000001.jsonl.open"
    path.parent.mkdir(parents=True)
    original = b'{"local_seq":1}\n{"local_seq":2'
    path.write_bytes(original)
    recovery = recover_open_files(tmp_path)[0]
    directory = Path(recovery["evidence_dir"])
    assert path.read_bytes() == original
    assert (directory / "valid-prefix.jsonl").read_bytes() == b'{"local_seq":1}\n'
    assert (directory / "tail.bin").read_bytes() == b'{"local_seq":2'
    assert recovery["gap"] == "unresolved_after_process_interruption"


def test_all_asset_channel_targets_remain_unknown():
    """Test all asset channel targets remain unknown."""
    report = plan([], budget=1)
    assert len(report["by_asset_channel"]) == 4
    assert all(len(channels) == 4 for channels in report["by_asset_channel"].values())
    assert all(
        cell["target_status"] == "unknown"
        for channels in report["by_asset_channel"].values()
        for cell in channels.values()
    )


def test_disk_failure_and_queue_full_are_reported(tmp_path, monkeypatch):
    """Test disk failure and queue full are reported."""
    import threading

    from openbb_futu.archive import Archive, TickWriter

    entered, release = threading.Event(), threading.Event()

    def blocked_append(_writer, _record):
        """Handle blocked append."""
        entered.set()
        assert release.wait(5)
        raise OSError(28, "simulated disk full")

    monkeypatch.setattr(TickWriter, "append", blocked_append)
    archive = Archive(tmp_path, run_id="fault", capacity=1)
    raw = '{"type":"UNKNOWN","data":{}}'
    assert archive.accept(raw, "c")
    assert entered.wait(2)
    assert archive.accept(raw, "c")
    assert not archive.accept(raw, "c")
    release.set()
    result = archive.close()
    assert result["error"] == "writer failure: OSError"
    assert result["rejected_frames"] == 1
    assert result["queue_depth"] == 1


def test_sealed_segment_sigkill_before_manifest(tmp_path):
    """Kill a child after the real rename and verify run-scoped orphan evidence."""
    import signal
    import subprocess
    import sys

    script = """
import os
import sys
from pathlib import Path
from openbb_futu.archive import Archive
real_rename = os.rename
def handshake(source, target, *args, **kwargs):
    real_rename(source, target, *args, **kwargs)
    print("sealed", flush=True)
    sys.stdin.readline()
os.rename = handshake
archive = Archive(Path(sys.argv[1]), run_id="killed-run")
assert archive.accept('{"type":"QUOTE","symbol":"US.AAPL","data":{}}', "conn")
archive.close()
"""
    child = subprocess.Popen(  # noqa: S603
        [sys.executable, "-c", script, str(tmp_path)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        import select

        ready, _, _ = select.select([child.stdout], [], [], 10)
        assert ready and child.stdout.readline().strip() == "sealed"
        child.send_signal(signal.SIGKILL)
        child.wait(timeout=10)
        assert child.returncode == -signal.SIGKILL
        sealed = list(tmp_path.glob("source_id=futu/recv_date=*/session=*/*.jsonl"))
        assert len(sealed) == 1
        before = sealed[0].read_bytes()
        assert not list((tmp_path / "futu-manifests").glob("*.json"))
        report = recover_open_files(tmp_path)[0]
        assert report["run_ids"] == ["killed-run"]
        assert report["gap"] == "unresolved_after_process_interruption"
        assert sealed[0].read_bytes() == before
        assert Path(report["evidence_dir"], "recovery.json").exists()
    finally:
        if child.poll() is None:
            child.kill()
            child.wait(timeout=10)
        for stream in (child.stdin, child.stdout, child.stderr):
            if stream:
                stream.close()


def test_manifest_for_different_run_does_not_hide_orphan(tmp_path):
    """A session manifest cannot account for another run's sealed records."""
    path = tmp_path / "source_id=futu/recv_date=2026-01-01/session=s/part-000001.jsonl"
    path.parent.mkdir(parents=True)
    path.write_text('{"local_seq":1,"collector_run_id":"original"}\n')
    manifests = tmp_path / "futu-manifests"
    manifests.mkdir()
    (manifests / "s.json").write_text(json.dumps({"session_id": "s", "run_id": "other", "files": [{"file": str(path)}]}))
    assert recover_open_files(tmp_path)[0]["run_ids"] == ["original"]


def test_discovery_rejects_sensitive_and_uninspectable_bodies(tmp_path, monkeypatch):
    """Never persist sensitive or malformed REST bodies, including failure bodies."""
    monkeypatch.setenv("FUTU_COORDINATION_DIR", str(tmp_path / "coord"))
    for body in (b'{"password":"hidden"}', b'{"detail":"Bearer abc"}', b'{"detail":"refresh-value"}', b"not json"):

        class SensitiveResponse(Response):
            def iter_content(self, _size):
                yield body

        with patch("openbb_futu.discovery.make_request", return_value=SensitiveResponse()):
            result = discover(
                [{"asset": "option", "symbol": "HK.00700"}],
                "access-value",
                tmp_path / "raw",
                secrets=("refresh-value",),
            )
        assert "raw_file" not in result["requests"][0]
    assert not list((tmp_path / "raw").glob("**/*.bin"))


def test_coverage_read_only(tmp_path):
    """Reconstruction after interruption does not publish or manufacture success."""
    from openbb_futu.coverage import prepare, report

    run_id = "a" * 32
    plan, _ = prepare(
        {
            "dimensions": [{"asset": "equity", "symbol": "US.AAPL", "channel": "ticker"}],
            "subscription_budget": 1,
        }
    )
    (tmp_path / f"futu-plan-{run_id}.json").write_text(json.dumps(plan))
    (tmp_path / f"futu-events-{run_id}.jsonl").write_text('{"event":"stopped","status":"stopped"}\n')
    before = set(tmp_path.iterdir())
    result = report(tmp_path, run_id, publish=False)
    assert result["run_status"] == "interrupted"
    assert result["dimensions"][0]["durable_observations"] == []
    assert set(tmp_path.iterdir()) == before


def test_discovery_plan_checks_raw_and_preserves_unknown(tmp_path, monkeypatch):
    """A seed result references verified bytes without claiming market entitlement."""
    import hashlib

    from openbb_futu.coverage import prepare, write_json

    class Success(Response):
        status_code = 200

        def iter_content(self, _size):
            yield b'{"ret_code":0,"data":{"option_chain":[{"code":"US.OPT"}]}}'

    monkeypatch.setenv("FUTU_COORDINATION_DIR", str(tmp_path / "coord"))
    with patch("openbb_futu.discovery.make_request", return_value=Success()):
        discovery = discover([{"asset": "option", "symbol": "US.SPY"}], "secret", tmp_path / "raw")
    path = Path(discovery["evidence_dir"]) / "discovery.json"
    write_json(path, discovery)
    config = {
        "dimensions": [{"asset": "option", "symbol": "US.OPT", "channel": "ticker"}],
        "subscription_budget": 1,
        "discovery_report": str(path),
    }
    planned, channels = prepare(config)
    assert channels == {"ticker": ["US.OPT"]}
    assert planned["selected"][0]["discovery_refs"] == [0]
    assert planned["discovery"]["report_sha256"] == hashlib.sha256(path.read_bytes()).hexdigest()
    assert planned["discovery"]["universe"] == planned["discovery"]["entitlement"] == "unknown"
    Path(discovery["requests"][0]["raw_file"]).write_bytes(b"{}")
    import pytest

    with pytest.raises(ValueError, match="checksum"):
        prepare(config)

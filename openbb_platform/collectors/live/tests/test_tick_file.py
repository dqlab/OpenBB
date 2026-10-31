"""Offline checks for raw tick file primitives."""

import json
import sys

import pytest

from dq_live_market_data_collector.tick_file import TickWriter, encode_value, inspect_file


def test_values_and_duplicate_callbacks(tmp_path):
    payload = {
        "LastPrice": sys.float_info.max,
        "Volume": 0,
        "empty": "",
        "x": float("nan"),
        "y": float("inf"),
        "bytes": b"\x00\xff",
    }
    writer = TickWriter(tmp_path, "replay", max_bytes=1)
    for seq in (1, 2):
        writer.append(
            {
                "local_seq": seq,
                "received_at_utc": "2026-10-30T00:00:00Z",
                "raw_payload": encode_value(payload),
            }
        )
    writer.close()
    paths = sorted(tmp_path.rglob("*.jsonl"))
    assert len(paths) == 2
    records = [json.loads(path.read_text()) for path in paths]
    assert [r["local_seq"] for r in records] == [1, 2]
    assert records[0]["raw_payload"]["LastPrice"] == sys.float_info.max
    assert records[0]["raw_payload"]["x"] == {"__float64__": "nan"}
    assert records[0]["raw_payload"]["y"] == {"__float64__": "+inf"}
    assert records[0]["raw_payload"]["bytes"] == {"__bytes_base64__": "AP8="}
    assert writer.durable == 2


def test_tail_is_preserved(tmp_path):
    path = tmp_path / "old.jsonl.open"
    original = b'{"local_seq":1}\n{"a":'
    path.write_bytes(original)
    result = inspect_file(path)
    assert result["records"] == 1
    assert result["valid_bytes"] == 16
    assert result["unread_bytes"] == len(original) - 16
    assert result["error"] == "incomplete_line"
    assert path.read_bytes() == original


def test_session_isolation_and_path_validation(tmp_path):
    one, two = TickWriter(tmp_path, "replay"), TickWriter(tmp_path, "replay")
    assert one.session_id != two.session_id
    with pytest.raises(ValueError):
        TickWriter(tmp_path, "../escape")
    with pytest.raises(TypeError):
        encode_value(object())


def test_ingress_copies_before_return(tmp_path):
    from dq_live_market_data_collector.tick_file import TickIngress

    writer = TickWriter(tmp_path, "replay")
    ingress = TickIngress(writer, profile="replay", environment="synthetic")
    payload = {"InstrumentID": "au2612", "Volume": 17, "nested": [1]}
    assert ingress.callback(payload)
    assert ingress.callback(payload)
    payload["nested"][0] = 9
    assert ingress.close()
    rows = [json.loads(line) for line in next(tmp_path.rglob("*.jsonl")).read_text().splitlines()]
    assert [r["raw_payload"]["nested"] for r in rows] == [[1], [1]]
    assert ingress.status()["durable_seq"] == 2


def test_disk_failure_is_visible(tmp_path, capsys):
    from dq_live_market_data_collector.tick_file import TickIngress

    writer = TickWriter(tmp_path, "replay")

    def fail(_record):
        raise OSError("disk full")

    writer.append = fail
    ingress = TickIngress(writer, profile="replay", environment="synthetic")
    assert ingress.callback({"InstrumentID": "x"})
    assert not ingress.close()
    assert "disk full" in ingress.status()["error"]
    assert "disk full" in capsys.readouterr().err


def test_offline_cli_replay_and_inspect(tmp_path, capsys):
    from pathlib import Path

    from dq_live_market_data_collector.tick_cli import main

    fixture = Path(__file__).parent / "fixtures" / "ticks_synthetic.jsonl"
    assert main(["replay", str(fixture), "--root", str(tmp_path)]) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["status"]["durable_seq"] == 3
    assert main(["verify", str(tmp_path)]) == 0
    assert json.loads(capsys.readouterr().out)[0]["records"] == 3


def test_inspection_reports_sequence_gap_without_modifying_evidence(tmp_path):
    path = tmp_path / "part.jsonl.open"
    original = b'{"local_seq":1}\n{"local_seq":3}\n'
    path.write_bytes(original)
    result = inspect_file(path)
    assert result["error"] == "noncontiguous_sequence"
    assert result["last_seq"] == 1
    assert path.read_bytes() == original


def test_queue_full_stops_without_overwriting(tmp_path):
    import threading

    from dq_live_market_data_collector.tick_file import TickIngress

    writer = TickWriter(tmp_path, "replay")
    entered, release = threading.Event(), threading.Event()
    append = writer.append

    def slow(record):
        entered.set()
        assert release.wait(5)
        append(record)

    writer.append = slow
    ingress = TickIngress(writer, profile="replay", environment="synthetic", capacity=1)
    assert ingress.callback({"InstrumentID": "a"})
    assert entered.wait(5)
    assert ingress.callback({"InstrumentID": "b"})
    assert not ingress.callback({"InstrumentID": "c"})
    release.set()
    assert not ingress.close()
    assert ingress.status()["rejected"] == 1
    assert ingress.status()["durable_seq"] == 2


def test_concurrent_replay_processes_and_restart_preserve_records(tmp_path):
    import os
    import subprocess
    from pathlib import Path

    fixture = Path(__file__).parent / "fixtures" / "ticks_synthetic.jsonl"
    cmd = [
        sys.executable, "-m", "dq_live_market_data_collector.tick_cli", "replay",
        str(fixture), "--root", str(tmp_path),
    ]
    procs = [
        subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=os.environ.copy())
        for _ in range(2)
    ]
    for proc in procs:
        out, err = proc.communicate(timeout=15)
        assert proc.returncode == 0, (out, err)
    before = {p: p.read_bytes() for p in tmp_path.rglob("*.jsonl")}
    assert len(before) == 2
    result = subprocess.run(cmd, capture_output=True, timeout=15, check=False)
    assert result.returncode == 0, result.stderr
    paths = list(tmp_path.rglob("*.jsonl"))
    assert len(paths) == 3
    assert all(p.read_bytes() == content for p, content in before.items())
    assert all(inspect_file(p)["records"] == 3 for p in paths)


def test_sync_failure_does_not_claim_durable_checkpoint(tmp_path, monkeypatch):
    import os

    writer = TickWriter(tmp_path, "replay")
    writer.append({"local_seq": 1, "received_at_utc": "2026-10-30T00:00:00Z", "raw_payload": {}})
    def fail(_fd):
        raise OSError("fsync failed")
    monkeypatch.setattr(os, "fsync", fail)
    with pytest.raises(OSError, match="fsync failed"):
        writer.sync()
    assert writer.written == 1 and writer.durable == 0
    assert inspect_file(writer.path)["records"] == 1
    writer.stream.close()


def test_callback_copy_failure_and_callback_after_stop_are_accounted(tmp_path):
    from dq_live_market_data_collector.tick_file import TickIngress

    ingress = TickIngress(TickWriter(tmp_path, "test"), profile="test", environment="test")
    def broken_copy():
        raise RuntimeError("synthetic SDK property failure")
    assert not ingress.callback(broken_copy)
    assert not ingress.callback({"InstrumentID": "late"})
    assert not ingress.close()
    status = ingress.status()
    assert (status["received"], status["enqueued"], status["rejected"]) == (2, 0, 2)
    assert "synthetic SDK property failure" in status["error"]


def test_verify_excludes_events_and_detects_cross_segment_gaps(tmp_path, capsys):
    from dq_live_market_data_collector.tick_cli import main

    segment_dir = tmp_path / "session=one"
    segment_dir.mkdir()
    (tmp_path / "events.jsonl").write_text('{"event":"connected"}\n')
    (segment_dir / "part-000001.jsonl").write_text('{"local_seq":1}\n')
    (segment_dir / "part-000002.jsonl.open").write_text('{"local_seq":3}\n')
    assert main(["verify", str(tmp_path)]) == 1
    reports = json.loads(capsys.readouterr().out)
    assert len(reports) == 2
    assert reports[1]["error"] == "noncontiguous_segment_sequence"
    (segment_dir / "part-000002.jsonl.open").write_text('{"payload":1}\n')
    assert main(["verify", str(tmp_path)]) == 1
    assert json.loads(capsys.readouterr().out)[1]["error"] == "missing_sequence"


def test_verify_corrupt_segment_taints_later_session_prefix(tmp_path, capsys):
    from dq_live_market_data_collector.tick_cli import main

    directory = tmp_path / "session=one"
    directory.mkdir()
    (directory / "part-000001.jsonl").write_text('{"local_seq":1}\nBAD\n')
    (directory / "part-000002.jsonl").write_text('{"local_seq":2}\n')
    assert main(["verify", str(tmp_path)]) == 1
    first, second = json.loads(capsys.readouterr().out)
    assert first["session_corruption"]["error"] == "invalid_json"
    assert first["session_trusted_bytes"] == len('{"local_seq":1}\n')
    assert second["segment_readable_bytes"] > 0
    assert second["session_trusted_bytes"] == 0
    assert second["session_corruption"] == first["session_corruption"]


def test_native_dispatch_rejects_missing_duplicate_and_unsupported(monkeypatch):
    from types import SimpleNamespace

    import pytest

    from dq_live_market_data_collector import tick_cli

    args = SimpleNamespace(profile="ctp_futures")
    for entries in ([], [object(), object()]):
        monkeypatch.setattr(
            tick_cli.importlib.metadata, "entry_points", lambda entries=entries, **_: entries
        )
        with pytest.raises(RuntimeError, match="exactly one"):
            tick_cli.run_native(args)
    for profile in ("securities", "securities_options"):
        with pytest.raises(ValueError, match="unsupported source profile"):
            tick_cli.run_native(SimpleNamespace(profile=profile))


def test_retired_callback_preserves_sequence_evidence(tmp_path):
    from dq_live_market_data_collector.tick_file import TickIngress

    class Retired(Exception):
        rejection_reason = "retired_generation"

    def retired():
        raise Retired()

    writer = TickWriter(tmp_path, "test")
    ingress = TickIngress(writer, profile="ctp_futures", environment="synthetic")
    assert ingress.callback({"InstrumentID": "first"})
    assert ingress.callback(retired)
    assert ingress.callback({"InstrumentID": "last"})
    assert ingress.close()
    records = [json.loads(line) for line in writer.path.read_text().splitlines()]
    assert [(r["local_seq"], r["record_type"]) for r in records] == [
        (1, "market_snapshot"), (2, "rejected_callback"), (3, "market_snapshot")
    ]
    assert records[1]["reason"] == "retired_generation"
    assert inspect_file(writer.path)["records"] == 3
    assert inspect_file(writer.path)["error"] is None
    assert ingress.status()["rejected"] == 1


def test_verify_segment_gap_and_corruption_keep_both_boundaries(tmp_path, capsys):
    from dq_live_market_data_collector.tick_cli import main

    directory = tmp_path / "session=one"
    directory.mkdir()
    (directory / "part-000001.jsonl").write_text('{"local_seq":1}\n')
    (directory / "part-000002.jsonl").write_text('{"local_seq":3}\nBAD\n')
    assert main(["verify", str(tmp_path)]) == 1
    _, second = json.loads(capsys.readouterr().out)
    assert second["error"] == "invalid_json"
    assert second["boundary_error"] == "noncontiguous_segment_sequence"
    assert second["session_trusted_bytes"] == 0
    assert second["session_corruption"]["error"] == "noncontiguous_segment_sequence"


def test_empty_replay_does_not_report_success(tmp_path, capsys):
    from dq_live_market_data_collector.tick_cli import main

    fixture = tmp_path / "empty.jsonl"
    fixture.write_text("")
    assert main(["replay", str(fixture), "--root", str(tmp_path / "out")]) == 1
    capsys.readouterr()
    assert main(["verify", str(tmp_path / "out")]) == 1

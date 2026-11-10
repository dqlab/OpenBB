"""Isolated Archive integration using the real writer and filesystem."""

# ruff: noqa: D103

import errno
import json
import os
from pathlib import Path

import pytest
from openbb_futu.archive import Archive
from openbb_futu.offline import replay, verify


def make_archive(root, **kwargs):
    return Archive._isolated_for_test(root, run_id="a" * 32, **kwargs)


def test_replay_rotation_and_verify(tmp_path):
    raw = '{"type":"QUOTE","symbol":"US.A","data":{"price":42}}'
    source = tmp_path / "input"
    source.write_text((raw + "\n") * 2)
    root = tmp_path / "archive"
    root.mkdir()
    def factory(path, **kwargs):
        return Archive._isolated_for_test(path, max_bytes=1, **kwargs)
    result = replay(source, root, _archive_factory=factory)
    records = []
    assert verify(Path(result["manifest"]), on_record=records.append)["status"] == "verified"
    assert len(result["files"]) == len(records) == 2
    assert [r["local_seq"] for r in records] == [1, 2]
    assert records[0]["raw_payload"] == records[1]["raw_payload"]


def test_symlink_manifest_directory_rejected(tmp_path):
    root, outside = tmp_path / "root", tmp_path / "outside"
    root.mkdir()
    outside.mkdir()
    (root / "futu-manifests").symlink_to(outside, target_is_directory=True)
    archive = make_archive(root)
    with pytest.raises(OSError):
        archive.close()
    assert not list(outside.iterdir())
    assert not archive._root_handle.fds


def test_segment_fsync_failure_retains_evidence(tmp_path, monkeypatch):
    archive = make_archive(tmp_path)
    original = os.fsync
    def fail(fd):
        if os.readlink(f"/proc/self/fd/{fd}").endswith(".jsonl.open"):
            raise OSError(errno.EIO, "injected")
        return original(fd)
    monkeypatch.setattr(os, "fsync", fail)
    assert archive.accept('{"type":"QUOTE","symbol":"US.A","data":{}}', "c")
    result = archive.close()
    assert result["error_errno"] == errno.EIO and result["durable_seq"] == 0
    assert next(tmp_path.rglob("*.jsonl.open")).read_bytes()
    assert not archive._root_handle.fds


def test_manifest_publish_failure_keeps_temp(tmp_path, monkeypatch):
    archive = make_archive(tmp_path)
    archive.stop.set()
    archive.thread.join()
    original = os.link
    def fail(src, dst, **kwargs):
        if str(dst).endswith(".json"):
            raise OSError(errno.EIO, "injected")
        return original(src, dst, **kwargs)
    monkeypatch.setattr(os, "link", fail)
    with pytest.raises(OSError) as exc:
        archive.close()
    assert exc.value.errno == errno.EIO
    assert json.loads(next((tmp_path / "futu-manifests").glob("*.tmp")).read_text())["run_id"] == "a" * 32
    assert not archive._root_handle.fds


def test_sealed_segment_symlink_rejected(tmp_path):
    archive = make_archive(tmp_path)
    assert archive.accept('{"type":"QUOTE","symbol":"US.A","data":{}}', "c")
    archive.stop.set()
    archive.thread.join()
    segment = archive.writer.sealed_paths[0]
    outside = tmp_path / "outside"
    outside.write_bytes(b"original")
    segment.unlink()
    segment.symlink_to(outside)
    with pytest.raises(OSError):
        archive.close()
    assert outside.read_bytes() == b"original"
    assert not archive._root_handle.fds


def test_existing_manifest_is_preserved(tmp_path):
    archive = make_archive(tmp_path)
    directory = tmp_path / "futu-manifests"
    directory.mkdir()
    target = directory / (archive.writer.session_id + ".json")
    target.write_bytes(b"prior evidence")
    with pytest.raises(FileExistsError):
        archive.close()
    assert target.read_bytes() == b"prior evidence"
    assert list(directory.glob("*.tmp"))
    assert not archive._root_handle.fds

"""Exercise the shared writer against real descriptor-anchored directories."""

# ruff: noqa: D103

import json
import os
from functools import partial

import pytest
from dq_live_market_data_collector.tick_file import TickWriter
from openbb_futu.directory_chain import DirectoryChain
from openbb_futu.root_handle import RootHandle


def writer_for(root, handle, **kwargs):
    return TickWriter(root, "futu", directory_chain_factory=partial(DirectoryChain, handle), **kwargs)


def record(seq):
    return {"local_seq": seq, "received_at_utc": "2026-10-30T00:00:00Z", "raw_payload": {"price": 42}}


def test_duplicates_and_rotation(tmp_path):
    with RootHandle(tmp_path) as root:
        writer = writer_for(tmp_path, root, max_bytes=1)
        writer.append(record(1))
        writer.append(record(2))
        writer.close()
        files = sorted(tmp_path.rglob("*.jsonl"))
        expected = [(json.dumps(record(i), separators=(",", ":")) + "\n").encode() for i in (1, 2)]
        assert [p.read_bytes() for p in files] == expected
        assert writer.written == writer.durable == 2


def test_fsync_failure_preserves_open_file(tmp_path, monkeypatch):
    with RootHandle(tmp_path) as root:
        writer = writer_for(tmp_path, root)
        writer.append(record(1))
        original = os.fsync
        def fail(fd):
            if fd == writer.stream.fileno():
                raise OSError("synthetic fsync failure")
            original(fd)
        monkeypatch.setattr(os, "fsync", fail)
        with pytest.raises(OSError, match="synthetic"):
            writer.close()
        assert writer.durable == 0
        assert writer.path.read_bytes() == (json.dumps(record(1), separators=(",", ":")) + "\n").encode()
        monkeypatch.setattr(os, "fsync", original)
        writer.close()


def test_abort_after_fsync_failure_closes_descriptors_and_keeps_evidence(tmp_path, monkeypatch):
    with RootHandle(tmp_path) as root:
        writer = writer_for(tmp_path, root)
        writer.append(record(1))
        file_fd = writer.stream.fileno()
        directory_fds = list(writer._chain.fds)
        original = os.fsync

        def fail(fd):
            if fd == file_fd:
                raise OSError("synthetic fsync failure")
            original(fd)

        monkeypatch.setattr(os, "fsync", fail)
        with pytest.raises(OSError):
            writer.close()
        writer.abort()
        writer.abort()
        for fd in [file_fd, *directory_fds]:
            with pytest.raises(OSError):
                os.fstat(fd)
        assert writer.durable == 0
        assert writer.path.read_bytes() == (json.dumps(record(1), separators=(",", ":")) + "\n").encode()


def test_existing_target_not_overwritten(tmp_path):
    with RootHandle(tmp_path) as root:
        writer = writer_for(tmp_path, root)
        writer.append(record(1))
        target = writer.path.with_suffix("")
        target.write_bytes(b"existing")
        with pytest.raises(FileExistsError):
            writer.close()
        assert target.read_bytes() == b"existing"
        assert writer.path.read_bytes().endswith(b"\n")
        writer._chain.close()


def test_symlink_directory_rejected(tmp_path):
    root_path, outside = tmp_path / "root", tmp_path / "outside"
    root_path.mkdir()
    outside.mkdir()
    (root_path / "source_id=futu").symlink_to(outside, target_is_directory=True)
    with RootHandle(root_path) as root:
        writer = writer_for(root_path, root)
        with pytest.raises(OSError):
            writer.append(record(1))
    assert list(outside.iterdir()) == []


def test_replaced_directory_rejected_before_write(tmp_path):
    root_path, outside = tmp_path / "root", tmp_path / "outside"
    root_path.mkdir()
    outside.mkdir()
    with RootHandle(root_path) as root:
        writer = writer_for(root_path, root)
        writer.append(record(1))
        writer.sync()
        source = root_path / "source_id=futu"
        source.rename(outside / "displaced")
        (outside / "redirect").mkdir()
        source.symlink_to(outside / "redirect", target_is_directory=True)
        with pytest.raises(Exception):
            writer.append(record(2))
        with pytest.raises(Exception):
            writer.close()
        writer.stream.close()
        writer._chain.close()
    assert list((outside / "redirect").iterdir()) == []
    data = next((outside / "displaced").rglob("*.open")).read_bytes()
    assert data == (json.dumps(record(1), separators=(",", ":")) + "\n").encode()

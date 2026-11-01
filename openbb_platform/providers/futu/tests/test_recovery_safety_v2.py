"""Regression tests for publication and legacy recovery readers."""
import json
import os

import pytest
from openbb_futu.archive import recover_open_files


def source(root):
    """Source."""
    path = root / "source_id=futu/recv_date=2025-01-01/session=abc/part-000001.jsonl.open"
    path.parent.mkdir(parents=True)
    path.write_bytes(b'{"local_seq":1}\n')
    return path


def test_legacy_reader_never_sees_success_after_publication(tmp_path):
    """Test legacy reader never sees success after publication."""
    path = source(tmp_path)
    result = recover_open_files(tmp_path)[0]
    path.write_bytes(path.read_bytes() + b"growth")
    assert not list(tmp_path.glob("futu-recovery/*/recovery.json"))
    report = json.loads(next(tmp_path.glob("futu-recovery/*/recovery-unknown-v2.json")).read_text())
    assert report["verification_status"] == "unknown"
    assert report["error"] and report["records"] is None
    assert result["error"] and result["records"] is None


def test_interruption_after_publication_has_no_hardlink_alias(tmp_path, monkeypatch):
    """Test interruption after publication has no hardlink alias."""
    source(tmp_path)
    from openbb_futu import archive
    original = archive._rename_noreplace
    def interrupt(*args):
        original(*args)
        raise KeyboardInterrupt
    monkeypatch.setattr(archive, "_rename_noreplace", interrupt)
    with pytest.raises(KeyboardInterrupt):
        recover_open_files(tmp_path)
    files = [p for p in (tmp_path / "futu-recovery").rglob("*") if p.is_file()]
    assert files and all(p.stat().st_nlink == 1 for p in files)
    assert not list(tmp_path.glob("futu-recovery/*/recovery.json"))


def test_rename_noreplace_preserves_destination(tmp_path):
    """Test rename noreplace preserves destination."""
    from openbb_futu.archive import _rename_noreplace
    (tmp_path / "src").write_bytes(b"new")
    (tmp_path / "dst").write_bytes(b"old")
    fd = os.open(tmp_path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        with pytest.raises(FileExistsError):
            _rename_noreplace(fd, "src", "dst")
    finally:
        os.close(fd)
    assert (tmp_path / "src").read_bytes() == b"new"
    assert (tmp_path / "dst").read_bytes() == b"old"


def test_source_change_at_report_publication_is_detected_and_safe(tmp_path, monkeypatch):
    """Test source change at report publication is detected and safe."""
    from openbb_futu import archive
    from openbb_futu.read_budget import EvidenceChanged
    path = source(tmp_path)
    rename = archive._rename_noreplace
    def race(fd, src, dst):
        rename(fd, src, dst)
        if dst == "recovery-unknown-v2.json":
            with path.open("ab") as stream:
                stream.write(b"growth")
    monkeypatch.setattr(archive, "_rename_noreplace", race)
    with pytest.raises(EvidenceChanged):
        recover_open_files(tmp_path)
    report = json.loads(next(tmp_path.glob("futu-recovery/*/recovery-unknown-v2.json")).read_text())
    assert report["error"] and report["records"] is None
    assert not list(tmp_path.glob("futu-recovery/*/recovery.json"))


def test_retry_preserves_partial_evidence_and_repeated_ticks(tmp_path, monkeypatch):
    """Test retry preserves partial evidence and repeated ticks."""
    from openbb_futu import archive
    path = source(tmp_path)
    data = b'{"local_seq":1,"frame":"same"}\n{"local_seq":2,"frame":"same"}\npartial'
    path.write_bytes(data)
    rename = archive._rename_noreplace
    def fail(fd, src, dst):
        rename(fd, src, dst)
        raise PermissionError("injected after publication")
    with monkeypatch.context() as m:
        m.setattr(archive, "_rename_noreplace", fail)
        with pytest.raises(PermissionError):
            recover_open_files(tmp_path)
    previous = {p: p.read_bytes() for p in (tmp_path / "futu-recovery").rglob("*") if p.is_file()}
    result = recover_open_files(tmp_path)[0]
    from pathlib import Path
    directory = Path(result["evidence_dir"])
    assert (directory / "valid-prefix.jsonl").read_bytes() + (directory / "tail.bin").read_bytes() == data
    assert result["snapshot"]["records"] == 2
    assert all(p.read_bytes() == content for p, content in previous.items())
    assert path.read_bytes() == data


def test_unsupported_platform_fails_closed(tmp_path, monkeypatch):
    """Test unsupported platform fails closed."""
    from openbb_futu import archive
    path = source(tmp_path)
    monkeypatch.setattr(archive.sys, "platform", "unsupported")
    with pytest.raises(OSError):
        recover_open_files(tmp_path)
    assert path.exists()
    assert not list(tmp_path.glob("futu-recovery/*/recovery*.json"))

"""Recovery reads real files under held directories and preserves evidence."""
import json
import os

import pytest
from openbb_futu import archive
from openbb_futu.archive import recover_open_files
from openbb_futu.read_budget import Budget, BudgetExceeded, EvidenceChanged


def segment(root, date="2025-01-01", name="part-000001.jsonl.open"):
    """Create an interrupted real segment."""
    path = root / "source_id=futu" / f"recv_date={date}" / "session=abc" / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b'{"local_seq":1,"collector_run_id":"r"}\n' * 2 + b'{"local_seq":')
    return path


def test_recovery_preserves_identical_frames_and_tail(tmp_path):
    """Preserve repeated frames and incomplete bytes."""
    path = segment(tmp_path)
    # Identical payloads with consecutive sequence numbers are separate records.
    data = b'{"local_seq":1,"frame":"same"}\n{"local_seq":2,"frame":"same"}\npartial'
    path.write_bytes(data)
    report = recover_open_files(tmp_path)[0]
    from pathlib import Path
    out = Path(report["evidence_dir"])
    assert (out / "valid-prefix.jsonl").read_bytes() + (out / "tail.bin").read_bytes() == data
    v2 = json.loads((out / "recovery-unknown-v2.json").read_bytes())
    assert not (out / "recovery.json").exists()
    assert v2["records"] is None
    assert v2["snapshot"]["records"] == 2
    assert v2["snapshot"]["valid_bytes"] == len(data) - len(b"partial")
    assert v2["snapshot"]["unread_bytes"] == len(b"partial")
    assert path.read_bytes() == data


def test_shared_budget_across_dates_preserves_old_evidence(tmp_path):
    """Do not publish after the shared budget runs out."""
    one = segment(tmp_path)
    two = segment(tmp_path, "2025-01-02")
    old = tmp_path / "futu-recovery" / "old" / "tail.bin"
    old.parent.mkdir(parents=True)
    old.write_bytes(b"old")
    with pytest.raises(BudgetExceeded):
        recover_open_files(tmp_path, budget=Budget(bytes_limit=len(one.read_bytes()) + 1))
    assert old.read_bytes() == b"old"
    assert len(list(old.parent.parent.iterdir())) == 1
    assert one.exists() and two.exists()


@pytest.mark.parametrize("kind", ["fifo", "symlink"])
def test_special_segment_rejected(tmp_path, kind):
    """Never block or follow a special file."""
    path = segment(tmp_path)
    path.unlink()
    if kind == "fifo":
        os.mkfifo(path)
    else:
        path.symlink_to("missing")
    with pytest.raises((ValueError, OSError)):
        recover_open_files(tmp_path)
    assert not (tmp_path / "futu-recovery").exists()


def test_growth_during_read_rejected(tmp_path, monkeypatch):
    """Detect a concurrent append during the single descriptor read."""
    path = segment(tmp_path)
    real_read = os.read
    changed = False
    def growing(fd, size):
        nonlocal changed
        chunk = real_read(fd, size)
        if chunk and not changed:
            changed = True
            with path.open("ab") as stream:
                stream.write(b"growth")
        return chunk
    monkeypatch.setattr(os, "read", growing)
    with pytest.raises(EvidenceChanged):
        recover_open_files(tmp_path)
    assert not (tmp_path / "futu-recovery").exists()


def test_legacy_sealed_manifest_is_skipped(tmp_path):
    """A matching historical sealed segment stays untouched."""
    import hashlib
    path = segment(tmp_path, name="part-000001.jsonl")
    data = b'{"local_seq":1,"collector_run_id":"r"}\n'
    path.write_bytes(data)
    manifests = tmp_path / "futu-manifests"
    manifests.mkdir()
    (manifests / "abc.json").write_text(json.dumps({
        "run_id": "r", "session_id": "abc",
        "files": [{"file": str(path), "bytes": len(data),
                   "sha256": hashlib.sha256(data).hexdigest()}],
    }))
    assert recover_open_files(tmp_path) == []
    assert path.read_bytes() == data
    assert not (tmp_path / "futu-recovery").exists()


def test_directory_replacement_during_read_fails(tmp_path, monkeypatch):
    """Reject a replaced session directory before publishing."""
    path = segment(tmp_path)
    real_read = os.read
    changed = False
    def replace(fd, size):
        nonlocal changed
        chunk = real_read(fd, size)
        if chunk and not changed:
            changed = True
            path.parent.rename(path.parent.with_name("moved"))
            path.parent.mkdir()
        return chunk
    monkeypatch.setattr(os, "read", replace)
    with pytest.raises(EvidenceChanged):
        recover_open_files(tmp_path)
    assert not (tmp_path / "futu-recovery").exists()


def test_manifest_created_after_absence_is_detected(tmp_path, monkeypatch):
    """A manifest appearing at the first publication mkdir invalidates preflight."""
    path = segment(tmp_path, name="part-000001.jsonl")
    path.write_bytes(b'{"local_seq":1,"collector_run_id":"r"}\n')
    real_mkdir = os.mkdir
    def racing_mkdir(name, *args, **kwargs):
        if name == "futu-recovery":
            manifests = tmp_path / "futu-manifests"
            manifests.mkdir(exist_ok=True)
            (manifests / "abc.json").write_text("{}")
        return real_mkdir(name, *args, **kwargs)
    monkeypatch.setattr(os, "mkdir", racing_mkdir)
    with pytest.raises(EvidenceChanged):
        recover_open_files(tmp_path)
    for report in tmp_path.glob("futu-recovery/*/recovery-unknown-v2.json"):
        assert json.loads(report.read_bytes())["records"] is None
        assert json.loads(report.read_bytes())["verification_status"] == "unknown"


def test_source_growth_after_report_rename_leaves_unknown(tmp_path, monkeypatch):
    """The published report cannot assert verification if a source grows at rename."""
    path = segment(tmp_path)
    real_link = archive._rename_noreplace
    def racing_link(fd, src, dst):
        result = real_link(fd, src, dst)
        if dst == "recovery-unknown-v2.json":
            with path.open("ab") as stream:
                stream.write(b"growth")
        return result
    monkeypatch.setattr(archive, "_rename_noreplace", racing_link)
    with pytest.raises(EvidenceChanged):
        recover_open_files(tmp_path)
    reports = list(tmp_path.glob("futu-recovery/*/recovery-unknown-v2.json"))
    assert reports
    assert json.loads(reports[0].read_bytes())["records"] is None
    assert json.loads(reports[0].read_bytes())["verification_status"] == "unknown"


@pytest.mark.parametrize("sealed", [False, True])
@pytest.mark.parametrize("change", ["replace", "grow"])
def test_segment_changes_at_publication_are_detected(tmp_path, monkeypatch, sealed, change):
    """Change actual segment at the exclusive evidence rename boundary."""
    path = segment(tmp_path, name="part-000001.jsonl" if sealed else "part-000001.jsonl.open")
    if sealed:
        path.write_bytes(b'{"local_seq":1,"collector_run_id":"r"}\n')
    real_link = archive._rename_noreplace
    changed = False
    def racing_link(fd, src, dst):
        nonlocal changed
        result = real_link(fd, src, dst)
        if not changed:
            changed = True
            if change == "replace":
                path.rename(path.with_suffix(path.suffix + ".saved"))
                path.write_bytes(b"replacement")
            else:
                with path.open("ab") as stream:
                    stream.write(b"growth")
        return result
    monkeypatch.setattr(archive, "_rename_noreplace", racing_link)
    with pytest.raises(EvidenceChanged):
        recover_open_files(tmp_path)
    assert changed
    for report in tmp_path.glob("futu-recovery/*/recovery-unknown-v2.json"):
        assert json.loads(report.read_bytes())["records"] is None
        assert json.loads(report.read_bytes())["verification_status"] == "unknown"


@pytest.mark.parametrize("failure", [PermissionError(13, "denied"), KeyboardInterrupt()])
def test_report_rename_failure_preserves_existing_evidence(tmp_path, monkeypatch, failure):
    """Real rename failure or interruption cannot create a final report or touch old data."""
    path = segment(tmp_path)
    original = path.read_bytes()
    old = tmp_path / "futu-recovery" / "old" / "recovery.json"
    old.parent.mkdir(parents=True)
    old.write_bytes(b"old report")
    real_link = archive._rename_noreplace
    def failed_link(fd, src, dst):
        if dst == "recovery-unknown-v2.json":
            raise failure
        return real_link(fd, src, dst)
    monkeypatch.setattr(archive, "_rename_noreplace", failed_link)
    with pytest.raises(type(failure)):
        recover_open_files(tmp_path)
    assert path.read_bytes() == original and old.read_bytes() == b"old report"
    assert list(tmp_path.glob("futu-recovery/*/recovery.json")) == [old]
    assert not list(tmp_path.glob("futu-recovery/*/recovery-unknown-v2.json"))


def test_manifest_file_created_in_existing_directory(tmp_path, monkeypatch):
    """Catch creation of a previously missing manifest within a held directory."""
    path = segment(tmp_path, name="part-000001.jsonl")
    path.write_bytes(b'{"local_seq":1,"collector_run_id":"r"}\n')
    manifests = tmp_path / "futu-manifests"
    manifests.mkdir()
    real_link = archive._rename_noreplace
    def racing_link(fd, src, dst):
        result = real_link(fd, src, dst)
        (manifests / "abc.json").write_text("{}")
        return result
    monkeypatch.setattr(archive, "_rename_noreplace", racing_link)
    with pytest.raises(EvidenceChanged):
        recover_open_files(tmp_path)
    for report in tmp_path.glob("futu-recovery/*/recovery-unknown-v2.json"):
        assert json.loads(report.read_bytes())["records"] is None
        assert json.loads(report.read_bytes())["verification_status"] == "unknown"


def test_recovery_directory_moved_at_rename(tmp_path, monkeypatch):
    """Detect a real rename of the held publication directory after rename."""
    segment(tmp_path)
    real_link = archive._rename_noreplace
    def racing_link(fd, src, dst):
        result = real_link(fd, src, dst)
        parent = tmp_path / "futu-recovery"
        directory = next(parent.iterdir())
        directory.rename(parent / "moved")
        directory.mkdir()
        return result
    monkeypatch.setattr(archive, "_rename_noreplace", racing_link)
    with pytest.raises((EvidenceChanged, ValueError)):
        recover_open_files(tmp_path)
    assert not list(tmp_path.glob("futu-recovery/*/recovery-unknown-v2.json"))


def test_shared_budget_across_sessions(tmp_path):
    """The invocation budget is not reset for a second session."""
    one = segment(tmp_path)
    two = one.parent.parent / "session=def" / one.name
    two.parent.mkdir()
    two.write_bytes(one.read_bytes())
    with pytest.raises(BudgetExceeded):
        recover_open_files(tmp_path, budget=Budget(bytes_limit=len(one.read_bytes()) + 1))
    assert one.exists() and two.exists()
    assert not (tmp_path / "futu-recovery").exists()

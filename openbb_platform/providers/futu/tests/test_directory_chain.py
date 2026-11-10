"""Tests for the held directory chain used by future anchored I/O."""

import os

import pytest
from openbb_futu.directory_chain import DirectoryChain
from openbb_futu.read_budget import EvidenceChanged
from openbb_futu.root_handle import RootHandle


def test_created_chain_detects_replaced_ancestor(tmp_path):
    """A moved directory is detected and preserved."""
    with RootHandle(tmp_path) as root, DirectoryChain(root, ["a", "b"], create=True) as chain:
        (tmp_path / "a").rename(tmp_path / "old")
        (tmp_path / "a").mkdir()
        with pytest.raises(EvidenceChanged):
            chain.check()
        assert (tmp_path / "old" / "b").is_dir()


@pytest.mark.parametrize("kind", ["symlink", "fifo"])
def test_non_directory_component_rejected(tmp_path, kind):
    """Links and FIFOs cannot become directory chain components."""
    outside = tmp_path / "outside"
    outside.mkdir()
    if kind == "symlink":
        (tmp_path / "a").symlink_to(outside, target_is_directory=True)
    else:
        os.mkfifo(tmp_path / "a")
    with RootHandle(tmp_path) as root, pytest.raises(OSError):
        DirectoryChain(root, ["a", "b"], create=True)
    assert not list(outside.iterdir())

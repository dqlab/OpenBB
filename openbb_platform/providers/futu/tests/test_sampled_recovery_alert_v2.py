"""Recovery uncertainty must stop the real sampler before collection."""

import contextlib

import pytest
from openbb_futu import sampled_snapshot


def test_sampler_stops_on_unresolved_recovery(monkeypatch, tmp_path):
    """Do not start an archive when recovery reports uncertainty."""
    monkeypatch.setattr(sampled_snapshot, "Owner", lambda *_: contextlib.nullcontext())
    monkeypatch.setattr(sampled_snapshot, "recover_open_files", lambda *_: [{"verification_status": "unknown"}])
    monkeypatch.setattr(sampled_snapshot, "Archive", lambda *a, **k: pytest.fail("archive must not start"))
    config = {"symbols": [{"asset": "equity", "symbol": "HK.00700"}]}
    with pytest.raises(RuntimeError, match="recovery_unresolved"):
        sampled_snapshot.run(config, "token", tmp_path)


def test_sampler_cli_reports_unresolved_recovery(monkeypatch, tmp_path, capsys):
    """The real CLI returns failure and names the unresolved recovery."""
    config = tmp_path / "config.json"
    config.write_text('{"symbols": [{"asset": "equity", "symbol": "HK.00700"}]}')
    monkeypatch.setattr(sampled_snapshot, "Owner", lambda *_: contextlib.nullcontext())
    monkeypatch.setattr(sampled_snapshot, "recover_open_files", lambda *_: [{"verification_status": "unknown"}])
    monkeypatch.setattr(sampled_snapshot.TokenStore, "read", lambda *_: {
        "access_token": "token", "refresh_token": "refresh", "client_id": "client"
    })
    monkeypatch.setattr("sys.argv", ["sampler", "--config", str(config), "--token-file", "unused",
                                    "--output", str(tmp_path / "out")])
    assert sampled_snapshot.main() == 1
    assert "recovery_unresolved" in capsys.readouterr().err

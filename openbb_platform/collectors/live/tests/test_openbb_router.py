"""Offline checks for the OpenBB live-collector boundary."""

import json
import threading
from datetime import date

import pytest
import yaml
from conftest import Clock, FakeProvider

pytest.importorskip("openbb_core")
pytest.importorskip("openbb_collection")

from fastapi import FastAPI  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402
from openbb_collection import live_router as commands  # noqa: E402
from openbb_core.app.model.abstract.error import OpenBBError  # noqa: E402

from dq_live_market_data_collector import service  # noqa: E402
from dq_live_market_data_collector.cli import main  # noqa: E402
from dq_live_market_data_collector.config import load_config  # noqa: E402
from dq_live_market_data_collector.service import Collector  # noqa: E402
from dq_live_market_data_collector.storage import Journal  # noqa: E402


@pytest.fixture
def config_path(raw_config, tmp_path):
    raw_config["storage"]["root"] = "data"
    path = tmp_path / "live.yaml"
    path.write_text(yaml.safe_dump(raw_config))
    return str(path)


def test_cli_parity_and_reads_do_not_create_storage(config_path, tmp_path, capsys):
    expected = commands.validate_config(config_path).results
    assert main(["validate-config", "--config", config_path]) == 0
    assert json.loads(capsys.readouterr().out) == expected
    expected = commands.plan(config_path, date(2026, 9, 7)).results
    assert main(["plan", "--config", config_path, "--date", "2026-09-07"]) == 0
    assert json.loads(capsys.readouterr().out) == expected
    assert expected["collections"][0]["start_utc"] == "2026-09-07T13:30:00+00:00"
    assert commands.status(config_path).results == {"sessions": [], "pending_output_records": 0}
    assert not (tmp_path / "data").exists()
    expected = commands.export(config_path, max_batches=1).results
    assert main(["export", "--config", config_path, "--max-batches", "1"]) == 0
    assert json.loads(capsys.readouterr().out) == expected == {"exported_records": 0}


def test_once_preserves_checkpoints_and_status(config_path, monkeypatch, capsys):
    clock = Clock()
    provider = FakeProvider(clock)
    monkeypatch.setattr(
        service,
        "Collector",
        lambda config, journal: Collector(config, journal, provider=provider, clock=clock),
    )
    result = commands.once(config_path, max_seconds=5).results
    assert result["event"] == "collection_pass_complete" and not result["interrupted"]
    assert len(result["sessions"]) == 1 and len(provider.calls) == 2
    assert provider.closed and result["pending_output_records"] == 0
    assert main(["status", "--config", config_path]) == 0
    cli_status = json.loads(capsys.readouterr().out)
    assert commands.status(config_path).results == cli_status
    assert result == {"event": "collection_pass_complete", "interrupted": False, **cli_status}
    commands.once(config_path, max_seconds=5)
    assert len(provider.calls) == 2
    with Journal(load_config(config_path).storage) as journal:
        assert journal.db.execute("SELECT COUNT(*) FROM observations").fetchone()[0] == 2


def test_once_outside_session_does_not_fetch(config_path, monkeypatch):
    clock = Clock()
    clock.advance(-3600)
    provider = FakeProvider(clock)
    monkeypatch.setattr(
        service,
        "Collector",
        lambda config, journal: Collector(config, journal, provider=provider, clock=clock),
    )
    result = commands.once(config_path).results
    assert result["sessions"] == [] and not provider.calls
    assert provider.closed


@pytest.mark.parametrize("fails", [False, True])
def test_deadline_or_error_shuts_down_and_unlocks(config_path, monkeypatch, fails):
    closed = threading.Event()

    class WaitingCollector:
        def __init__(self, config, journal):
            self.stop = threading.Event()

        def step(self):
            if fails:
                raise RuntimeError("SECRET-provider-password /private/storage")
            assert self.stop.wait(2)

        def shutdown(self, reason):
            closed.set()

    monkeypatch.setattr(service, "Collector", WaitingCollector)
    if fails:
        with pytest.raises(OpenBBError, match="^invalid_configuration_or_runtime$"):
            commands.once(config_path, max_seconds=0.01)
    else:
        assert commands.once(config_path, max_seconds=0.01).results["interrupted"]
    assert closed.is_set()
    with Journal(load_config(config_path).storage):
        pass


@pytest.mark.parametrize("duration", [0, -1, 601, float("nan"), float("inf")])
def test_sessions_are_bounded(config_path, tmp_path, duration):
    with pytest.raises(OpenBBError):
        commands.once(config_path, max_seconds=duration)
    assert not (tmp_path / "data").exists()


@pytest.mark.parametrize("limit", [0, 101])
def test_status_retains_original_limit(config_path, limit):
    with pytest.raises(OpenBBError):
        commands.status(config_path, limit=limit)


@pytest.mark.parametrize("batches", [0, 10001])
def test_export_rejects_unbounded_batch_count(config_path, tmp_path, batches):
    with pytest.raises(OpenBBError):
        commands.export(config_path, max_batches=batches)
    assert not (tmp_path / "data").exists()


def test_live_http_methods_and_calendar_plan(config_path):
    app = FastAPI()
    app.include_router(commands.router.api_router, prefix="/collection")
    paths = app.openapi()["paths"]
    assert set(paths) == {
        f"/collection/live/{name}"
        for name in ("validate_config", "plan", "once", "status", "export")
    }
    for name in ("once", "export"):
        assert "post" in paths[f"/collection/live/{name}"]
        assert "get" not in paths[f"/collection/live/{name}"]
    with TestClient(app) as api:
        response = api.get(
            "/collection/live/plan",
            params={"config_path": config_path, "session_date": "2026-09-07"},
        )
    assert response.status_code == 200
    assert response.json()["results"] == commands.plan(config_path, date(2026, 9, 7)).results

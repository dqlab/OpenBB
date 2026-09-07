"""Offline acceptance for the relocated OpenBB extension boundary."""

import json
import threading
from datetime import UTC, date, datetime

import pytest
import yaml

pytest.importorskip("openbb_core")

from fastapi import FastAPI  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402
from openbb_core.app.model.abstract.error import OpenBBError  # noqa: E402

from dq_historical_market_data_collector.cli import main  # noqa: E402
from dq_historical_market_data_collector.config import load_config  # noqa: E402
from dq_historical_market_data_collector.runtime import Collector  # noqa: E402
from openbb_collection import collection_router as commands  # noqa: E402


@pytest.fixture
def config_path(config_dict, tmp_path):
    # Exercise the original YAML-relative storage contract in the new location.
    config_dict["storage"]["root"] = "data"
    path = tmp_path / "collector.yaml"
    path.write_text(yaml.safe_dump(config_dict))
    return str(path)


def test_router_plan_matches_cli_and_reads_do_not_create_storage(config_path, tmp_path, capsys):
    assert commands.validate_config(config_path).results["status"] == "valid"
    expected = commands.plan(config_path, as_of=date(2026, 9, 7), limit=1).results
    assert main(["plan", "--config", config_path, "--as-of", "2026-09-07", "--limit", "1"]) == 0
    assert json.loads(capsys.readouterr().out) == expected
    assert commands.status(config_path).results == {"sessions": []}
    assert commands.query(config_path, "aapl", datetime.now(UTC)).results == {"records": []}
    assert not (tmp_path / "data").exists()


def test_router_collection_and_readback_use_original_journal(
    config_path, client, now, monkeypatch, capsys
):
    def factory(config, journal, **kwargs):
        return Collector(config, journal, client=client, clock=lambda: now, **kwargs)

    monkeypatch.setattr(commands, "Collector", factory)
    report = commands.once(config_path, as_of=now.date(), max_seconds=5).results
    assert report["status"] == "complete"
    assert report["reconciliation"]["accepted"] == 3
    assert client.closed
    assert commands.status(config_path).results["sessions"][0] == report
    records = commands.query(config_path, "aapl", now, source="yahoo", limit=2).results
    assert len(records["records"]) == 2
    assert (
        main(
            [
                "query",
                "--config",
                config_path,
                "--instrument",
                "aapl",
                "--source",
                "yahoo",
                "--as-of",
                now.isoformat(),
                "--limit",
                "2",
            ]
        )
        == 0
    )
    assert json.loads(capsys.readouterr().out) == records
    # A later session uses the migrated collector's existing checkpoint identities.
    resumed = commands.once(config_path, as_of=now.date()).results
    assert resumed["resumed_chunks"] == 1 and len(client.calls) == 1


def test_router_deadline_cancels_worker_and_releases_journal(config_path, monkeypatch):
    stopped = threading.Event()
    closed = threading.Event()

    class WaitingCollector:
        def __init__(self, config, journal, stop, config_path):
            self.stop = stop

        def collect(self, **kwargs):
            assert self.stop.wait(2), "deadline did not cancel the collector"
            stopped.set()
            return {"status": "interrupted"}

        def close(self):
            closed.set()

    monkeypatch.setattr(commands, "Collector", WaitingCollector)
    assert commands.once(config_path, max_seconds=0.01).results["status"] == "interrupted"
    assert stopped.is_set() and closed.is_set()
    with commands.Journal(load_config(config_path).storage):
        pass


def test_router_runtime_error_closes_collector_and_hides_details(config_path, monkeypatch):
    closed = threading.Event()

    class FailingCollector:
        def __init__(self, *args, **kwargs):
            pass

        def collect(self, **kwargs):
            raise RuntimeError("SECRET-provider-password /private/storage")

        def close(self):
            closed.set()

    monkeypatch.setattr(commands, "Collector", FailingCollector)
    with pytest.raises(OpenBBError, match="^invalid_configuration_or_runtime$"):
        commands.once(config_path)
    assert closed.is_set()
    with commands.Journal(load_config(config_path).storage):
        pass


@pytest.mark.parametrize("limit", [0, 1001])
@pytest.mark.parametrize("command", [commands.plan, commands.status])
def test_router_rejects_unbounded_reads(config_path, command, limit):
    with pytest.raises(OpenBBError):
        command(config_path, limit=limit)


@pytest.mark.parametrize("duration", [0, -1, 601, float("nan"), float("inf")])
def test_router_rejects_unbounded_sessions(config_path, tmp_path, duration):
    with pytest.raises(OpenBBError):
        commands.once(config_path, max_seconds=duration)
    assert not (tmp_path / "data").exists()


def test_router_rejects_naive_availability_cutoff(config_path):
    with pytest.raises(OpenBBError):
        commands.query(config_path, "aapl", datetime(2026, 9, 7))


def test_openapi_registration_and_http_plan(config_path):
    app = FastAPI()
    app.include_router(commands.router.api_router, prefix="/collection")
    paths = app.openapi()["paths"]
    assert set(paths) == {
        "/collection/validate_config",
        "/collection/plan",
        "/collection/once",
        "/collection/status",
        "/collection/query",
        "/collection/live/validate_config",
        "/collection/live/plan",
        "/collection/live/once",
        "/collection/live/status",
        "/collection/live/export",
    }
    assert "post" in paths["/collection/once"] and "get" not in paths["/collection/once"]
    with TestClient(app) as api:
        response = api.get(
            "/collection/plan",
            params={"config_path": config_path, "as_of": "2026-09-07", "limit": 1},
        )
    assert response.status_code == 200
    assert (
        response.json()["results"]
        == commands.plan(config_path, as_of=date(2026, 9, 7), limit=1).results
    )


def test_http_query_requires_timezone_aware_cutoff(config_path):
    app = FastAPI()
    app.include_router(commands.router.api_router, prefix="/collection")
    with TestClient(app) as api:
        response = api.get(
            "/collection/query",
            params={
                "config_path": config_path,
                "instrument": "aapl",
                "as_of": "2026-09-07T12:00:00",
            },
        )
    assert response.status_code == 422

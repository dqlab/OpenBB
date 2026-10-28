"""Offline acceptance for the date-wide CFFEX collector. No vendor calls."""

from __future__ import annotations

import ast
import contextlib
import copy
import fcntl
import hashlib
import json
import os
import signal
import socket
import sqlite3
import subprocess
import sys
import threading
import time
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest
import requests
from openbb_collector_core.models import ProviderFailure, Response
from pydantic import ValidationError

from dq_historical_market_data_collector.cffex import acquire as cffex_acquire
from dq_historical_market_data_collector.cffex import cli
from dq_historical_market_data_collector.cffex import parse as cffex_parse
from dq_historical_market_data_collector.cffex import runtime as cffex_runtime
from dq_historical_market_data_collector.cffex import source as cffex_source
from dq_historical_market_data_collector.cffex.config import (
    UNIT_FIELDS,
    Config,
    Source,
    TradingCalendar,
    eligible_day,
    iso_date,
    load_calendar,
    load_config,
)
from dq_historical_market_data_collector.cffex.normalize import (
    DataError,
    UnitAuthority,
    normalize,
)
from dq_historical_market_data_collector.cffex.runtime import (
    CALENDAR_DELIVERY_VERSION,
    DELIVERY_VERSION,
    EVIDENCE_VERSION,
    MAX_PLAN_ENTRIES,
    PLAN_VERSION,
    Collector,
    EvidenceStateError,
    NoSourceClient,
    acquire,
    read_calendar_delivery,
    read_delivery,
    refresh_calendar,
)
from dq_historical_market_data_collector.cffex.source import CffexCsvClient
from dq_historical_market_data_collector.cffex.storage import (
    Journal,
    JournalError,
    LockBusy,
    export_csv,
    status,
)

DAY = date(2026, 9, 18)
NOW = datetime(2026, 9, 18, 11, tzinfo=UTC)  # 19:00 Shanghai


class FrozenDateTime(datetime):
    @classmethod
    def now(cls, tz=None):
        return NOW.astimezone(tz) if tz else NOW.replace(tzinfo=None)


def row(day=DAY, symbol="IF2610", **changes):
    result = {
        "date": day.strftime("%Y%m%d"),
        "symbol": symbol,
        "variety": symbol[:-4],
        "open": "100",
        "high": "103",
        "low": "99",
        "close": "102",
        "settle": "101",
        "pre_settle": "100",
        "volume": "12.0",
        "open_interest": "34",
        "turnover": "567.8",
    }
    result.update(changes)
    return result


class FakeClient:
    def __init__(self, rows=None, failures=0):
        self.rows = rows
        self.failures = failures
        self.calls = []
        self.closed = False

    def fetch(self, source_id, source, request):
        self.calls.append((source_id, source.model_dump(mode="json"), request.copy()))
        if self.failures:
            self.failures -= 1
            raise ProviderFailure("test_failure")
        data = self.rows(request) if callable(self.rows) else self.rows
        if data is None:
            data = [row(iso_date(request["date"]))]
        return Response(
            copy.deepcopy(data),
            1,
            {"openbb-core": {"version": "offline"}, "openbb-akshare": {"version": "offline"}},
        )

    def close(self):
        self.closed = True


# The committed path refuses a payload whose units nobody stated
# (``units_undetermined``), so the fixture declares them once for the deployment,
# exactly as an operator must. The direct ``normalize`` tests below keep their
# original assertions: they call it without an authority, which is the
# pre-existing provider-transformed contract.
UNITS = {
    "price_unit": "source:index_point",
    "volume_unit": "source:lot",
    "open_interest_unit": "source:lot",
    "turnover_unit": "source:CNY",
}
UNIT_YAML = "units:\n" + "".join(f"  {key}: {value}\n" for key, value in UNITS.items())


@pytest.fixture
def config(tmp_path):
    return Config(
        root=tmp_path / "data",
        calendar_file=tmp_path / "calendar.json",
        start_date="2026-09-01",
        units=UNITS,
        source=Source(retries=2, retry_delay_seconds=0),
    )


@pytest.fixture
def calendar():
    return TradingCalendar(
        timezone="Asia/Shanghai",
        coverage_start="2026-09-01",
        coverage_end="2026-10-30",
        source="offline explicit fixture",
        trading_dates=[
            date(2026, 9, 1) + timedelta(days=i)
            for i in range(60)
            if (date(2026, 9, 1) + timedelta(days=i)).weekday() < 5
        ],
    )


def collector(config, journal, calendar, client, now=NOW, stop=None):
    return Collector(config, journal, calendar, client, stop, clock=lambda: now)


def patch_source_clients(monkeypatch, factory):
    """Both lane clients: a CLI test must not depend on which command picks which."""
    monkeypatch.setattr(cli, "ProcessClient", factory)
    monkeypatch.setattr(cli, "CffexCsvClient", factory)


def count(journal, table):
    assert table in {"futures_daily", "revisions", "attempts"}
    return journal.db.execute(f"SELECT count(*) FROM {table}").fetchone()[0]


@pytest.mark.parametrize(
    "value",
    ["20260918", "2026-9-18", "2026-09-18T00:00:00", "2026-02-30", 20260918, datetime(2026, 9, 18)],
)
def test_config_exact_dates(value):
    with pytest.raises(ValueError):
        iso_date(value)


@pytest.mark.parametrize(
    "changes",
    [
        {"extra_setting": True},
        {"source": {"provider": "yfinance"}},
        {"source": {"model": "AkshareToolTradeDateHistSina"}},
        {"source": {"credentials_env": {"x": "ENV"}}},
        {"source": {"retries": 3}},
        {"source": {"timeout_seconds": 301}},
        {"source": {"max_rows": True}},
        {"not_before": "18:30:10"},
    ],
)
def test_config_forbids_bad_settings(tmp_path, changes):
    with pytest.raises(ValidationError):
        Config.model_validate({"calendar_file": str(tmp_path / "cal.json"), **changes})


def test_config_resolves_relative_paths_and_launch_default(tmp_path):
    path = tmp_path / "job.yaml"
    path.write_text("calendar_file: calendar.json\nroot: state\nnot_before: '18:30'\n")
    loaded = load_config(path)
    assert loaded.root == tmp_path / "state"
    assert loaded.calendar_file == tmp_path / "calendar.json"
    assert loaded.start_date == date(2010, 4, 16)
    assert loaded.source.model == "AkshareFuturesHistDailyCffex"


def test_publication_cutoff_uses_shanghai(config):
    assert eligible_day(config, datetime(2026, 9, 18, 10, 29, tzinfo=UTC)) == date(2026, 9, 17)
    assert eligible_day(config, datetime(2026, 9, 18, 10, 30, tzinfo=UTC)) == DAY
    with pytest.raises(ValueError):
        eligible_day(config, datetime(2026, 9, 18, 10, tzinfo=UTC), DAY)
    with pytest.raises(ValueError):
        eligible_day(config, datetime(2026, 9, 18))


@pytest.mark.parametrize(
    "changes,code",
    [
        ({"date": "2026-02-30"}, "invalid_row_date"),
        ({"date": "2026-09-17"}, "row_date_mismatch"),
        ({"date": "2026-09-18T00:00:00"}, "invalid_row_date"),
        ({"symbol": "IO2610-C-4000"}, "invalid_futures_contract"),
        ({"symbol": "CU2610"}, "invalid_futures_contract"),
        ({"symbol": "IF2613"}, "invalid_futures_contract"),
        ({"symbol": "IF2600"}, "invalid_futures_contract"),
        ({"symbol": "if2610"}, "invalid_futures_contract"),
        ({"variety": "T"}, "variety_mismatch"),
        ({"open": "NaN"}, "invalid_open"),
        ({"high": "Infinity"}, "invalid_high"),
        ({"low": "1e999"}, "invalid_low"),
        ({"open": None}, "missing_or_zero_ohlc_with_volume"),
        ({"open": 0}, "missing_or_zero_ohlc_with_volume"),
        ({"high": 100}, "ohlc_order"),
        ({"settle": 0}, "invalid_settle"),
        ({"pre_settle": -1}, "invalid_pre_settle"),
        ({"volume": "1.1"}, "invalid_volume"),
        ({"volume": -1}, "invalid_volume"),
        ({"volume": True}, "invalid_volume"),
        ({"volume": None}, "missing_volume"),
        ({"open_interest": "34.5"}, "invalid_open_interest"),
        ({"open_interest": "9223372036854775808"}, "invalid_open_interest"),
        ({"turnover": -1}, "invalid_turnover"),
        ({"contract_symbol": "T2610"}, "conflicting_field_aliases"),
    ],
)
def test_invalid_rows_reject_entire_day(changes, code):
    with pytest.raises(DataError, match=code):
        normalize([row(symbol="IH2610"), row(**changes)], DAY)


@pytest.mark.parametrize("product", ["IF", "IH", "IC", "IM", "TS", "TF", "T", "TL"])
def test_supported_products_and_source_units(product):
    result = normalize([row(symbol=product + "2610", turnover_unit="source:万元")], DAY)[0]
    assert result["product"] == product and result["volume"] == 12
    assert result["turnover"] == 567.8
    assert result["source_units"]["turnover_unit"] == "source:万元"
    assert result["source_units"]["price_unit"] == "provider_defined"


def test_zero_volume_missing_and_zero_ohlc_retained():
    normalized = normalize([row(volume=0, open=0, low=0, high=None, close="--")], DAY)[0]
    assert normalized["open"] == 0 and normalized["close"] is None
    assert normalized["quality_flags"] == [
        "missing_ohlc_zero_volume",
        "zero_ohlc_sentinel_zero_volume",
    ]
    assert "expiration" not in normalized


def test_empty_and_duplicate_days():
    with pytest.raises(DataError, match="empty_trading_day"):
        normalize([], DAY)
    with pytest.raises(DataError, match="duplicate_contract"):
        normalize([row(), row()], DAY)


def test_calendar_requires_coverage_and_explicit_closed_days(config, calendar):
    assert calendar.days(date(2026, 9, 19), date(2026, 9, 20)) == []
    with pytest.raises(ValueError, match="coverage"):
        calendar.days(date(2026, 8, 31), DAY)
    with pytest.raises(ValueError, match="coverage"):
        calendar.days(DAY, date(2026, 10, 31))
    client = FakeClient()
    with Journal(config.root) as journal:
        report = collector(
            config, journal, calendar, client, datetime(2026, 9, 20, 11, tzinfo=UTC)
        ).backfill(date(2026, 9, 19), date(2026, 9, 20))
        assert report["status"] == "complete" and report["planned_days"] == 0
        assert not client.calls and count(journal, "futures_daily") == 0


def test_calendar_duplicate_and_out_of_coverage(calendar):
    payload = calendar.model_dump(mode="json")
    payload["trading_dates"].append(payload["trading_dates"][0])
    with pytest.raises(ValidationError):
        TradingCalendar.model_validate(payload)
    payload["trading_dates"][-1] = "2026-08-31"
    with pytest.raises(ValidationError):
        TradingCalendar.model_validate(payload)


def test_basket_one_call_per_day_idempotency_and_metadata(config, calendar):
    client = FakeClient([row(), row(symbol="T2612")])
    with Journal(config.root) as journal:
        engine = collector(config, journal, calendar, client)
        result = engine.backfill(DAY, DAY)
        assert result["status"] == "complete" and result["rows_committed"] == 2
        assert result["provider_calls"] == 1 and result["end_inclusive"] is True
        assert client.calls[0][2] == {"date": "2026-09-18"}
        assert client.calls[0][1]["credentials_env"] == {}
        again = engine.backfill(DAY, DAY)
        assert again["skipped_days"] == 1 and len(client.calls) == 1
        assert count(journal, "futures_daily") == 2 and count(journal, "revisions") == 1
        current = dict(journal.db.execute("SELECT * FROM futures_daily LIMIT 1").fetchone())
        assert current["event_time_precision"] == "date" and current["publication_time"] is None
        assert current["observed_at"].endswith("+00:00")
        assert json.loads(current["metadata_json"])["openbb-akshare"]["version"] == "offline"
        raw = list((config.root / "raw").rglob("*.json"))
        assert len(raw) == 1 and raw[0].stat().st_mode & 0o222 == 0
        assert json.loads(raw[0].read_text())["rows"][0]["volume"] == "12.0"
    assert status(config.root)["row_count"] == 2


def test_immutable_raw_refuses_overwrite(config):
    with Journal(config.root) as journal:
        raw = journal.archive("fixed", {"request": {"date": "2026-09-18"}, "rows": [row()]})
        before = (config.root / raw).read_bytes()
        with pytest.raises(FileExistsError):
            journal.archive("fixed", {"request": {"date": "2026-09-18"}, "rows": []})
        assert (config.root / raw).read_bytes() == before


def test_refresh_corrections_history_and_unchanged(config, calendar):
    client = FakeClient([row(), row(symbol="T2612")])
    with Journal(config.root) as journal:
        engine = collector(config, journal, calendar, client)
        engine.backfill(DAY, DAY)
        client.rows = [row(close=101.5), row(symbol="T2612")]
        result = engine.backfill(DAY, DAY, refresh=True)
        assert result["changed_days"] == 1
        assert count(journal, "futures_daily") == 2 and count(journal, "revisions") == 2
        histories = [
            json.loads(r[0])
            for r in journal.db.execute("SELECT rows_json FROM revisions ORDER BY observed_at")
        ]
        assert len(histories[0]) == 2 and histories[1][0]["close"] == 101.5
        again = engine.backfill(DAY, DAY, refresh=True)
        assert again["unchanged_days"] == 1 and count(journal, "revisions") == 2
        assert count(journal, "attempts") == 3


def test_empty_trading_day_retries_fails_and_restart(config, calendar):
    client = FakeClient([])
    with Journal(config.root) as journal:
        result = collector(config, journal, calendar, client).backfill(DAY, DAY)
        assert result["status"] == "failed" and result["provider_calls"] == 3
        assert result["errors"] == [{"trading_date": "2026-09-18", "code": "empty_trading_day"}]
        assert count(journal, "futures_daily") == count(journal, "revisions") == 0
        assert count(journal, "attempts") == 3
        assert not journal.complete("2026-09-18", "akshare", config.source.model)
    with Journal(config.root) as journal:
        result = collector(config, journal, calendar, FakeClient()).backfill(DAY, DAY)
        assert result["status"] == "complete" and count(journal, "futures_daily") == 1


def test_provider_retry_success_and_bound(config, calendar):
    with Journal(config.root) as journal:
        result = collector(config, journal, calendar, FakeClient(failures=2)).backfill(DAY, DAY)
        assert result["status"] == "complete" and result["provider_calls"] == 3
        result = collector(config, journal, calendar, FakeClient(failures=10)).backfill(
            DAY, DAY, refresh=True
        )
        assert result["status"] == "failed" and result["provider_calls"] == 3
        assert count(journal, "futures_daily") == 1  # previous good revision preserved
        assert count(journal, "revisions") == 1


def test_max_days_partial_and_resume(config, calendar):
    client = FakeClient()
    with Journal(config.root) as journal:
        engine = collector(config, journal, calendar, client)
        first = engine.backfill(date(2026, 9, 16), DAY, max_days=1)
        assert first["status"] == "partial" and first["deferred_days"] == 2
        assert first["provider_calls"] == 1
        second = engine.backfill(date(2026, 9, 16), DAY, max_days=2)
        assert second["status"] == "complete" and second["skipped_days"] == 1
        assert len(client.calls) == 3


def test_atomic_day_rollback_after_first_insert(config, calendar):
    client = FakeClient([row(), row(symbol="T2612")])
    with Journal(config.root) as journal:
        journal.db.execute(
            "CREATE TRIGGER inject_failure BEFORE INSERT ON futures_daily "
            "WHEN NEW.contract_symbol='T2612' BEGIN SELECT RAISE(ABORT,'test'); END"
        )
        with pytest.raises(sqlite3.IntegrityError):
            collector(config, journal, calendar, client).backfill(DAY, DAY)
        assert count(journal, "futures_daily") == count(journal, "revisions") == 0
        assert not journal.complete("2026-09-18", "akshare", config.source.model)
        assert len(list((config.root / "raw").rglob("*.json"))) == 1
        journal.db.execute("DROP TRIGGER inject_failure")
    with Journal(config.root) as journal:
        assert journal.db.execute("SELECT status FROM attempts").fetchone()[0] == "failed"
        result = collector(config, journal, calendar, client).backfill(DAY, DAY)
        assert result["status"] == "complete" and count(journal, "futures_daily") == 2


def test_crashed_running_run_recovery(config):
    with Journal(config.root) as journal:
        run = journal.start_run("backfill", {"status": "running"})
        journal.begin_attempt(run, "2026-09-18")
    with Journal(config.root) as journal:
        assert journal.db.execute("SELECT status FROM runs").fetchone()[0] == "interrupted"
        assert journal.db.execute("SELECT error FROM day_status").fetchone()[0] == "interrupted"
        assert journal.db.execute("SELECT status FROM attempts").fetchone()[0] == "interrupted"


def test_one_writer_lock_and_reader(config):
    with Journal(config.root):
        with pytest.raises(LockBusy):
            with Journal(config.root):
                pass
        assert status(config.root)["status"] == "ok"
    with Journal(config.root):
        pass


def test_update_rolling_seven_calendar_days_and_durable_catchup(config, calendar):
    client = FakeClient()
    with Journal(config.root) as journal:
        engine = collector(config, journal, calendar, client)
        first = engine.update(DAY)
        expected = calendar.days(date(2026, 9, 12), DAY)
        assert first["provider_calls"] == len(expected)
        assert journal.get_meta("update_cursor") == DAY.isoformat()
        client.calls.clear()
        engine.clock = lambda: datetime(2026, 9, 30, 11, tzinfo=UTC)
        second = engine.update(date(2026, 9, 30))
        requested = [entry[2]["date"] for entry in client.calls]
        assert requested == [
            day.isoformat() for day in calendar.days(date(2026, 9, 19), date(2026, 9, 30))
        ]
        assert second["update_cursor"] == "2026-09-30"
        client.calls.clear()
        again = engine.update(date(2026, 9, 30))
        assert [entry[2]["date"] for entry in client.calls] == [
            day.isoformat() for day in calendar.days(date(2026, 9, 24), date(2026, 9, 30))
        ]
        assert again["changed_days"] == 0


def test_explicit_update_origin_failure_retry_and_cursor_gap(config, calendar):
    config.update_start_date = date(2026, 9, 1)
    client = FakeClient(
        lambda request: [] if request["date"] == "2026-09-02" else [row(iso_date(request["date"]))]
    )
    with Journal(config.root) as journal:
        result = collector(config, journal, calendar, client).update(DAY)
        assert result["status"] == "partial" and result["update_cursor"] == "2026-09-01"
        assert journal.get_meta("update_origin") == "2026-09-01"
    client = FakeClient()
    with Journal(config.root) as journal:
        result = collector(config, journal, calendar, client).update(DAY)
        assert result["status"] == "complete" and result["update_cursor"] == "2026-09-18"
        assert "2026-09-02" in [call[2]["date"] for call in client.calls]
        assert "2026-09-01" not in [call[2]["date"] for call in client.calls]


def test_update_closed_day_advances_cursor_without_provider(config, calendar):
    config.update_start_date = date(2026, 9, 14)
    client = FakeClient()
    with Journal(config.root) as journal:
        engine = collector(config, journal, calendar, client, datetime(2026, 9, 20, 11, tzinfo=UTC))
        result = engine.update(date(2026, 9, 20))
        assert result["update_cursor"] == "2026-09-20"
        assert all(call[2]["date"] <= "2026-09-18" for call in client.calls)


def test_cancel_and_preflight_coverage_make_no_provider_calls(config, calendar):
    client = FakeClient()
    stop = threading.Event()
    stop.set()
    with Journal(config.root) as journal:
        result = collector(config, journal, calendar, client, stop=stop).backfill(DAY, DAY)
        assert result["status"] == "interrupted" and not client.calls
        with pytest.raises(ValueError):
            collector(config, journal, calendar, client).backfill(date(2026, 8, 31), DAY)
        assert not client.calls


def test_export_inclusive_bounds_and_uninitialized_status(config, calendar, tmp_path):
    assert status(config.root)["status"] == "not_initialized"
    with Journal(config.root) as journal:
        collector(config, journal, calendar, FakeClient()).backfill(date(2026, 9, 16), DAY)
    output = tmp_path / "quotes.csv"
    assert export_csv(config.root, output, "2026-09-17", "2026-09-18") == 2
    text = output.read_text()
    assert "2026-09-17" in text and "2026-09-16" not in text
    assert "publication_time" in text and "revision_id" in text
    with pytest.raises(ValueError):
        export_csv(config.root, config.root / "cffex.sqlite3")


def test_calendar_refresh_without_old_file_and_reference_semantics(config):
    client = FakeClient([{"trade_date": "2026-09-18"}, {"trade_date": "2026-09-17"}])
    with Journal(config.root) as journal:
        result = refresh_calendar(config, journal, client)
        assert result["status"] == "complete" and result["trading_dates"] == 2
        assert client.calls[0][1]["model"] == "AkshareToolTradeDateHistSina"
        assert client.calls[0][2] == {}
        parsed = load_calendar(config.calendar_file)
        assert parsed.coverage_start == date(2026, 9, 17) and parsed.coverage_end == DAY
        assert parsed.trading_dates == [date(2026, 9, 17), DAY]
        assert (
            "Sina/SSE reference calendar" in parsed.source and parsed.captured_at.tzinfo is not None
        )


@pytest.mark.parametrize(
    "bad_rows",
    [
        [],
        [{"trade_date": "20260918"}],
        [{"trade_date": "2026-09-19"}],
        [{"trade_date": "2026-09-18"}] * 2,
        [{"date": "2026-09-18"}],
    ],
)
def test_bad_calendar_refresh_preserves_prior(config, bad_rows):
    good = FakeClient([{"trade_date": "2026-09-17"}, {"trade_date": "2026-09-18"}])
    with Journal(config.root) as journal:
        refresh_calendar(config, journal, good)
        before = config.calendar_file.read_bytes()
        bad = FakeClient(bad_rows)
        with pytest.raises(ProviderFailure):
            refresh_calendar(config, journal, bad)
        assert len(bad.calls) == 3 and config.calendar_file.read_bytes() == before


def test_calendar_refresh_rejects_coverage_regression(config):
    with Journal(config.root) as journal:
        refresh_calendar(
            config,
            journal,
            FakeClient([{"trade_date": "2026-09-17"}, {"trade_date": "2026-09-18"}]),
        )
        before = config.calendar_file.read_bytes()
        with pytest.raises(ProviderFailure):
            refresh_calendar(config, journal, FakeClient([{"trade_date": "2026-09-18"}]))
        assert config.calendar_file.read_bytes() == before


def test_cli_backfill_partial_status_export_and_config_error(
    config, calendar, tmp_path, monkeypatch, capsys
):
    settings = tmp_path / "job.yaml"
    settings.write_text(
        f"calendar_file: {config.calendar_file}\nroot: {config.root}\nstart_date: '2026-09-01'\n"
        + UNIT_YAML
    )
    config.calendar_file.write_text(calendar.model_dump_json())
    clients = []

    def factory(*_):
        client = FakeClient()
        clients.append(client)
        return client

    patch_source_clients(monkeypatch, factory)
    monkeypatch.setattr(cli, "datetime", FrozenDateTime)
    assert (
        cli.main(
            [
                "backfill",
                "--config",
                str(settings),
                "--start-date",
                "2026-09-16",
                "--end-date",
                "2026-09-18",
                "--max-days",
                "1",
            ]
        )
        == 3
    )
    result = json.loads(capsys.readouterr().out)
    assert result["status"] == "partial" and result["deferred_days"] == 2 and clients[0].closed
    assert (
        cli.main(
            [
                "backfill",
                "--config",
                str(settings),
                "--start-date",
                "2026-09-16",
                "--end-date",
                "2026-09-18",
            ]
        )
        == 0
    )
    capsys.readouterr()
    assert cli.main(["status", "--config", str(settings)]) == 0
    assert json.loads(capsys.readouterr().out)["row_count"] == 3
    assert (
        cli.main(["export", "--config", str(settings), "--output", str(tmp_path / "export.csv")])
        == 0
    )
    assert json.loads(capsys.readouterr().out)["row_count"] == 3
    assert cli.main(["backfill", "--config", str(settings), "--max-days", "0"]) == 2
    assert json.loads(capsys.readouterr().out)["status"] == "error"


def test_cli_calendar_refresh_without_calendar_and_lock_exit(config, tmp_path, monkeypatch, capsys):
    settings = tmp_path / "job.yaml"
    settings.write_text(f"calendar_file: {config.calendar_file}\nroot: {config.root}\n")
    client = FakeClient([{"trade_date": "2026-09-17"}, {"trade_date": "2026-09-18"}])
    patch_source_clients(monkeypatch, lambda *_: client)
    assert cli.main(["calendar-refresh", "--config", str(settings)]) == 0
    assert json.loads(capsys.readouterr().out)["status"] == "complete" and client.closed
    with Journal(config.root):
        assert cli.main(["calendar-refresh", "--config", str(settings)]) == 5
    assert json.loads(capsys.readouterr().out)["code"] == "writer_locked"


def test_yaml_duplicate_keys_fail(tmp_path):
    settings = tmp_path / "job.yaml"
    settings.write_text("calendar_file: one.json\ncalendar_file: two.json\n")
    with pytest.raises(ValueError, match="unique"):
        load_config(settings)


def test_invalid_refresh_cannot_replace_good_snapshot(config, calendar):
    client = FakeClient([row(), row(symbol="T2612")])
    with Journal(config.root) as journal:
        engine = collector(config, journal, calendar, client)
        engine.backfill(DAY, DAY)
        old = [
            tuple(value)
            for value in journal.db.execute("SELECT * FROM futures_daily ORDER BY contract_symbol")
        ]
        client.rows = [row(close=101.5), row(symbol="T2612", settle=-1)]
        result = engine.backfill(DAY, DAY, refresh=True)
        assert result["status"] == "failed" and result["provider_calls"] == 3
        assert [
            tuple(value)
            for value in journal.db.execute("SELECT * FROM futures_daily ORDER BY contract_symbol")
        ] == old
        assert count(journal, "revisions") == 1
        assert len(list((config.root / "raw").rglob("*.json"))) == 4


def test_atomic_correction_rollback_preserves_previous(config, calendar):
    client = FakeClient([row(), row(symbol="T2612")])
    with Journal(config.root) as journal:
        engine = collector(config, journal, calendar, client)
        engine.backfill(DAY, DAY)
        journal.db.execute(
            "CREATE TRIGGER inject_update_failure BEFORE UPDATE ON futures_daily "
            "WHEN NEW.contract_symbol='T2612' BEGIN SELECT RAISE(ABORT,'test'); END"
        )
        client.rows = [row(close=101.5), row(symbol="T2612", close=101.5)]
        with pytest.raises(sqlite3.IntegrityError):
            engine.backfill(DAY, DAY, refresh=True)
        assert count(journal, "revisions") == 1
        assert [value[0] for value in journal.db.execute("SELECT close FROM futures_daily")] == [
            102.0,
            102.0,
        ]


def test_update_picks_up_corrected_rolling_values(config, calendar):
    client = FakeClient()
    with Journal(config.root) as journal:
        engine = collector(config, journal, calendar, client)
        engine.update(DAY)
        before = count(journal, "revisions")
        client.rows = lambda request: [row(iso_date(request["date"]), close=101.5)]
        result = engine.update(DAY)
        assert result["changed_days"] == len(calendar.days(date(2026, 9, 12), DAY))
        assert count(journal, "revisions") == before + result["changed_days"]
        assert (
            journal.db.execute(
                "SELECT close FROM futures_daily WHERE trading_date=?", (DAY.isoformat(),)
            ).fetchone()[0]
            == 101.5
        )


def test_calendar_atomic_replace_failure_leaves_old_file(config, monkeypatch):
    import dq_historical_market_data_collector.cffex.runtime as runtime

    good = FakeClient([{"trade_date": "2026-09-17"}, {"trade_date": "2026-09-18"}])
    with Journal(config.root) as journal:
        refresh_calendar(config, journal, good)
        before = config.calendar_file.read_bytes()

        def fail_replace(*_):
            raise OSError("injected filesystem failure")

        monkeypatch.setattr(runtime.os, "replace", fail_replace)
        with pytest.raises(OSError):
            refresh_calendar(config, journal, good)
        assert config.calendar_file.read_bytes() == before
        assert not list(config.calendar_file.parent.glob(".calendar.json.*.tmp"))
        assert (
            journal.db.execute(
                "SELECT status FROM runs ORDER BY started_at DESC LIMIT 1"
            ).fetchone()[0]
            == "failed"
        )


def test_calendar_failures_journaled_without_date_checkpoint(config):
    with Journal(config.root) as journal:
        with pytest.raises(ProviderFailure, match="empty_calendar"):
            refresh_calendar(config, journal, FakeClient([]))
        report = json.loads(journal.db.execute("SELECT summary_json FROM runs").fetchone()[0])
        assert report["provider_calls"] == 3 and len(report["raw_paths"]) == 3
        assert report["status"] == "failed"
        assert journal.db.execute("SELECT count(*) FROM day_status").fetchone()[0] == 0
        assert not config.calendar_file.exists()


def test_export_inside_root_but_not_protected_storage(config, calendar):
    with Journal(config.root) as journal:
        collector(config, journal, calendar, FakeClient()).backfill(DAY, DAY)
    assert export_csv(config.root, config.root / "exports" / "quotes.csv") == 1
    for path in (
        config.root / "writer.lock",
        config.root / "cffex.sqlite3-wal",
        config.root / "raw" / "oops.csv",
    ):
        with pytest.raises(ValueError, match="protected"):
            export_csv(config.root, path)


def test_cli_update_cutoff_and_error_codes(config, calendar, tmp_path, monkeypatch, capsys):
    settings = tmp_path / "job.yaml"
    settings.write_text(
        f"calendar_file: {config.calendar_file}\nroot: {config.root}\nstart_date: '2026-09-01'\n"
        + UNIT_YAML
    )
    config.calendar_file.write_text(calendar.model_dump_json())
    client = FakeClient()
    patch_source_clients(monkeypatch, lambda *_: client)
    monkeypatch.setattr(cli, "datetime", FrozenDateTime)
    assert cli.main(["update", "--config", str(settings)]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["end_date"] == "2026-09-18" and result["update_cursor"] == "2026-09-18"
    assert cli.main(["update", "--config", str(settings), "--as-of", "2026-09-19"]) == 2
    assert json.loads(capsys.readouterr().out)["code"] == "publication_cutoff_exceeded"
    assert (
        cli.main(["export", "--config", str(settings), "--output", str(config.calendar_file)]) == 2
    )
    assert json.loads(capsys.readouterr().out)["code"] == "protected_export_path"


def test_cli_failures_and_validation_diagnostics(config, calendar, tmp_path, monkeypatch, capsys):
    settings = tmp_path / "job.yaml"
    settings.write_text(
        f"calendar_file: {config.calendar_file}\nroot: {config.root}\n"
        "start_date: '2026-09-01'\nsource:\n  retries: 0\n" + UNIT_YAML
    )
    config.calendar_file.write_text(calendar.model_dump_json())
    patch_source_clients(monkeypatch, lambda *_: FakeClient([]))
    monkeypatch.setattr(cli, "datetime", FrozenDateTime)
    assert (
        cli.main(
            [
                "backfill",
                "--config",
                str(settings),
                "--start-date",
                "2026-09-18",
                "--end-date",
                "2026-09-18",
            ]
        )
        == 1
    )
    assert json.loads(capsys.readouterr().out)["status"] == "failed"
    before = config.calendar_file.read_bytes()
    assert cli.main(["calendar-refresh", "--config", str(settings)]) == 1
    assert json.loads(capsys.readouterr().out)["code"] == "empty_calendar"
    assert config.calendar_file.read_bytes() == before
    settings.write_text(
        f"calendar_file: {config.calendar_file}\nsource:\n  credentials_env:\n"
        "    secret: DO_NOT_REPORT_THIS_VALUE\n"
    )
    assert cli.main(["status", "--config", str(settings)]) == 2
    text = capsys.readouterr().out
    assert json.loads(text)["code"] == "invalid_configuration"
    assert "DO_NOT_REPORT_THIS_VALUE" not in text


@pytest.mark.parametrize("command", ["backfill", "update"])
def test_same_day_contract_shrink_fails_closed_and_retries(config, calendar, command):
    client = FakeClient(
        lambda request: [
            row(iso_date(request["date"])),
            row(iso_date(request["date"]), symbol="T2612"),
        ]
    )
    with Journal(config.root) as journal:
        engine = collector(config, journal, calendar, client)
        engine.update(DAY)
        before = [
            tuple(value)
            for value in journal.db.execute(
                "SELECT * FROM futures_daily ORDER BY trading_date,contract_symbol"
            )
        ]
        revision_count = count(journal, "revisions")
        client.rows = lambda request: [row(iso_date(request["date"]), close=101.5)]
        result = (
            engine.update(DAY) if command == "update" else engine.backfill(DAY, DAY, refresh=True)
        )
        assert result["completed_days"] == 0
        assert result["provider_calls"] == result["requested_days"] * 3
        assert {error["code"] for error in result["errors"]} == {"contract_set_shrink"}
        assert [
            tuple(value)
            for value in journal.db.execute(
                "SELECT * FROM futures_daily ORDER BY trading_date,contract_symbol"
            )
        ] == before
        assert count(journal, "revisions") == revision_count
        failed = journal.db.execute(
            "SELECT raw_path,error FROM attempts WHERE status='failed'"
        ).fetchall()
        assert len(failed) == result["provider_calls"]
        assert all(
            value[1] == "contract_set_shrink" and (config.root / value[0]).exists()
            for value in failed
        )
        assert not journal.complete(DAY.isoformat(), "akshare", config.source.model)
        if command == "backfill":
            assert journal.get_meta("update_cursor") is None
        client.rows = lambda request: [
            row(iso_date(request["date"])),
            row(iso_date(request["date"]), symbol="T2612"),
        ]
        retry = engine.backfill(DAY, DAY)
        assert retry["status"] == "complete" and retry["provider_calls"] == 1
        assert count(journal, "revisions") == revision_count


def test_storage_defensively_rejects_contract_shrink_before_mutation(config, calendar):
    with Journal(config.root) as journal:
        client = FakeClient([row(), row(symbol="T2612")])
        collector(config, journal, calendar, client).backfill(DAY, DAY)
        before = [tuple(value) for value in journal.db.execute("SELECT * FROM futures_daily")]
        run_id = journal.start_run("backfill", {"status": "running"})
        attempt = journal.begin_attempt(run_id, DAY.isoformat())
        with pytest.raises(DataError, match="^contract_set_shrink$"):
            journal.commit_day(
                attempt,
                DAY.isoformat(),
                normalize([row(close=101.5)], DAY),
                {},
                NOW.isoformat(),
                "akshare",
                config.source.model,
            )
        assert not journal.db.in_transaction
        assert count(journal, "revisions") == 1
        assert [
            tuple(value) for value in journal.db.execute("SELECT * FROM futures_daily")
        ] == before
        revision_id = journal.db.execute("SELECT revision_id FROM day_status").fetchone()[0]
        assert revision_id == before[0][-1]


def test_contract_sets_may_grow_and_change_between_days(config, calendar):
    client = FakeClient([row(), row(symbol="T2612")])
    with Journal(config.root) as journal:
        engine = collector(config, journal, calendar, client)
        engine.backfill(DAY, DAY)
        client.rows = [row(), row(symbol="T2612"), row(symbol="IH2610")]
        assert engine.backfill(DAY, DAY, refresh=True)["rows_committed"] == 3
        next_day = date(2026, 9, 21)
        engine.clock = lambda: datetime(2026, 9, 21, 11, tzinfo=UTC)
        client.rows = [row(next_day, symbol="IH2610")]
        assert engine.backfill(next_day, next_day)["status"] == "complete"
        assert count(journal, "futures_daily") == 4


def test_calendar_refresh_rejects_interior_session_removal(config):
    dates = ["2026-09-16", "2026-09-17", "2026-09-18"]
    with Journal(config.root) as journal:
        refresh_calendar(config, journal, FakeClient([{"trade_date": value} for value in dates]))
        before = config.calendar_file.read_bytes()
        client = FakeClient([{"trade_date": dates[0]}, {"trade_date": dates[-1]}])
        with pytest.raises(ProviderFailure, match="calendar_sessions_removed"):
            refresh_calendar(config, journal, client)
        assert config.calendar_file.read_bytes() == before and len(client.calls) == 3
        report = json.loads(
            journal.db.execute(
                "SELECT summary_json FROM runs ORDER BY started_at DESC LIMIT 1"
            ).fetchone()[0]
        )
        assert report["status"] == "failed" and len(report["raw_paths"]) == 3
        assert all((config.root / path).exists() for path in report["raw_paths"])
        assert {error["code"] for error in report["errors"]} == {"calendar_sessions_removed"}


def test_calendar_refresh_allows_added_sessions(config):
    with Journal(config.root) as journal:
        refresh_calendar(
            config,
            journal,
            FakeClient([{"trade_date": "2026-09-16"}, {"trade_date": "2026-09-18"}]),
        )
        result = refresh_calendar(
            config,
            journal,
            FakeClient(
                [
                    {"trade_date": day}
                    for day in ["2026-09-16", "2026-09-17", "2026-09-18", "2026-09-21"]
                ]
            ),
        )
        assert result["status"] == "complete" and result["trading_dates"] == 4


@pytest.mark.parametrize("rollback", ["ABORT", "ROLLBACK"])
def test_fatal_update_terminalizes_work_invalidates_cursor_and_retries(config, calendar, rollback):
    client = FakeClient()
    with Journal(config.root) as journal:
        engine = collector(config, journal, calendar, client)
        engine.update(DAY)
        assert journal.get_meta("update_cursor") == DAY.isoformat()
        before = [tuple(value) for value in journal.db.execute("SELECT * FROM futures_daily")]
        revisions = count(journal, "revisions")
        journal.db.execute(
            "CREATE TRIGGER inject_failure BEFORE UPDATE ON futures_daily "
            f"WHEN NEW.trading_date='2026-09-14' BEGIN SELECT RAISE({rollback},'test fatal'); END"
        )
        client.rows = lambda request: [row(iso_date(request["date"]), close=101.5)]
        with pytest.raises(sqlite3.IntegrityError, match="test fatal"):
            engine.update(DAY)
        assert not journal.db.in_transaction
        assert journal.get_meta("update_cursor") is None
        assert status(config.root)["update_cursor"] is None
        assert [
            tuple(value) for value in journal.db.execute("SELECT * FROM futures_daily")
        ] == before
        assert count(journal, "revisions") == revisions
        running_attempts = journal.db.execute(
            "SELECT count(*) FROM attempts WHERE status='running'"
        ).fetchone()[0]
        assert running_attempts == 0
        running_days = journal.db.execute(
            "SELECT count(*) FROM day_status WHERE status='running'"
        ).fetchone()[0]
        assert running_days == 0
        report = json.loads(
            journal.db.execute(
                "SELECT summary_json FROM runs ORDER BY started_at DESC LIMIT 1"
            ).fetchone()[0]
        )
        assert report["status"] == "failed"
        assert report["errors"][-1]["error_type"] == "IntegrityError"
    with Journal(config.root) as journal:
        assert journal.get_meta("update_cursor") is None
        assert not journal.complete("2026-09-14", "akshare", config.source.model)
        journal.db.execute("DROP TRIGGER inject_failure")
        client.calls.clear()
        result = collector(config, journal, calendar, client).update(DAY)
        assert result["status"] == "complete" and result["update_cursor"] == DAY.isoformat()
        assert client.calls[0][2]["date"] == "2026-09-14"


@pytest.mark.parametrize("running_kind", ["attempt", "day"])
def test_recovery_invalidates_cursor_for_running_work(config, calendar, running_kind):
    with Journal(config.root) as journal:
        collector(config, journal, calendar, FakeClient()).update(DAY)
        run_id = journal.start_run("backfill", {"status": "running"})
        if running_kind == "attempt":
            journal.begin_attempt(run_id, "2026-09-14")
        else:
            journal.db.execute(
                "UPDATE day_status SET status='running',last_run_id=? "
                "WHERE trading_date='2026-09-14'",
                (run_id,),
            )
        journal.set_meta("update_cursor", DAY.isoformat())  # Legacy crash state.
    with Journal(config.root) as journal:
        assert journal.get_meta("update_cursor") is None
        assert not journal.complete("2026-09-14", "akshare", config.source.model)
        result = collector(config, journal, calendar, FakeClient()).update(DAY)
        assert result["status"] == "complete" and result["update_cursor"] == DAY.isoformat()


def test_read_only_status_and_clean_open_preserve_valid_cursor(config, calendar):
    with Journal(config.root) as journal:
        collector(config, journal, calendar, FakeClient()).update(DAY)
        assert status(config.root)["update_cursor"] == DAY.isoformat()
        assert journal.get_meta("update_cursor") == DAY.isoformat()
    with Journal(config.root) as journal:
        assert journal.get_meta("update_cursor") == DAY.isoformat()


@pytest.mark.parametrize("interrupt", ["stop", "keyboard"])
def test_interrupted_update_invalidates_cursor_and_terminalizes_work(config, calendar, interrupt):
    with Journal(config.root) as journal:
        collector(config, journal, calendar, FakeClient()).update(DAY)
        stop = threading.Event()
        if interrupt == "stop":
            stop.set()
            result = collector(config, journal, calendar, FakeClient(), stop=stop).update(DAY)
            assert result["status"] == "interrupted" and result["update_cursor"] is None
        else:

            def interrupted_fetch(_):
                raise KeyboardInterrupt

            with pytest.raises(KeyboardInterrupt):
                collector(config, journal, calendar, FakeClient(interrupted_fetch)).update(DAY)
        assert journal.get_meta("update_cursor") is None
        running_attempts = journal.db.execute(
            "SELECT count(*) FROM attempts WHERE status='running'"
        ).fetchone()[0]
        assert running_attempts == 0
        running_days = journal.db.execute(
            "SELECT count(*) FROM day_status WHERE status='running'"
        ).fetchone()[0]
        assert running_days == 0
        run_status = journal.db.execute(
            "SELECT status FROM runs ORDER BY started_at DESC LIMIT 1"
        ).fetchone()[0]
        assert run_status == "interrupted"


def test_fatal_cleanup_does_not_mask_original_error(config, calendar, monkeypatch):
    with Journal(config.root) as journal:

        def fatal_fetch(_):
            raise RuntimeError("original failure")

        def broken_finish(*_):
            raise OSError("cleanup failure")

        monkeypatch.setattr(journal, "finish_run", broken_finish)
        with pytest.raises(RuntimeError, match="original failure"):
            collector(config, journal, calendar, FakeClient(fatal_fetch)).backfill(DAY, DAY)


@pytest.mark.parametrize("cleanup_fails", [False, True])
def test_update_cursor_finalization_failure_preserves_data_and_error(
    config, calendar, monkeypatch, cleanup_fails
):
    config.update_start_date = DAY
    with Journal(config.root) as journal:
        engine = collector(config, journal, calendar, FakeClient())
        engine.update(DAY)
        journal.db.execute(
            "CREATE TRIGGER cursor_failure BEFORE INSERT ON meta "
            "WHEN NEW.key='update_cursor' BEGIN SELECT RAISE(ROLLBACK,'cursorfatal'); END"
        )
        if cleanup_fails:

            def broken_abort(*_):
                raise OSError("cleanup failure")

            monkeypatch.setattr(journal, "abort_run", broken_abort)
        with pytest.raises(sqlite3.IntegrityError, match="cursorfatal") as caught:
            engine.update(DAY)
        if cleanup_fails:
            assert "CFFEX cleanup failed: OSError" in caught.value.__notes__
    with Journal(config.root) as journal:
        latest = journal.db.execute(
            "SELECT status,summary_json FROM runs ORDER BY started_at DESC LIMIT 1"
        ).fetchone()
        assert latest[0] == ("interrupted" if cleanup_fails else "failed")
        report = json.loads(latest[1])
        assert report["status"] == latest[0]
        assert report.get("update_cursor") is None
        assert journal.get_meta("update_cursor") is None
        assert count(journal, "futures_daily") == count(journal, "revisions") == 1
        assert journal.db.execute("SELECT close FROM futures_daily").fetchone()[0] == 102.0
        if not cleanup_fails:
            assert report["errors"][-1]["error_type"] == "IntegrityError"


@pytest.mark.parametrize("command", ["backfill", "update"])
def test_cancel_empty_selection_does_not_complete(config, calendar, command):
    start, end = date(2026, 9, 19), date(2026, 9, 20)
    config.update_start_date = start
    stop = threading.Event()
    stop.set()
    client = FakeClient()
    with Journal(config.root) as journal:
        journal.set_meta("update_cursor", DAY.isoformat())
        engine = collector(
            config, journal, calendar, client, datetime(2026, 9, 20, 11, tzinfo=UTC), stop
        )
        report = engine.update(end) if command == "update" else engine.backfill(start, end)
        assert report["status"] == "interrupted"
        assert report["requested_days"] == report["provider_calls"] == 0
        assert not client.calls
    with Journal(config.root) as journal:
        assert journal.get_meta("update_cursor") is None
        assert journal.db.execute("SELECT status FROM runs").fetchone()[0] == "interrupted"


@pytest.mark.parametrize("stage", ["after_collect", "cursor_scan", "finish_run"])
def test_update_cancellation_during_finalization(config, calendar, monkeypatch, stage):
    config.update_start_date = DAY
    stop = threading.Event()
    with Journal(config.root) as journal:
        engine = collector(config, journal, calendar, FakeClient(), stop=stop)
        if stage == "after_collect":
            original = engine._collect

            def stop_after_collect(*args, **kwargs):
                report = original(*args, **kwargs)
                stop.set()
                return report

            monkeypatch.setattr(engine, "_collect", stop_after_collect)
        elif stage == "cursor_scan":
            original_complete = journal.complete
            original_collect = engine._collect

            def stop_during_scan(*args):
                stop.set()
                return original_complete(*args)

            def arm_cursor_scan(*args, **kwargs):
                report = original_collect(*args, **kwargs)
                monkeypatch.setattr(journal, "complete", stop_during_scan)
                return report

            monkeypatch.setattr(engine, "_collect", arm_cursor_scan)
        else:
            original_finish = journal.finish_run

            def stop_after_finish(*args):
                original_finish(*args)
                stop.set()

            monkeypatch.setattr(journal, "finish_run", stop_after_finish)
        try:
            report = engine.update(DAY)
        except KeyboardInterrupt:
            pass
        else:
            assert report["status"] == "interrupted" and report["update_cursor"] is None
    with Journal(config.root) as journal:
        latest = journal.db.execute("SELECT status,summary_json FROM runs").fetchone()
        assert latest[0] == "interrupted"
        assert json.loads(latest[1]).get("update_cursor") is None
        assert journal.get_meta("update_cursor") is None


@pytest.mark.parametrize("failure", ["cursorfatal", "sigterm"])
def test_cli_update_finalization_failure_and_empty_cancellation(
    config, calendar, tmp_path, monkeypatch, capsys, failure
):
    import os
    import signal

    settings = tmp_path / "job.yaml"
    origin = "2026-09-19" if failure == "sigterm" else DAY.isoformat()
    settings.write_text(
        f"calendar_file: {config.calendar_file}\nroot: {config.root}\n"
        f"start_date: '2026-09-01'\nupdate_start_date: '{origin}'\n" + UNIT_YAML
    )
    config.calendar_file.write_text(calendar.model_dump_json())
    client = FakeClient()
    patch_source_clients(monkeypatch, lambda *_: client)

    class WeekendDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 9, 20, 11, tzinfo=UTC).astimezone(tz)

    monkeypatch.setattr(cli, "datetime", WeekendDateTime)
    original_enter = Journal.__enter__

    def enter_with_fault(self):
        journal = original_enter(self)
        if failure == "sigterm":
            os.kill(os.getpid(), signal.SIGTERM)
        else:
            journal.db.execute(
                "CREATE TRIGGER cursor_failure BEFORE INSERT ON meta "
                "WHEN NEW.key='update_cursor' BEGIN SELECT RAISE(ROLLBACK,'cursorfatal'); END"
            )
        return journal

    with monkeypatch.context() as patch:
        patch.setattr(Journal, "__enter__", enter_with_fault)
        assert cli.main(["update", "--config", str(settings)]) == (4 if failure == "sigterm" else 2)
    output = json.loads(capsys.readouterr().out)
    assert client.closed
    if failure == "sigterm":
        assert output["status"] == "interrupted" and not client.calls
    else:
        assert output["error_type"] == "IntegrityError"
    with Journal(config.root) as journal:
        latest = journal.db.execute("SELECT status,summary_json FROM runs").fetchone()
        assert latest[0] == ("interrupted" if failure == "sigterm" else "failed")
        assert json.loads(latest[1]).get("update_cursor") is None
        assert journal.get_meta("update_cursor") is None
        assert (
            count(journal, "futures_daily")
            == count(journal, "revisions")
            == (0 if failure == "sigterm" else 1)
        )


@pytest.mark.parametrize("bounded", [False, True], ids=["empty-complete", "clean-deferred"])
def test_cli_backfill_sigterm_at_finish_run(
    config, calendar, tmp_path, monkeypatch, capsys, bounded
):
    import os
    import signal

    from dq_historical_market_data_collector.cffex import scheduled

    settings = tmp_path / "job.yaml"
    settings.write_text(
        f"calendar_file: {config.calendar_file}\nroot: {config.root}\n"
        "start_date: '2026-09-01'\n" + UNIT_YAML
    )
    config.calendar_file.write_text(calendar.model_dump_json())

    class WeekendDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 9, 20, 11, tzinfo=UTC).astimezone(tz)

    monkeypatch.setattr(cli, "datetime", WeekendDateTime)
    client = FakeClient()
    patch_source_clients(monkeypatch, lambda *_: client)
    with Journal(config.root) as journal:
        journal.set_meta("update_cursor", DAY.isoformat())
    args = [
        "backfill",
        "--config",
        str(settings),
        "--start-date",
        "2026-09-16" if bounded else "2026-09-19",
        "--end-date",
        "2026-09-18" if bounded else "2026-09-20",
    ]
    if bounded:
        args.extend(["--max-days", "1"])
    original_finish = Journal.finish_run
    finish_statuses = []

    def finish_with_sigterm(self, run_id, report):
        finish_statuses.append(report["status"])
        if len(finish_statuses) == 1:
            os.kill(os.getpid(), signal.SIGTERM)
        else:
            # The cancelled completion must have rolled back before abort cleanup.
            assert (
                self.db.execute("SELECT status FROM runs WHERE id=?", (run_id,)).fetchone()[0]
                == "running"
            )
        original_finish(self, run_id, report)

    with monkeypatch.context() as patch:
        patch.setattr(Journal, "finish_run", finish_with_sigterm)
        code = cli.main(args)
    output = json.loads(capsys.readouterr().out)
    assert code == 4
    assert output["status"] == "interrupted" and client.closed
    assert finish_statuses == ["partial" if bounded else "complete", "interrupted"]
    with Journal(config.root) as journal:
        latest = journal.db.execute("SELECT status,summary_json FROM runs").fetchone()
        report = json.loads(latest[1])
        assert latest[0] == report["status"] == "interrupted"
        assert report["completed_days"] == int(bounded)
        assert report["deferred_days"] == (2 if bounded else 0)
        assert journal.get_meta("update_cursor") is None
        assert count(journal, "futures_daily") == count(journal, "revisions") == int(bounded)
        assert count(journal, "attempts") == int(bounded)
        assert len(list((config.root / "raw").rglob("*.json"))) == int(bounded)
        if bounded:
            assert journal.complete("2026-09-16", "akshare", config.source.model)
    assert not scheduled.deferred_success(code, report)
    monkeypatch.setattr(
        scheduled,
        "invoke",
        lambda command, *_: (
            (0, {"status": "complete"}) if command == "calendar-refresh" else (code, output)
        ),
    )
    adapter_code, adapter_report = scheduled.one_pass("backfill", settings, 1)
    assert adapter_code == adapter_report["exit_code"] == 4
    assert adapter_report["status"] != "deferred"

    # The same completion path still accepts an uncancelled normal pass.
    normal_code = cli.main(args)
    normal_report = json.loads(capsys.readouterr().out)
    assert normal_code == (3 if bounded else 0)
    assert normal_report["status"] == ("partial" if bounded else "complete")
    assert scheduled.deferred_success(normal_code, normal_report) is bounded


def stalled_worker(connection, source):
    """Spawn-safe offline child proves the real shared process boundary is killable."""
    time.sleep(30)


def test_real_process_client_hard_timeout(monkeypatch):
    import openbb_collector_core.process as process

    monkeypatch.setattr(process, "_worker", stalled_worker)
    client = process.ProcessClient()
    started = time.monotonic()
    try:
        with pytest.raises(ProviderFailure, match="timeout"):
            client.fetch("cffex", Source(timeout_seconds=0.1), {"date": DAY.isoformat()})
        assert not client._workers and time.monotonic() - started < 5
    finally:
        client.close()


# ---------------------------------------------------------------------------
# Offline transport guard: the layers this lane's provider actually uses
#
# The CFFEX provider (`openbb_akshare/utils/cffex.py`) downloads the exchange
# daily CSV through `make_request` (requests -> urllib3 -> socket). Those are the
# layers that can leave the host, so those are the layers that are trapped. Each
# trap carries a unique label; the positive control proves every declared trap
# fires its own label, and the falsifiability matrix proves that disarming one
# entry point removes exactly that label.
# ---------------------------------------------------------------------------

TRANSPORT_TRAPS = (
    ("socket", socket, "socket", "connect"),
    ("socket", socket, "socket", "connect_ex"),
    ("socket", socket, "create_connection", None),
    ("socket", socket, "getaddrinfo", None),
    ("requests", requests.sessions, "Session", "request"),
    ("requests", requests.sessions, "Session", "send"),
    ("provider", None, "make_request", None),
    ("provider", None, "fetch_daily_csv", None),
)
TRAFFIC_LABELS = tuple(
    f"transport_trap:{layer}:{name if attribute is None else name + '.' + attribute}"
    for layer, _owner, name, attribute in TRANSPORT_TRAPS
)


def _provider_module():
    import openbb_akshare.utils.cffex as module

    return module


def _trap_targets():
    """(owner, attribute, label) for the layers that actually carry a request."""
    module = _provider_module()
    for layer, owner, name, attribute in TRANSPORT_TRAPS:
        if layer == "provider":
            yield module, name, f"transport_trap:{layer}:{name}"
        elif attribute is None:
            yield owner, name, f"transport_trap:{layer}:{name}"
        else:
            yield getattr(owner, name), attribute, f"transport_trap:{layer}:{name}.{attribute}"


class TransportTrap:
    def __init__(self, label):
        self.label = label

    def __call__(self, *_args, **_kwargs):
        raise AssertionError(f"network access attempted in an offline test [{self.label}]")


def install_transport_traps():
    installed = []
    for owner, attribute, label in _trap_targets():
        if not hasattr(owner, attribute):
            raise AssertionError(f"offline_guard_target_missing:{label}")
        setattr(owner, attribute, TransportTrap(label))
        if getattr(getattr(owner, attribute), "label", None) != label:
            raise AssertionError(f"offline_guard_target_not_trapped:{label}")
        installed.append(label)
    if tuple(installed) != TRAFFIC_LABELS or len(set(installed)) != len(installed):
        raise AssertionError("offline_guard_label_conflict")
    return installed


def assert_all_traps_armed() -> list[str]:
    """Every trap target, checked by its own attribute -- not just ``make_request``.

    ``install_transport_traps`` verifies each target as it sets it; this function is
    the *separate* check the matrix and the probes call after a disarm, so a trap
    that silently reverted to a live entry point is caught by name rather than by
    whichever one happens to be inspected.
    """
    armed = []
    for owner, attribute, label in _trap_targets():
        if getattr(owner, attribute, None) is None or (
            getattr(getattr(owner, attribute), "label", None) != label
        ):
            raise AssertionError(f"offline_guard_target_not_trapped:{label}")
        armed.append(label)
    if tuple(armed) != TRAFFIC_LABELS:
        raise AssertionError("offline_guard_label_conflict")
    return armed


def disarm_transport_trap(label, originals=None):
    """Restore exactly one entry point; returns the value that was in force."""
    originals = _ORIGINALS if originals is None else originals
    for owner, attribute, candidate in _trap_targets():
        if candidate == label:
            previous = originals[label]
            setattr(owner, attribute, previous)
            return previous
    raise AssertionError(f"offline_guard_label_unknown:{label}")


_ORIGINALS = {}


@pytest.fixture(autouse=True)
def transport_guard():
    """Every test in this module runs with the transport guard armed."""
    for owner, attribute, label in _trap_targets():
        if hasattr(owner, attribute):
            _ORIGINALS.setdefault(label, getattr(owner, attribute))
    installed = install_transport_traps()
    assert set(installed) == set(TRAFFIC_LABELS)
    try:
        yield installed
    finally:
        for owner, attribute, label in _trap_targets():
            setattr(owner, attribute, _ORIGINALS[label])


def traffic_call_shapes():
    """One call shape per declared trap, all loopback only."""
    module = _provider_module()

    def socket_connect():
        with socket.socket() as sock:
            sock.connect(("127.0.0.1", 1))

    def socket_connect_ex():
        with socket.socket() as sock:
            sock.connect_ex(("127.0.0.1", 1))

    def session_request():
        requests.Session().request("GET", "http://127.0.0.1:1/")

    def session_send():
        prepared = requests.Request("GET", "http://127.0.0.1:1/").prepare()
        requests.Session().send(prepared)

    return {
        "transport_trap:socket:socket.connect": socket_connect,
        "transport_trap:socket:socket.connect_ex": socket_connect_ex,
        "transport_trap:socket:create_connection": lambda: socket.create_connection(
            ("127.0.0.1", 1), timeout=0.1
        ),
        "transport_trap:socket:getaddrinfo": lambda: socket.getaddrinfo("127.0.0.1", 1),
        "transport_trap:requests:Session.request": session_request,
        "transport_trap:requests:Session.send": session_send,
        "transport_trap:provider:make_request": lambda: module.make_request("http://127.0.0.1:1/"),
        "transport_trap:provider:fetch_daily_csv": lambda: module.fetch_daily_csv("20260918"),
    }


def test_transport_guard_traps_every_layer_before_a_request_is_attempted(transport_guard):
    shapes = traffic_call_shapes()
    assert set(shapes) == set(TRAFFIC_LABELS) == set(transport_guard)
    for label, shape in shapes.items():
        with pytest.raises(AssertionError, match=f"\\[{label}\\]"):
            shape()


@pytest.mark.parametrize("label", sorted(TRAFFIC_LABELS))
def test_each_transport_trap_label_is_falsifiable(label):
    shapes = traffic_call_shapes()
    shape = shapes[label]
    with pytest.raises(AssertionError, match=f"\\[{label}\\]"):
        shape()
    disarm_transport_trap(label)
    observed = None
    try:
        shape()
    except AssertionError as exc:
        text = str(exc)
        start = text.find("[transport_trap:")
        observed = text[start + 1 : text.index("]", start)] if start != -1 else None
    except OSError:
        # A disarmed socket trap really dials the loopback port and is refused.
        observed = None
    # The disarmed trap's own label is gone: either the shape reached the next
    # trapped layer (a different label) or it reached nothing at all.
    assert observed != label
    if observed is not None:
        assert observed in TRAFFIC_LABELS


# ---------------------------------------------------------------------------
# Task 5B-3 — raw-only capture, one versioned delivery, downstream consumer
#
# The tests above are the pre-existing suite for the business predicates and
# the legacy report/checkpoint contract. They are unchanged on purpose: the
# rules did not change, they moved. The tests below exercise the boundary that
# moved them out of acquisition, and nothing here weakens an older assertion.
# ---------------------------------------------------------------------------


def evidence_source(config):
    return cffex_runtime.source_of(config.source)


def day_plan(config, day, attempt=0, token="configured", source=None):
    """The literal plan for one requested day, built without a Collector."""
    source = source or evidence_source(config)
    task_id = cffex_runtime.digest(
        {
            "kind": "cffex-daily",
            "source_id": config.source_id,
            "provider": source.provider,
            "model": source.model,
            "date": day.isoformat(),
        }
    )
    generation = cffex_runtime.digest(
        {"day": day.isoformat(), "attempt": attempt, "token": token, "synthetic": True}
    )
    entry = {
        "kind": "daily",
        "trade_date": day.isoformat(),
        "source_id": config.source_id,
        "attempt": attempt,
        "source": source.model_dump(),
        "task": {
            "version": EVIDENCE_VERSION,
            "task_id": task_id,
            "generation": generation,
            "source": {"provider": source.provider, "model": source.model},
            "requests": [{"request_id": config.source_id, "params": {"date": day.isoformat()}}],
        },
    }
    entry["entry_id"] = cffex_runtime.entry_identity(entry)
    return {
        "version": PLAN_VERSION,
        "generation": cffex_runtime.digest([task_id, generation]),
        "entries": [entry],
    }


def captures(config):
    base = config.evidence / EVIDENCE_VERSION
    return sorted(p for p in base.iterdir() if p.is_dir()) if base.is_dir() else []


def captures_for(config, day):
    found = []
    for folder in captures(config):
        task = json.loads((folder / "task.json").read_text())
        if task["requests"][0]["params"].get("date") == day.isoformat():
            found.append(folder)
    return found


def bodies(folder):
    return [json.loads(path.read_text()) for path in sorted(folder.glob("responses/*/body.bin"))]


def deliveries(config, day):
    return sorted((config.root / "raw" / day.isoformat()).glob("*.json"))


def tree_hashes(root):
    import hashlib

    return {
        str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(Path(root).rglob("*"))
        if path.is_file()
    }


def test_capture_is_written_before_any_business_predicate(config, monkeypatch):
    # If acquisition reached a predicate, this test fails instead of quietly
    # passing on an already-normalized payload.
    def forbidden(*_args, **_kwargs):
        raise AssertionError("acquisition ran a downstream predicate")

    monkeypatch.setattr(cffex_runtime, "normalize", forbidden)
    monkeypatch.setattr(cffex_runtime, "accept_calendar", forbidden)
    monkeypatch.setattr(cffex_runtime, "read_delivery", forbidden)
    client = FakeClient([row(), row(symbol="T2612")])
    results = acquire(config.evidence, day_plan(config, DAY), client=client)
    assert results[0]["status"] == "captured" and results[0]["source_calls"] == 1
    (folder,) = captures_for(config, DAY)
    (body,) = bodies(folder)
    assert [item["symbol"] for item in body["rows"]] == ["IF2610", "T2612"]
    # Verbatim source strings: no numeric coercion happened at capture time.
    assert body["rows"][0]["volume"] == "12.0"


def test_raw_envelope_is_content_addressed_versioned_and_immutable(config):
    import hashlib

    client = FakeClient()
    results = acquire(config.evidence, day_plan(config, DAY), client=client)
    locator = results[0]["locator"]
    assert locator.startswith(EVIDENCE_VERSION + "/")
    digest_part = locator.split("/", 1)[1]
    assert len(digest_part) == 64 and all(char in "0123456789abcdef" for char in digest_part)
    folder = config.evidence / locator
    receipt = json.loads((folder / "responses/000000/receipt.json").read_text())
    raw = (folder / "responses/000000/body.bin").read_bytes()
    assert receipt["sha256"] == hashlib.sha256(raw).hexdigest() and receipt["size"] == len(raw)
    assert receipt["version"] == EVIDENCE_VERSION and receipt["status"] == "captured"
    assert receipt["evidence_kind"] == "provider-transformed"
    assert receipt["transformation"] == "openbb-process-response-json-v1"
    assert receipt["source_time"] is None  # the source never stated one
    assert (folder / "terminal.json").is_file() and (folder / "durable.json").is_file()

    # The stored bytes are bound to the receipt: an edit is detected, not used.
    task = json.loads((folder / "task.json").read_text())
    before = raw
    (folder / "responses/000000/body.bin").chmod(0o644)
    (folder / "responses/000000/body.bin").write_bytes(before[:-1])
    with pytest.raises(EvidenceStateError, match="evidence_state_unreadable") as caught:
        cffex_runtime.existing_state(config.evidence, task["task_id"], task["generation"])
    assert caught.value.detail["reason"] in {"body_checksum", "evidence_size_limit"}
    (folder / "responses/000000/body.bin").write_bytes(before)

    # Replaying the same generation never calls the source again.
    again = acquire(config.evidence, day_plan(config, DAY), client=client)
    assert again[0]["source_calls"] == 0 and again[0]["locator"] == locator
    assert len(client.calls) == 1


def test_capture_never_states_a_trading_date_of_its_own(config, calendar):
    client = FakeClient([row(date(2026, 9, 17))])
    with Journal(config.root) as journal:
        report = collector(config, journal, calendar, client).backfill(DAY, DAY)
    assert report["status"] == "failed" and report["failed_days"] == 1
    assert len(client.calls) == 3
    found = captures_for(config, DAY)
    assert len(found) == 3  # one generation per attempt, all preserved
    for folder in found:
        task = json.loads((folder / "task.json").read_text())
        assert task["requests"][0]["params"] == {"date": "2026-09-18"}
        assert "trading_date" not in task
        for body in bodies(folder):
            assert body["rows"][0]["date"] == "20260917"
            assert "trading_date" not in body["rows"][0]
        for path in folder.rglob("*.json"):
            assert "trading_date" not in path.read_text()


def test_source_date_disagreement_is_captured_verbatim_and_rejected_downstream(config, calendar):
    client = FakeClient([row(date(2026, 9, 17))])
    with Journal(config.root) as journal:
        report = collector(config, journal, calendar, client).backfill(DAY, DAY)
        assert report["errors"] == [{"trading_date": "2026-09-18", "code": "row_date_mismatch"}]
        assert count(journal, "futures_daily") == 0 and count(journal, "revisions") == 0
        rows = journal.evidence_rows()
        assert rows and all(item["verdict"] == "rejected" for item in rows)
        assert all(item["code"] == "row_date_mismatch" for item in rows)
        detail = json.loads(rows[0]["detail_json"])
        # Both sides of the disagreement are recorded; neither is rewritten.
        assert detail["requested_date"] == "2026-09-18"
        assert detail["source_dates"] == ["2026-09-17"]
    found = deliveries(config, DAY)
    assert len(found) == 3  # one delivery per attempt, every one preserved
    for delivery in found:
        raw = json.loads(delivery.read_text())
        assert raw["rows"][0]["date"] == "20260917"
        assert raw["request"]["date"] == "2026-09-18"


def test_daily_and_backfill_write_one_format_and_one_reader_consumes_both(config, calendar):
    client = FakeClient()
    with Journal(config.root) as journal:
        engine = collector(config, journal, calendar, client)
        assert engine.backfill(DAY, DAY)["status"] == "complete"
        first = read_delivery(deliveries(config, DAY)[-1], capture_root=config.evidence)
        # The rolling lane refreshes the same day at the same clock instant.
        assert engine.update(DAY)["status"] == "complete"
        newest = sorted(deliveries(config, DAY), key=lambda p: p.stat().st_mtime_ns)[-1]
        second = read_delivery(newest, capture_root=config.evidence)
    assert first["version"] == second["version"] == DELIVERY_VERSION
    assert set(first) == set(second)
    # Identity and clocks differ by construction; every payload-bearing key must be
    # equal, and the capture block must at least have the same shape.
    stable = {"evidence", "capture", "plan", "captured_at", "observed_at"}
    assert set(first["capture"]) == set(second["capture"])
    assert {k: v for k, v in first.items() if k not in stable} == {
        k: v for k, v in second.items() if k not in stable
    }
    # Different identity (freshness token), same format: that is the split.
    assert first["evidence"]["locator"] != second["evidence"]["locator"]
    assert first["plan"]["version"] == second["plan"]["version"] == PLAN_VERSION


def test_delivery_reader_refuses_unknown_version_and_moved_delivery(config, calendar, tmp_path):
    with Journal(config.root) as journal:
        collector(config, journal, calendar, FakeClient()).backfill(DAY, DAY)
    (delivery,) = deliveries(config, DAY)
    payload = json.loads(delivery.read_text())
    payload["version"] = "cffex-raw-delivery-v0"
    tampered = tmp_path / DAY.isoformat() / "tampered.json"
    tampered.parent.mkdir()
    tampered.write_text(json.dumps(payload))
    with pytest.raises(EvidenceStateError, match="unsupported_delivery_version"):
        read_delivery(tampered)
    moved = tmp_path / "2026-09-17" / "moved.json"
    moved.parent.mkdir()
    moved.write_bytes(delivery.read_bytes())
    with pytest.raises(EvidenceStateError, match="delivery_binding"):
        read_delivery(moved)


def test_replay_of_captured_evidence_performs_zero_source_calls(config, calendar):
    client = FakeClient()
    with Journal(config.root) as journal:
        journal.db.execute(
            "CREATE TRIGGER inject_failure BEFORE INSERT ON futures_daily "
            "BEGIN SELECT RAISE(ABORT,'test'); END"
        )
        with pytest.raises(sqlite3.IntegrityError):
            collector(config, journal, calendar, client).backfill(DAY, DAY)
        assert len(client.calls) == 1
        assert count(journal, "futures_daily") == 0
        # Capture and delivery survived the crash; no verdict was recorded.
        assert journal.evidence_rows() == []
        journal.db.execute("DROP TRIGGER inject_failure")
    with Journal(config.root) as journal:
        result = collector(config, journal, calendar, client).backfill(DAY, DAY)
        assert result["status"] == "complete" and result["provider_calls"] == 0
        assert result["evidence_replays"] == 1 and len(client.calls) == 1
        assert count(journal, "futures_daily") == 1


def test_cancellation_leaves_a_durable_veto(config, calendar):
    stop = threading.Event()
    stop.set()
    plan = day_plan(config, DAY)
    client = FakeClient()
    results = acquire(config.evidence, plan, client=client, stop=stop)
    assert results[0]["status"] == "failed" and results[0]["source_calls"] == 0
    assert not client.calls
    folder = config.evidence / results[0]["locator"]
    assert (folder / "cancel.json").is_file()
    # The veto is durable: the same generation replays as cancelled, no call.
    again = acquire(config.evidence, plan, client=client)
    assert again[0]["status"] == "failed" and again[0]["reason"] == "cancelled"
    assert again[0]["source_calls"] == 0 and not client.calls
    # Progress is still possible: the collector advances to a new generation.
    with Journal(config.root) as journal:
        report = collector(config, journal, calendar, FakeClient()).backfill(DAY, DAY)
        assert report["status"] == "complete" and report["provider_calls"] == 1
        assert report["evidence_captures"] == 1


def test_interrupted_capture_is_resumed_not_duplicated(config):
    class Exploding(FakeClient):
        def fetch(self, *_args, **_kwargs):
            raise RuntimeError("transport died mid-capture")

    plan = day_plan(config, DAY)
    with pytest.raises(RuntimeError, match="transport died"):
        acquire(config.evidence, plan, client=Exploding())
    (folder,) = captures(config)
    assert (folder / "task.json").is_file()
    assert not list(folder.glob("responses/*"))
    client = FakeClient()
    results = acquire(config.evidence, plan, client=client)
    assert results[0]["locator"] == f"{EVIDENCE_VERSION}/{folder.name}"
    assert results[0]["complete"] and results[0]["source_calls"] == 1
    assert len(client.calls) == 1


def test_unknown_or_corrupt_evidence_refuses_typed_without_mutation_or_call(config, calendar):
    client = FakeClient()
    with Journal(config.root) as journal:
        journal.db.execute(
            "CREATE TRIGGER inject_failure BEFORE INSERT ON futures_daily "
            "BEGIN SELECT RAISE(ABORT,'test'); END"
        )
        with pytest.raises(sqlite3.IntegrityError):
            collector(config, journal, calendar, client).backfill(DAY, DAY)
        journal.db.execute("DROP TRIGGER inject_failure")
    (folder,) = captures_for(config, DAY)
    body_path = folder / "responses/000000/body.bin"
    body_path.chmod(0o644)
    content = bytearray(body_path.read_bytes())
    content[10] = content[10] ^ 0xFF
    body_path.write_bytes(bytes(content))
    before = tree_hashes(config.evidence)
    with Journal(config.root) as journal:
        runs = journal.db.execute("SELECT count(*) FROM runs").fetchone()[0]
        attempts = journal.db.execute("SELECT count(*) FROM attempts").fetchone()[0]
        probe = FakeClient()
        with pytest.raises(EvidenceStateError, match="evidence_state_unreadable") as caught:
            collector(config, journal, calendar, probe).backfill(DAY, DAY)
        assert caught.value.detail["reason"] in {"body_checksum", "evidence_size_limit"}
        assert not probe.calls
        assert journal.db.execute("SELECT count(*) FROM runs").fetchone()[0] == runs
        assert journal.db.execute("SELECT count(*) FROM attempts").fetchone()[0] == attempts
    assert tree_hashes(config.evidence) == before


def test_unknown_journal_schema_refuses_typed_without_mutation(config):
    with Journal(config.root):
        pass
    db = sqlite3.connect(config.root / "cffex.sqlite3")
    try:
        db.execute("PRAGMA user_version=99")
        db.commit()
    finally:
        db.close()
    before = tree_hashes(config.root)
    with pytest.raises(JournalError, match="journal_schema_mismatch") as caught:
        with Journal(config.root):
            pass
    assert caught.value.detail["version"] == 99
    assert tree_hashes(config.root) == before


def test_bounded_batch_defers_and_refuses_an_oversized_plan(config, calendar):
    client = FakeClient()
    with Journal(config.root) as journal:
        first = collector(config, journal, calendar, client).backfill(
            date(2026, 9, 16), DAY, max_days=1
        )
        assert first["status"] == "partial" and first["deferred_days"] == 2
        assert len(client.calls) == 1 and len(captures(config)) == 1
    entry = day_plan(config, DAY)["entries"][0]
    oversized = {
        "version": PLAN_VERSION,
        "generation": "x" * 8,
        "entries": [json.loads(json.dumps(entry)) for _ in range(MAX_PLAN_ENTRIES + 1)],
    }
    store = config.evidence / "untouched"
    with pytest.raises(ValueError, match="plan_entry_limit"):
        acquire(store, oversized, client=FakeClient())
    assert not store.exists()


def test_oversize_response_is_refused_and_prior_evidence_is_kept(config, calendar):
    config.source.max_response_bytes = 1024
    huge = row(extra="x" * 2000)
    client = FakeClient([row(), huge])
    with Journal(config.root) as journal:
        report = collector(config, journal, calendar, client).backfill(DAY, DAY)
        assert report["status"] == "failed" and report["provider_calls"] == 3
        assert {item["code"] for item in report["errors"]} == {"adapter_response_limit_exceeded"}
        assert count(journal, "futures_daily") == 0
    for folder in captures_for(config, DAY):
        assert (folder / "task.json").is_file()
        assert not list(folder.glob("responses/*/body.bin"))
    assert deliveries(config, DAY) == []


def test_row_budget_is_enforced_downstream_from_captured_bytes(config, calendar):
    config.source.max_rows = 1
    client = FakeClient([row(), row(symbol="T2612")])
    with Journal(config.root) as journal:
        report = collector(config, journal, calendar, client).backfill(DAY, DAY)
        assert report["status"] == "failed"
        assert {item["code"] for item in report["errors"]} == {"response_limit"}
        assert count(journal, "futures_daily") == 0
    found = captures_for(config, DAY)
    assert len(found) == 3
    assert len(bodies(found[0])[0]["rows"]) == 2  # the bytes were captured honestly


def test_missing_provider_model_is_an_honest_failure(config, monkeypatch):
    import multiprocessing as mp

    from openbb_collector_core import process as core_process

    initialise = core_process.ProcessClient.__init__

    def forked(self, stop=None):
        initialise(self, stop)
        self._context = mp.get_context("fork")

    monkeypatch.setattr(core_process.ProcessClient, "__init__", forked)
    source = cffex_runtime.EvidenceSource(
        provider="akshare", model="AkshareMissingModel", max_response_bytes=1_000_000
    )
    client = core_process.ProcessClient()
    try:
        results = acquire(config.evidence, day_plan(config, DAY, source=source), client=client)
    finally:
        client.close()
    assert results[0]["status"] == "failed" and results[0]["complete"] is True
    assert [item["error"] for item in results[0]["responses"]] == ["provider_failure"]
    (folder,) = captures_for(config, DAY)
    assert not list(folder.glob("responses/*/body.bin"))


def test_consume_applies_business_rules_without_a_source_call(
    config, calendar, monkeypatch, capsys
):
    with Journal(config.root) as journal:
        journal.db.execute(
            "CREATE TRIGGER inject_failure BEFORE INSERT ON futures_daily "
            "BEGIN SELECT RAISE(ABORT,'test'); END"
        )
        with pytest.raises(sqlite3.IntegrityError):
            collector(config, journal, calendar, FakeClient()).backfill(DAY, DAY)
        journal.db.execute("DROP TRIGGER inject_failure")
    settings = config.root.parent / "job.yaml"
    settings.write_text(
        f"calendar_file: {config.calendar_file}\nroot: {config.root}\nstart_date: '2026-09-01'\n"
        + UNIT_YAML
    )
    config.calendar_file.write_text(calendar.model_dump_json())
    factory_calls = []

    def factory(*_args):
        factory_calls.append(1)
        raise AssertionError("consume constructed a provider client")

    patch_source_clients(monkeypatch, factory)
    monkeypatch.setattr(cli, "datetime", FrozenDateTime)
    code = cli.main(["consume", "--config", str(settings), "--start-date", "2026-09-18"])
    report = json.loads(capsys.readouterr().out)
    assert code == 0 and report["command"] == "consume"
    assert report["provider_calls"] == 0 and report["completed_days"] == 1
    assert not factory_calls
    with Journal(config.root) as journal:
        assert count(journal, "futures_daily") == 1
        assert journal.complete("2026-09-18", "akshare", config.source.model)


def test_no_source_client_refuses_to_fetch():
    with pytest.raises(AssertionError, match="source call"):
        NoSourceClient().fetch("cffex", None, {})


CENTRAL_CHILD = r"""
import json, os, sys, sqlite3

if os.environ.get("OPENBB_AUTO_BUILD") != "false":
    raise SystemExit("OPENBB_AUTO_BUILD must be false before Python starts")

opened = []


def audit(event, args):
    if event == "open" and args and isinstance(args[0], (str, bytes, os.PathLike)):
        opened.append(os.path.abspath(os.fspath(args[0])))


sys.addaudithook(audit)

root = os.path.abspath(sys.argv[1])
central = os.path.abspath(sys.argv[2])
private = root + os.sep
real_connect = sqlite3.connect


def poisoned(*args, **kwargs):
    target = str(args[0]) if args else ""
    if target.startswith("file:"):
        target = target[5:].split("?", 1)[0]
    target = os.path.abspath(target)
    if not (target == root or target.startswith(private)):
        raise AssertionError("database access outside the private root: " + target)
    return real_connect(*args, **kwargs)


sqlite3.connect = poisoned


class Blocker:
    def find_spec(self, name, path=None, target=None):
        if name.split(".")[0] in {"duckdb", "_duckdb"} or name.startswith("dq_quant_invest_data"):
            raise AssertionError("central module imported: " + name)
        return None


sys.meta_path.insert(0, Blocker())

from datetime import UTC, date, datetime, timedelta
from openbb_collector_core.models import Response
from dq_historical_market_data_collector.cffex.config import Config, Source, TradingCalendar
from dq_historical_market_data_collector.cffex.runtime import Collector
from dq_historical_market_data_collector.cffex.storage import Journal

DAY = date(2026, 9, 18)
NOW = datetime(2026, 9, 18, 11, tzinfo=UTC)
ROWS = [
    {
        "date": "20260918", "symbol": "IF2610", "variety": "IF", "open": "100", "high": "103",
        "low": "99", "close": "102", "settle": "101", "pre_settle": "100", "volume": "12.0",
        "open_interest": "34", "turnover": "567.8",
    }
]
CALLS = {"value": 0}


class Client:
    def fetch(self, source_id, source, request):
        CALLS["value"] += 1
        return Response([dict(row) for row in ROWS], 1, {"openbb-core": {"version": "offline"}})

    def close(self):
        return None


config = Config(
    root=os.path.join(root, "private"),
    calendar_file=os.path.join(root, "private", "calendar.json"),
    start_date="2026-09-01",
    units={
        "price_unit": "source:index_point",
        "volume_unit": "source:lot",
        "open_interest_unit": "source:lot",
        "turnover_unit": "source:CNY",
    },
    source=Source(retries=2, retry_delay_seconds=0),
)
calendar = TradingCalendar(
    timezone="Asia/Shanghai",
    coverage_start="2026-09-01",
    coverage_end="2026-10-30",
    source="offline child fixture",
    trading_dates=[
        date(2026, 9, 1) + timedelta(days=i)
        for i in range(60)
        if (date(2026, 9, 1) + timedelta(days=i)).weekday() < 5
    ],
)
with Journal(config.root) as journal:
    report = Collector(config, journal, calendar, Client(), clock=lambda: NOW).backfill(DAY, DAY)

# Falsifiability: the same hook must observe a central path when one is touched.
try:
    with open(os.path.join(central, "probe"), "w", encoding="utf-8") as stream:
        stream.write("x")
except OSError:
    pass
falsification = [path for path in opened if path.startswith(central + os.sep)]
blocked = None
try:
    sqlite3.connect(os.path.join(central, "journal.sqlite3"))
except AssertionError:
    blocked = "poisoned"

print(json.dumps({
    "config_module": sys.modules["dq_historical_market_data_collector.cffex.config"].__file__,
    "status": report["status"],
    "rows": report["rows_committed"],
    "calls": CALLS["value"],
    "central_opens": [
        path for path in opened if path == central
        or (path.startswith(central + os.sep) and path not in falsification)
    ],
    "falsification_opens": falsification,
    "central_database_blocked": blocked,
    "duckdb_loaded": "duckdb" in sys.modules,
    "central_exists": os.path.exists(central),
    "auto_build_effective": os.environ.get("OPENBB_AUTO_BUILD"),
}))
"""


@contextlib.contextmanager
def hold_exclusive(path):
    """A real exclusive ``flock`` on ``path``, held for the caller's whole block.

    A second descriptor must be refused, so "locked" means a lock somebody holds
    rather than a file that merely exists.
    """
    primary = path.open("a+b")
    second = path.open("a+b")
    try:
        fcntl.flock(primary.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            fcntl.flock(second.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            pass
        else:
            raise AssertionError("central journal lock is not exclusive")
        yield primary
    finally:
        second.close()
        fcntl.flock(primary.fileno(), fcntl.LOCK_UN)
        primary.close()


def lock_is_held(path):
    """True when another descriptor cannot take the exclusive lock."""
    import fcntl as _fcntl

    with path.open("a+b") as stream:
        try:
            _fcntl.flock(stream.fileno(), _fcntl.LOCK_EX | _fcntl.LOCK_NB)
        except BlockingIOError:
            return True
        _fcntl.flock(stream.fileno(), _fcntl.LOCK_UN)
        return False


def run_central_child(tmp_path, central):
    script = tmp_path / "central_child.py"
    script.write_text(CENTRAL_CHILD)
    env = os.environ.copy()
    # The child's import path is set explicitly, not inherited: it must run the
    # working-tree sources -- the collector, the collector core and the provider --
    # and never an installed copy in site-packages.
    source = Path(cffex_runtime.__file__).resolve().parents[2]
    repo = Path(cffex_runtime.__file__).resolve().parents[3]
    openbb = repo.parents[1]
    roots = [
        source,
        repo,
        openbb / "providers/akshare",
        openbb / "core",
        openbb / "collectors/core/src",
    ]
    env["PYTHONPATH"] = os.pathsep.join(str(item) for item in roots)
    env["OPENBB_AUTO_BUILD"] = "false"
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    child = subprocess.run(
        [sys.executable, str(script), str(tmp_path / "private"), str(central)],
        cwd=str(tmp_path),
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    return child, (json.loads(child.stdout) if child.returncode == 0 else None)


@pytest.mark.parametrize("state", ["absent", "locked", "inaccessible"])
def test_private_run_touches_no_central_store(tmp_path, state):
    (tmp_path / "private").mkdir()
    central = tmp_path / f"central-{state}"
    if state != "absent":
        central.mkdir()
        (central / "journal.sqlite3").write_bytes(b"")
        (central / "catalog.duckdb").write_bytes(b"")
    if state == "inaccessible":
        central.chmod(0)
    # "locked" is a lock a writer really holds on the central journal, not a file
    # that exists: it is acquired before the child starts and verified after it
    # exits, while still held.
    held = hold_exclusive(central / "journal.sqlite3") if state == "locked" else None
    observed = {}
    try:
        with held if held is not None else contextlib.nullcontext():
            child, payload = run_central_child(tmp_path, central)
            if state == "locked":
                observed["lock_still_held_after_child"] = lock_is_held(central / "journal.sqlite3")
    finally:
        if state == "inaccessible":
            central.chmod(0o700)
    assert child.returncode == 0, child.stderr
    assert payload["status"] == "complete" and payload["rows"] == 1 and payload["calls"] == 1
    assert payload["central_opens"] == []
    assert payload["duckdb_loaded"] is False
    assert payload["central_database_blocked"] == "poisoned"
    assert payload["auto_build_effective"] == "false"
    assert payload["central_exists"] is (state != "absent")
    source_root = Path(cffex_runtime.__file__).resolve().parents[2]
    assert payload["config_module"].startswith(str(source_root))
    if state == "locked":
        assert observed["lock_still_held_after_child"] is True
    # The audit hook really observes central paths in this very child, whether
    # the open succeeds (writable) or fails (absent/chmod 000).
    assert payload["falsification_opens"] == [str(central / "probe")]


# ---------------------------------------------------------------------------
# Task 5B-3 repair — literal source bytes, capture binding, unit authority
#
# These nodes exist because the first implementation captured the provider's
# *parsed* response, which the provider had already date-stamped. Everything
# below is measured against the transport, the store or the delivery bytes.
# ---------------------------------------------------------------------------

CSV_HEADER = (
    "合约代码,今开盘,最高价,最低价,成交量,成交金额,持仓量,持仓变化,"
    "今收盘,今结算,前结算,涨跌1,涨跌2,Delta"
)
CSV_FUTURES = "IF2610  ,100,103,99,12,567.8,34,0,102,101,100,1,1,--"
CSV_SECOND = "T2612   ,101,104,100,7,70.5,9,0,103,102,101,1,1,--"
CSV_OPTION = "IO2610-C-4000,10,11,9,3,1.5,5,0,10,10,10,0,0,0.1"
CSV_TOTAL = "合计,0,0,0,15,569.3,43,0,0,0,0,0,0,--"


def csv_body(rows=None, header=None):
    """A synthetic CFFEX rtj body: the real layout, no vendor bytes, no date column."""
    chosen = CSV_FUTURES if rows is None else rows
    if isinstance(chosen, str):
        chosen = (chosen,)
    return (("\n".join([header or CSV_HEADER, *chosen])) + "\n").encode("gbk")


class FakeResponse:
    def __init__(self, body, status=200, headers=None):
        self.content = body
        self.status_code = status
        self.headers = headers if headers is not None else {"Content-Type": "text/csv"}

    def raise_for_status(self):
        return None


def raw_bodies(folder):
    return [path.read_bytes() for path in sorted(folder.glob("responses/*/body.bin"))]


def test_literal_source_bytes_are_captured_before_any_parser(config, calendar):
    body = csv_body((CSV_FUTURES, CSV_SECOND, CSV_OPTION, CSV_TOTAL))
    sent = []

    def sender(url, timeout=30):
        sent.append(url)
        return FakeResponse(body)

    with Journal(config.root) as journal:
        report = collector(config, journal, calendar, CffexCsvClient(sender=sender)).backfill(
            DAY, DAY
        )
        committed = [
            dict(item)
            for item in journal.db.execute(
                "SELECT trading_date, contract_symbol, close, source_units_json "
                "FROM futures_daily ORDER BY contract_symbol"
            )
        ]
    assert report["status"] == "complete" and report["rows_committed"] == 2
    assert sent == [cffex_source.daily_csv_url(DAY)]
    # The stored bytes are the transport body, byte for byte -- no parser ran first.
    (folder,) = captures_for(config, DAY)
    (stored,) = raw_bodies(folder)
    assert stored == body
    assert b"20260918" not in stored and "日期" not in stored.decode("gbk").split("\n")[0]
    receipt = json.loads((folder / "responses/000000/receipt.json").read_text())
    assert receipt["evidence_kind"] == "raw-bytes" and receipt["transformation"] is None
    assert receipt["sha256"] == hashlib.sha256(body).hexdigest()
    # The delivery says where the date came from instead of pretending otherwise.
    delivery = read_delivery(deliveries(config, DAY)[-1], capture_root=config.evidence)
    provenance = delivery["provenance"]
    assert provenance["date_source"] == "request_parameter"
    assert provenance["layout"] == cffex_parse.SOURCE_LAYOUT
    assert provenance["date_column"] is None
    assert provenance["filtered"] == {"summary": 1, "option": 1}
    assert all("date" not in row for row in delivery["rows"])
    assert delivery["capture"]["sha256"] == receipt["sha256"]
    # Committed date is the requested date, and the row records that provenance.
    assert [item["trading_date"] for item in committed] == ["2026-09-18", "2026-09-18"]
    assert json.loads(committed[0]["source_units_json"])["price_unit"] == UNITS["price_unit"]


def test_owned_transport_url_equals_the_provider_url(monkeypatch):
    """The owned URL template cannot drift from the provider's (read-only) one."""
    recorded = {}

    def fake(url, timeout=10, **_kwargs):
        recorded["url"], recorded["timeout"] = url, timeout
        return FakeResponse(csv_body())

    from openbb_akshare.utils import cffex as provider_module

    # This node deliberately exercises the provider's transport call, so exactly
    # the two provider-layer traps are disarmed for it; the rest stay armed.
    disarm_transport_trap("transport_trap:provider:fetch_daily_csv")
    disarm_transport_trap("transport_trap:provider:make_request")
    monkeypatch.setattr(provider_module, "make_request", fake)
    provider_module.fetch_daily_csv("20260918")
    assert recorded["url"] == cffex_source.daily_csv_url(DAY)
    assert recorded["timeout"] == 30
    # And the owned helper resolves the provider's attribute at call time.
    assert cffex_source.transport() is provider_module.make_request


@pytest.mark.parametrize(
    "body,code",
    [
        (csv_body(("IF2610  ,100,103,99,12,567.8,34,0,102,101",)), "source_row_truncated"),
        (csv_body(header=CSV_HEADER + ",今收盘"), "source_header_duplicate"),
        (
            csv_body(("CU2610  ,100,103,99,12,567.8,34,0,102,101,100,1,1,--",)),
            "source_contract_unexpected:0",
        ),
        (b"\xff\xfe not a gbk csv at all", "source_body_undecodable"),
        (csv_body((CSV_FUTURES, CSV_FUTURES)), "source_duplicate_contract"),
    ],
)
def test_malformed_bodies_are_captured_then_rejected_downstream(config, calendar, body, code):
    """A body the provider would have raised on is written to the store first."""
    with Journal(config.root) as journal:
        report = collector(
            config,
            journal,
            calendar,
            CffexCsvClient(sender=lambda url, timeout=30: FakeResponse(body)),
        ).backfill(DAY, DAY)
        assert count(journal, "futures_daily") == 0 and count(journal, "revisions") == 0
    assert report["status"] == "failed"
    assert {item["code"] for item in report["errors"]} == {code}
    found = captures_for(config, DAY)
    assert len(found) == 3  # one captured generation per attempt, all preserved
    for folder in found:
        assert raw_bodies(folder) == [body]
    stored = deliveries(config, DAY)
    assert len(stored) == 3
    for path in stored:
        delivery = read_delivery(path, capture_root=config.evidence)
        assert delivery["provenance"]["parse_error"] == code
        assert delivery["rows"] == []


def _delivery_copy(config, day, tmp_path, name):
    (delivery,) = deliveries(config, day)
    payload = json.loads(delivery.read_text())
    target = tmp_path / day.isoformat() / f"{name}.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    return payload, target


def _reseal(payload):
    """Recompute every digest the envelope carries, as a careful forger would."""
    payload["capture"]["row_sha256"] = [cffex_runtime.digest(row) for row in payload["rows"]]
    for index, row in enumerate(payload["rows"]):
        payload["row_provenance"][index]["source_sha256"] = cffex_runtime.digest(row)


def test_delivery_refuses_rows_the_capture_does_not_support(config, calendar, tmp_path):
    with Journal(config.root) as journal:
        collector(config, journal, calendar, FakeClient([row()])).backfill(DAY, DAY)
    payload, target = _delivery_copy(config, DAY, tmp_path, "mutated")
    payload["rows"][0]["close"] = "999999"
    payload["rows"][0]["symbol"] = "IF9999"
    target.write_text(json.dumps(payload))
    with pytest.raises(EvidenceStateError, match="delivery_row_digest"):
        read_delivery(target, capture_root=config.evidence)

    # A forger who recomputes every digest still cannot make the bytes agree.
    _reseal(payload)
    target.write_text(json.dumps(payload))
    with pytest.raises(EvidenceStateError, match="delivery_capture_mismatch"):
        read_delivery(target, capture_root=config.evidence)

    # Neither can a fabricated delivery that points at no capture at all.
    payload["capture"]["sha256"] = "0" * 64
    target.write_text(json.dumps(payload))
    with pytest.raises(EvidenceStateError, match="delivery_capture_mismatch"):
        read_delivery(target, capture_root=config.evidence)

    # And nothing may be consumed without the capture root to verify against.
    payload["capture"]["sha256"] = json.loads(deliveries(config, DAY)[0].read_text())["capture"][
        "sha256"
    ]
    target.write_text(json.dumps(payload))
    with pytest.raises(EvidenceStateError, match="delivery_unbound"):
        read_delivery(target)


def test_scale_consistent_x10_body_is_refused_without_a_declared_unit(tmp_path, calendar):
    """The gate's wrong-units attack: ×10 prices, no unit field anywhere."""
    scaled = "IF2610  ,1000,1030,990,12,5678.0,34,0,1020,1010,1000,10,10,--"
    body = csv_body((scaled,))
    root = tmp_path / "data"
    config = Config(
        root=root,
        calendar_file=tmp_path / "calendar.json",
        start_date="2026-09-01",
        source=Source(retries=0, retry_delay_seconds=0),
    )  # no units declared: absent is refused, not labelled
    with Journal(config.root) as journal:
        report = collector(
            config,
            journal,
            calendar,
            CffexCsvClient(sender=lambda url, timeout=30: FakeResponse(body)),
        ).backfill(DAY, DAY)
        assert count(journal, "futures_daily") == 0
    assert report["status"] == "failed"
    assert {item["code"] for item in report["errors"]} == {"units_undetermined"}
    delivery = read_delivery(deliveries(config, DAY)[-1], capture_root=config.evidence)
    assert delivery["provenance"]["unit_authority"] == "absent"
    assert delivery["rows"][0]["open"] == "1000"  # the bytes are kept verbatim
    with pytest.raises(DataError, match="units_undetermined"):
        normalize(
            [
                {
                    "date": DAY.strftime("%Y%m%d"),
                    "symbol": "IF2610",
                    "open": "1000",
                    "high": "1030",
                    "low": "990",
                    "close": "1020",
                    "settle": "1010",
                    "pre_settle": "1000",
                    "volume": "12",
                    "open_interest": "34",
                    "turnover": "567.8",
                }
            ],
            DAY,
            units=UnitAuthority(authority="absent"),
        )


def test_row_with_its_own_date_must_still_agree_with_the_request(tmp_path):
    """row_date_mismatch stays reachable: a date column that disagrees is refused."""
    header = CSV_HEADER + ",日期"
    body = csv_body((CSV_FUTURES + ",20260917",), header=header)
    parsed = cffex_parse.parse_daily_csv(body)
    assert parsed.date_column == "日期" and parsed.provenance()["unit_source"] == "absent"
    with pytest.raises(DataError, match="row_date_mismatch"):
        normalize(parsed.rows, DAY, date_source="request_parameter", units=None)


def test_consume_applies_the_same_response_budget(config, calendar):
    with Journal(config.root) as journal:
        report = collector(
            config, journal, calendar, FakeClient([row(), row(symbol="T2612")])
        ).backfill(DAY, DAY)
        assert report["status"] == "complete"
        config.source.max_rows = 1  # the bound changed after the delivery was written
        consumed = collector(config, journal, calendar, NoSourceClient()).consume(DAY, DAY)
        assert consumed["status"] == "failed"
        assert {item["code"] for item in consumed["errors"]} == {"response_limit"}
        assert count(journal, "futures_daily") == 2  # nothing new committed, nothing lost


def test_calendar_delivery_has_its_own_version_and_reader(config, calendar):
    client = FakeClient([{"trade_date": "2026-09-17"}, {"trade_date": "2026-09-18"}])
    config.units = UNITS
    with Journal(config.root) as journal:
        report = refresh_calendar(config, journal, client)
        assert report["status"] == "complete"
        calendar_delivery = config.root / report["raw_path"]
        delivery = read_calendar_delivery(calendar_delivery, capture_root=config.evidence)
        assert delivery["version"] == CALENDAR_DELIVERY_VERSION != DELIVERY_VERSION
        assert delivery["evidence"]["entry_id"] and delivery["evidence"]["plan_sha256"]
        with pytest.raises(EvidenceStateError, match="unsupported_delivery_version"):
            read_delivery(calendar_delivery, capture_root=config.evidence)
        # A daily delivery is not a calendar delivery either.
        collector(config, journal, calendar, FakeClient()).backfill(DAY, DAY)
        daily = deliveries(config, DAY)[-1]
        with pytest.raises(EvidenceStateError, match="unsupported_delivery_version"):
            read_calendar_delivery(daily, capture_root=config.evidence)


def test_two_writers_cannot_share_a_generation(config):
    """The capture store's per-generation flock is real: the second writer is busy."""
    release = threading.Event()
    entered = threading.Event()

    class Slow(FakeClient):
        def fetch(self, *args, **kwargs):
            entered.set()
            assert release.wait(10)
            return super().fetch(*args, **kwargs)

    results, errors = {}, {}

    def run(name, client):
        try:
            results[name] = acquire(config.evidence, day_plan(config, DAY), client=client)[0]
        except Exception as exc:  # noqa: BLE001 - the code is the measurement
            errors[name] = getattr(exc, "code", str(exc))

    first = threading.Thread(target=run, args=("first", Slow([row()])))
    first.start()
    assert entered.wait(10)
    second = threading.Thread(target=run, args=("second", FakeClient([row()])))
    second.start()
    second.join(20)
    release.set()
    first.join(20)
    busy = (results.get("second") or {}).get("reason") or errors.get("second")
    assert busy == "acquisition_busy"
    assert results["first"]["status"] == "captured"
    (folder,) = captures_for(config, DAY)
    assert len(raw_bodies(folder)) == 1


def test_missing_receipt_is_refused_typed(config, calendar, tmp_path):
    with Journal(config.root) as journal:
        collector(config, journal, calendar, FakeClient([row()])).backfill(DAY, DAY)
    (folder,) = captures_for(config, DAY)
    task = json.loads((folder / "task.json").read_text())
    receipt = folder / "responses/000000/receipt.json"
    receipt.chmod(0o644)
    receipt.unlink()
    with pytest.raises(EvidenceStateError, match="evidence_state_unreadable") as caught:
        cffex_runtime.existing_state(config.evidence, task["task_id"], task["generation"])
    assert caught.value.detail["reason"]
    assert not receipt.exists()  # reading never repairs or recreates evidence


def test_every_trap_wrapper_is_attribute_asserted():
    install_transport_traps()
    assert assert_all_traps_armed() == list(TRAFFIC_LABELS)
    for label in TRAFFIC_LABELS:
        install_transport_traps()
        disarm_transport_trap(label)
        with pytest.raises(AssertionError, match="offline_guard_target_not_trapped"):
            assert_all_traps_armed()
        install_transport_traps()


def test_owned_cffex_sources_import_no_root_collector_module():
    """Premise correction: the root dependency enters only through tests/conftest.py."""
    import ast

    package = Path(cffex_runtime.__file__).resolve().parent
    offenders = {}
    for path in sorted(package.glob("*.py")):
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            names = []
            if isinstance(node, ast.Import):
                names = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
                names = [node.module]
            for name in names:
                if name.startswith("dq_historical_market_data_collector"):
                    offenders.setdefault(path.name, set()).add(name)
    assert offenders == {}
    conftest = Path(cffex_runtime.__file__).resolve().parents[3] / "tests" / "conftest.py"
    tree = ast.parse(conftest.read_text())
    imported = {
        node.module
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
        and (node.module or "").startswith("dq_historical_market_data_collector")
    }
    assert imported == {
        "dq_historical_market_data_collector.config",
        "dq_historical_market_data_collector.providers",
    }


# ---------------------------------------------------------------------------
# Acquisition only (`cffex acquire`): a literal plan in, raw evidence out
#
# The parent requirement separates *collection+saving* from *acquisition*: this
# task must write the source's bytes and then stop. It must not read a collector
# configuration file, open a journal, normalize or commit anything, and it must
# not import the modules that could -- including transitively.
#
# It must also hand those bytes to the collection task: the capture identity is
# derived by `cffex.identity`, the module both lanes call, so `Collector` finds
# the acquire generation and replays it with zero source calls.
# ---------------------------------------------------------------------------

ACQUIRE_HEADER = CSV_HEADER
ACQUIRE_BODY = csv_body((CSV_FUTURES, CSV_SECOND, CSV_OPTION, CSV_TOTAL))
LATER = date(2026, 9, 21)


def acquire_source(**changes):
    """The collector's transport authority, shaped as ``SourceLimits`` dumps it."""
    source = {
        "provider": "akshare",
        "credentials_env": {},
        "timeout_seconds": 60.0,
        "min_interval_seconds": 0.0,
        "max_rows": 10000,
        "max_response_bytes": 8_000_000,
        "retries": 2,
        "retry_delay_seconds": 0.0,
        "model": "AkshareFuturesHistDailyCffex",
    }
    source.update(changes)
    return source


def acquire_plan(
    plan_id="cffex-plan-1",
    dates=(DAY,),
    *,
    source=None,
    source_changes=None,
    units=None,
    requests=None,
    **changes,
):
    """A literal ``cffex-acquire-plan-v2`` document, JSON-ready."""
    plan = {
        "version": cffex_acquire.PLAN_VERSION,
        "plan_id": plan_id,
        "source_id": "cffex",
        "units": dict(UNITS if units is None else units),
        "token": "configured",
        "attempt": 0,
        "max_attempts": 2,
        "source": acquire_source(**(source_changes or {})) if source is None else source,
        "requests": requests
        if requests is not None
        else {"dates": [day.isoformat() for day in dates]},
    }
    plan.update(changes)
    return plan


def acquire_plan_for(config, plan_id="cffex-plan-1", dates=(DAY,), *, units=None, **changes):
    """The plan a configured deployment would hand an acquire-only run."""
    return acquire_plan(
        plan_id,
        dates,
        source=config.source.model_dump(mode="json"),
        units=dict(config.units) if units is None else units,
        **changes,
    )


def acquire_sender(body, calls=None, stop=None):
    def sender(url, timeout=30):
        if calls is not None:
            calls.append(url)
        if stop is not None:
            stop.set()
        return FakeResponse(body)

    return sender


def acquire_failing_sender(calls=None):
    def sender(url, timeout=30):
        if calls is not None:
            calls.append(url)
        raise ProviderFailure("transport_failure")

    return sender


def test_acquire_http_failure_body_is_preserved_and_never_replayed_as_success(tmp_path):
    root = tmp_path / "evidence"
    plan = acquire_plan(max_attempts=1)
    body = b"service unavailable"
    report = cffex_acquire.run(root, plan, sender=lambda url, timeout: FakeResponse(body, 503))
    assert report["status"] == "failed"
    assert report["results"][0]["http_status"] == 503
    assert raw_bodies(acquire_folders(root)[0]) == [body]
    calls = []
    again = cffex_acquire.run(root, plan, sender=acquire_sender(ACQUIRE_BODY, calls))
    assert len(calls) == 1
    assert again["status"] == "complete"


COMMITTED_COLUMNS = (
    "trading_date, contract_symbol, product, source_variety, open, high, low, "
    "close, settle, pre_settle, volume, open_interest, turnover, "
    "quality_flags_json, source_units_json, provider, model, observed_at, "
    "event_time_precision, publication_time, metadata_json"
)


def committed_rows(journal):
    """The judged business rows, without the per-capture revision id."""
    return [
        tuple(item)
        for item in journal.db.execute(
            f"SELECT {COMMITTED_COLUMNS} FROM futures_daily ORDER BY contract_symbol"
        ).fetchall()
    ]


def acquire_folders(root):
    return sorted(
        path
        for path in (root / EVIDENCE_VERSION).iterdir()
        if path.is_dir() and not path.name.startswith(".")
    )


def test_cffex_acquire_only_writes_transport_bytes_and_stops_there(tmp_path):
    root = tmp_path / "evidence"
    calls = []
    report = cffex_acquire.run(root, acquire_plan(), sender=acquire_sender(ACQUIRE_BODY, calls))
    assert report["status"] == "complete" and report["source_calls"] == 1
    assert report["captured"] == 1 and report["replayed"] == 0 and report["unprocessed"] == 0
    assert calls == [cffex_source.daily_csv_url(DAY)]
    (folder,) = acquire_folders(root)
    # The stored bytes are the transport body: gbk, summary/option rows included.
    assert raw_bodies(folder) == [ACQUIRE_BODY]
    receipt = json.loads((folder / "responses/000000/receipt.json").read_text())
    assert receipt["evidence_kind"] == "raw-bytes" and receipt["transformation"] is None
    assert receipt["sha256"] == hashlib.sha256(ACQUIRE_BODY).hexdigest()
    assert receipt["size"] == len(ACQUIRE_BODY)
    assert receipt["status"] == "captured" and receipt["error"] is None
    # Nothing downstream of the bytes exists: no parse result, delivery or journal.
    assert {path.relative_to(root).parts[0] for path in root.rglob("*")} == {EVIDENCE_VERSION}
    assert not (root / "raw").exists() and not list(root.rglob("journal.sqlite3"))
    rendered = json.dumps(report)
    for downstream in ("rows_committed", "trading_date", "delivery", "revisions"):
        assert downstream not in rendered
    entry = report["results"][0]
    assert entry["status"] == "captured" and entry["complete"] is True
    assert entry["terminal"] is True and entry["epoch"] == 0
    assert all("body" not in item for item in entry["responses"])


def test_cffex_acquire_only_imports_no_config_journal_normalize_or_storage(tmp_path):
    """The acquire task's module closure, measured in a fresh interpreter."""
    probe = tmp_path / "closure.py"
    probe.write_text(
        "import json, sys\n"
        "import dq_historical_market_data_collector.cffex.acquire as module\n"
        "print(json.dumps({'version': module.PLAN_VERSION, 'modules': sorted(sys.modules)}))\n"
    )
    env = os.environ.copy()
    source_root = Path(cffex_runtime.__file__).resolve().parents[2]
    repo = Path(cffex_runtime.__file__).resolve().parents[3]
    openbb = repo.parents[1]
    env["PYTHONPATH"] = os.pathsep.join(
        str(item)
        for item in (
            source_root,
            repo,
            openbb / "providers/akshare",
            openbb / "core",
            openbb / "collectors/core/src",
        )
    )
    env["OPENBB_AUTO_BUILD"] = "false"
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    child = subprocess.run(
        [sys.executable, str(probe)],
        cwd=str(tmp_path),
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert child.returncode == 0, child.stderr
    payload = json.loads(child.stdout)
    modules = set(payload["modules"])
    loaded = sorted(
        name for name in modules if name.startswith("dq_historical_market_data_collector")
    )
    assert loaded == [
        "dq_historical_market_data_collector",
        "dq_historical_market_data_collector.cffex",
        "dq_historical_market_data_collector.cffex.acquire",
        "dq_historical_market_data_collector.cffex.identity",
        "dq_historical_market_data_collector.cffex.source",
    ]
    for forbidden in ("sqlite3", "duckdb", "_duckdb", "dq_quant_invest_data", "pydantic", "yaml"):
        assert forbidden not in modules
    # The module's own imports, as written: no collector configuration module.
    tree = ast.parse(Path(cffex_acquire.__file__).read_text())
    relative, absolute = set(), set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            if node.level:
                relative.add("." * node.level + (node.module or ""))
            elif node.module:
                absolute.add(node.module)
        elif isinstance(node, ast.Import):
            absolute |= {alias.name for alias in node.names}
    # ``from . import identity`` parses as a level-1 import with no module name.
    assert relative == {".", ".source"}
    assert "dq_historical_market_data_collector.cffex.config" not in absolute
    assert absolute == {
        "__future__",
        "argparse",
        "hashlib",
        "json",
        "os",
        "re",
        "signal",
        "sys",
        "threading",
        "dataclasses",
        "datetime",
        "pathlib",
        "typing",
        "openbb_collector_core.raw_acquisition",
    }


def test_cffex_acquire_only_identity_is_the_collectors_capture_identity(config, calendar):
    """F1: both lanes derive the same task and generation, by calling the same code."""
    authority = cffex_acquire.validate_plan(acquire_plan_for(config))
    assert authority["fingerprint"] == config.fingerprint()
    with Journal(config.root) as journal:
        planned = collector(config, journal, calendar, NoSourceClient())._plan(
            DAY, 0, "configured"
        )["entries"][0]["task"]
    assert cffex_acquire.task_for(authority, DAY) == planned
    # The plan label names the plan, not the capture: identity ignores it.
    other = cffex_acquire.validate_plan(acquire_plan_for(config, plan_id="a-different-label"))
    assert cffex_acquire.task_for(other, DAY) == planned
    # A different authority names a different capture, so evidence cannot be mixed.
    changed = cffex_acquire.validate_plan(acquire_plan_for(config, units={}))
    assert changed["fingerprint"] != authority["fingerprint"]
    assert cffex_acquire.task_for(changed, DAY)["generation"] != planned["generation"]
    # The model/provider vocabulary is the collector's, not a second opinion.
    assert set(cffex_acquire.identity.ALLOWED_MODELS) == set(
        Source.model_fields["model"].annotation.__args__
    )
    assert tuple(cffex_acquire.identity.UNIT_FIELDS) == tuple(UNIT_FIELDS)


PRE_ROUND_FINGERPRINTS = {
    "default": "cf6850e5c753db2dabe0ec408f0e9981ef9bed40a0e0c88c8fcd70b2a95b34e8",
    "units": "bbfdcc0f422ee72b09cd8989e924e6eb23e2c921850e4a4d8ab3a29f0479fbda",
    "custom": "8e0a36ccc3ca70230d305a3f51d8c9010bef5b7155d273e7e0a4cdd3b32e8af7",
}


def test_cffex_acquire_only_identity_preserves_the_existing_fingerprint_value(tmp_path):
    """Moving the formula into `identity` renamed no existing capture.

    The digests below were measured from the collection lane *before* this round
    delegated ``Config.fingerprint`` to :mod:`cffex.identity`; every stored
    ``raw-acquisition-v1`` generation is named from them, so they must not move.
    """
    cases = {
        "default": Config(calendar_file=tmp_path / "c.json"),
        "units": Config(
            calendar_file=tmp_path / "c.json",
            start_date="2026-09-01",
            units=UNITS,
            source=Source(retries=2, retry_delay_seconds=0),
        ),
        "custom": Config(
            calendar_file=tmp_path / "c.json",
            source_id="cffex-b",
            units={"price_unit": "x"},
            source=Source(max_rows=5),
        ),
    }
    assert {name: config.fingerprint() for name, config in cases.items()} == (
        PRE_ROUND_FINGERPRINTS
    )
    # The same function, called directly, agrees with the configuration object.
    for config in cases.values():
        assert (
            cffex_acquire.identity.fingerprint(
                config.source_id, config.source.model_dump(mode="json"), config.units
            )
            == config.fingerprint()
        )


def test_cffex_acquire_only_evidence_is_consumed_by_backfill_with_zero_source_calls(
    config, calendar, tmp_path
):
    """F1 end to end: acquire -> Collector.backfill -> 0 calls -> identical rows."""
    plan = acquire_plan_for(config)
    acquired_calls = []
    acquired = cffex_acquire.run(
        config.evidence, plan, sender=acquire_sender(ACQUIRE_BODY, acquired_calls)
    )
    assert acquired["status"] == "complete" and len(acquired_calls) == 1

    with Journal(config.root) as journal:
        report = collector(config, journal, calendar, NoSourceClient()).backfill(DAY, DAY)
        warmed = committed_rows(journal)
    assert report["status"] == "complete"
    # Zero refetch: the acquire generation was found and replayed, not repeated.
    assert report["provider_calls"] == 0 and report["evidence_replays"] == 1
    assert report["evidence_captures"] == 0
    assert report["rows_committed"] == len(warmed) > 0
    # And there is exactly one capture: no second generation was written.
    assert len(acquire_folders(config.evidence)) == 1

    # The same body, fetched the ordinary way in a second deployment, commits the
    # same rows -- the reuse is a replay, not a different reading of the bytes.
    cold = Config(
        root=tmp_path / "cold",
        calendar_file=tmp_path / "cold_calendar.json",
        start_date="2026-09-01",
        units=UNITS,
        source=Source(retries=2, retry_delay_seconds=0),
    )
    cold_calls = []
    with Journal(cold.root) as cold_journal:
        client = CffexCsvClient(threading.Event(), sender=acquire_sender(ACQUIRE_BODY, cold_calls))
        cold_report = collector(cold, cold_journal, calendar, client).backfill(DAY, DAY)
        fetched = committed_rows(cold_journal)
    assert cold_report["provider_calls"] == 1 and len(cold_calls) == 1
    assert fetched == warmed


def test_cffex_acquire_only_failed_request_is_retryable_under_the_same_plan_id(tmp_path):
    """F3: a failed (non-cancelled) receipt is an attempt, not a tombstone."""
    root = tmp_path / "evidence"
    plan = acquire_plan()
    first_calls = []
    first = cffex_acquire.run(root, plan, sender=acquire_failing_sender(first_calls))
    failed = first["results"][0]
    # max_attempts=2 is the *only* bound on retrying this date: one run makes two
    # attempts, each in its own generation, and then stops.
    assert first["status"] == "failed" and len(first_calls) == 2
    assert failed["status"] == "failed" and failed["reason"] == "provider_failure"
    assert failed["code"] == "attempts_exhausted"
    assert failed["complete"] is False and failed["terminal"] is True
    assert failed["epoch"] == 1 and failed["attempts"] == 2
    assert [item["epoch"] for item in failed["epochs"]] == [0, 1]
    assert failed["http_status"] is None
    assert len(acquire_folders(root)) == 2

    # The same plan id retries again on the next run: one call, and it completes.
    second_calls = []
    second = cffex_acquire.run(root, plan, sender=acquire_sender(ACQUIRE_BODY, second_calls))
    retried = second["results"][0]
    assert len(second_calls) == 1 and second["source_calls"] == 1
    assert second["status"] == "complete"
    assert retried["status"] == "captured" and retried["complete"] is True
    assert retried["epoch"] == 2 and retried["attempts"] == 1
    assert retried["locator"] != failed["locator"]
    assert [item["epoch"] for item in retried["epochs"]] == [2]

    # Both failed generations are immutable evidence of the attempts, body-free.
    states = []
    for folder in acquire_folders(root):
        task = json.loads((folder / "task.json").read_text())
        states.append(
            cffex_runtime.read_capture(
                root, task["task_id"], task["generation"], version=EVIDENCE_VERSION
            )
        )
    assert sorted(state["status"] for state in states) == ["captured", "failed", "failed"]
    for folder, state in zip(acquire_folders(root), states, strict=True):
        assert (folder / "terminal.json").is_file()
        if state["status"] == "failed":
            assert state["reason"] == "finished"
            assert not (folder / "responses/000000/body.bin").exists()

    # From then on the plan replays: zero calls, same generation.
    third_calls = []
    third = cffex_acquire.run(root, plan, sender=acquire_sender(ACQUIRE_BODY, third_calls))
    assert third_calls == [] and third["source_calls"] == 0
    assert third["replayed"] == 1 and third["results"][0]["generation"] == retried["generation"]


def test_cffex_acquire_only_retry_is_bounded_by_the_plan(tmp_path):
    """F3, the bound: a run opens at most ``max_attempts`` generations per date."""
    root = tmp_path / "evidence"
    plan = acquire_plan(max_attempts=1)
    first_calls = []
    first = cffex_acquire.run(root, plan, sender=acquire_failing_sender(first_calls))
    assert first["results"][0]["attempts"] == 1 and len(first_calls) == 1
    second_calls = []
    second = cffex_acquire.run(root, plan, sender=acquire_failing_sender(second_calls))
    entry = second["results"][0]
    assert len(second_calls) == 1  # exactly one attempt, then the run stops
    assert entry["attempts"] == 1 and entry["status"] == "failed"
    # The receipt's cause is kept; the run's own verdict is the typed code.
    assert entry["reason"] == "provider_failure" and entry["code"] == "attempts_exhausted"
    # Two failed generations on disk, and the next run moves on to the third.
    assert len(acquire_folders(root)) == 2
    assert cffex_acquire.EXIT_CODES[second["status"]] == 1


def test_cffex_acquire_only_source_calls_count_every_attempt(tmp_path):
    """N2: ``source_calls`` is every real call, never the last attempt's counter."""
    root = tmp_path / "evidence"
    seen = []

    def flaky(url, timeout=30):
        seen.append(url)
        if len(seen) == 1:
            raise ProviderFailure("transport_failure")
        return FakeResponse(ACQUIRE_BODY)

    retried = cffex_acquire.run(root, acquire_plan(), sender=flaky)
    entry = retried["results"][0]
    assert len(seen) == 2 and entry["attempts"] == 2
    assert entry["status"] == "captured" and retried["status"] == "complete"
    # Two real provider calls: the failed epoch and the capture that replaced it.
    assert entry["source_calls"] == 2 == retried["source_calls"]
    assert [item["source_calls"] for item in entry["epochs"]] == [1, 1]
    assert [item["epoch"] for item in entry["epochs"]] == [0, 1]

    # The same count when the retries are exhausted rather than successful.
    exhausted_root = tmp_path / "exhausted"
    exhausted_calls = []
    exhausted = cffex_acquire.run(
        exhausted_root, acquire_plan(), sender=acquire_failing_sender(exhausted_calls)
    )
    failed = exhausted["results"][0]
    assert len(exhausted_calls) == 2 and failed["attempts"] == 2
    assert failed["source_calls"] == 2 == exhausted["source_calls"]
    assert failed["code"] == "attempts_exhausted" and failed["status"] == "failed"

    # One attempt, one call; and a replay of the warm capture calls nothing, so
    # the counter is a measurement, not a constant.
    single_root = tmp_path / "single"
    single_calls = []
    single = cffex_acquire.run(
        single_root, acquire_plan(max_attempts=1), sender=acquire_failing_sender(single_calls)
    )
    assert len(single_calls) == 1 and single["source_calls"] == 1
    replay_calls = []
    replay = cffex_acquire.run(
        root, acquire_plan(), sender=acquire_sender(ACQUIRE_BODY, replay_calls)
    )
    assert replay_calls == [] and replay["source_calls"] == 0


def test_cffex_acquire_only_entry_complete_agrees_with_its_status(tmp_path):
    """F4: an entry is complete only when it is captured, never on a failed one."""
    root = tmp_path / "evidence"
    plan = acquire_plan(dates=(DAY, LATER))

    def mixed(url, timeout=30):
        if DAY.strftime("%Y%m%d") in url:
            return FakeResponse(ACQUIRE_BODY)
        raise ProviderFailure("transport_failure")

    report = cffex_acquire.run(root, plan, sender=mixed)
    captured, failed = report["results"]
    assert captured["status"] == "captured" and captured["complete"] is True
    assert failed["status"] == "failed" and failed["complete"] is False
    assert failed["terminal"] is True and failed["reason"] == "provider_failure"
    assert failed["code"] == "attempts_exhausted"
    assert report["status"] == "partial"
    assert report["captured"] == 1 and report["failed"] == 1
    # The invariant a consumer may rely on, over every entry of the report.
    assert all(item["complete"] is (item["status"] == "captured") for item in report["results"])
    assert cffex_acquire.EXIT_CODES[report["status"]] == 3


def test_cffex_acquire_only_partial_exits_three_like_the_collection_cli():
    """F7: the acquire partial code is the collection CLI's, not a private one."""
    assert cffex_acquire.EXIT_CODES["partial"] == cli.EXIT_CODES["partial"] == 3
    assert cffex_acquire.EXIT_CODES["complete"] == cli.EXIT_CODES["complete"] == 0
    assert cffex_acquire.EXIT_CODES["failed"] == cli.EXIT_CODES["failed"] == 1


def test_cffex_acquire_only_reports_plan_and_file_digests_separately(tmp_path):
    """F6: the normalised-plan digest is not the file digest; both are reported."""
    root = tmp_path / "evidence"
    path = tmp_path / "plan.json"
    value = acquire_plan()
    path.write_text(json.dumps(value, indent=2, sort_keys=True))
    read, file_digest = cffex_acquire.read_plan(path)
    report = cffex_acquire.run(
        root, read, sender=acquire_sender(ACQUIRE_BODY), plan_file_sha256=file_digest
    )
    assert report["plan_sha256"] == cffex_acquire.digest(cffex_acquire.validate_plan(value))
    assert report["plan_file_sha256"] == hashlib.sha256(path.read_bytes()).hexdigest()
    assert report["plan_sha256"] != report["plan_file_sha256"]
    # Re-indenting the file changes the bytes, not the normalised plan.
    path.write_text(json.dumps(value, indent=4))
    _, reformatted = cffex_acquire.read_plan(path)
    assert reformatted != file_digest
    again = cffex_acquire.run(
        root, value, sender=acquire_sender(ACQUIRE_BODY), plan_file_sha256=reformatted
    )
    assert again["plan_sha256"] == report["plan_sha256"]
    assert again["plan_file_sha256"] == reformatted


ACQUIRE_REFUSALS = {
    "empty": ({}, "unknown_plan"),
    "extra-key": ({**acquire_plan(), "entries": []}, "unknown_plan"),
    "round-2-version": (
        {**acquire_plan(), "version": "cffex-acquire-plan-v1"},
        "unsupported_version",
    ),
    "raw-plan-version": (
        {**acquire_plan(), "version": "cffex-raw-plan-v1"},
        "unsupported_version",
    ),
    "empty-plan-id": (acquire_plan(plan_id=""), "invalid_plan_id"),
    "source-field": (
        {
            **acquire_plan(),
            "source": {
                key: value
                for key, value in acquire_plan()["source"].items()
                if key != "min_interval_seconds"
            },
        },
        "invalid_source",
    ),
    "provider": (acquire_plan(source_changes={"provider": "yfinance"}), "unsupported_provider"),
    "model": (
        acquire_plan(source_changes={"model": "AkshareToolTradeDateHistSina"}),
        "unsupported_model",
    ),
    "max-rows": (acquire_plan(source_changes={"max_rows": 0}), "invalid_max_rows"),
    "max-bytes": (
        acquire_plan(source_changes={"max_response_bytes": True}),
        "invalid_max_response_bytes",
    ),
    "timeout": (acquire_plan(source_changes={"timeout_seconds": 0}), "invalid_timeout"),
    "interval": (
        acquire_plan(source_changes={"min_interval_seconds": -1}),
        "invalid_min_interval",
    ),
    "retries": (acquire_plan(source_changes={"retries": 3}), "invalid_retries"),
    "retry-delay": (
        acquire_plan(source_changes={"retry_delay_seconds": -1}),
        "invalid_retry_delay",
    ),
    "units-key": (acquire_plan(units={"price": "x"}), "unknown_unit_field"),
    "units-value": (acquire_plan(units={"price_unit": ""}), "invalid_unit"),
    "token": (acquire_plan(token=""), "invalid_token"),
    "attempt": (acquire_plan(attempt=3), "invalid_attempt"),
    "max-attempts-low": (acquire_plan(max_attempts=0), "invalid_max_attempts"),
    "max-attempts-high": (acquire_plan(max_attempts=6), "invalid_max_attempts"),
    "no-requests": ({**acquire_plan(), "requests": {}}, "unknown_requests"),
    "both-forms": (
        {**acquire_plan(), "requests": {"dates": ["2026-09-18"], "range": {}}},
        "unknown_requests",
    ),
    "empty-dates": (acquire_plan(requests={"dates": []}), "plan_entry_limit"),
    "short-date": (acquire_plan(requests={"dates": ["2026-9-18"]}), "invalid_date"),
    "impossible-date": (acquire_plan(requests={"dates": ["2026-02-30"]}), "invalid_date"),
    "duplicate-date": (
        acquire_plan(requests={"dates": ["2026-09-18", "2026-09-18"]}),
        "duplicate_date",
    ),
    "too-many-dates": (
        acquire_plan(
            requests={
                "dates": [
                    (date(2026, 1, 1) + timedelta(days=offset)).isoformat() for offset in range(367)
                ]
            }
        ),
        "plan_entry_limit",
    ),
    "range-order": (
        acquire_plan(
            requests={
                "range": {
                    "start": "2026-09-21",
                    "end": "2026-09-18",
                    "weekdays_only": True,
                }
            }
        ),
        "invalid_range_bounds",
    ),
    "range-span": (
        acquire_plan(
            requests={
                "range": {
                    "start": "2026-01-01",
                    "end": "2027-12-31",
                    "weekdays_only": True,
                }
            }
        ),
        "range_span_limit",
    ),
    "range-weekdays": (
        acquire_plan(
            requests={
                "range": {
                    "start": "2026-09-01",
                    "end": "2026-09-30",
                    "weekdays_only": "yes",
                }
            }
        ),
        "invalid_weekdays_only",
    ),
    "range-field": (
        acquire_plan(requests={"range": {"start": "2026-09-01", "end": "2026-09-30"}}),
        "unknown_range",
    ),
}


@pytest.mark.parametrize("shape", sorted(ACQUIRE_REFUSALS))
def test_cffex_acquire_only_refuses_a_malformed_plan_before_any_source_call(
    tmp_path, shape, capsys
):
    plan, code = ACQUIRE_REFUSALS[shape]
    root = tmp_path / "evidence"
    calls = []
    with pytest.raises(cffex_acquire.PlanError) as caught:
        cffex_acquire.run(root, plan, sender=acquire_sender(ACQUIRE_BODY, calls))
    assert caught.value.code == code
    assert calls == [] and not root.exists()
    # And the CLI boundary refuses typed, with the same code and no store.
    path = tmp_path / "plan.json"
    path.write_text(json.dumps(plan))
    assert cffex_acquire.main(["--plan", str(path), "--root", str(root)]) == 2
    emitted = json.loads(capsys.readouterr().out)
    assert emitted["status"] == "error" and emitted["code"] == code
    assert not root.exists()


def test_cffex_acquire_only_replays_a_completed_plan_with_zero_source_calls(tmp_path):
    root = tmp_path / "evidence"
    first_calls = []
    first = cffex_acquire.run(
        root, acquire_plan(), sender=acquire_sender(ACQUIRE_BODY, first_calls)
    )
    files = {
        path.relative_to(root): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in root.rglob("*")
        if path.is_file()
    }
    second_calls = []
    second = cffex_acquire.run(
        root, acquire_plan(), sender=acquire_sender(ACQUIRE_BODY, second_calls)
    )
    assert first["status"] == second["status"] == "complete"
    assert second["source_calls"] == 0 and second_calls == []
    assert second["replayed"] == 1 and second["captured"] == 1
    assert second["results"][0]["locator"] == first["results"][0]["locator"]
    # A replay verifies the store; it does not rewrite it.
    assert files == {
        path.relative_to(root): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in root.rglob("*")
        if path.is_file()
    }


def test_cffex_acquire_only_cancellation_writes_a_durable_veto(tmp_path):
    root = tmp_path / "evidence"
    later = date(2026, 9, 21)
    stop = threading.Event()
    calls = []
    report = cffex_acquire.run(
        root,
        acquire_plan(dates=(DAY, later)),
        sender=acquire_sender(ACQUIRE_BODY, calls, stop=stop),
        stop=stop,
    )
    assert report["status"] == "cancelled" and report["source_calls"] == 1
    assert report["attempted"] == 1 and report["unprocessed"] == 1
    # N1: the stopped entry is typed ``cancelled`` and counted as such, so the
    # plan-level counters agree with the plan-level status.
    assert report["results"][0]["status"] == "cancelled"
    assert report["results"][0]["reason"] == "cancelled"
    assert report["cancelled"] == 1 and report["failed"] == 0
    (folder,) = acquire_folders(root)
    assert (folder / "cancel.json").is_file()
    task = json.loads((folder / "task.json").read_text())
    state = cffex_runtime.read_capture(
        root, task["task_id"], task["generation"], version=EVIDENCE_VERSION
    )
    assert state["reason"] == "cancelled" and state["complete"] is False
    # The veto is durable: that date never calls the source again under this plan.
    resumed_calls = []
    again = cffex_acquire.run(
        root, acquire_plan(dates=(DAY, later)), sender=acquire_sender(ACQUIRE_BODY, resumed_calls)
    )
    assert resumed_calls == [cffex_source.daily_csv_url(later)]
    assert again["results"][0]["reason"] == "cancelled"
    assert again["results"][0]["source_calls"] == 0
    assert again["results"][1]["status"] == "captured"


def test_cffex_acquire_only_cancelled_replay_reports_cancelled(tmp_path, capsys):
    """N1: replaying a vetoed plan is a typed ``cancelled`` outcome with exit 4."""
    root = tmp_path / "evidence"
    stop = threading.Event()
    cffex_acquire.run(
        root,
        acquire_plan(dates=(DAY,)),
        sender=acquire_sender(ACQUIRE_BODY, stop=stop),
        stop=stop,
    )
    before = {
        path.relative_to(root): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in root.rglob("*")
        if path.is_file()
    }
    replay_calls = []
    replay = cffex_acquire.run(
        root, acquire_plan(dates=(DAY,)), sender=acquire_sender(ACQUIRE_BODY, replay_calls)
    )
    assert replay_calls == [] and replay["source_calls"] == 0
    entry = replay["results"][0]
    # One word for the store's reason, this run's verdict and the exit code.
    assert entry["status"] == entry["reason"] == entry["code"] == "cancelled"
    assert replay["status"] == "cancelled"
    assert replay["cancelled"] == 1
    assert replay["failed"] == 0 and replay["captured"] == 0 and replay["replayed"] == 0
    assert entry["complete"] is False
    assert cffex_acquire.EXIT_CODES[replay["status"]] == 4
    # The CLI boundary reports the same thing and returns 4, not 1: a scheduler
    # can distinguish an outstanding veto from a source failure.
    path = tmp_path / "plan.json"
    path.write_text(json.dumps(acquire_plan(dates=(DAY,))))
    assert cffex_acquire.main(["--plan", str(path), "--root", str(root)]) == 4
    emitted = json.loads(capsys.readouterr().out)
    assert emitted["status"] == "cancelled" and emitted["cancelled"] == 1
    assert emitted["source_calls"] == 0 and emitted["failed"] == 0
    assert emitted["plan_sha256"] == replay["plan_sha256"]
    # Still one generation, and the replay wrote nothing: the veto is a read.
    (folder,) = acquire_folders(root)
    assert (folder / "cancel.json").is_file()
    assert before == {
        path.relative_to(root): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in root.rglob("*")
        if path.is_file()
    }
    assert len(acquire_folders(root)) == 1


def test_cffex_acquire_only_range_is_a_literal_calendar_range():
    """No exchange calendar exists without configuration; the plan says so."""
    authority = cffex_acquire.validate_plan(
        acquire_plan(
            requests={
                "range": {
                    "start": "2026-09-17",
                    "end": "2026-09-22",
                    "weekdays_only": True,
                }
            }
        )
    )
    assert authority["dates"] == ["2026-09-17", "2026-09-18", "2026-09-21", "2026-09-22"]
    every_day = cffex_acquire.validate_plan(
        acquire_plan(
            requests={
                "range": {
                    "start": "2026-09-17",
                    "end": "2026-09-22",
                    "weekdays_only": False,
                }
            }
        )
    )
    assert every_day["dates"] == [
        "2026-09-17",
        "2026-09-18",
        "2026-09-19",
        "2026-09-20",
        "2026-09-21",
        "2026-09-22",
    ]


ACQUIRE_CHILD = r"""
import contextlib
import io
import json
import os
import sqlite3
import sys

if os.environ.get("OPENBB_AUTO_BUILD") != "false":
    raise SystemExit("OPENBB_AUTO_BUILD must be false before Python starts")

opened = []


def audit(event, args):
    if event == "open" and args and isinstance(args[0], (str, bytes, os.PathLike)):
        opened.append(os.path.abspath(os.fspath(args[0])))


sys.addaudithook(audit)

root = os.path.abspath(sys.argv[1])
central = os.path.abspath(sys.argv[2])
plan_path = sys.argv[3]
body = bytes.fromhex(sys.argv[4])
private = root + os.sep
real_connect = sqlite3.connect
attempts = []


def poisoned(*args, **kwargs):
    target = str(args[0]) if args else ""
    if target.startswith("file:"):
        target = target[5:].split("?", 1)[0]
    target = os.path.abspath(target)
    if not (target == root or target.startswith(private)):
        attempts.append(target)
        raise AssertionError("database access outside the private root: " + target)
    return real_connect(*args, **kwargs)


sqlite3.connect = poisoned


class Blocker:
    def find_spec(self, name, path=None, target=None):
        if name.split(".")[0] in {"duckdb", "_duckdb"} or name.startswith("dq_quant_invest_data"):
            raise AssertionError("central module imported: " + name)
        return None


sys.meta_path.insert(0, Blocker())

from dq_historical_market_data_collector.cffex import acquire as acquire_task
from dq_historical_market_data_collector.cffex import source as cffex_source

CALLS = []


class Response:
    def __init__(self, content):
        self.content = content
        self.status_code = 200
        self.headers = {"Content-Type": "text/csv"}


def fake(url, timeout=30, **_kwargs):
    CALLS.append(url)
    return Response(body)


# Only the network hop is replaced: source.fetch_body still reads response.content.
cffex_source.transport = lambda: fake

buffer = io.StringIO()
with contextlib.redirect_stdout(buffer):
    code = acquire_task.main(["--plan", plan_path, "--root", root])
report = json.loads(buffer.getvalue())

# Falsifiability: the same hook must observe a central path when one is touched.
try:
    with open(os.path.join(central, "probe"), "w", encoding="utf-8") as stream:
        stream.write("x")
except OSError:
    pass
falsification = [path for path in opened if path.startswith(central + os.sep)]
blocked = None
try:
    sqlite3.connect(os.path.join(central, "journal.sqlite3"))
except AssertionError:
    blocked = "poisoned"

written = (
    sorted(
        {
            os.path.relpath(os.path.join(folder, name), root).split(os.sep)[0]
            for folder, _dirs, names in os.walk(root)
            for name in names
        }
    )
    if os.path.isdir(root)
    else []
)
print(
    json.dumps(
        {
            "exit": code,
            "report": report,
            "calls": CALLS,
            "heavy_modules": sorted(
                name
                for name in sys.modules
                if name.startswith("dq_historical_market_data_collector.cffex.")
            ),
            "config_loaded": "dq_historical_market_data_collector.cffex.config" in sys.modules,
            "journal_loaded": "dq_historical_market_data_collector.cffex.storage" in sys.modules,
            "sqlite_attempts_outside_root": attempts,
            "central_opens": [
                path
                for path in opened
                if (path == central or path.startswith(central + os.sep))
                and path not in falsification
            ],
            "falsification_opens": falsification,
            "central_database_blocked": blocked,
            "duckdb_loaded": "duckdb" in sys.modules,
            "central_exists": os.path.exists(central),
            "auto_build_effective": os.environ.get("OPENBB_AUTO_BUILD"),
            "written_roots": written,
        }
    )
)
"""


def run_acquire_child(tmp_path, central, plan, body, root):
    script = tmp_path / "acquire_child.py"
    script.write_text(ACQUIRE_CHILD)
    plan_path = tmp_path / "acquire_plan.json"
    plan_path.write_text(json.dumps(plan))
    env = os.environ.copy()
    source_root = Path(cffex_runtime.__file__).resolve().parents[2]
    repo = Path(cffex_runtime.__file__).resolve().parents[3]
    openbb = repo.parents[1]
    env["PYTHONPATH"] = os.pathsep.join(
        str(item)
        for item in (
            source_root,
            repo,
            openbb / "providers/akshare",
            openbb / "core",
            openbb / "collectors/core/src",
        )
    )
    env["OPENBB_AUTO_BUILD"] = "false"
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    child = subprocess.run(
        [sys.executable, str(script), str(root), str(central), str(plan_path), body.hex()],
        cwd=str(tmp_path),
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    return child, (json.loads(child.stdout) if child.returncode == 0 else None)


@pytest.mark.parametrize("state", ["absent", "locked", "inaccessible"])
def test_cffex_acquire_only_runs_with_no_config_and_no_central_store(tmp_path, state):
    """The acquisition task completes with no config file and no reachable centre."""
    central = tmp_path / f"central-{state}"
    if state != "absent":
        central.mkdir()
        (central / "journal.sqlite3").write_bytes(b"")
        (central / "catalog.duckdb").write_bytes(b"")
    if state == "inaccessible":
        central.chmod(0)
    root = tmp_path / "private"
    held = hold_exclusive(central / "journal.sqlite3") if state == "locked" else None
    observed = {}
    try:
        with held if held is not None else contextlib.nullcontext():
            child, payload = run_acquire_child(
                tmp_path, central, acquire_plan(), ACQUIRE_BODY, root
            )
            if state == "locked":
                observed["lock_still_held_after_child"] = lock_is_held(central / "journal.sqlite3")
    finally:
        if state == "inaccessible":
            central.chmod(0o700)
    assert child.returncode == 0, child.stderr
    report = payload["report"]
    assert report["status"] == "complete" and report["source_calls"] == 1
    assert payload["calls"] == [cffex_source.daily_csv_url(DAY)]
    # The acquire task's own closure: no configuration, journal or normalizer.
    assert payload["heavy_modules"] == [
        "dq_historical_market_data_collector.cffex.acquire",
        "dq_historical_market_data_collector.cffex.identity",
        "dq_historical_market_data_collector.cffex.source",
    ]
    assert payload["config_loaded"] is False and payload["journal_loaded"] is False
    # The private root is the only writable store and holds only raw evidence.
    assert payload["written_roots"] == [EVIDENCE_VERSION]
    (folder,) = acquire_folders(root)
    assert raw_bodies(folder) == [ACQUIRE_BODY]
    receipt = json.loads((folder / "responses/000000/receipt.json").read_text())
    assert receipt["evidence_kind"] == "raw-bytes" and receipt["transformation"] is None
    # Zero central access, with every control proven live in that same child.
    assert payload["central_opens"] == []
    assert payload["duckdb_loaded"] is False
    assert payload["central_database_blocked"] == "poisoned"
    assert payload["sqlite_attempts_outside_root"] == [str(central / "journal.sqlite3")]
    assert payload["falsification_opens"] == [str(central / "probe")]
    assert payload["auto_build_effective"] == "false"
    assert payload["central_exists"] is (state != "absent")
    if state == "locked":
        assert observed["lock_still_held_after_child"] is True


def test_cffex_cli_acquire_alias_returns_the_acquire_exit_code(tmp_path, capfd):
    """`cffex acquire` is wired into the collector CLI as an isolated child.

    The alias runs in a fresh interpreter (this module imports the journal for the
    collection commands), so its output is read at the file-descriptor level.
    """
    path = tmp_path / "plan.json"
    path.write_text('{"version": "cffex-acquire-plan-v2"}')
    code = cli.main(["acquire", "--plan", str(path), "--root", str(tmp_path / "evidence")])
    emitted = json.loads(capfd.readouterr().out)
    assert code == 2 and emitted["command"] == "acquire" and emitted["code"] == "unknown_plan"
    assert not (tmp_path / "evidence").exists()


def test_http_status_is_bound_and_replay_reports_it(tmp_path):
    root = tmp_path / "evidence"
    plan = acquire_plan(max_attempts=1)
    first = cffex_acquire.run(root, plan, sender=acquire_sender(ACQUIRE_BODY))
    assert first["results"][0]["http_status"] == 200
    replay = cffex_acquire.run(root, plan, sender=acquire_failing_sender())
    assert replay["results"][0]["http_status"] == 200
    assert replay["source_calls"] == 0
    (folder,) = acquire_folders(root)
    receipt = folder / "responses/000000/receipt.json"
    record = json.loads(receipt.read_text())
    record["http_status"] = 503
    receipt.chmod(0o644)
    receipt.write_text(json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n")
    with pytest.raises(ValueError, match="terminal_conflict"):
        cffex_acquire.run(root, plan, sender=acquire_failing_sender())


def test_legacy_statusless_raw_capture_is_not_replayed(tmp_path):
    from openbb_collector_core.raw_acquisition import RawResponse, capture

    root = tmp_path / "evidence"
    plan = acquire_plan(max_attempts=1)
    authority = cffex_acquire.validate_plan(plan)
    task = cffex_acquire.task_for(authority, DAY)
    task["version"] = "raw-acquisition-v1"
    capture(root, task, lambda _: RawResponse(ACQUIRE_BODY))
    calls = []
    with pytest.raises(ValueError, match="legacy_http_status_missing"):
        cffex_acquire.run(root, plan, sender=acquire_sender(ACQUIRE_BODY, calls))
    assert not calls
    assert not (root / "raw-acquisition-v2").exists()


def test_evidence_versions_are_isolated_and_v1_rejects_status(tmp_path):
    from openbb_collector_core.raw_acquisition import RawResponse, capture, read_capture

    task = cffex_acquire.task_for(cffex_acquire.validate_plan(acquire_plan()), DAY)
    assert task["version"] == "raw-acquisition-v2"
    old = {**task, "version": "raw-acquisition-v1"}
    capture(tmp_path, old, lambda _: RawResponse(b"old"))
    capture(tmp_path, task, lambda _: RawResponse(b"new", http_status=200))
    assert (
        read_capture(tmp_path, task["task_id"], task["generation"])["responses"][0]["body"]
        == b"old"
    )
    assert (
        read_capture(tmp_path, task["task_id"], task["generation"], version=task["version"])[
            "responses"
        ][0]["body"]
        == b"new"
    )
    old2 = {**old, "generation": "other"}
    with pytest.raises(ValueError, match="invalid_fields"):
        capture(tmp_path, old2, lambda _: RawResponse(b"bad", http_status=200))


def test_missing_transport_status_is_never_assumed_success():
    class MissingStatus:
        content = b"body"
        headers = {}

    with pytest.raises(ValueError, match="invalid_source_status"):
        cffex_source.fetch_body(DAY, fetch=lambda *args, **kwargs: MissingStatus())


def test_interrupted_http_failure_resumes_without_losing_status(tmp_path):
    root = tmp_path / "evidence"
    plan = acquire_plan(max_attempts=1)
    first = cffex_acquire.run(root, plan, sender=lambda url, timeout: FakeResponse(b"bad", 503))
    assert first["status"] == "failed"
    (folder,) = acquire_folders(root)
    (folder / "durable.json").unlink()
    calls = []
    second = cffex_acquire.run(root, plan, sender=acquire_sender(ACQUIRE_BODY, calls))
    assert second["status"] == "failed"
    assert second["results"][0]["http_status"] == 503
    assert second["source_calls"] == 0 and not calls
    assert raw_bodies(folder) == [b"bad"]
    third = cffex_acquire.run(root, plan, sender=acquire_sender(ACQUIRE_BODY, calls))
    assert third["status"] == "complete" and len(calls) == 1


def test_collector_rejects_503_even_when_body_is_valid_csv(config, calendar):
    body = csv_body()
    with Journal(config.root) as journal:
        report = collector(
            config,
            journal,
            calendar,
            CffexCsvClient(sender=lambda url, timeout: FakeResponse(body, 503)),
        ).backfill(DAY, DAY)
        assert journal.db.execute("SELECT count(*) FROM futures_daily").fetchone()[0] == 0
    assert report["status"] == "failed"
    folders = captures_for(config, DAY)
    assert folders and all(raw_bodies(folder) == [body] for folder in folders)
    assert all(
        json.loads((folder / "responses/000000/receipt.json").read_text())["http_status"] == 503
        for folder in folders
    )


@pytest.mark.parametrize(
    "http_status,version",
    [
        (None, "raw-acquisition-v1"),
        (None, EVIDENCE_VERSION),
        (503, EVIDENCE_VERSION),
        (200, EVIDENCE_VERSION),
    ],
)
def test_consume_requires_bound_successful_raw_http_status(config, calendar, http_status, version):
    """Consistent legacy/failure evidence must never become an offline commit."""
    from openbb_collector_core.raw_acquisition import RawResponse, capture, read_capture

    body = csv_body((CSV_FUTURES, CSV_SECOND))
    with Journal(config.root) as journal:
        worker = collector(
            config,
            journal,
            calendar,
            CffexCsvClient(sender=lambda url, timeout: FakeResponse(body)),
        )
        assert worker.backfill(DAY, DAY)["status"] == "complete"
        (path,) = deliveries(config, DAY)
        envelope = json.loads(path.read_text())
        old = read_capture(
            config.evidence,
            envelope["capture"]["task_id"],
            envelope["capture"]["generation"],
            version=EVIDENCE_VERSION,
        )
        # Make independently valid capture evidence, including legacy no-status
        # receipts, using the real shared writer in a separate, fresh root.
        alternate = config.root / "attack-evidence"
        state = capture(
            alternate,
            {**old["task"], "version": version},
            lambda _: RawResponse(body, http_status=http_status),
        )
        envelope["provenance"]["http_status"] = http_status
        envelope["capture"]["evidence_version"] = version
        envelope["capture"]["locator"] = state["locator"]
        attack_path = config.root / "attack" / DAY.isoformat() / "delivery.json"
        attack_path.parent.mkdir(parents=True)
        attack_path.write_text(json.dumps(envelope))
        path = attack_path
        worker.delivered = lambda day: attack_path
        worker.client = NoSourceClient()
        # consume uses config.evidence; point its read at the independent root.
        original_reader = cffex_runtime.read_delivery

        def bound_reader(delivery_path, capture_root=None):
            return original_reader(delivery_path, capture_root=alternate)

        from unittest.mock import patch

        before = {
            p.relative_to(alternate): p.read_bytes() for p in alternate.rglob("*") if p.is_file()
        }
        baseline = (count(journal, "futures_daily"), count(journal, "revisions"))
        assert state["complete"] and state["status"] == "captured"
        if http_status == 200:
            assert len(original_reader(path, capture_root=alternate)["rows"]) == 2
            with patch.object(cffex_runtime, "read_delivery", bound_reader):
                report = worker.consume(DAY, DAY)
            assert report["status"] == "complete" and report["provider_calls"] == 0
        else:
            with pytest.raises(EvidenceStateError, match="http_status"):
                original_reader(path, capture_root=alternate)
            with patch.object(cffex_runtime, "read_delivery", bound_reader):
                with pytest.raises(EvidenceStateError, match="http_status"):
                    worker.consume(DAY, DAY)
            assert (count(journal, "futures_daily"), count(journal, "revisions")) == baseline
        assert before == {
            p.relative_to(alternate): p.read_bytes() for p in alternate.rglob("*") if p.is_file()
        }


@pytest.mark.parametrize(
    "damage,code",
    [("interrupted", "delivery_capture_incomplete"), ("tampered", "evidence_state_unreadable")],
)
def test_delivery_refuses_nonterminal_or_tampered_capture(config, calendar, damage, code):
    with Journal(config.root) as journal:
        worker = collector(
            config,
            journal,
            calendar,
            CffexCsvClient(
                sender=lambda url, timeout: FakeResponse(csv_body((CSV_FUTURES, CSV_SECOND)))
            ),
        )
        assert worker.backfill(DAY, DAY)["status"] == "complete"
        (path,) = deliveries(config, DAY)
        (folder,) = captures_for(config, DAY)
        if damage == "interrupted":
            (folder / "durable.json").unlink()
        else:
            receipt = folder / "responses/000000/receipt.json"
            value = json.loads(receipt.read_text())
            value["http_status"] = 503
            receipt.chmod(0o600)
            receipt.write_text(json.dumps(value))
        before = {
            p.relative_to(config.evidence): p.read_bytes()
            for p in config.evidence.rglob("*")
            if p.is_file()
        }
        baseline = (count(journal, "futures_daily"), count(journal, "revisions"))
        worker.client = NoSourceClient()
        with pytest.raises(EvidenceStateError, match=code):
            read_delivery(path, capture_root=config.evidence)
        with pytest.raises(EvidenceStateError, match=code):
            worker.consume(DAY, DAY)
        assert (count(journal, "futures_daily"), count(journal, "revisions")) == baseline
        assert before == {
            p.relative_to(config.evidence): p.read_bytes()
            for p in config.evidence.rglob("*")
            if p.is_file()
        }


def test_calendar_delivery_requires_durability_and_receipt_binding(config, calendar):
    config.units = UNITS
    with Journal(config.root) as journal:
        report = refresh_calendar(config, journal, FakeClient([
            {"trade_date": "2026-09-17"}, {"trade_date": "2026-09-18"}
        ]))
        path = config.root / report["raw_path"]
        envelope = json.loads(path.read_text())
        assert len(read_calendar_delivery(path, capture_root=config.evidence)["rows"]) == 2
        envelope["capture"]["sha256"] = "0" * 64
        altered = path.parent / "altered.json"
        altered.write_text(json.dumps(envelope))
        with pytest.raises(EvidenceStateError, match="delivery_capture_mismatch"):
            read_calendar_delivery(altered, capture_root=config.evidence)
        (config.evidence / envelope["capture"]["locator"] / "durable.json").unlink()
        with pytest.raises(EvidenceStateError, match="delivery_capture_incomplete"):
            read_calendar_delivery(path, capture_root=config.evidence)


@pytest.mark.parametrize("status_code", [None, 503])
def test_calendar_rejects_unsuccessful_raw_http(config, calendar, status_code):
    from openbb_collector_core.raw_acquisition import RawResponse, capture, read_capture

    config.units = UNITS
    with Journal(config.root) as journal:
        report = refresh_calendar(config, journal, FakeClient([{"trade_date": "2026-09-18"}]))
        path = config.root / report["raw_path"]
        envelope = json.loads(path.read_text())
        state = read_capture(config.evidence, envelope["capture"]["task_id"],
                             envelope["capture"]["generation"], version=EVIDENCE_VERSION)
        independent = config.root / "independent-calendar"
        capture(independent, state["task"], lambda _: RawResponse(b"x", http_status=status_code))
        with pytest.raises(EvidenceStateError, match="delivery_http_status"):
            read_calendar_delivery(path, capture_root=independent)


def test_new_root_real_subprocess_capture_and_calendar(tmp_path):
    """Exercise a new local root in a fresh interpreter, with offline transports."""
    script = r"""
import json, runpy, sys
from pathlib import Path
from dq_historical_market_data_collector.cffex import identity, acquire
from dq_historical_market_data_collector.cffex.config import Config, load_calendar
from dq_historical_market_data_collector.cffex.runtime import (
    refresh_calendar, read_calendar_delivery,
)
from dq_historical_market_data_collector.cffex.storage import Journal
helpers = runpy.run_path(sys.argv[1])
base = Path(sys.argv[2]); root = base / 'new-root'
plan = helpers['acquire_plan'](max_attempts=1)
response = helpers['FakeResponse']; calls = []
def send(url, timeout=30):
    calls.append(url)
    return response(helpers['ACQUIRE_BODY'])
first = acquire.run(root, plan, sender=send)
assert first['status'] == 'complete' and len(calls) == 1
again = acquire.run(root, plan, sender=send)
assert again['replayed'] == 1 and len(calls) == 1
failure_plan = helpers['acquire_plan'](max_attempts=1, token='failure')
failed = acquire.run(root, failure_plan, sender=lambda url, timeout: response(b'unavailable', 503))
assert failed['status'] == 'failed' and failed['results'][0]['http_status'] == 503
assert any(p.read_bytes() == b'unavailable' for p in root.rglob('body.bin'))
config = Config(root=base / 'journal', evidence_root=root, calendar_file=base / 'calendar.json')
client = helpers['FakeClient']([{'trade_date':'2026-09-17'}, {'trade_date':'2026-09-18'}])
with Journal(config.root) as journal:
    report = refresh_calendar(config, journal, client)
    assert report['status'] == 'complete'
    delivered = read_calendar_delivery(config.root / report['raw_path'], capture_root=root)
    assert len(delivered['rows']) == 2
assert len(load_calendar(config.calendar_file).trading_dates) == 2
calendar_path = config.root / report['raw_path']
envelope = json.loads(calendar_path.read_text())
envelope['capture']['sha256'] = '0' * 64
calendar_path = base / 'altered-calendar.json'
calendar_path.write_text(json.dumps(envelope))
try:
    read_calendar_delivery(calendar_path, capture_root=root)
    raise AssertionError('corrupted calendar evidence accepted')
except RuntimeError as exc:
    assert 'delivery_capture_mismatch' in str(exc)
"""
    completed = subprocess.run([sys.executable, "-c", script, __file__, str(tmp_path)],
                               capture_output=True, text=True, timeout=60)
    assert completed.returncode == 0, completed.stderr


def test_acquire_real_cli_offline_http_and_recovery(tmp_path):
    """Run the actual module entry point with an offline transport in each child."""
    hook = tmp_path / "sitecustomize.py"
    hook.write_text('''
import os
from pathlib import Path
from dq_historical_market_data_collector.cffex import source
class Response:
    status_code = int(os.environ.get("TEST_HTTP_STATUS", "200"))
    content = bytes.fromhex(os.environ.get("TEST_HTTP_BODY", ""))
    headers = {"Content-Type": "text/csv"}
def send(url, timeout):
    with Path(os.environ["TEST_CALLS"]).open("a") as calls:
        calls.write("call\\n")
    if os.environ.get("TEST_FAIL"):
        raise OSError("offline fault")
    return Response()
source.transport = lambda: send
''')
    env = os.environ.copy()
    env["PYTHONPATH"] = str(tmp_path) + os.pathsep + env.get("PYTHONPATH", "")
    calls = tmp_path / "calls"
    env["TEST_CALLS"] = str(calls)
    plan_path = tmp_path / "plan.json"
    plan_path.write_text(json.dumps(acquire_plan(max_attempts=1)))

    def invoke(root, status=200, body=b"", fault=False):
        child_env = {**env, "TEST_HTTP_STATUS": str(status), "TEST_HTTP_BODY": body.hex()}
        if fault:
            child_env["TEST_FAIL"] = "1"
        child = subprocess.run(
            [sys.executable, "-m", "dq_historical_market_data_collector.cffex.acquire",
             "--plan", str(plan_path), "--root", str(root)],
            env=child_env, text=True, capture_output=True, timeout=30,
        )
        return child.returncode, json.loads(child.stdout)

    root = tmp_path / "success"
    code, first = invoke(root, body=ACQUIRE_BODY)
    assert code == 0 and first["results"][0]["http_status"] == 200
    before = calls.read_text()
    code, replay = invoke(root, status=503, fault=True)
    assert code == 0 and replay["replayed"] == 1 and calls.read_text() == before
    code, failed = invoke(tmp_path / "failure", status=503, body=b"unavailable")
    assert code != 0 and failed["results"][0]["http_status"] == 503
    assert [p.read_bytes() for p in (tmp_path / "failure").rglob("body.bin")] == [b"unavailable"]
    code, retry = invoke(tmp_path / "failure", body=ACQUIRE_BODY)
    assert code == 0 and retry["source_calls"] == 1 and retry["replayed"] == 0
    code, empty = invoke(tmp_path / "empty", body=b"")
    assert code == 0 and empty["results"][0]["http_status"] == 200
    assert [p.read_bytes() for p in (tmp_path / "empty").rglob("body.bin")] == [b""]
    code, fault = invoke(tmp_path / "fault", fault=True)
    assert code != 0 and fault["status"] != "complete"
    receipt = next(root.rglob("receipt.json"))
    receipt.chmod(0o644)
    record = json.loads(receipt.read_text())
    record["http_status"] = 503
    receipt.write_text(json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n")
    before = calls.read_text()
    code, corrupt = invoke(root)
    assert code != 0 and corrupt["status"] != "complete" and calls.read_text() == before


def test_acquire_cli_overlapping_windows_and_crash_recovery(tmp_path):
    """Separate CLI processes reuse inclusive dates and durable crash prefixes."""
    hook = tmp_path / "sitecustomize.py"
    hook.write_text('''
import os, signal
from pathlib import Path
from dq_historical_market_data_collector.cffex import source
class Response:
    status_code = 200
    content = b"offline evidence"
    headers = {"Content-Type": "text/csv"}
def send(url, timeout):
    with Path(os.environ["TEST_CALLS"]).open("a") as calls:
        calls.write(url + "\\n")
    if os.environ.get("TEST_CRASH") and "20260918" in url:
        os.kill(os.getpid(), signal.SIGKILL)
    return Response()
source.transport = lambda: send
''')
    env = os.environ.copy()
    env["PYTHONPATH"] = str(tmp_path) + os.pathsep + env.get("PYTHONPATH", "")
    calls = tmp_path / "calls"
    env["TEST_CALLS"] = str(calls)
    plan_path = tmp_path / "plan.json"

    def invoke(root, start, end, crash=False):
        plan_path.write_text(json.dumps(acquire_plan(
            requests={"range": {"start": start, "end": end, "weekdays_only": False}},
            max_attempts=1,
        )))
        child = subprocess.run(
            [sys.executable, "-m", "dq_historical_market_data_collector.cffex.acquire",
             "--plan", str(plan_path), "--root", str(root)],
            env={**env, **({"TEST_CRASH": "1"} if crash else {})},
            text=True, capture_output=True, timeout=30,
        )
        return child, json.loads(child.stdout) if child.stdout else None

    root = tmp_path / "overlap"
    first, a = invoke(root, "2026-09-16", "2026-09-18")
    second, b = invoke(root, "2026-09-18", "2026-09-20")
    repeat, c = invoke(root, "2026-09-18", "2026-09-20")
    assert [first.returncode, second.returncode, repeat.returncode] == [0, 0, 0]
    assert [a["source_calls"], b["source_calls"], c["source_calls"]] == [3, 2, 0]
    assert [b["replayed"], c["replayed"]] == [1, 3]
    assert [item["trade_date"] for item in a["results"]] == [
        "2026-09-16", "2026-09-17", "2026-09-18"
    ]
    assert [item["trade_date"] for item in b["results"]] == [
        "2026-09-18", "2026-09-19", "2026-09-20"
    ]
    assert len(calls.read_text().splitlines()) == 5

    calls.unlink()
    crashed, _ = invoke(tmp_path / "crash", "2026-09-17", "2026-09-19", crash=True)
    assert crashed.returncode == -signal.SIGKILL
    resumed, report = invoke(tmp_path / "crash", "2026-09-17", "2026-09-19")
    assert resumed.returncode == 0
    assert report["source_calls"] == 2 and report["replayed"] == 1
    assert len(calls.read_text().splitlines()) == 4

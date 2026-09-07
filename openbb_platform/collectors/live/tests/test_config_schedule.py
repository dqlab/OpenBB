from datetime import UTC, date, datetime

import pytest
import yaml
from openbb_collector_core import ProviderFailure
from pydantic import ValidationError

from dq_live_market_data_collector.config import CollectorConfig, ResetConfig, Schedule, load_config
from dq_live_market_data_collector.providers import validate_providers
from dq_live_market_data_collector.schedule import active_window, reset_period, window_on


@pytest.mark.parametrize(
    "mutation",
    [
        lambda cfg: cfg["sources"]["primary"].update(route="ibkr.positions"),
        lambda cfg: cfg["sources"]["primary"].update(parameters={"api_key": "secret"}),
        lambda cfg: cfg["sources"]["primary"].update(parameters={"symbol": "MSFT"}),
        lambda cfg: cfg["collections"]["quotes"].update(primary="missing"),
        lambda cfg: cfg["collections"]["quotes"].update(required_fields=["open_interest"]),
        lambda cfg: cfg["collections"]["quotes"].update(frequency_seconds=0),
        lambda cfg: cfg["collections"]["quotes"].update(frequency_seconds=float("nan")),
        lambda cfg: cfg["instruments"]["apple"].update(symbol="AAPL,MSFT"),
        lambda cfg: cfg["collections"]["quotes"]["schedule"].update(timezone="not/a/timezone"),
        lambda cfg: cfg["collections"]["quotes"]["schedule"].update(weekdays=[0, 0]),
        lambda cfg: cfg["collections"]["quotes"]["schedule"].update(start="09:30+08:00"),
        lambda cfg: cfg["instruments"]["apple"].update(asset_type="future"),
        lambda cfg: cfg.update(unknown_config=True),
    ],
)
def test_invalid_config_fails_without_connecting(raw_config, mutation):
    mutation(raw_config)
    with pytest.raises((ValidationError, ProviderFailure)):
        validate_providers(CollectorConfig.model_validate(raw_config))


def test_ibkr_derivatives_need_explicit_conid(raw_config):
    raw_config["sources"] = {
        "primary": {
            "provider": "ibkr",
            "model": "MarketQuote",
            "parameter_map": {k: k for k in ("symbol", "asset_type", "currency", "con_id")},
        }
    }
    raw_config["collections"]["quotes"]["secondary"] = []
    raw_config["instruments"]["apple"].update(symbol="ES", asset_type="future")
    with pytest.raises(ProviderFailure, match="invalid_provider_parameters"):
        validate_providers(CollectorConfig.model_validate(raw_config))
    raw_config["instruments"]["apple"]["con_id"] = 123456
    assert CollectorConfig.model_validate(raw_config).instruments["apple"].con_id == 123456


def test_delayed_request_is_opt_in(raw_config):
    raw_config["sources"]["primary"] = {
        "provider": "sample",
        "model": "SampleQuote",
        "requested_feed_type": "delayed",
        "parameter_map": {"symbol": "symbol", "feed_type": "requested_feed_type"},
    }
    with pytest.raises(ValidationError, match="delayed"):
        CollectorConfig.model_validate(raw_config)
    raw_config["collections"]["quotes"]["accepted_feed_types"] = ["delayed", "unknown"]
    CollectorConfig.model_validate(raw_config)


def test_yaml_paths_resolve_against_config(tmp_path, raw_config, monkeypatch):
    raw_config["storage"]["root"] = "data/live"
    path = tmp_path / "collector.yaml"
    path.write_text(yaml.safe_dump(raw_config))
    monkeypatch.chdir("/")
    assert load_config(path).storage.root == tmp_path / "data/live"


def test_holidays_early_close_and_weekend_override():
    schedule = Schedule.model_validate(
        {
            "timezone": "America/New_York",
            "start": "09:30",
            "end": "16:00",
            "holidays": ["2026-09-07"],
            "overrides": {
                "2026-11-27": {"start": "09:30", "end": "13:00"},
                "2026-09-12": {"start": "10:00", "end": "12:00"},
                "2026-09-08": None,
            },
        }
    )
    assert window_on(schedule, date(2026, 9, 7)) is None
    assert window_on(schedule, date(2026, 9, 8)) is None
    assert window_on(schedule, date(2026, 9, 6)) is None
    assert window_on(schedule, date(2026, 11, 27)).end.hour == 18
    assert window_on(schedule, date(2026, 9, 12)).start.hour == 14


def test_overnight_window_uses_start_day_calendar():
    schedule = Schedule(timezone="UTC", start="18:00", end="17:00", weekdays=[4])
    now = datetime(2026, 9, 12, 1, tzinfo=UTC)
    window = active_window(schedule, now)
    assert window.day == date(2026, 9, 11)
    assert active_window(schedule, datetime(2026, 9, 12, 17, tzinfo=UTC)) is None


def test_dst_24_hour_sessions_are_23_or_25_elapsed_hours():
    schedule = Schedule(
        timezone="America/New_York", start="00:00", end="00:00", weekdays=list(range(7))
    )
    for day, expected in ((date(2026, 3, 8), 23), (date(2026, 11, 1), 25)):
        window = window_on(schedule, day)
        assert (window.end - window.start).total_seconds() == expected * 3600


def test_nonexistent_boundary_is_skipped_and_repeated_hour_covers_both_folds():
    schedule = Schedule(
        timezone="America/New_York", start="02:30", end="04:00", weekdays=list(range(7))
    )
    assert window_on(schedule, date(2026, 3, 8)) is None
    schedule = Schedule(
        timezone="America/New_York", start="01:15", end="01:45", weekdays=list(range(7))
    )
    window = window_on(schedule, date(2026, 11, 1))
    assert (window.end - window.start).total_seconds() == 90 * 60


def test_iso_week_year_and_month_resets():
    utc = datetime(2027, 1, 1, tzinfo=UTC)
    assert reset_period(ResetConfig(period="weekly"), utc) == "2026-W53"
    assert reset_period(ResetConfig(period="monthly"), utc) == "2027-01"
    assert reset_period(ResetConfig(period="never"), utc) == "never"


def test_source_symbol_can_map_to_a_custom_query_field(raw_config):
    raw_config["sources"]["primary"]["parameter_map"] = {"ticker": "symbol"}
    config = CollectorConfig.model_validate(raw_config)
    assert config.sources["primary"].parameter_map == {"ticker": "symbol"}

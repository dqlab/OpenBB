from datetime import UTC, date, datetime, time

import pytest
from openbb_collector_core import ProviderFailure
from pydantic import ValidationError

from dq_historical_market_data_collector.config import (
    Calendar,
    CollectorConfig,
    Hours,
    Schedule,
    load_config,
)
from dq_historical_market_data_collector.planner import (
    Chunk,
    chunks,
    due_days,
    expected_times,
    reset_period,
)
from dq_historical_market_data_collector.providers import validate_providers


@pytest.mark.parametrize(
    "change",
    [
        {"frequency": "tick"},
        {"required_fields": ["not_selected"]},
        {"secondary": ["yahoo"]},
        {"instrument_ids": ["missing"]},
        {"start_date": "2026-09-04"},
        {"use_rth": False},
    ],
)
def test_reject_invalid_contract(config_dict, change):
    config_dict["collections"]["daily"].update(change)
    with pytest.raises((ValidationError, ProviderFailure)):
        validate_providers(CollectorConfig.model_validate(config_dict))


@pytest.mark.parametrize(
    "parameters",
    [
        {"api_key": "secret"},
        {"read_only": False},
        {"port": -1},
        {"client_id": True},
        {"delayed": "false"},
        {"what_to_show": "BID"},
    ],
)
def test_reject_unsafe_source_parameters(config_dict, parameters):
    config_dict["sources"]["ibkr"]["parameters"] = parameters
    with pytest.raises((ValidationError, ProviderFailure)):
        validate_providers(CollectorConfig.model_validate(config_dict))


@pytest.mark.parametrize("asset", ["future", "option", "future_option"])
def test_derivatives_require_identity_and_intraday_options(config_dict, asset):
    config_dict["collections"]["daily"].update(primary="ibkr", secondary=[], frequency="5m")
    config_dict["instruments"]["aapl"]["asset_type"] = asset
    with pytest.raises((ValidationError, ProviderFailure)):
        validate_providers(CollectorConfig.model_validate(config_dict))
    config_dict["instruments"]["aapl"]["con_id"] = 123
    validate_providers(CollectorConfig.model_validate(config_dict))
    if asset != "future":
        config_dict["collections"]["daily"]["frequency"] = "1d"
        with pytest.raises((ValidationError, ProviderFailure)):
            validate_providers(CollectorConfig.model_validate(config_dict))


def test_chunk_ranges_and_budget(config):
    config.collections["daily"].end_date = date(2026, 9, 10)
    result = list(chunks(config, date(2026, 9, 9)))
    assert [(x.start, x.end) for x in result] == [
        (date(2026, 9, 1), date(2026, 9, 4)),
        (date(2026, 9, 4), date(2026, 9, 7)),
        (date(2026, 9, 7), date(2026, 9, 9)),
    ]
    config.collections["daily"].frequency = "1m"
    assert all((x.end - x.start).days == 1 for x in chunks(config, date(2026, 9, 9)))
    config.sources["yahoo"].max_rows = 1
    with pytest.raises(ValueError):
        list(chunks(config, date(2026, 9, 9)))


def test_explicit_calendar_holiday_and_override(config):
    collection = config.collections["daily"]
    collection.calendar = Calendar(
        holidays={date(2026, 9, 2)},
        overrides={date(2026, 9, 3): None},
    )
    expected = expected_times(
        collection,
        config.instruments["aapl"],
        Chunk("daily", "aapl", date(2026, 9, 1), date(2026, 9, 4)),
    )
    assert expected == {"2026-09-01"}
    collection.frequency = "1h"
    collection.calendar.overrides[date(2026, 9, 1)] = Hours(start=time(9), end=time(11))
    expected = expected_times(
        collection,
        config.instruments["aapl"],
        Chunk("daily", "aapl", date(2026, 9, 1), date(2026, 9, 2)),
    )
    assert expected == {"2026-09-01T13:00:00+00:00", "2026-09-01T14:00:00+00:00"}


def test_schedule_dst_catchup_and_resets():
    schedule = Schedule(at=time(2, 30), catch_up_days=1)
    assert due_days(schedule, datetime(2026, 3, 8, 12, tzinfo=UTC)) == []
    schedule.at = time(1, 30)
    assert due_days(schedule, datetime(2026, 11, 1, 7, tzinfo=UTC)) == [date(2026, 11, 1)]
    schedule.holidays = {date(2026, 11, 1)}
    assert due_days(schedule, datetime(2026, 11, 1, 7, tzinfo=UTC)) == []
    assert reset_period(schedule, datetime(2026, 1, 1, 12, tzinfo=UTC)) == "2026-W01"
    schedule.reset = "monthly"
    assert reset_period(schedule, datetime(2026, 1, 1, 12, tzinfo=UTC)) == "2026-01"
    with pytest.raises(ValueError):
        due_days(schedule, datetime(2026, 1, 1))


def test_yaml_paths_and_fingerprint(config_dict, tmp_path):
    import yaml

    config_dict["storage"]["root"] = "data/test"
    path = tmp_path / "example.yaml"
    path.write_text(yaml.safe_dump(config_dict))
    config = load_config(path)
    assert config.storage.root == tmp_path / "data/test"
    original = config.fingerprint()
    config.schedule.reset = "monthly"
    config.storage.format = "sqlite"
    assert config.fingerprint() == original
    config.collections["daily"].frequency = "5m"
    assert config.fingerprint() != original


def test_custom_query_field_names_and_explicit_fixed_frequency(config_dict):
    config_dict["sources"]["yahoo"]["parameter_map"] = {
        "ticker": "symbol",
        "from_date": "start_date",
        "until_date": "end_date",
        "adjustment": "adjustment",
    }
    with pytest.raises(ValidationError, match="fixed_frequency"):
        CollectorConfig.model_validate(config_dict)
    config_dict["sources"]["yahoo"]["fixed_frequency"] = "1d"
    config = CollectorConfig.model_validate(config_dict)
    assert config.sources["yahoo"].parameter_map["ticker"] == "symbol"
    config_dict["collections"]["daily"]["frequency"] = "5m"
    with pytest.raises(ValidationError, match="fixed_frequency"):
        CollectorConfig.model_validate(config_dict)

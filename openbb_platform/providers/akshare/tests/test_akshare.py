"""Offline tests for the AKShare provider: validation, normalization, and mapping."""

import asyncio
from datetime import date

import pandas as pd
import pytest
from openbb_akshare.models import (
    futures_historical as fh,
    native as native_module,
)
from openbb_akshare.models.futures_historical import AkshareFuturesHistoricalFetcher
from openbb_akshare.models.native import NATIVE_FETCHERS
from openbb_akshare.utils.catalog import DATASET_BY_METHOD, DATASETS, resolve_method
from openbb_akshare.utils.helpers import (
    clip_dates,
    frame_to_records,
    normalize_period,
    normalize_symbol,
    normalize_vars_list,
    source_date,
)
from openbb_core.provider.utils.errors import EmptyDataError

CFFEX_FRAME = pd.DataFrame(
    {
        "symbol": ["IF2610", "IF2612"],
        "date": ["20260918", "20260918"],
        "open": [4488.0, 4431.2],
        "high": [4520.0, 4450.0],
        "low": [4470.0, 4400.0],
        "close": [4488.0, 4431.2],
        "volume": [28325, 60879],
        "open_interest": [54976, 161098],
        "turnover": [1.0, 2.0],
        "settle": [4482.0, 4426.6],
        "pre_settle": [4470.0, 4410.0],
        "variety": ["IF", "IF"],
    }
)

SINA_DAILY = pd.DataFrame(
    {
        "date": [date(2026, 10, 10), date(2026, 10, 13)],
        "open": [4200.0, 4183.2],
        "high": [4250.0, 4257.4],
        "low": [4190.0, 4151.0],
        "close": [4233.6, 4217.6],
        "volume": [55056, 78395],
        "hold": [171368, 178602],
        "settle": [0.0, 0.0],
    }
)


# --------------------------------------------------------------------------- catalog


def test_catalog_models_and_commands_are_unique():
    """Every dataset maps to one distinct model and command."""
    models = [d.model for d in DATASETS]
    commands = [d.command for d in DATASETS]
    assert len(set(models)) == len(models)
    assert len(set(commands)) == len(commands)
    assert DATASET_BY_METHOD["get_cffex_daily"].model == "AkshareCffexDaily"
    assert DATASET_BY_METHOD["get_cffex_rank_table"].command == "cffex_rank_table"


def test_native_fetchers_cover_every_dataset():
    """The provider exposes a fetcher for each catalogued interface."""
    assert set(NATIVE_FETCHERS) == {d.model for d in DATASETS}


def test_resolve_method_rejects_unknown_method():
    """Only catalogued AKShare methods can be resolved."""
    with pytest.raises(KeyError):
        resolve_method("os.system")


# --------------------------------------------------------------------------- helpers


def test_normalize_symbol_upper_cases_and_validates():
    """Symbols are upper-cased and malformed identifiers are rejected."""
    assert normalize_symbol(" if2610 ") == "IF2610"
    assert normalize_symbol("rb0") == "RB0"
    for bad in ("", "2610", "IF-2610", "IF 2610", "IF;DROP"):
        with pytest.raises(ValueError):
            normalize_symbol(bad)


def test_normalize_period_restricts_frequency():
    """Intraday periods are limited to the source-supported set."""
    assert normalize_period("5") == "5"
    with pytest.raises(ValueError):
        normalize_period("7")


def test_source_date_serializes_supported_inputs():
    """Dates are serialized to the ``YYYYMMDD`` form the source expects."""
    assert source_date(date(2026, 9, 18)) == "20260918"
    assert source_date("2026-09-18") == "20260918"
    assert source_date(None) is None


def test_normalize_vars_list_accepts_both_forms():
    """OpenBB's joined string and a true list both normalize to product codes."""
    assert normalize_vars_list("if, t") == ["IF", "T"]
    assert normalize_vars_list(["if", "t"]) == ["IF", "T"]
    assert normalize_vars_list(None) is None
    assert normalize_vars_list(" , ") is None
    with pytest.raises(ValueError):
        normalize_vars_list("IF;DROP")


def test_frame_to_records_preserves_nulls():
    """NaN and None become null without dropping or reordering columns."""
    frame = pd.DataFrame({"a": [1.0, float("nan")], "b": ["x", None]})
    records = frame_to_records(frame)
    assert records == [{"a": 1.0, "b": "x"}, {"a": None, "b": None}]


def test_frame_to_records_flattens_mapping_of_frames():
    """The per-contract ranking payload is flattened into one record list."""
    records = frame_to_records({"IF2610": CFFEX_FRAME.head(1), "IF2612": CFFEX_FRAME.tail(1)})
    assert [row["symbol"] for row in records] == ["IF2610", "IF2612"]


def test_clip_dates_applies_inclusive_bounds():
    """The requested date window keeps only in-range rows."""
    rows = frame_to_records(SINA_DAILY)
    assert len(clip_dates(rows, date(2026, 10, 11), None)) == 1
    assert clip_dates(rows, None, date(2026, 10, 9)) == []


# --------------------------------------------------------------------------- native fetchers


def test_cffex_daily_requires_an_explicit_date():
    """A date is mandatory so AKShare's stale default is never used silently."""
    model = NATIVE_FETCHERS["AkshareCffexDaily"]
    with pytest.raises(Exception):
        model.transform_query({})
    query = model.transform_query({"date": "2026-09-18"})
    assert query.date == date(2026, 9, 18)


def test_native_fetcher_serializes_source_date(monkeypatch):
    """The fetcher passes a serialized date and preserves source rows."""
    captured = {}

    async def fake_query_frame(method, params):
        captured["method"] = method
        captured["params"] = params
        return frame_to_records(CFFEX_FRAME)

    monkeypatch.setattr(native_module, "query_frame", fake_query_frame)
    model = NATIVE_FETCHERS["AkshareCffexDaily"]
    query = model.transform_query({"date": "2026-09-18"})
    data = asyncio.run(model.aextract_data(query, None))
    rows = model.transform_data(query, data)
    assert captured["method"] == "get_cffex_daily"
    assert captured["params"] == {"date": "20260918"}
    assert rows[0].symbol == "IF2610"


def test_rank_fetcher_declares_multiple_items_allowed():
    """OpenBB only accepts a joined product list when the provider allows it."""
    model = native_module.AkshareCffexRankTableQueryParams
    assert model.__json_schema_extra__ == {"vars_list": {"multiple_items_allowed": True}}


def test_rank_fetcher_serializes_joined_vars_list(monkeypatch):
    """A joined product string reaches AKShare as a list of codes."""
    captured = {}

    async def fake_query_frame(method, params):
        captured["method"] = method
        captured["params"] = params
        return frame_to_records(CFFEX_FRAME)

    monkeypatch.setattr(native_module, "query_frame", fake_query_frame)
    model = NATIVE_FETCHERS["AkshareCffexRankTable"]
    query = model.transform_query({"date": "2026-09-18", "vars_list": "if,t"})
    asyncio.run(model.aextract_data(query, None))
    assert captured["method"] == "get_cffex_rank_table"
    assert captured["params"] == {"date": "20260918", "vars_list": ["IF", "T"]}


def test_rank_fetcher_omits_an_empty_vars_list(monkeypatch):
    """An omitted product filter is not sent to the source at all."""
    captured = {}

    async def fake_query_frame(method, params):
        captured["params"] = params
        return frame_to_records(CFFEX_FRAME)

    monkeypatch.setattr(native_module, "query_frame", fake_query_frame)
    model = NATIVE_FETCHERS["AkshareCffexRankTable"]
    query = model.transform_query({"date": "2026-09-18"})
    asyncio.run(model.aextract_data(query, None))
    assert captured["params"] == {"date": "20260918"}


# --------------------------------------------------------------------------- futures historical


def test_futures_historical_maps_and_filters(monkeypatch):
    """Daily bars are filtered to the window and mapped to the standard schema."""
    async def fake_query_frame(method, params):
        assert method == "futures_zh_daily_sina"
        assert params == {"symbol": "IF0"}
        return frame_to_records(SINA_DAILY)

    monkeypatch.setattr(fh, "query_frame", fake_query_frame)
    query = AkshareFuturesHistoricalFetcher.transform_query(
        {"symbol": "if0", "start_date": "2026-10-13", "end_date": "2026-10-13"}
    )
    assert query.symbol == "IF0"
    data = asyncio.run(AkshareFuturesHistoricalFetcher.aextract_data(query, None))
    rows = AkshareFuturesHistoricalFetcher.transform_data(query, data)
    assert len(rows) == 1
    assert rows[0].symbol == "IF0"
    assert rows[0].open_interest == 178602
    assert rows[0].settle == 0.0


def test_futures_historical_rejects_reversed_dates():
    """A reversed window is rejected during query validation."""
    with pytest.raises(Exception):
        AkshareFuturesHistoricalFetcher.transform_query(
            {"symbol": "IF0", "start_date": "2026-10-13", "end_date": "2026-10-01"}
        )


def test_futures_historical_raises_on_empty_window(monkeypatch):
    """An empty in-range result raises instead of returning a hollow frame."""
    async def fake_query_frame(method, params):
        return frame_to_records(SINA_DAILY)

    monkeypatch.setattr(fh, "query_frame", fake_query_frame)
    query = AkshareFuturesHistoricalFetcher.transform_query(
        {"symbol": "IF0", "start_date": "2020-01-01", "end_date": "2020-01-31"}
    )
    data = asyncio.run(AkshareFuturesHistoricalFetcher.aextract_data(query, None))
    with pytest.raises(EmptyDataError):
        AkshareFuturesHistoricalFetcher.transform_data(query, data)

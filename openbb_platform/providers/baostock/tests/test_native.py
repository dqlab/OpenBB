"""Full SDK coverage, source preservation, input contracts, and exposed commands."""

import asyncio
import inspect
import json
from importlib import import_module
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import baostock as bs
import pytest
from baostock.data.resultset import ResultData
from openbb_baostock.models.index_constituents import BaostockIndexConstituentsFetcher
from openbb_baostock.models.native import NATIVE_FETCHERS, sdk_parameters
from openbb_baostock.utils import helpers
from openbb_baostock.utils.catalog import DATASETS, resolve_method
from openbb_core.app.model.abstract.error import OpenBBError
from pydantic import ValidationError

SOURCE_ROW = {
    "code": "sh.600000",
    "pubDate": "2024-04-29",
    "statDate": "2024-03-31",
    "roeAvg": "0.010020030040050060",
    "forecastType": "01",
    "CFOToOR": "",
    "providerNewColumn": "1.2300",
    "sourceNumber": 123,
}

LIVE_SAMPLES = json.loads((Path(__file__).parent / "fixtures" / "sdk_0_9_3_samples.json").read_text(encoding="utf-8"))[
    "records"
]


def source_result(rows):
    """Construct a real SDK result cursor from copied source rows."""
    result = ResultData()
    result.error_code = "0"
    result.fields = list(rows[0]) if rows else list(SOURCE_ROW)
    result.data = [list(row.values()) for row in rows]
    return result


def sample_params(dataset):
    """Exercise all parameters in each source signature, including date variants."""
    fields = NATIVE_FETCHERS[dataset.model].query_params_type.model_fields
    values = {
        "code": "600000.SH",
        "code_name": "浦发",
        "year": 2024,
        "quarter": 1,
        "date": "2024-01-02",
        "day": "2024-01-02",
        "fields": "date,code,close",
        "frequency": "5",
        "adjustflag": "2",
        "year_type": "report",
        "start_date": "2024-01-01",
        "end_date": "2024-02-29",
    }
    if dataset.parameters == "reserve":
        values["year_type"] = "1"
    if dataset.parameters == "month_range":
        values.update(start_date="2024-01", end_date="2024-02")
    if dataset.parameters == "year_range":
        values.update(start_date="2023", end_date="2024")
    return {name: values[name] for name in fields}


@pytest.fixture
def native_sdk(monkeypatch):
    """Keep all native route tests offline while exercising real cursor parsing."""
    login, logout = Mock(return_value=SimpleNamespace(error_code="0")), Mock(return_value=SimpleNamespace(error_code="0"))
    monkeypatch.setattr(bs, "login", login)
    monkeypatch.setattr(bs, "logout", logout)
    monkeypatch.setattr(helpers.context, "default_socket", None, raising=False)
    queries = {}
    for dataset in DATASETS:
        module = bs if dataset.public else import_module(f"baostock.{dataset.module}")
        query = Mock(side_effect=lambda **params: source_result([SOURCE_ROW]))
        monkeypatch.setattr(module, dataset.method, query)
        queries[dataset.method] = query
    return SimpleNamespace(login=login, logout=logout, queries=queries)


def test_catalog_covers_every_sdk_data_function_and_parameter():
    """Fail if an SDK update introduces a data query or input not exposed by OpenBB."""
    sdk_methods = {}
    for module_name in {dataset.module for dataset in DATASETS}:
        for name, function in inspect.getmembers(import_module(f"baostock.{module_name}"), inspect.isfunction):
            if name.startswith("query_"):
                sdk_methods[name] = function
    assert {dataset.method for dataset in DATASETS} == set(sdk_methods)
    assert {dataset.method for dataset in DATASETS if dataset.public} == {
        name for name, function in inspect.getmembers(bs, inspect.isfunction) if name.startswith("query_")
    }
    assert len({dataset.command for dataset in DATASETS}) == len(DATASETS)
    for dataset in DATASETS:
        query_fields = NATIVE_FETCHERS[dataset.model].query_params_type.model_fields
        assert {"yearType" if key == "year_type" else key for key in query_fields} == set(
            inspect.signature(sdk_methods[dataset.method]).parameters
        )


@pytest.mark.parametrize("dataset", DATASETS, ids=lambda item: item.command)
def test_native_fetcher_parameters_and_exact_source_data(dataset, native_sdk):
    """Every source API receives the expected arguments and returns all source values."""
    params = sample_params(dataset)
    result = asyncio.run(NATIVE_FETCHERS[dataset.model].fetch_data(params))
    expected = {"yearType" if key == "year_type" else key: value for key, value in params.items()}
    if "code" in expected:
        expected["code"] = "sh.600000"
    native_sdk.queries[dataset.method].assert_called_once_with(**expected)
    native_sdk.login.assert_called_once_with()
    native_sdk.logout.assert_called_once_with()
    assert result[0].model_dump() == SOURCE_ROW
    assert result[0].model_dump(by_alias=True) == SOURCE_ROW


@pytest.mark.parametrize("dataset", DATASETS, ids=lambda item: item.command)
def test_native_public_command(dataset, native_sdk):
    """Check routing, argument binding, response fields, and dataframe output for every API."""
    from openbb import obb

    result = getattr(obb.baostock, dataset.command)(provider="baostock", **sample_params(dataset))
    assert result.provider == "baostock"
    assert result.results[0].model_dump() == SOURCE_ROW
    assert result.to_df().iloc[0]["roeAvg"] == SOURCE_ROW["roeAvg"]
    assert result.to_df().iloc[0]["CFOToOR"] == ""


@pytest.mark.parametrize(
    "model,params",
    [
        ("BaostockProfitData", {"code": "sh.600000", "quarter": 5}),
        ("BaostockProfitData", {"code": "sh.600000", "year": -1}),
        ("BaostockProfitData", {"code": "000001"}),
        ("BaostockProfitData", {"code": "sh.600000,sz.000001"}),
        ("BaostockStockBasic", {"code_name": "name\x01bad"}),
        ("BaostockTradeDates", {"start_date": "2024-03-01", "end_date": "2024-01-01"}),
        ("BaostockMoneySupplyDataMonth", {"start_date": "2024-13"}),
        ("BaostockMoneySupplyDataMonth", {"start_date": "2024-01-01"}),
        ("BaostockMoneySupplyDataYear", {"end_date": "2024-01"}),
        ("BaostockMoneySupplyDataYear", {"start_date": "2025", "end_date": "2024"}),
        ("BaostockDividendData", {"code": "sh.600000", "year_type": "0"}),
        ("BaostockRequiredReserveRatioData", {"year_type": "operate"}),
        ("BaostockHistoryKDataPlus", {"code": "sh.600000", "fields": "date,date"}),
        ("BaostockHistoryKDataPlus", {"code": "sh.600000", "fields": "date;code"}),
        ("BaostockStockBasic", {"not_a_parameter": "value"}),
    ],
)
def test_invalid_native_queries_fail_before_login(model, params, native_sdk):
    """Validate source-specific date formats, ranges, enums, and protocol boundaries."""
    with pytest.raises(ValidationError):
        asyncio.run(NATIVE_FETCHERS[model].fetch_data(params))
    native_sdk.login.assert_not_called()


def test_defaults_empty_results_and_server_errors(native_sdk):
    """Do not replace SDK defaults or confuse successful emptiness with a provider failure."""
    fetcher = NATIVE_FETCHERS["BaostockStockBasic"]
    query = fetcher.transform_query({"code": "", "code_name": None})
    assert sdk_parameters(query) == {}
    native_sdk.queries["query_stock_basic"].side_effect = lambda **params: source_result([])
    assert asyncio.run(fetcher.fetch_data({})) == []
    native_sdk.queries["query_stock_basic"].assert_called_once_with()
    failure = source_result([])
    failure.error_code, failure.error_msg = "10001006", "source permission denied"
    native_sdk.queries["query_stock_basic"].side_effect = lambda **params: failure
    with pytest.raises(OpenBBError, match="10001006.*source permission denied"):
        asyncio.run(fetcher.fetch_data({}))


def test_only_catalogued_methods_are_callable():
    """Never turn a data endpoint into arbitrary SDK attribute or module access."""
    with pytest.raises(KeyError):
        resolve_method("logout")


@pytest.mark.parametrize("sample", LIVE_SAMPLES, ids=lambda record: record["command"])
def test_copied_live_source_rows(sample):
    """Reconcile saved source samples without changing types, columns, or values."""
    dataset = next(item for item in DATASETS if item.command == sample["command"])
    fetcher = NATIVE_FETCHERS[dataset.model]
    query = fetcher.transform_query(sample["params"])
    copied = helpers.read_result(source_result(sample["sample"]), dataset.method)
    result = fetcher.transform_data(query, copied)
    assert len(result) == len(sample["sample"])
    assert [row.model_dump() for row in result] == sample["sample"]


@pytest.mark.parametrize(
    "symbol,method",
    [
        ("HS300", "query_hs300_stocks"),
        ("000300.SH", "query_hs300_stocks"),
        ("SZ50", "query_sz50_stocks"),
        ("ZZ500", "query_zz500_stocks"),
    ],
)
def test_standard_constituents(symbol, method, native_sdk):
    """Keep the index choice separate from returned constituent stock identifiers."""
    raw = {"updateDate": "2024-01-02", "code": "sh.600000", "code_name": "浦发银行"}
    native_sdk.queries[method].side_effect = lambda **params: source_result([raw])
    result = asyncio.run(BaostockIndexConstituentsFetcher.fetch_data({"symbol": symbol, "date": "2024-01-02"}))
    native_sdk.queries[method].assert_called_once_with(date="2024-01-02")
    assert result[0].symbol == "SH.600000" and result[0].name == "浦发银行"
    assert result[0].date.isoformat() == "2024-01-02"


def test_public_standard_constituents(native_sdk):
    """The conventional index endpoint also exposes BaoStock membership."""
    pytest.importorskip("openbb_index")
    from openbb import obb

    raw = {"updateDate": "2024-01-02", "code": "sh.600000", "code_name": "浦发银行"}
    native_sdk.queries["query_hs300_stocks"].side_effect = lambda **params: source_result([raw])
    result = obb.index.constituents(symbol="HS300", date="2024-01-02", provider="baostock")
    assert result.results[0].symbol == "SH.600000"

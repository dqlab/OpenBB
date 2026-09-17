"""Offline BaoStock contract and session tests using synthetic SDK responses."""

import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime
from threading import Event
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from openbb_baostock import baostock_provider
from openbb_baostock.models.equity_historical import BaostockEquityHistoricalFetcher as Equity
from openbb_baostock.models.equity_profile import BaostockEquityProfileFetcher as Profile
from openbb_baostock.models.equity_search import BaostockEquitySearchFetcher as Search
from openbb_baostock.models.index_historical import BaostockIndexHistoricalFetcher as Index
from openbb_baostock.utils import helpers
from openbb_core.app.model.abstract.error import OpenBBError
from openbb_core.provider.utils.errors import EmptyDataError
from pydantic import ValidationError

PARAMS = {"symbol": "sh.600000", "start_date": "2024-01-02", "end_date": "2024-01-05"}
BAR = {
    "date": "2024-01-02",
    "code": "sh.600000",
    "open": "10.0",
    "high": "11.0",
    "low": "9.0",
    "close": "10.5",
    "preclose": "10",
    "volume": "123456",
    "amount": "1296288.0",
    "adjustflag": "3",
    "turn": "1.25",
    "tradestatus": "1",
    "pctChg": "5.0",
    "peTTM": "6.1",
    "pbMRQ": "0.5",
    "psTTM": "",
    "pcfNcfTTM": "-3",
    "isST": "0",
}
STOCK = {
    "code": "sh.600000",
    "code_name": "浦发银行",
    "ipoDate": "1999-11-10",
    "outDate": "",
    "type": "1",
    "status": "1",
}


class Result:
    """Minimal SDK cursor, with page failures independent of initial success."""

    def __init__(self, rows, error="0", final_error="0"):
        """Set synthetic records and initial/end-of-cursor status."""
        self.fields = list(rows[0]) if rows else ["code"]
        self.rows = iter([list(row.values()) for row in rows])
        self.error_code = error
        self.error_msg = "synthetic status"
        self.final_error = final_error

    def next(self):
        """Advance the synthetic cursor."""
        self.row = next(self.rows, None)
        if self.row is None:
            self.error_code = self.final_error
        return self.row is not None

    def get_row_data(self):
        """Return the current source row."""
        return self.row


@pytest.fixture
def sdk(monkeypatch):
    """Mock the SDK entry points while exercising the real session wrapper."""
    login = Mock(return_value=SimpleNamespace(error_code="0"))
    logout = Mock(return_value=SimpleNamespace(error_code="0"))
    history = Mock(side_effect=lambda **params: Result([{**BAR, "code": params["code"]}]))
    basic = Mock(side_effect=lambda **params: Result([STOCK]))
    connection = Mock()
    monkeypatch.setattr(helpers.bs, "login", login)
    monkeypatch.setattr(helpers.bs, "logout", logout)
    monkeypatch.setattr(helpers.bs, "query_history_k_data_plus", history)
    monkeypatch.setattr(helpers.bs, "query_stock_basic", basic)
    monkeypatch.setattr(helpers.context, "default_socket", connection, raising=False)
    return SimpleNamespace(login=login, logout=logout, history=history, basic=basic, connection=connection)


def fetch(fetcher=Equity, params=None):
    """Exercise the entire async fetcher pipeline."""
    return asyncio.run(fetcher.fetch_data(params or PARAMS))


@pytest.mark.parametrize(
    "symbol,expected",
    [
        (" sh.600000 ", "SH.600000"),
        ("600000.SH", "SH.600000"),
        ("600000.SS", "SH.600000"),
        ("000001.SZ", "SZ.000001"),
        ("sh.600000,600000.SH,sz.000001", "SH.600000,SZ.000001"),
    ],
)
def test_symbols(symbol, expected):
    """Exchange identity survives normalization and aliases are deduplicated."""
    assert Equity.transform_query({**PARAMS, "symbol": symbol}).symbol == expected


@pytest.mark.parametrize("symbol", ["000001", "AAPL", "bj.430047", "sh.123", "sh.600000,", "sh.600000\x01"])
def test_invalid_symbols(symbol, sdk):
    """Reject ambiguous or unsupported identifiers before login."""
    with pytest.raises(ValidationError):
        fetch(params={**PARAMS, "symbol": symbol})
    sdk.login.assert_not_called()


def test_dates_and_parameter_validation(sdk):
    """Use a bounded historical default and reject unsupported query combinations."""
    params = {"symbol": "sh.600000", "end_date": "2024-01-01"}
    query = Equity.transform_query(params)
    assert query.start_date == date(2023, 1, 1)
    assert params == {"symbol": "sh.600000", "end_date": "2024-01-01"}
    for changes in [{"start_date": "2024-02-01"}, {"interval": "1m"}, {"adjustment": "splits_only"}]:
        with pytest.raises(ValidationError):
            Equity.transform_query({**PARAMS, **changes})
    for changes in [{"interval": "5m"}, {"adjustment": "forward"}]:
        with pytest.raises(ValidationError):
            Index.transform_query({**PARAMS, **changes})
    sdk.login.assert_not_called()


@pytest.mark.parametrize("adjustment,flag", [("unadjusted", "3"), ("forward", "2"), ("backward", "1")])
def test_daily_mapping_and_adjustments(sdk, adjustment, flag):
    """Preserve source units and convert percentages exactly once."""
    sdk.history.side_effect = lambda **params: Result([{**BAR, "adjustflag": flag}])
    row = fetch(params={**PARAMS, "adjustment": adjustment})[0]
    assert row.symbol == "SH.600000"
    assert row.date == date(2024, 1, 2)
    assert row.open == 10 and row.close == 10.5 and row.volume == 123456
    assert row.amount == 1296288 and row.turnover_rate == 0.0125
    assert row.change_percent == 0.05 and row.pe_ttm == 6.1 and row.ps_ttm is None
    assert row.trade_status == 1 and row.is_st is False and row.adjustment == adjustment
    assert sdk.history.call_args.kwargs["adjustflag"] == flag
    sdk.login.assert_called_once_with()
    sdk.logout.assert_called_once_with()
    sdk.connection.close.assert_called_once_with()


@pytest.mark.parametrize(
    "interval,frequency", [("1W", "w"), ("1M", "m"), ("5m", "5"), ("15m", "15"), ("30m", "30"), ("60m", "60")]
)
def test_frequency_fields_and_intraday_time(sdk, interval, frequency):
    """Request frequency-compatible fields and retain the bar-end timestamp."""

    def response(**params):
        raw = {**BAR, "time": "20240102103000000"}
        return Result([{field: raw[field] for field in params["fields"].split(",")}])

    sdk.history.side_effect = response
    row = fetch(params={**PARAMS, "interval": interval})[0]
    request = sdk.history.call_args.kwargs
    assert request["frequency"] == frequency
    assert "tradestatus" not in request["fields"]
    if interval.endswith("m"):
        assert row.date == datetime(2024, 1, 2, 10, 30, tzinfo=helpers.SHANGHAI)
        assert row.date.utcoffset().total_seconds() == 28800
        assert "turn" not in request["fields"]
    else:
        assert row.date == date(2024, 1, 2)
        assert "time" not in request["fields"]


def test_suspended_missing_values(sdk):
    """Keep suspended sessions and missing prices without inventing zeros."""
    raw = {**BAR, **dict.fromkeys(["open", "high", "low", "close", "volume", "turn", "isST"], ""), "tradestatus": "0"}
    sdk.history.side_effect = lambda **params: Result([raw])
    row = fetch()[0]
    assert row.open is None and row.close is None and row.volume is None
    assert row.turnover_rate is None and row.is_st is None and row.trade_status == 0


def test_index_history(sdk):
    """Index requests retain their explicit exchange and omit stock-only fields."""
    rows = fetch(Index, {**PARAMS, "symbol": "sh.000001,sz.399001"})
    assert [row.symbol for row in rows] == ["SH.000001", "SZ.399001"]
    for call in sdk.history.call_args_list:
        assert "turn" not in call.kwargs["fields"] and "peTTM" not in call.kwargs["fields"]
    sdk.login.assert_called_once()
    sdk.logout.assert_called_once()


@pytest.mark.parametrize(
    "change,match",
    [
        ({"adjustflag": "2"}, "adjustment"),
        ({"date": "2024-01-06"}, "outside"),
        ({"code": "sz.000001"}, "unexpected symbol"),
    ],
)
def test_reject_mismatched_history(sdk, change, match):
    """Reject data that violates the requested identity or window."""
    sdk.history.side_effect = lambda **params: Result([{**BAR, **change}])
    with pytest.raises(OpenBBError, match=match):
        fetch()
    sdk.logout.assert_called_once()


def test_duplicates_rejected(sdk):
    """Reject duplicate observation keys rather than silently dropping source rows."""
    sdk.history.side_effect = lambda **params: Result([BAR, BAR])
    with pytest.raises(OpenBBError, match="duplicate"):
        fetch()


def test_missing_symbol_is_not_partial_success(sdk):
    """Require every requested symbol to return data."""
    sdk.history.side_effect = [Result([BAR]), Result([])]
    with pytest.raises(EmptyDataError, match="SZ.000001"):
        fetch(params={**PARAMS, "symbol": "sh.600000,sz.000001"})
    sdk.logout.assert_called_once()


@pytest.mark.parametrize("error,final_error", [("10004011", "0"), ("0", "10002007")])
def test_provider_and_pagination_errors(sdk, error, final_error):
    """Failures on the first or later page cannot look like a successful response."""
    sdk.history.side_effect = lambda **params: Result([BAR], error, final_error)
    with pytest.raises(OpenBBError, match=error if error != "0" else final_error):
        fetch()
    sdk.logout.assert_called_once()
    sdk.connection.close.assert_called_once()


def test_silent_full_page_failure(sdk):
    """Detect the SDK's unchanged success status on a transport failure during next()."""
    result = Result([BAR])
    result.data = [None] * helpers.BAOSTOCK_PER_PAGE_COUNT
    sdk.history.side_effect = lambda **params: result
    with pytest.raises(OpenBBError, match="incomplete"):
        fetch()


def test_sdk_successful_pagination(monkeypatch):
    """Consume a real SDK cursor through a full page and a successful final page."""
    from baostock.data import resultset

    result = resultset.ResultData()
    result.fields = ["code"]
    result.error_code = "0"
    result.msg_type = "95"
    result.data = [["sh.600000"]] * helpers.BAOSTOCK_PER_PAGE_COUNT
    result.cur_page_num = "1"
    result.msg_body = "query\x01anonymous\x011\x012000"
    response = (
        "00.9.30\x0196\x010000000000" + '0\x01success\x01query\x01anonymous\x012\x012000\x01{"record":[["sz.000001"]]}\n'
    )
    request = Mock(return_value=response)
    monkeypatch.setattr(resultset.sock, "send_msg", request)
    rows = helpers.read_result(result, "pagination")
    assert len(rows) == helpers.BAOSTOCK_PER_PAGE_COUNT + 1
    assert rows[-1] == {"code": "sz.000001"}
    request.assert_called_once()


def test_bad_row_width(sdk):
    """Do not zip malformed rows into plausible but incomplete data."""
    result = Result([BAR])
    result.fields.append("unexpected")
    sdk.history.side_effect = lambda **params: result
    with pytest.raises(OpenBBError, match="field count"):
        fetch()


def test_login_failure_closes_socket_without_query(sdk):
    """A failed login cannot proceed to data access or leak its socket."""
    sdk.login.return_value = SimpleNamespace(error_code="10001004", error_msg="client expired")
    with pytest.raises(OpenBBError, match="10001004.*client expired"):
        fetch()
    sdk.history.assert_not_called()
    sdk.logout.assert_not_called()
    sdk.connection.close.assert_called_once()


def test_logout_error_preserves_original_failure(sdk):
    """Cleanup errors must not overwrite a useful query error."""
    sdk.history.side_effect = OSError("source unavailable")
    sdk.logout.side_effect = RuntimeError("cleanup failed")
    with pytest.raises(OpenBBError, match="source unavailable"):
        fetch()
    sdk.connection.close.assert_called_once()


def test_search_filters_stock_types_and_preserves_delisted_metadata(sdk):
    """Exclude indices and retain delisting dates and listing status."""
    sdk.basic.side_effect = lambda **params: Result(
        [
            STOCK,
            {**STOCK, "code": "sh.000001", "type": "2"},
            {**STOCK, "code": "sh.600001", "status": "0", "outDate": "2020-01-01"},
        ]
    )
    rows = fetch(Search, {"query": "6000", "is_symbol": True})
    assert [row.symbol for row in rows] == ["SH.600000", "SH.600001"]
    assert rows[0].delisting_date is None and rows[0].is_active is True
    assert rows[1].delisting_date == date(2020, 1, 1) and rows[1].is_active is False
    assert len(fetch(Search, {"query": "", "limit": 1})) == 1
    assert fetch(Search, {"query": "9999", "is_symbol": True}) == []


def test_search_pushes_exact_code_and_name_queries(sdk):
    """Use source-side filters where available."""
    assert fetch(Search, {"query": "600000.SH", "is_symbol": True})[0].name == "浦发银行"
    sdk.basic.assert_called_with(code="sh.600000")
    fetch(Search, {"query": "浦发"})
    sdk.basic.assert_called_with(code_name="浦发")
    with pytest.raises(ValidationError):
        Search.transform_query({"query": "test\x01bad"})


def test_profile_basic_fields_and_missing_symbol(sdk):
    """Map available fields while leaving full company details unset."""
    row = fetch(Profile, {"symbol": "sh.600000"})[0]
    assert row.symbol == "SH.600000" and row.stock_exchange == "SSE"
    assert row.ipo_date == date(1999, 11, 10) and row.delisting_date is None
    assert row.long_description is None and row.is_active is True
    sdk.basic.side_effect = lambda **params: Result([{**STOCK, "type": "2"}])
    with pytest.raises(EmptyDataError):
        fetch(Profile, {"symbol": "sh.000001"})


def test_session_serialization(sdk):
    """A second request cannot log in until the first session has logged out."""
    entered, release, second_started = Event(), Event(), Event()
    order = []

    def query(**params):
        order.append(params["code"])
        if params["code"] == "first":
            entered.set()
            assert release.wait(5)
        return Result([STOCK])

    def second():
        second_started.set()
        return helpers._query_batch("query_stock_basic", [{"code": "second"}])

    sdk.basic.side_effect = query
    sdk.login.side_effect = lambda: order.append("login") or SimpleNamespace(error_code="0")
    sdk.logout.side_effect = lambda: order.append("logout") or SimpleNamespace(error_code="0")
    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(helpers._query_batch, "query_stock_basic", [{"code": "first"}])
        try:
            assert entered.wait(5)
            other = pool.submit(second)
            assert second_started.wait(5)
            assert order == ["login", "first"]
        finally:
            release.set()
        first.result(timeout=5)
        other.result(timeout=5)
    assert order == ["login", "first", "logout", "login", "second", "logout"]


def test_provider_registration():
    """Register all implemented standard endpoints without credential requirements."""
    assert baostock_provider.name == "baostock"
    assert {"EquityHistorical", "EquityInfo", "EquitySearch", "IndexHistorical", "IndexConstituents"} <= set(
        baostock_provider.fetcher_dict
    )
    assert all(not fetcher.require_credentials for fetcher in baostock_provider.fetcher_dict.values())


def test_public_openbb_commands(sdk):
    """Exercise registered commands after openbb-build when the routers are installed."""
    pytest.importorskip("openbb_equity")
    pytest.importorskip("openbb_index")
    from openbb import obb

    prices = obb.equity.price.historical(**PARAMS, provider="baostock")
    assert prices.provider == "baostock" and prices.results[0].close == 10.5
    indices = obb.index.price.historical(**{**PARAMS, "symbol": "sh.000001"}, provider="baostock")
    assert indices.results[0].symbol == "SH.000001"
    search = obb.equity.search(query="浦发", provider="baostock")
    assert search.results[0].symbol == "SH.600000"
    profile = obb.equity.profile(symbol="sh.600000", provider="baostock")
    assert profile.results[0].ipo_date == date(1999, 11, 10)

    def intraday(**params):
        raw = {**BAR, "time": "20240102103000000", "adjustflag": params["adjustflag"]}
        return Result([{field: raw[field] for field in params["fields"].split(",")}])

    sdk.history.side_effect = intraday
    prices = obb.equity.price.historical(**PARAMS, interval="5m", adjustment="forward", provider="baostock")
    assert prices.results[0].date == datetime(2024, 1, 2, 10, 30, tzinfo=helpers.SHANGHAI)
    assert prices.results[0].adjustment == "forward"
    with pytest.raises(OpenBBError, match="Invalid value '5m'"):
        obb.index.price.historical(**PARAMS, interval="5m", provider="baostock")
    with pytest.raises(OpenBBError, match="Invalid value 'forward'"):
        obb.index.price.historical(**PARAMS, adjustment="forward", provider="baostock")

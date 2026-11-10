"""CFFEX provider compatibility tests using explicit synthetic CSV fixtures."""

from unittest.mock import Mock

import pytest
from openbb_akshare.utils import cffex, helpers
from openbb_core.app.model.abstract.error import OpenBBError

HEADER = "合约代码,今开盘,最高价,最低价,成交量,成交金额,持仓量,持仓变化,今收盘,今结算,前结算,涨跌1,涨跌2,Delta"
FUTURE_ROW = "IF1005  ,3450,3488,3413.2,48988,5053880.532,2702,2702,3415.6,3431.2,3399,16.6,32.2,--"


@pytest.mark.parametrize("legacy", [False, True])
def test_named_columns_support_old_and_new_layouts(legacy):
    """The extra legacy volatility column must not shift the OHLC/settlement mapping."""
    header = HEADER.replace(",Delta", ",隐含波动率(%),Delta") if legacy else HEADER
    row = FUTURE_ROW + ",--" if legacy else FUTURE_ROW
    result = cffex.parse_daily_csv(header + "\n" + row, "2010-04-16")
    assert result.loc[0, "symbol"] == "IF1005"
    assert result.loc[0, "close"] == 3415.6
    assert result.loc[0, "settle"] == 3431.2
    assert result.loc[0, "date"] == "20100416"
    assert result.loc[0, "volume"] == 48988


def test_options_and_summary_rows_are_excluded():
    """Only individual futures survive; options and totals are not pseudo-contracts."""
    option = FUTURE_ROW.replace("IF1005  ", "IO1005-C-3000")
    total = FUTURE_ROW.replace("IF1005  ", "合计")
    frame = cffex.parse_daily_csv(HEADER + "\n" + FUTURE_ROW + "\n" + option + "\n" + total, "2010-04-16")
    assert frame.symbol.tolist() == ["IF1005"]


@pytest.mark.parametrize("replacement", ["IF1013", "NEW1005", "IF0"])
def test_malformed_or_unknown_contract_fails(replacement):
    """Never discard unknown products or guess malformed contract identifiers."""
    with pytest.raises(OpenBBError):
        cffex.parse_daily_csv(HEADER + "\n" + FUTURE_ROW.replace("IF1005", replacement), "2010-04-16")


def test_missing_prices_remain_null():
    """A no-trade price sentinel is missing, not an observed zero price."""
    text = HEADER + "\n" + FUTURE_ROW.replace("3450,3488,3413.2,48988", "--,--,--,0")
    records = helpers.frame_to_records(cffex.parse_daily_csv(text, "2010-04-16"))
    assert records[0]["open"] is None
    assert records[0]["high"] is None
    assert records[0]["low"] is None
    assert records[0]["volume"] == 0


def test_duplicate_contracts_fail():
    """A provider day must be unique per contract before collector validation."""
    with pytest.raises(OpenBBError):
        cffex.parse_daily_csv(HEADER + "\n" + FUTURE_ROW + "\n" + FUTURE_ROW, "2010-04-16")


def test_http_uses_openbb_helper_and_validated_date(monkeypatch):
    """The provider adapter retains the SDK source but uses OpenBB HTTP settings."""
    response = Mock(content=(HEADER + "\n" + FUTURE_ROW).encode("gbk"))
    request = Mock(return_value=response)
    monkeypatch.setattr(cffex, "make_request", request)
    assert len(cffex.fetch_daily_csv("20100416")) == 1
    request.assert_called_once_with(
        "http://www.cffex.com.cn/sj/hqsj/rtj/201004/16/20100416_1.csv", timeout=30
    )
    response.raise_for_status.assert_called_once()
    with pytest.raises(ValueError):
        cffex.fetch_daily_csv("2010-04-16")


def test_calendar_native_model_has_no_query_parameters():
    """The saved reference calendar is obtained through an explicitly registered provider model."""
    from openbb_akshare.models.native import NATIVE_FETCHERS

    fetcher = NATIVE_FETCHERS["AkshareToolTradeDateHistSina"]
    query = fetcher.transform_query({})
    assert query.model_dump() == {}
    result = fetcher.transform_data(query, [{"trade_date": "2026-10-13"}])
    assert result[0].trade_date == "2026-10-13"


@pytest.mark.parametrize("column", ["今收盘", " 今收盘 "])
def test_duplicate_headers_are_rejected_before_pandas(column):
    """Pandas must not silently mangle conflicting source header names."""
    with pytest.raises(OpenBBError, match="duplicates"):
        cffex.parse_daily_csv(HEADER + "," + column + "\n" + FUTURE_ROW + ",9999", "2010-04-16")


@pytest.mark.parametrize("legacy", [False, True])
def test_truncated_rows_fail_instead_of_padding_nulls(legacy):
    """Structurally absent source cells are corrupt, not legitimate missing prices."""
    header = HEADER.replace(",Delta", ",隐含波动率(%),Delta") if legacy else HEADER
    row = ",".join(FUTURE_ROW.split(",")[:9])
    with pytest.raises(OpenBBError, match="truncated"):
        cffex.parse_daily_csv(header + "\n" + row, "2010-04-16")


@pytest.mark.parametrize("position", [6, 9])
@pytest.mark.parametrize("token", ["inf", "-inf", "Infinity"])
def test_nonfinite_numbers_cannot_be_normalized_to_null(position, token):
    """Invalid OI/settlement values must fail before the shared JSON-null conversion."""
    values = FUTURE_ROW.split(",")
    values[position] = token
    with pytest.raises(OpenBBError, match="nonfinite"):
        helpers.frame_to_records(cffex.parse_daily_csv(HEADER + "\n" + ",".join(values), "2010-04-16"))


def test_legacy_short_totals_do_not_hide_truncated_contracts():
    """Only explicitly labelled summary rows may have a short historical width."""
    header = HEADER.replace(",Delta", ",隐含波动率(%),Delta")
    row = FUTURE_ROW + ",--"
    short_total = ",".join(FUTURE_ROW.replace("IF1005  ", "合计").split(","))
    frame = cffex.parse_daily_csv(header + "\n" + row + "\n" + short_total, "2010-04-16")
    assert frame.symbol.tolist() == ["IF1005"]
    with pytest.raises(OpenBBError, match="truncated"):
        cffex.parse_daily_csv(header + "\n" + FUTURE_ROW, "2010-04-16")


def test_registered_calendar_reaches_sdk_via_query_executor(monkeypatch):
    """The production dispatch path calls the catalogued SDK calendar, not local weekdays."""
    import asyncio

    import pandas as pd
    from openbb_akshare import akshare_provider
    from openbb_core.provider.query_executor import QueryExecutor
    from openbb_core.provider.registry import Registry

    call = Mock(return_value=pd.DataFrame({"trade_date": ["2026-10-13"]}))
    monkeypatch.setattr(helpers, "resolve_method", lambda method: call)
    registry = Registry()
    registry.include_provider(akshare_provider)
    result = asyncio.run(QueryExecutor(registry).execute(
        provider_name="akshare", model_name="AkshareToolTradeDateHistSina", params={}, credentials={}
    ))
    call.assert_called_once_with()
    assert result[0].trade_date == "2026-10-13"

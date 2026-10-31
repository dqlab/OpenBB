"""SDK field copy checks; this test never creates an API or connects."""
import os
from pathlib import Path

import pytest


def _sdk():
    """Avoid loading a wrapper that aborts when its required locale is absent."""
    locale_path = os.environ.get("LOCPATH", "")
    if not any((Path(p) / "zh_CN.GB18030").is_dir() for p in locale_path.split(":") if p):
        if os.environ.get("CTP_REQUIRE_SDK"):
            pytest.fail("required zh_CN.GB18030 locale unavailable")
        pytest.skip("set LOCPATH to a directory containing zh_CN.GB18030")
    if os.environ.get("CTP_REQUIRE_SDK"):
        import importlib

        return importlib.import_module("openctp_ctp.thostmduserapi")
    return pytest.importorskip("openctp_ctp.thostmduserapi")


def test_real_swig_snapshot_is_copied():
    """All exposed SDK properties are copied before the proxy changes."""
    md = _sdk()
    from openbb_ctp.native import copy_fields

    item = md.CThostFtdcDepthMarketDataField()
    item.InstrumentID = "au2612"
    item.LastPrice = 42.25
    copied = copy_fields(item, md.CThostFtdcDepthMarketDataField)
    item.InstrumentID = "changed"
    item.LastPrice = 0
    assert copied["InstrumentID"] == "au2612"
    assert copied["LastPrice"] == 42.25
    properties = [x for x in vars(md.CThostFtdcDepthMarketDataField).values() if isinstance(x, property)]
    assert len(copied) == len(properties) - 1


def test_sdk_subscription_argument_offline(tmp_path):
    """The installed wrapper takes bytes; an uninitialized API cannot subscribe."""
    md = _sdk()
    api = md.CThostFtdcMdApi.CreateFtdcMdApi(str(tmp_path) + "/")
    try:
        assert api.SubscribeMarketData([b"au2612"], 1) != 0
    finally:
        api.Release()

"""Keep BaoStock price bases explicit when reading persisted A-share bars."""

import pytest
from openbb_core.app.model.abstract.error import OpenBBError
from openbb_duckdb.models.equity_historical import DuckDBEquityHistoricalFetcher as F


def source_row():
    """Return a daily bar with independently persisted source adjustment modes."""
    return dict(
        symbol="SH.600000",
        date="2024-01-02",
        open=10,
        high=12,
        low=9,
        close=11,
        volume=100,
        base_adjustment="unadjusted",
        trade_status=1,
        forward_open=8,
        forward_high=9.6,
        forward_low=7.2,
        forward_close=8.8,
        backward_open=20,
        backward_high=24,
        backward_low=18,
        backward_close=22,
    )


@pytest.mark.parametrize("mode,close", [("unadjusted", 11), ("forward", 8.8), ("backward", 22)])
def test_a_share_modes_preserve_source_prices_and_volume(mode, close):
    """Choose the recorded series without applying dividend factors to volume."""
    query = F.transform_query(dict(symbol="SH.600000", adjustment=mode))
    row = F.transform_data(query, [source_row()])[0]
    assert row.close == close
    assert row.volume == 100
    assert row.price_adjustment == mode


@pytest.mark.parametrize("mode", ["splits_only", "splits_and_dividends"])
def test_a_shares_do_not_inherit_yahoo_adjustment_semantics(mode):
    """Reject unavailable adjustment conventions instead of relabeling prices."""
    with pytest.raises(OpenBBError):
        F.transform_data(F.transform_query(dict(symbol="SH.600000", adjustment=mode)), [source_row()])


def test_unadjusted_requires_explicit_source_basis():
    """An older split-adjusted US relation cannot be called unadjusted."""
    row = source_row()
    row.pop("base_adjustment")
    with pytest.raises(OpenBBError, match="declare unadjusted"):
        F.transform_data(F.transform_query(dict(symbol="SH.600000", adjustment="unadjusted")), [row])


def test_missing_suspended_adjusted_prices_stay_missing():
    """A source-marked suspension may have genuinely unavailable prices."""
    row = source_row()
    row.update(trade_status=0)
    for field in ("open", "high", "low", "close"):
        row["forward_" + field] = None
    result = F.transform_data(F.transform_query(dict(symbol="SH.600000", adjustment="forward")), [row])
    assert result[0].close is None

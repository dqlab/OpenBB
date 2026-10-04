"""Actual bt cash/share/fee and drifted-holding timing reconciliations."""

import pytest
from openbb_core.provider.standard_models.alpha_research import BtRequest, DatasetInput
from openbb_polars_ta import PolarsFetcher

from openbb_alpha.datasets import register
from openbb_alpha.fixture import synthetic
from openbb_alpha.store import Store


def factors(raw):
    ref = register(DatasetInput.model_validate(raw))
    return PolarsFetcher.extract_data(
        PolarsFetcher.transform_query(
            {
                "request": {
                    "dataset_id": ref.id,
                    "factors": ["reversal_5"],
                }
            }
        ),
        None,
    ).artifact_id


def test_real_bt_next_open_cash_shares_fees():
    pytest.importorskip("bt")
    from openbb_bt import backtest

    raw = synthetic(1, 14).model_dump(mode="json")
    for i, row in enumerate(raw["rows"]):
        row.update(price=100, raw_close=100, open=10 if i <= 6 else 11)
    result = backtest(
        BtRequest(
            factor_artifact_id=factors(raw),
            factor_id="reversal_5",
            top_k=1,
            rebalance_sessions=252,
            initial_cash=1000,
            commission_bps=100,
            spread_slippage_bps=0,
        )
    )
    store = Store()
    trades = store.table(result.trades).to_pylist()
    assert len(trades) == 1
    trade = trades[0]
    assert trade["quantity"] == pytest.approx(1000 / 10.1)
    assert trade["commission"] == pytest.approx(1000 / 101)
    assert trade["session"] == raw["calendar"][6]["session"]
    assert trade["signal_time"] == raw["calendar"][5]["decision_time"].replace("Z", "+00:00")
    assert trade["execution_time"] == raw["calendar"][6]["open_time"].replace("Z", "+00:00")
    net, gross = store.table(result.net).to_pylist(), store.table(result.gross).to_pylist()
    assert net[-1]["nav"] == pytest.approx(1000 / 10.1 * 11)
    assert net[-1]["cash"] == pytest.approx(0, abs=1e-6)
    assert gross[-1]["nav"] == pytest.approx(1100)


def test_real_bt_rebalances_drifted_holdings():
    pytest.importorskip("bt")
    from openbb_bt import backtest

    raw = synthetic(2, 10).model_dump(mode="json")
    for row in raw["rows"]:
        row.update(price=100, raw_close=100, open=10)
        if row["instrument_id"] == "SYN:000" and row["session"] >= raw["calendar"][7]["session"]:
            row["open"] = 20
    result = backtest(
        BtRequest(
            factor_artifact_id=factors(raw),
            factor_id="reversal_5",
            top_k=2,
            rebalance_sessions=1,
            initial_cash=1000,
            commission_bps=0,
            spread_slippage_bps=0,
        )
    )
    store = Store()
    trades = [
        r
        for r in store.table(result.trades).to_pylist()
        if r["session"] == raw["calendar"][7]["session"]
    ]
    assert len(trades) == 2
    assert trades[0]["shares_before"] == pytest.approx(50)
    assert trades[0]["quantity"] == pytest.approx(-12.5)
    assert trades[1]["quantity"] == pytest.approx(25)
    row = store.table(result.net).to_pylist()[7]
    assert row["pretrade_nav"] == pytest.approx(1500)
    assert row["traded_notional_over_pretrade_nav"] == pytest.approx(1 / 3)


@pytest.mark.parametrize("fractional", [True, False])
def test_rotation_with_costs_never_borrows(fractional):
    pytest.importorskip("bt")
    from openbb_bt import backtest

    raw = synthetic(10, 80).model_dump(mode="json")
    result = backtest(
        BtRequest(
            factor_artifact_id=factors(raw),
            factor_id="reversal_5",
            top_k=3,
            commission_bps=20,
            spread_slippage_bps=30,
            fractional_shares=fractional,
        )
    )
    store = Store()
    assert min(r["cash"] for r in store.table(result.net).to_pylist()) >= -1e-6
    holdings = store.table(result.holdings).to_pylist()
    assert min(r["shares"] for r in holdings) >= 0
    if not fractional:
        assert all(r["shares"] == int(r["shares"]) for r in holdings)

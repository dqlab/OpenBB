"""Actual bt long-only top-K simulation, with next-open ledger reconciliation."""


def backtest(request):
    import numpy as np
    import pandas as pd
    import pyarrow as pa
    from openbb_alpha.datasets import load
    from openbb_alpha.results import load_factors
    from openbb_alpha.store import Store
    from openbb_core.provider.standard_models.alpha_research import BtResult
    from openbb_research.workflow import record_adapter

    try:
        import bt
    except ImportError as exc:
        raise ValueError("Install openbb-bt with the actual bt dependency") from exc

    factor, table = load_factors(request.factor_artifact_id)
    if request.factor_id not in [s.id for s in factor.factors]:
        raise ValueError("factor not present in artifact")
    dataset, panel = load(factor.dataset_id)
    if dataset.open_convention != "synthetic_no_actions":
        raise ValueError(
            "v1 bt requires synthetic_no_actions; real corporate actions are unsupported"
        )
    bars = panel.to_pandas()
    prices = bars.pivot(index="session", columns="instrument_id", values="open").sort_index()
    if prices.isna().any().any() or (prices <= 0).any().any():
        raise ValueError("bt requires complete positive next-open marks; no filling missing prices")
    dates = list(prices.index)
    prices.index = pd.to_datetime(prices.index)
    if request.top_k > len(prices.columns):
        raise ValueError("top_k exceeds declared universe")
    features = table.to_pandas()
    features = features[features.factor_id == request.factor_id]
    value = "rank_value" if factor.transform == "centered_rank" else "value"
    targets, signal_times = {}, {}
    first = None
    for i, day in enumerate(dates[:-1]):
        scores = features[(features.session == day) & features.eligible & features[value].notna()]
        if first is None:
            if len(scores) < request.top_k:
                continue
            first = i
        if (i - first) % request.rebalance_sessions:
            continue
        selected = scores.sort_values([value, "instrument_id"], ascending=[False, True]).head(
            request.top_k
        )
        weights = {instrument: 0.0 for instrument in prices.columns}
        if len(selected) == request.top_k:
            weights.update(
                {instrument: 1.0 / request.top_k for instrument in selected.instrument_id}
            )
        execution = prices.index[i + 1]
        targets[execution] = weights
        signal_times[execution] = dataset.calendar[i].decision_time.isoformat()
    if not targets:
        raise ValueError("no eligible top-K rebalance observations")
    weights = pd.DataFrame.from_dict(targets, orient="index").sort_index()
    rate = (request.commission_bps + request.spread_slippage_bps) / 10000

    def execute(name, fee_rate):
        class CashSafeRebalance(bt.Algo):
            """Solve equal weights after linear costs, then use bt's trade ledger."""

            def __call__(self, target):
                names = list(prices.columns)
                marks = target.universe.loc[target.now, names].to_numpy(dtype=float)
                old = np.array(
                    [target.children[n].position if n in target.children else 0 for n in names]
                )
                desired = np.array([target.temp["weights"].get(n, 0) for n in names])
                nav = target.value

                def quantities(capital):
                    shares = capital * desired / marks
                    return shares if request.fractional_shares else np.floor(shares)

                # Holdings plus all actual trade fees must fit current marked NAV.
                # The objective is monotone for nonnegative weights and rate < 1.
                low, high = 0.0, nav
                for _ in range(64):
                    mid = (low + high) / 2
                    shares = quantities(mid)
                    outlay = (shares * marks).sum() + (
                        np.abs(shares - old) * marks
                    ).sum() * fee_rate
                    if outlay <= nav:
                        low = mid
                    else:
                        high = mid
                changes = quantities(low) - old
                # Sell first; bt handles commissions, cash and position accounting.
                for index in np.argsort(changes):
                    if abs(changes[index]) > 1e-10:
                        target.transact(float(changes[index]), child=names[index])
                target.root.update(target.now)
                return True

        strategy = bt.Strategy(name, [bt.algos.WeighTarget(weights), CashSafeRebalance()])
        simulation = bt.Backtest(
            strategy,
            prices,
            initial_capital=request.initial_cash,
            integer_positions=not request.fractional_shares,
            commissions=lambda q, p: abs(q * p) * fee_rate,
            progress_bar=False,
        )
        simulation.run()
        return simulation

    gross, net = execute("gross", 0), execute("net", rate)
    positions = net.positions.reindex(
        index=prices.index, columns=prices.columns, fill_value=0
    ).fillna(0)
    previous = positions.shift(1, fill_value=0)
    delta = positions - previous
    notionals = delta.abs() * prices
    fees = notionals * rate
    market_values = positions * prices
    net_values = net.strategy.values.reindex(prices.index)
    gross_values = gross.strategy.values.reindex(prices.index)
    cash = net_values - market_values.sum(axis=1)
    pretrade_nav = cash.shift(1, fill_value=request.initial_cash) + (previous * prices).sum(axis=1)
    expected_cash = (
        cash.shift(1, fill_value=request.initial_cash)
        - (delta * prices).sum(axis=1)
        - fees.sum(axis=1)
    )
    if not np.allclose(cash, expected_cash, rtol=1e-9, atol=1e-6):
        raise ValueError("bt cash/share/fee ledger reconciliation failed")
    if cash.min() < -1e-5 or positions.min().min() < -1e-9:
        raise ValueError("bt produced borrowing or short holdings outside the MVP policy")
    holdings, trades, gross_rows, net_rows = [], [], [], []
    session_map = {s.session.isoformat(): s for s in dataset.calendar}
    for day in prices.index:
        session = day.date().isoformat()
        gross_rows.append(
            {
                "session": session,
                "nav": float(gross_values[day]),
                "return_from_initial": float(gross_values[day] / request.initial_cash - 1),
            }
        )
        net_rows.append(
            {
                "session": session,
                "nav": float(net_values[day]),
                "cash": float(cash[day]),
                "fees": float(fees.loc[day].sum()),
                "pretrade_nav": float(pretrade_nav[day]),
                "traded_notional_over_pretrade_nav": float(
                    notionals.loc[day].sum() / pretrade_nav[day]
                ),
                "return_from_initial": float(net_values[day] / request.initial_cash - 1),
            }
        )
        for instrument in prices.columns:
            quantity = float(delta.loc[day, instrument])
            price = float(prices.loc[day, instrument])
            holdings.append(
                {
                    "session": session,
                    "instrument_id": instrument,
                    "shares": float(positions.loc[day, instrument]),
                    "open_price": price,
                    "market_value": float(market_values.loc[day, instrument]),
                }
            )
            if abs(quantity) > 1e-10:
                trades.append(
                    {
                        "session": session,
                        "instrument_id": instrument,
                        "signal_time": signal_times[day],
                        "execution_time": session_map[session].open_time.isoformat(),
                        "shares_before": float(previous.loc[day, instrument]),
                        "quantity": quantity,
                        "price": price,
                        "commission": abs(quantity * price) * request.commission_bps / 10000,
                        "spread_slippage": abs(quantity * price)
                        * request.spread_slippage_bps
                        / 10000,
                    }
                )
    store = Store()
    refs = {
        name: store.put_table(pa.Table.from_pylist(rows))
        for name, rows in (
            ("holdings", holdings),
            ("trades", trades),
            ("gross", gross_rows),
            ("net", net_rows),
        )
    }
    assumptions = {
        "engine": "bt",
        "coordinate": "synthetic raw-share prices, no corporate actions",
        "execution": "completed t signal at raw open t+1, shifted exactly once",
        "cost_model": "linear commissions and spread/slippage charged as cash fees at open",
        "allocation": "post-cost equal weights solved against drifted holdings; sell before buy",
        "initial_cash": request.initial_cash,
        "fractional_shares": request.fractional_shares,
        "commission_bps": request.commission_bps,
        "spread_slippage_bps": request.spread_slippage_bps,
        "rebalance_sessions": request.rebalance_sessions,
        "rebalance_anchor": dates[first],
        "ties": "descending score then canonical ID",
        "insufficient_top_k": "cash at scheduled rebalance",
        "financing": "none",
        "gross": "separate zero-cost bt simulation; share counts can differ from net",
        "turnover": "sum absolute realized trades / drifted pre-trade NAV; not half-turnover",
    }
    manifest_id = record_adapter(
        "bt",
        request,
        {"factors": request.factor_artifact_id},
        {**{k: v.model_dump(mode="json") for k, v in refs.items()}, "assumptions": assumptions},
        dataset.source.model_dump(mode="json"),
    )
    return BtResult(manifest_id=manifest_id, assumptions=assumptions, **refs)

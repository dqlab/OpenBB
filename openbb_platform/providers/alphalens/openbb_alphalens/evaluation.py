"""Deliberate pandas boundary; no forward-return recomputation or filtering."""

import math
from importlib.metadata import version

from openbb_alpha.labels import load_labels
from openbb_alpha.results import load_factors
from openbb_alpha.store import Store
from openbb_core.provider.standard_models.alpha_research import AlphaEvaluateData, FactorEvaluation


def number(value):
    return float(value) if value is not None and math.isfinite(value) else None


def series_records(series, key="value", sessions=None):
    return [
        {"session": sessions[d] if sessions is not None else str(d), key: number(v)}
        for d, v in series.items()
    ]


def evaluate(request):
    import pandas as pd
    from alphalens import performance

    factors, factor_table = load_factors(request.factor_artifact_id)
    labels, label_table = load_labels(request.label_artifact_id)
    if factors.dataset_id != labels.dataset_id:
        raise ValueError("factor and label dataset identities differ")
    feature = factor_table.to_pandas()
    label = label_table.to_pandas()
    # Alphalens uses pandas frequency shifts. An ordinal daily axis represents
    # exact declared sessions, including holidays/gaps, without calendar inference.
    actual_sessions = sorted(feature.session.unique())
    axis = pd.date_range("2000-01-01", periods=len(actual_sessions), freq="D")
    ordinal = dict(zip(actual_sessions, range(len(axis)), strict=True))
    session_map = dict(zip(axis, actual_sessions, strict=True))
    instruments = sorted(feature.instrument_id.unique())
    asset_codes = {name: i for i, name in enumerate(instruments)}
    keys = ["session", "instrument_id"]
    metrics, coverage = [], []
    warnings = {
        "No confidence intervals: overlapping labels require dependence-aware inference.",
        "Quantile spreads and IC are diagnostics, not executable strategy NAV.",
        "D labels below denote trading-session horizons, not elapsed calendar days.",
    }
    selected = "rank_value" if factors.transform == "centered_rank" else "value"
    reason_col = "rank_null_reason" if factors.transform == "centered_rank" else "null_reason"
    for spec in factors.factors:
        f = feature[feature.factor_id == spec.id].copy()
        for h in labels.spec.horizons:
            horizon_labels = label[label.horizon == h]
            joined = f.merge(
                horizon_labels,
                on=keys,
                how="left",
                suffixes=("_factor", "_label"),
                validate="one_to_one",
                indicator=True,
            )
            factor_col = "value_factor" if selected == "value" else selected
            null_col = "null_reason_factor" if reason_col == "null_reason" else reason_col
            reasons = pd.Series("used", index=joined.index)
            reasons.loc[joined.value_label.isna()] = "label:" + joined.null_reason_label.fillna(
                "missing_join"
            )
            reasons.loc[joined[factor_col].isna()] = "factor:" + joined[null_col].fillna(
                "unknown_null"
            )
            reasons.loc[~joined.eligible_factor] = "ineligible"
            reasons.loc[joined._merge != "both"] = "missing_join"
            expected = {"used", "ineligible", "factor:warmup", "label:sample_tail"}
            unexpected = int((~reasons.isin(expected)).sum())
            counts = {str(k): int(v) for k, v in reasons.value_counts().items()}
            total, used = len(joined), int((reasons == "used").sum())
            coverage.append(
                {
                    "factor_id": spec.id,
                    "horizon_sessions": h,
                    "input_rows": total,
                    "joined_rows": int((joined._merge == "both").sum()),
                    "used_rows": used,
                    "unexpected_loss": unexpected,
                    "drop_reasons": counts,
                }
            )
            denominator = max(1, used + unexpected)
            if unexpected / denominator > request.max_unexpected_loss:
                raise ValueError(f"unexpected evaluation loss for {spec.id}/{h}: {counts}")
            clean = joined[reasons == "used"].copy()
            period = f"{h}D"
            clean["date"] = clean.session.map(lambda s: axis[ordinal[s]])
            clean = (
                clean.rename(
                    columns={"instrument_id": "asset", factor_col: "factor", "value_label": period}
                )
                .set_index(["date", "asset"])
                .sort_index()
            )
            clean = clean[["factor", period]]
            clean.index = pd.MultiIndex(
                levels=[axis, instruments],
                codes=[
                    [axis.get_loc(d) for d in clean.index.get_level_values("date")],
                    [asset_codes[a] for a in clean.index.get_level_values("asset")],
                ],
                names=["date", "asset"],
            )
            if clean.empty:
                warnings.add(f"{spec.id}/{h}: no usable observations")
                metrics.append(
                    {
                        "factor_id": spec.id,
                        "horizon_sessions": h,
                        "daily_ic": [],
                        "ic_mean": None,
                        "ic_std": None,
                        "quantile_means": {},
                        "quantile_spread": None,
                        "rank_persistence": [],
                        "quantile_turnover": {},
                    }
                )
                continue
            ic = performance.factor_information_coefficient(clean, group_adjust=False)[period]
            quantile_frames = []
            for _day, section in clean.groupby(level="date"):
                if len(section) < request.quantiles:
                    warnings.add(f"{spec.id}/{h}: insufficient cross-section for quantiles")
                    continue
                try:
                    section = section.copy()
                    section["factor_quantile"] = (
                        pd.qcut(section.factor, request.quantiles, labels=False) + 1
                    )
                except ValueError:
                    warnings.add(f"{spec.id}/{h}: tied values prevent equal-count quantiles")
                    continue
                quantile_frames.append(section)
            quantile_means, spread, turnover = {}, None, {}
            if quantile_frames:
                qframe = pd.concat(quantile_frames)
                mean, _ = performance.mean_return_by_quantile(
                    qframe, demeaned=False, group_adjust=False
                )
                quantile_means = {str(q): number(v) for q, v in mean[period].items()}
                spread = number(mean[period].get(request.quantiles) - mean[period].get(1))
                for q in range(1, request.quantiles + 1):
                    turn = performance.quantile_turnover(qframe.factor_quantile, q, period=1)
                    turnover[str(q)] = series_records(turn, sessions=session_map)
            persistence = performance.factor_rank_autocorrelation(clean, period=1)
            if ic.isna().any():
                warnings.add(
                    f"{spec.id}/{h}: IC undefined for constant or insufficient cross-sections"
                )
            metrics.append(
                {
                    "factor_id": spec.id,
                    "horizon_sessions": h,
                    "daily_ic": series_records(ic, "spearman_ic", session_map),
                    "ic_mean": number(ic.mean()),
                    "ic_std": number(ic.std(ddof=1)),
                    "ic_sessions": int(ic.notna().sum()),
                    "quantile_means": quantile_means,
                    "quantile_spread": spread,
                    "rank_persistence": series_records(persistence, sessions=session_map),
                    "quantile_turnover": turnover,
                }
            )
    result = FactorEvaluation(
        factor_artifact_id=request.factor_artifact_id,
        label_artifact_id=request.label_artifact_id,
        engine="alphalens",
        engine_version=version("alphalens-reloaded"),
        configuration={
            **request.model_dump(mode="json"),
            "return_units": "simple_fractional",
            "compounding": "none",
            "demeaned": False,
            "group_adjust": False,
            "future_return_filter": None,
            "turnover_period_sessions": 1,
            "calendar_axis": "ordinal declared sessions; D means one session",
            "label_spec": labels.spec.model_dump(mode="json"),
        },
        metrics=tuple(metrics),
        coverage=tuple(coverage),
        warnings=tuple(sorted(warnings)),
    )
    key = Store().put_json("evaluation", result.model_dump(mode="json"))
    return AlphaEvaluateData(artifact_id=key, evaluation=result)

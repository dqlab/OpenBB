"""Real Qlib DatasetH/DataHandlerLP/LinearModel workflow over immutable artifacts."""


def train(request):
    import numpy as np
    import pandas as pd
    import pyarrow as pa
    from openbb_alpha.datasets import load
    from openbb_alpha.labels import load_labels
    from openbb_alpha.results import load_factors
    from openbb_alpha.store import Store
    from openbb_core.provider.standard_models.alpha_research import QlibResult
    from openbb_research.workflow import record_adapter

    try:
        from qlib.contrib.model.linear import LinearModel
        from qlib.data.dataset import DatasetH
        from qlib.data.dataset.handler import DataHandlerLP
    except ImportError as exc:
        raise ValueError("Install openbb-qlib with pyqlib==0.9.7") from exc

    factor, factors = load_factors(request.factor_artifact_id)
    label, labels = load_labels(request.label_artifact_id)
    if factor.dataset_id != label.dataset_id:
        raise ValueError("factor/label snapshots differ")
    dataset, _ = load(factor.dataset_id)
    sessions = [s.session.isoformat() for s in dataset.calendar]
    train_end, valid_end = str(request.train_end), str(request.validation_end)
    if (
        train_end not in sessions
        or valid_end not in sessions
        or not sessions[0] < train_end < valid_end < sessions[-1]
    ):
        raise ValueError("split boundaries must be chronological declared sessions")
    valid_start = sessions[sessions.index(train_end) + 1]
    test_start = sessions[sessions.index(valid_end) + 1]
    feature = factors.to_pandas()
    value = "rank_value" if factor.transform == "centered_rank" else "value"
    x = feature.pivot(index=["session", "instrument_id"], columns="factor_id", values=value)
    y = labels.to_pandas()
    y = y[y.horizon == request.horizon].set_index(["session", "instrument_id"])
    if y.empty:
        raise ValueError("requested horizon not in label snapshot")
    joined = x.join(y[["value", "exit_time", "available_at"]], validate="one_to_one")
    dates = joined.index.get_level_values("session")
    valid_open = dataset.calendar[sessions.index(valid_start)].open_time
    test_open = dataset.calendar[sessions.index(test_start)].open_time
    exits = pd.to_datetime(joined.exit_time, utc=True)
    available = pd.to_datetime(joined.available_at, utc=True)
    train_candidate = dates <= train_end
    # Both economic label intervals and actual label availability must precede
    # the protected validation interval; validation never participates in fit.
    train_mask = train_candidate & (exits < valid_open) & (available < valid_open)
    valid_candidate = (dates >= valid_start) & (dates <= valid_end)
    valid_mask = valid_candidate & (exits < test_open) & (available < test_open)
    complete = x.notna().all(axis=1) & joined.value.notna()
    fit = x[train_mask & complete]
    if len(fit) < max(10, x.shape[1] + 2):
        raise ValueError("insufficient purged training observations")
    mean, scale = fit.mean(), fit.std(ddof=0).replace(0, 1)
    normalized = (x - mean) / scale
    normalized.columns = pd.MultiIndex.from_product([["feature"], normalized.columns])
    # Test labels never enter Qlib's handler, and validation inclusion is false.
    normalized[("label", "target")] = joined.value.where(train_mask & complete)
    normalized.index = pd.MultiIndex.from_arrays(
        [pd.to_datetime(dates), joined.index.get_level_values("instrument_id")],
        names=["datetime", "instrument"],
    )
    handler = DataHandlerLP.from_df(normalized.sort_index())
    splits = {
        "train": [sessions[0], train_end],
        "valid": [valid_start, valid_end],
        "test": [test_start, sessions[-1]],
        "purged_train_rows": int((train_candidate & ~train_mask).sum()),
        "purged_validation_rows": int((valid_candidate & ~valid_mask).sum()),
        "train_rows": len(fit),
        "effective_fit_sessions": [
            str(fit.index.get_level_values("session").min()),
            str(fit.index.get_level_values("session").max()),
        ],
        "protected_validation_open": valid_open.isoformat(),
        "protected_test_open": test_open.isoformat(),
        "purge": "exit AND availability strictly before protected open",
    }
    qdata = DatasetH(
        handler=handler, segments={k: tuple(splits[k]) for k in ("train", "valid", "test")}
    )
    model = LinearModel(
        estimator="ridge", alpha=request.ridge_alpha, fit_intercept=True, include_valid=False
    )
    model.fit(qdata)
    predictions = []
    decision_times = {s.session.isoformat(): s.decision_time.isoformat() for s in dataset.calendar}
    metrics = {}
    for segment in ("valid", "test"):
        prediction = model.predict(qdata, segment=segment)
        actual = joined.value.copy()
        actual.index = normalized.index
        permitted = pd.Series(
            valid_mask if segment == "valid" else dates >= test_start, index=normalized.index
        )
        target = actual.reindex(prediction.index).where(permitted.reindex(prediction.index))
        mask = prediction.notna() & target.notna()
        metrics[f"{segment}_mse"] = (
            float(np.mean((prediction[mask] - target[mask]) ** 2)) if mask.any() else None
        )
        metrics[f"{segment}_scored_rows"] = int(mask.sum())
        for (day, instrument), value_ in prediction.items():
            predictions.append(
                {
                    "session": day.date().isoformat(),
                    "decision_time": decision_times[day.date().isoformat()],
                    "instrument_id": instrument,
                    "split": segment,
                    "prediction": float(value_) if np.isfinite(value_) else None,
                    "null_reason": None if np.isfinite(value_) else "missing_features",
                }
            )
    store = Store()
    prediction_ref = store.put_table(pa.Table.from_pylist(predictions))
    # Safe model artifact: numeric coefficients, not executable pickle/YAML.
    model_id = store.put_json(
        "qlib_ridge_model",
        {
            "implementation": "qlib.contrib.model.linear.LinearModel",
            "schema_version": "1",
            "features": list(x.columns),
            "coef": model.coef_.tolist(),
            "intercept": float(model.intercept_),
            "mean": mean.to_dict(),
            "scale": scale.to_dict(),
            "request": request.model_dump(mode="json"),
            "splits": splits,
            "dataset_id": factor.dataset_id,
        },
    )
    # Verify trusted identity before inspecting or reconstructing any saved model.
    store.get_json(model_id, "qlib_ridge_model")
    manifest_id = record_adapter(
        "qlib",
        request,
        {"factors": request.factor_artifact_id, "labels": request.label_artifact_id},
        {
            "predictions": prediction_ref.model_dump(mode="json"),
            "model_id": model_id,
            "metrics": metrics,
        },
        dataset.source.model_dump(mode="json"),
        splits,
    )
    return QlibResult(
        manifest_id=manifest_id,
        model_id=model_id,
        predictions=prediction_ref,
        metrics=metrics,
        splits=splits,
    )

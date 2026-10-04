"""Independent timing, losses and actual Alphalens integration."""

import pytest
from openbb_alphalens import AlphalensFetcher
from openbb_core.provider.standard_models.alpha_research import DatasetInput, LabelRequest
from openbb_polars_ta import PolarsFetcher

from openbb_alpha.datasets import register
from openbb_alpha.fixture import synthetic
from openbb_alpha.labels import labels, load_labels
from openbb_alpha.results import load_factors
from openbb_alpha.store import Store


def artifacts(data=None):
    ref = register(data or synthetic(10, 80))
    factor = PolarsFetcher.extract_data(
        PolarsFetcher.transform_query(
            {
                "request": {
                    "dataset_id": ref.id,
                    "factors": ["reversal_5"],
                }
            }
        ),
        None,
    )
    label = labels(LabelRequest(dataset_id=ref.id))
    return factor.artifact_id, label["artifact_id"]


def evaluate(factor, label, **kwargs):
    return AlphalensFetcher.extract_data(
        AlphalensFetcher.transform_query(
            {
                "request": {
                    "factor_artifact_id": factor,
                    "label_artifact_id": label,
                    **kwargs,
                }
            }
        ),
        None,
    ).evaluation


def test_next_open_timing_and_tail():
    data = synthetic(1, 30).model_dump(mode="json")
    for i, row in enumerate(data["rows"]):
        row["open"] = 100 + i
    ref = register(DatasetInput.model_validate(data))
    result = labels(LabelRequest(dataset_id=ref.id))
    _, panel = load_labels(result["artifact_id"])
    rows = panel.to_pylist()
    for h in (1, 5, 20):
        row = next(
            r for r in rows if r["session"] == data["calendar"][0]["session"] and r["horizon"] == h
        )
        assert row["value"] == pytest.approx((101 + h) / 101 - 1)
        assert row["entry_time"] == data["calendar"][1]["open_time"].replace("Z", "+00:00")
        assert row["exit_time"] == data["calendar"][1 + h]["open_time"].replace("Z", "+00:00")
        assert sum(r["null_reason"] == "sample_tail" for r in rows if r["horizon"] == h) == h + 1


def test_exact_join_counts():
    f, label_id = artifacts()
    result = evaluate(f, label_id)
    assert result.engine == "alphalens"
    assert len(result.metrics) == 3
    for c in result.coverage:
        assert c["input_rows"] == c["joined_rows"] == 800
        assert c["used_rows"] == (80 - 5 - c["horizon_sessions"] - 1) * 10
        assert c["unexpected_loss"] == 0
        assert sum(c["drop_reasons"].values()) == 800
    assert result.configuration["future_return_filter"] is None
    assert all("2023" in d["session"] for m in result.metrics for d in m["daily_ic"])


def test_unexpected_loss_fails_by_default():
    data = synthetic(10, 80).model_dump(mode="json")
    data["rows"][30]["open"] = None
    f, label_id = artifacts(DatasetInput.model_validate(data))
    with pytest.raises(ValueError, match="unexpected evaluation loss"):
        evaluate(f, label_id)
    result = evaluate(f, label_id, max_unexpected_loss=0.05)
    assert sum(c["unexpected_loss"] for c in result.coverage) > 0


def test_hand_checked_perfect_ic_and_ties():
    f, label_id = artifacts()
    factor, table = load_factors(f)
    label, label_table = load_labels(label_id)
    # Fixed, identified fixtures: strictly increasing cross-sectional scores and
    # identical ordering in labels give Spearman IC=1. No fitted transformation.
    import pyarrow as pa

    values = table.to_pylist()
    for row in values:
        if row["value"] is not None:
            row["value"] = int(row["instrument_id"].split(":")[1])
    label_values = label_table.to_pylist()
    for row in label_values:
        if row["value"] is not None:
            row["value"] = int(row["instrument_id"].split(":")[1]) / 100
    store = Store()
    factor = factor.model_copy(
        update={"panel": store.put_table(pa.Table.from_pylist(values, schema=table.schema))}
    )
    label = label.model_copy(
        update={
            "panel": store.put_table(pa.Table.from_pylist(label_values, schema=label_table.schema))
        }
    )
    f = store.put_json("factors", factor.model_dump(mode="json"))
    label_id = store.put_json("labels", label.model_dump(mode="json"))
    result = evaluate(f, label_id)
    assert all(m["ic_mean"] == pytest.approx(1) for m in result.metrics)
    assert result.metrics[0]["quantile_spread"] == pytest.approx(0.08)
    for row in values:
        if row["value"] is not None:
            row["value"] = 1
    factor = factor.model_copy(
        update={"panel": store.put_table(pa.Table.from_pylist(values, schema=table.schema))}
    )
    f = store.put_json("factors", factor.model_dump(mode="json"))
    result = evaluate(f, label_id)
    assert all(m["ic_mean"] is None for m in result.metrics)
    assert any("tied" in w for w in result.warnings)


def test_labels_refuse_real_unsupported_conventions():
    data = synthetic().model_dump(mode="json")
    data.update(price_convention="verified_total_return_index", open_convention="unsupported")
    ref = register(DatasetInput.model_validate(data))
    with pytest.raises(ValueError, match="synthetic_no_actions"):
        labels(LabelRequest(dataset_id=ref.id))

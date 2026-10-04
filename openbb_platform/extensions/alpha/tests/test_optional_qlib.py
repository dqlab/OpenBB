"""Real Qlib fit/predict, chronological purging and held-out label independence."""

import pyarrow as pa
import pytest
from openbb_core.provider.standard_models.alpha_research import LabelRequest, QlibRequest
from openbb_polars_ta import PolarsFetcher

from openbb_alpha.catalog import catalog
from openbb_alpha.datasets import register
from openbb_alpha.fixture import synthetic
from openbb_alpha.labels import labels, load_labels
from openbb_alpha.store import Store


def test_real_qlib_purging_and_test_label_isolation():
    pytest.importorskip("qlib")
    from openbb_qlib import train

    data = synthetic(10, 320)
    ref = register(data)
    f = PolarsFetcher.extract_data(
        PolarsFetcher.transform_query(
            {
                "request": {
                    "dataset_id": ref.id,
                    "factors": [s.id for s in catalog()],
                }
            }
        ),
        None,
    )
    label_id = labels(LabelRequest(dataset_id=ref.id))
    request = QlibRequest(
        factor_artifact_id=f.artifact_id,
        label_artifact_id=label_id["artifact_id"],
        train_end=data.calendar[270].session,
        validation_end=data.calendar[290].session,
    )
    first = train(request)
    assert first.predictions.rows == 490
    assert first.splits["purged_train_rows"] == 60
    assert first.splits["train_rows"] == 130
    assert first.metrics["test_scored_rows"] == 230
    store = Store()
    saved = store.get_json(first.model_id, "qlib_ridge_model")
    label, table = load_labels(label_id["artifact_id"])
    rows = table.to_pylist()
    for row in rows:
        if row["session"] > str(request.train_end) and row["value"] is not None:
            row["value"] += 999
    label = label.model_copy(
        update={"panel": store.put_table(pa.Table.from_pylist(rows, schema=table.schema))}
    )
    changed = store.put_json("labels", label.model_dump(mode="json"))
    second = train(request.model_copy(update={"label_artifact_id": changed}))
    reloaded = store.get_json(second.model_id, "qlib_ridge_model")
    assert saved["coef"] == reloaded["coef"]
    assert saved["mean"] == reloaded["mean"]
    assert saved["scale"] == reloaded["scale"]
    assert first.predictions.id == second.predictions.id

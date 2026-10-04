"""Verify a real minimal installation without any specialist libraries."""

from importlib.util import find_spec

from openbb import obb
from openbb_alpha.fixture import synthetic

for name in ("polars", "polars_ta", "alphalens", "qlib", "bt"):
    if find_spec(name) is not None:
        raise RuntimeError(f"Minimal environment unexpectedly contains {name}")
dataset = obb.alpha.register(dataset=synthetic(2, 30)).results
result = obb.alpha.compute(
    request={"dataset_id": dataset["id"], "factors": ["reversal_5"]},
    provider="alpha_reference",
)
if result.results.artifact.panel.rows != 60:
    raise RuntimeError("Minimal reference computation failed")
for method, request in (
    (
        obb.research.qlib_train,
        {
            "factor_artifact_id": "0" * 64,
            "label_artifact_id": "0" * 64,
            "train_end": "2023-02-01",
            "validation_end": "2023-03-01",
        },
    ),
    (obb.research.bt_backtest, {"factor_artifact_id": "0" * 64, "factor_id": "reversal_5"}),
):
    try:
        method(request=request)
    except Exception as exc:
        if "Install the separate openbb-" not in str(exc):
            raise
    else:
        raise RuntimeError("Absent optional dependency unexpectedly succeeded")
print("Minimal environment: reference compute passed; specialist libraries absent; actionable errors")  # noqa: T201

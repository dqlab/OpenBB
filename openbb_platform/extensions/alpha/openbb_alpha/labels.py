"""Independent next-open labels; never called by a factor calculation engine."""

import math
from collections import defaultdict

from openbb_core.provider.standard_models.alpha_research import LabelArtifact, LabelRequest

from openbb_alpha.datasets import load
from openbb_alpha.store import Store


def labels(request: LabelRequest):
    import pyarrow as pa

    dataset, table = load(request.dataset_id)
    if dataset.open_convention != "synthetic_no_actions":
        raise ValueError("v1 next-open labels support synthetic_no_actions only")
    if len(set(request.spec.horizons)) != len(request.spec.horizons):
        raise ValueError("duplicate label horizon")
    grouped = defaultdict(list)
    for row in table.to_pylist():
        grouped[row["instrument_id"]].append(row)
    records = []
    for instrument, rows in grouped.items():
        for t, signal in enumerate(rows):
            for h in request.spec.horizons:
                entry = rows[t + 1] if t + 1 < len(rows) else None
                exit_ = rows[t + 1 + h] if t + 1 + h < len(rows) else None
                reason, value = None, None
                if not signal["eligible"]:
                    reason = "ineligible"
                elif entry is None or exit_ is None:
                    reason = "sample_tail"
                elif not entry["present"] or not exit_["present"]:
                    reason = "missing_endpoint"
                elif entry["open"] is None or exit_["open"] is None:
                    reason = "missing_endpoint"
                elif entry["open"] <= 0 or exit_["open"] <= 0:
                    reason = "invalid_endpoint"
                else:
                    value = exit_["open"] / entry["open"] - 1
                    if not math.isfinite(value):
                        value, reason = None, "nonfinite_label"
                records.append(
                    {
                        "instrument_id": instrument,
                        "session": signal["session"],
                        "signal_time": signal["decision_time"],
                        "eligible": signal["eligible"],
                        "entry_time": entry["open_time"] if entry else None,
                        "exit_time": exit_["open_time"] if exit_ else None,
                        "available_at": max(entry["available_at"], exit_["available_at"])
                        if value is not None
                        else None,
                        "horizon": h,
                        "value": value,
                        "null_reason": reason,
                    }
                )
    schema = pa.schema(
        [
            ("instrument_id", pa.string()),
            ("session", pa.string()),
            ("signal_time", pa.string()),
            ("eligible", pa.bool_()),
            ("entry_time", pa.string()),
            ("exit_time", pa.string()),
            ("available_at", pa.string()),
            ("horizon", pa.int64()),
            ("value", pa.float64()),
            ("null_reason", pa.string()),
        ]
    )
    store = Store()
    artifact = LabelArtifact(
        dataset_id=dataset.id,
        spec=request.spec,
        panel=store.put_table(pa.Table.from_pylist(records, schema=schema)),
    )
    key = store.put_json("labels", artifact.model_dump(mode="json"))
    return {"artifact_id": key, "artifact": artifact.model_dump(mode="json")}


def load_labels(key):
    store = Store()
    artifact = LabelArtifact.model_validate(store.get_json(key, "labels"))
    return artifact, store.table(artifact.panel)

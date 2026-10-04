"""Real-library parity, no lookahead, calendar windows and instrument isolation."""

import pytest
from openbb_alpha_reference import ReferenceFetcher
from openbb_core.provider.standard_models.alpha_research import DatasetInput
from openbb_polars_ta import PolarsFetcher

from openbb_alpha.catalog import catalog
from openbb_alpha.datasets import register
from openbb_alpha.fixture import synthetic
from openbb_alpha.results import load_factors


def run(data, provider, transform="centered_rank"):
    dataset = register(data)
    query = provider.transform_query(
        {
            "request": {
                "dataset_id": dataset.id,
                "factors": [s.id for s in catalog()],
                "transform": transform,
            }
        }
    )
    result = provider.extract_data(query, None)
    return load_factors(result.artifact_id)[1].to_pylist()


def reconcile(left, right):
    assert len(left) == len(right)
    for a, b in zip(left, right, strict=True):
        for key in a:
            if key in ("value", "rank_value") and a[key] is not None:
                assert b[key] == pytest.approx(a[key], abs=1e-10, rel=1e-9), (key, a, b)
            else:
                assert a[key] == b[key], (key, a, b)


@pytest.mark.parametrize("edge", ["normal", "missing", "zero", "invalid", "late", "ties", "ipo"])
def test_parity(edge):
    raw = synthetic(10, 310).model_dump(mode="json")
    if edge == "missing":
        raw["rows"].pop(260)
        raw["rows"][280]["price"] = raw["rows"][280]["raw_close"] = None
    elif edge == "zero":
        for row in raw["rows"][:310]:
            row["raw_share_volume"] = 0
    elif edge == "invalid":
        raw["rows"][260]["price"] = raw["rows"][260]["raw_close"] = -1
        raw["rows"][275]["raw_share_volume"] = -2
    elif edge == "late":
        raw["rows"][260]["available_at"] = raw["calendar"][270]["decision_time"]
    elif edge == "ties":
        for row in raw["rows"]:
            row["price"] = row["raw_close"] = 100
    elif edge == "ipo":
        for row in raw["rows"][:250]:
            row["price"] = row["raw_close"] = None
            row["eligible"] = False
    data = DatasetInput.model_validate(raw)
    reconcile(run(data, ReferenceFetcher), run(data, PolarsFetcher))


@pytest.mark.parametrize("provider", [ReferenceFetcher, PolarsFetcher])
def test_future_perturbation_suffix_and_asset_isolation(provider):
    raw = synthetic(3, 320).model_dump(mode="json")
    cutoff = raw["calendar"][280]["session"]
    baseline = run(DatasetInput.model_validate(raw), provider, "raw")
    for row in raw["rows"]:
        if row["session"] > cutoff or row["instrument_id"] == "SYN:002":
            row["price"] *= 3
            row["raw_close"] *= 3
    changed = run(DatasetInput.model_validate(raw), provider, "raw")

    def select(rows):
        return [r for r in rows if r["session"] <= cutoff and r["instrument_id"] != "SYN:002"]

    reconcile(select(baseline), select(changed))
    raw["calendar"] = raw["calendar"][:281]
    raw["rows"] = [r for r in raw["rows"] if r["session"] <= cutoff]
    short = run(DatasetInput.model_validate(raw), provider, "raw")
    reconcile(select(baseline), select(short))


def test_warmed_chunks_and_rank_boundaries():
    raw = synthetic(3, 340).model_dump(mode="json")
    full = run(DatasetInput.model_validate(raw), PolarsFetcher)
    # Preserve all assets and 252 prior sessions at the chunk boundary.
    first = 300 - 252
    raw["calendar"] = raw["calendar"][first:]
    start = raw["calendar"][0]["session"]
    raw["rows"] = [r for r in raw["rows"] if r["session"] >= start]
    chunk = run(DatasetInput.model_validate(raw), PolarsFetcher)
    cutoff = raw["calendar"][252]["session"]
    reconcile(
        [r for r in full if r["session"] >= cutoff], [r for r in chunk if r["session"] >= cutoff]
    )

"""Hand-checkable factor and immutable dataset acceptance tests."""

import math

import pytest
from openbb_alpha_reference import ReferenceFetcher
from openbb_core.provider.standard_models.alpha_research import DatasetInput, ResearchDatasetRef
from pydantic import ValidationError

from openbb_alpha.catalog import catalog
from openbb_alpha.datasets import load, register
from openbb_alpha.fixture import synthetic
from openbb_alpha.results import load_factors
from openbb_alpha.store import Store


def compute(data, factors=None, transform="raw"):
    dataset = register(data)
    query = ReferenceFetcher.transform_query(
        {
            "request": {
                "dataset_id": dataset.id,
                "factors": factors or [s.id for s in catalog()],
                "transform": transform,
            }
        }
    )
    result = ReferenceFetcher.extract_data(query, None)
    return dataset, load_factors(result.artifact_id)[1].to_pylist()


def test_roundtrip_identity_and_zero():
    data = synthetic(2, 30)
    assert DatasetInput.model_validate_json(data.model_dump_json()) == data
    ref = register(data)
    assert ResearchDatasetRef.model_validate_json(ref.model_dump_json()) == ref
    assert register(data).id == ref.id
    _, table = load(ref.id)
    assert table.to_pylist()[0]["raw_share_volume"] == 0
    assert ref.source.access_adapter == "in_memory_fixture"
    assert ref.classification == "Lab"


@pytest.mark.parametrize(
    "mutation", ["duplicate", "production", "nonfinite", "unknown", "calendar"]
)
def test_invalid_contract(mutation):
    data = synthetic(2, 30).model_dump(mode="json")
    if mutation == "duplicate":
        data["rows"].append(data["rows"][0])
    elif mutation == "production":
        data["classification"] = "Production"
    elif mutation == "nonfinite":
        data["rows"][0]["price"] = float("nan")
    elif mutation == "unknown":
        data["rows"][0]["instrument_id"] = "not-canonical"
    else:
        data["calendar"].append(data["calendar"][0])
    with pytest.raises(ValidationError):
        DatasetInput.model_validate(data)


def test_golden_six_factors():
    data = synthetic(2, 270).model_dump(mode="json")
    for row in data["rows"]:
        t = next(i for i, s in enumerate(data["calendar"]) if s["session"] == row["session"])
        row.update(price=math.exp(t / 1000), raw_close=math.exp(t / 1000), raw_share_volume=10)
    _, rows = compute(DatasetInput.model_validate(data))
    last = {
        r["factor_id"]: r
        for r in rows
        if r["instrument_id"] == "SYN:000" and r["session"] == data["calendar"][-1]["session"]
    }
    for lookback in (63, 126, 252):
        assert last[f"momentum_{lookback}_21"]["value"] == pytest.approx(
            math.exp((lookback - 21) / 1000) - 1
        )
    assert last["reversal_5"]["value"] == pytest.approx(-(math.exp(0.005) - 1))
    assert last["negative_realized_volatility_20"]["value"] == pytest.approx(0, abs=1e-12)
    assert last["log_dollar_volume_20"]["value"] == pytest.approx(
        math.log(sum(10 * math.exp(t / 1000) for t in range(250, 270)) / 20)
    )


def test_missing_calendar_slot_is_not_compressed():
    data = synthetic(2, 80).model_dump(mode="json")
    data["rows"] = [
        r
        for r in data["rows"]
        if not (r["instrument_id"] == "SYN:000" and r["session"] == data["calendar"][20]["session"])
    ]
    _, rows = compute(DatasetInput.model_validate(data), ["momentum_63_21"])
    row = next(
        r
        for r in rows
        if r["session"] == data["calendar"][63]["session"] and r["instrument_id"] == "SYN:000"
    )
    assert row["value"] is None and row["null_reason"] == "missing_session"
    assert len(rows) == 160


def test_rank_ties_and_singleton():
    data = synthetic(2, 30).model_dump(mode="json")
    for row in data["rows"]:
        row.update(price=10, raw_close=10)
    _, rows = compute(DatasetInput.model_validate(data), ["reversal_5"], "centered_rank")
    assert {r["rank_value"] for r in rows if r["value"] is not None} == {0}
    _, rows = compute(synthetic(1, 30), ["reversal_5"], "centered_rank")
    assert all(r["rank_value"] is None for r in rows)
    assert rows[-1]["rank_null_reason"] == "insufficient_cross_section"


def test_traversal_corruption_symlinks(private_store, tmp_path):
    store = Store()
    with pytest.raises(ValueError, match="ID"):
        store.read("../outside")
    key = store.put(b"original")
    (private_store / key).write_bytes(b"corrupted")
    with pytest.raises(ValueError, match="integrity"):
        store.read(key)
    outside = tmp_path / "outside"
    outside.write_bytes(b"secret")
    link = private_store / ("a" * 64)
    link.symlink_to(outside)
    with pytest.raises(ValueError, match="unsafe"):
        store.read(link.name)


def test_no_formula_execution_or_fallback():
    ref = register(synthetic(2, 30))
    query = ReferenceFetcher.transform_query(
        {
            "request": {
                "dataset_id": ref.id,
                "factors": ["__import__('os')"],
            }
        }
    )
    with pytest.raises(ValueError, match="unsupported"):
        ReferenceFetcher.extract_data(query, None)

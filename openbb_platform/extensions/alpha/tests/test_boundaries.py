"""Saved native DQ mapping, eligibility and trusted artifact boundaries."""

import pytest
from openbb_core.provider.standard_models.alpha_research import DatasetInput
from openbb_polars_ta import PolarsFetcher

from openbb_alpha.datasets import from_dq_history, load, register
from openbb_alpha.fixture import synthetic
from openbb_alpha.store import MAX_BYTES, Store


def dq_input():
    fixture = synthetic(2, 30).model_dump(mode="json")
    rows, eligibility = [], {}
    sessions = {s["session"]: s for s in fixture["calendar"]}
    for row in fixture.pop("rows"):
        eligibility[(row["instrument_id"], row["session"])] = row["eligible"]
        rows.append(
            {
                "instrument_uid": row["instrument_id"],
                "bar_start": sessions[row["session"]]["open_time"],
                "first_published_at": row["available_at"],
                "kind": "bars",
                "frequency": "1d",
                "close": row["raw_close"],
                "open": row["open"],
                "volume": row["raw_share_volume"],
                "volume_unit": "shares",
                "currency": "USD",
                "adjustment": "unadjusted",
            }
        )
    fixture.update(price_convention="raw_close_only", open_convention="unsupported")
    payload = {
        "data": rows,
        "meta": {
            "publication_id": "pinned-publication",
            "next_cursor": None,
            "revision_mode": "latest",
            "availability_basis": "central",
        },
    }
    return payload, fixture, eligibility


def test_saved_dq_native_schema_no_collection():
    payload, metadata, eligibility = dq_input()
    ref = from_dq_history(payload, metadata=metadata, eligibility=eligibility, price_field=None)
    assert ref.source.snapshot == "pinned-publication"
    assert ref.source.access_adapter == "dq_quant_data"
    _, panel = load(ref.id)
    assert panel["raw_share_volume"][0].as_py() == 0
    assert panel["price"][0].as_py() is None
    query = PolarsFetcher.transform_query(
        {
            "request": {
                "dataset_id": ref.id,
                "factors": ["log_dollar_volume_20"],
            }
        }
    )
    assert PolarsFetcher.extract_data(query, None).artifact.source == ref.source
    query = PolarsFetcher.transform_query(
        {
            "request": {
                "dataset_id": ref.id,
                "factors": ["reversal_5"],
            }
        }
    )
    with pytest.raises(ValueError, match="consistent return index"):
        PolarsFetcher.extract_data(query, None)


@pytest.mark.parametrize("problem", ["pagination", "adjusted", "intraday", "identity", "units"])
def test_dq_fail_closed(problem):
    payload, metadata, eligibility = dq_input()
    if problem == "pagination":
        payload["meta"]["next_cursor"] = "more"
    elif problem == "adjusted":
        payload["data"][0]["adjustment"] = "dividend_adjusted"
    elif problem == "intraday":
        payload["data"][0]["frequency"] = "1m"
    elif problem == "identity":
        payload["meta"]["publication_id"] = None
    else:
        payload["data"][0]["volume_unit"] = "lots"
    with pytest.raises(ValueError):
        from_dq_history(payload, metadata=metadata, eligibility=eligibility, price_field=None)


def test_store_bounds_and_principal_isolation(private_store, tmp_path, monkeypatch):
    first = Store()
    key = first.put_json("test", {"value": 1})
    with pytest.raises(ValueError, match="64 MiB"):
        first.put(b"x" * (MAX_BYTES + 1))
    second = tmp_path / "second-principal"
    second.mkdir(mode=0o700)
    monkeypatch.setenv("OPENBB_ALPHA_ROOT", str(second))
    with pytest.raises(ValueError, match="unavailable"):
        Store().read(key)
    second.chmod(0o755)
    with pytest.raises(ValueError, match="0700"):
        Store()


def test_ranks_use_only_current_eligible_universe():
    from openbb_alpha.results import load_factors

    raw = synthetic(3, 30).model_dump(mode="json")
    for row in raw["rows"]:
        if row["instrument_id"] == "SYN:002":
            row["eligible"] = False
    ref = register(DatasetInput.model_validate(raw))
    result = PolarsFetcher.extract_data(
        PolarsFetcher.transform_query(
            {
                "request": {
                    "dataset_id": ref.id,
                    "factors": ["reversal_5"],
                    "transform": "centered_rank",
                }
            }
        ),
        None,
    )
    _, table = load_factors(result.artifact_id)
    rows = [r for r in table.to_pylist() if r["session"] == raw["calendar"][-1]["session"]]
    assert sorted(r["rank_value"] for r in rows if r["eligible"]) == [-0.5, 0.5]
    assert next(r for r in rows if not r["eligible"])["rank_value"] is None

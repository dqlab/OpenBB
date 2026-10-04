"""Generated Python/REST parity and complete offline research pipeline."""

import json

import pytest

from openbb_alpha.fixture import synthetic


def test_python_rest_and_report():
    from fastapi.testclient import TestClient
    from openbb import obb
    from openbb_core.api.rest_api import app

    raw = synthetic(10, 300).model_dump(mode="json")
    raw["source"]["vendor"] = "<script>alert('vendor')</script>"
    dataset = obb.alpha.register(dataset=raw).results
    request = {
        "dataset_id": dataset["id"],
        "factors": [s["id"] for s in obb.alpha.catalog().results],
    }
    client = TestClient(app)
    schema = client.get("/openapi.json").json()
    command = schema["paths"]["/api/v1/alpha/compute"]["post"]
    assert "requestBody" in command
    assert "alpha_reference" in json.dumps(command) and "polars_ta" in json.dumps(command)
    for engine in ("alpha_reference", "polars_ta"):
        py = obb.alpha.compute(request=request, provider=engine).results
        api = client.post("/api/v1/alpha/compute", params={"provider": engine}, json=request)
        assert api.status_code == 200, api.text
        assert api.json()["results"]["artifact_id"] == py.artifact_id
        assert py.artifact.panel.rows == 18000 and len(py.artifact.panel.preview) == 10
    labels = obb.alpha.labels(request={"dataset_id": dataset["id"]}).results
    evaluated = obb.alpha.evaluate(
        request={"factor_artifact_id": py.artifact_id, "label_artifact_id": labels["artifact_id"]},
        provider="alphalens",
    ).results
    assert len(evaluated.evaluation.metrics) == 18
    run = obb.research.run(
        request={"compute": request, "calculation_provider": "polars_ta"}
    ).results
    assert run["manifest"]["status"] == "complete", run
    assert run["manifest"]["outputs"]["evaluation"] == evaluated.artifact_id
    html = obb.research.report(manifest_id=run["manifest_id"]).results["html"]
    assert "<!doctype html>" in html
    assert "<script>" not in html and "&lt;script&gt;" in html
    assert 'src="http' not in html
    assert obb.research.result(manifest_id=run["manifest_id"]).results == run["manifest"]
    assert obb.research.runs().results["total"] == 1
    result = client.get("/api/v1/research/result", params={"manifest_id": run["manifest_id"]})
    assert result.status_code == 200 and result.json()["results"] == run["manifest"]
    response = client.post(
        "/api/v1/alpha/compute",
        params={"provider": "polars_ta"},
        json={**request, "purpose": "Production"},
    )
    assert response.status_code == 422


def test_failed_run_is_recorded():
    from openbb import obb

    result = obb.research.run(
        request={
            "compute": {
                "dataset_id": "0" * 64,
                "factors": ["reversal_5"],
            }
        }
    ).results
    assert result["manifest"]["status"] == "failed"
    assert result["manifest"]["errors"]
    assert obb.research.runs().results["runs"][0]["status"] == "failed"


def test_python_production_policy():
    from openbb import obb

    raw = synthetic().model_dump(mode="json")
    raw["classification"] = "Production"
    with pytest.raises(Exception, match="Lab"):
        obb.alpha.register(dataset=raw)


def test_optional_python_and_rest_commands():
    pytest.importorskip("qlib")
    pytest.importorskip("bt")
    from fastapi.testclient import TestClient
    from openbb import obb
    from openbb_core.api.rest_api import app

    data = synthetic(10, 50)
    dataset = obb.alpha.register(dataset=data).results
    factor = obb.alpha.compute(
        request={"dataset_id": dataset["id"], "factors": ["reversal_5"]}, provider="polars_ta"
    ).results
    label = obb.alpha.labels(request={"dataset_id": dataset["id"]}).results
    qrequest = {
        "factor_artifact_id": factor.artifact_id,
        "label_artifact_id": label["artifact_id"],
        "horizon": 1,
        "train_end": str(data.calendar[24].session),
        "validation_end": str(data.calendar[34].session),
    }
    trained = obb.research.qlib_train(request=qrequest).results
    client = TestClient(app)
    response = client.post("/api/v1/research/qlib_train", json=qrequest)
    assert response.status_code == 200, response.text
    assert response.json()["results"]["predictions"]["id"] == trained["predictions"]["id"]
    brequest = {"factor_artifact_id": factor.artifact_id, "factor_id": "reversal_5", "top_k": 2}
    simulated = obb.research.bt_backtest(request=brequest).results
    response = client.post("/api/v1/research/bt_backtest", json=brequest)
    assert response.status_code == 200, response.text
    assert response.json()["results"]["trades"]["id"] == simulated["trades"]["id"]

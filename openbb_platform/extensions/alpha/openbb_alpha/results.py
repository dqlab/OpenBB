"""Common artifact boundary; engine calculations remain independent."""

from openbb_core.provider.standard_models.alpha_research import AlphaComputeData, FactorArtifact

from openbb_alpha.store import Store, digest


def save_factors(request, dataset, table, specs, engine, engine_version):
    store = Store()
    artifact = FactorArtifact(
        dataset_id=dataset.id,
        panel=store.put_table(table),
        engine=engine,
        engine_version=engine_version,
        spec_hash=digest(
            {
                "specs": [s.model_dump(mode="json") for s in specs],
                "transform": request.transform,
                "transform_version": 1,
            }
        ),
        factors=specs,
        transform=request.transform,
        source=dataset.source,
    )
    key = store.put_json("factors", artifact.model_dump(mode="json"))
    return AlphaComputeData(artifact_id=key, artifact=artifact)


def load_factors(key):
    store = Store()
    artifact = FactorArtifact.model_validate(store.get_json(key, "factors"))
    return artifact, store.table(artifact.panel)

"""Content-addressed run records, explicit failures and native provider execution."""

import hashlib
import os
import subprocess
from datetime import UTC, datetime
from importlib.metadata import distributions
from pathlib import Path
from uuid import uuid4

from openbb_alpha.datasets import load
from openbb_alpha.labels import labels
from openbb_alpha.store import Store
from openbb_core.provider.query_executor import QueryExecutor
from openbb_core.provider.standard_models.alpha_research import (
    LabelRequest,
    ResearchRunManifest,
    RunRequest,
)


def provenance():
    packages = {
        d.metadata["Name"]: d.version
        for d in distributions()
        if d.metadata["Name"].lower().startswith(("openbb", "polars", "alphalens"))
        or d.metadata["Name"].lower() in ("pandas", "numpy", "pyarrow", "pyqlib", "bt")
    }
    try:
        location = Path(__file__).parent
        root = (
            subprocess.check_output(
                ["git", "rev-parse", "--show-toplevel"],
                cwd=location,
                stderr=subprocess.DEVNULL,
                timeout=10,
            )
            .decode()
            .strip()
        )
        revision = (
            subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, timeout=10)
            .decode()
            .strip()
        )
        dirty = subprocess.check_output(["git", "status", "--porcelain"], cwd=root, timeout=10)
        # Hash actual relevant source, including untracked implementation files;
        # no raw diffs, environment values, credentials or private paths are saved.
        scopes = [
            "openbb_platform/extensions/alpha",
            "openbb_platform/extensions/research",
            "openbb_platform/providers/alpha_reference",
            "openbb_platform/providers/polars_ta",
            "openbb_platform/providers/alphalens",
            "openbb_platform/providers/qlib",
            "openbb_platform/providers/bt",
            "openbb_platform/core/openbb_core",
        ]
        hasher = hashlib.sha256()
        for scope in scopes:
            for path in sorted((Path(root) / scope).rglob("*")):
                if path.is_file() and path.suffix in (".py", ".toml") and not path.is_symlink():
                    hasher.update(str(path.relative_to(root)).encode())
                    hasher.update(path.read_bytes())
        code = {
            "revision": revision,
            "worktree_dirty": bool(dirty),
            "source_fingerprint": hasher.hexdigest(),
            "fingerprint_scope": scopes,
            "warning": "Dirty source must be retained with this fingerprint for reproduction"
            if dirty
            else None,
        }
    except (OSError, subprocess.SubprocessError):
        code = {
            "revision": None,
            "source_fingerprint": None,
            "warning": "Source checkout unavailable: code revision is not reproducible",
        }
    return code, packages


async def run(request: RunRequest):
    store = Store()
    code, packages = provenance()
    outputs, errors, source = {}, (), {}
    stage = "dataset"
    try:
        dataset, _ = load(request.compute.dataset_id)
        source = dataset.source.model_dump(mode="json")
        stage = "factors"
        factors = await QueryExecutor().execute(
            request.calculation_provider,
            "AlphaCompute",
            {"request": request.compute.model_dump(mode="json")},
        )
        outputs["factors"] = factors.artifact_id
        stage = "labels"
        label = labels(LabelRequest(dataset_id=dataset.id, spec=request.labels))
        outputs["labels"] = label["artifact_id"]
        stage = "evaluation"
        evaluation = await QueryExecutor().execute(
            "alphalens",
            "AlphaEvaluate",
            {
                "request": {
                    "factor_artifact_id": factors.artifact_id,
                    "label_artifact_id": label["artifact_id"],
                    "quantiles": request.quantiles,
                    "max_unexpected_loss": request.max_unexpected_loss,
                }
            },
        )
        outputs["evaluation"] = evaluation.artifact_id
        status = "complete"
    except Exception as exc:  # Persist failure without serializing private exception details.
        status = "failed"
        errors = (f"{stage}: {type(exc).__name__}; verify inputs and installed capabilities",)
    manifest = ResearchRunManifest(
        run_id=uuid4().hex,
        created_at=datetime.now(UTC),
        status=status,
        inputs={"dataset_id": request.compute.dataset_id},
        code=code,
        packages=packages,
        parameters=request.model_dump(mode="json"),
        random_seeds=(request.seed,),
        splits={},
        outputs=outputs,
        provenance={
            "market_data": source,
            "calculation_provider": request.calculation_provider,
            "label_engine": "independent_next_open_v1",
        },
        errors=errors,
    )
    key = store.put_json("run", manifest.model_dump(mode="json"))
    return {"manifest_id": key, "manifest": manifest.model_dump(mode="json")}


def result(manifest_id):
    value = Store().get_json(manifest_id, "run")
    return ResearchRunManifest.model_validate(value).model_dump(mode="json")


def record_adapter(adapter, request, inputs, outputs, source, splits=None):
    """Record successful optional workflows using the same run contract."""
    code, packages = provenance()
    manifest = ResearchRunManifest(
        run_id=uuid4().hex,
        created_at=datetime.now(UTC),
        status="complete",
        inputs=inputs,
        code=code,
        packages=packages,
        parameters={"adapter": adapter, **request.model_dump(mode="json")},
        random_seeds=(getattr(request, "seed", 0),),
        splits=splits or {},
        outputs=outputs,
        provenance={"market_data": source, "workflow_adapter": adapter},
    )
    return Store().put_json("run", manifest.model_dump(mode="json"))


def runs(limit=20):
    if not 1 <= limit <= 100:
        raise ValueError("limit must be between 1 and 100")
    store = Store()
    manifests = []
    with os.scandir(store.root) as entries:
        for count, entry in enumerate(entries):
            if count >= 10000:
                raise ValueError("store scan exceeds 10000 artifacts; use an exact manifest ID")
            if entry.name.startswith(".pending-"):
                continue
            data = store.read(entry.name)
            if not data.startswith(b'{"kind":"run",'):
                continue
            manifest = result(entry.name)
            manifests.append(
                {
                    "manifest_id": entry.name,
                    "run_id": manifest["run_id"],
                    "created_at": manifest["created_at"],
                    "status": manifest["status"],
                }
            )
    manifests.sort(key=lambda m: (m["created_at"], m["run_id"]), reverse=True)
    return {"runs": manifests[:limit], "total": len(manifests), "has_more": len(manifests) > limit}

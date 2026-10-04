"""Offline demonstration through actual generated OpenBB commands."""

import argparse
import json
import os
import resource
import time
from pathlib import Path


def main():
    """Run and reconcile both engines, exporting reproducible reports."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    artifacts = args.output / "artifacts"
    artifacts.mkdir(mode=0o700, exist_ok=True)
    os.environ["OPENBB_ALPHA_ROOT"] = str(artifacts.resolve())

    from openbb import obb
    from openbb_alpha.fixture import synthetic
    from openbb_alpha.results import load_factors

    dataset = obb.alpha.register(dataset=synthetic(10, 320, seed=1729)).results
    factors = [s["id"] for s in obb.alpha.catalog().results]
    summary, tables = [], []
    for provider in ("alpha_reference", "polars_ta"):
        start = time.perf_counter()
        run = obb.research.run(
            request={
                "compute": {"dataset_id": dataset["id"], "factors": factors, "transform": "centered_rank"},
                "calculation_provider": provider,
                "seed": 1729,
            }
        ).results
        elapsed = time.perf_counter() - start
        if run["manifest"]["status"] != "complete":
            raise RuntimeError(run)
        report = obb.research.report(manifest_id=run["manifest_id"], format="html").results
        path = args.output / f"{provider}.html"
        path.write_text(report["html"], encoding="utf-8")
        (args.output / f"{provider}.manifest.json").write_text(json.dumps(run, indent=2))
        factor, table = load_factors(run["manifest"]["outputs"]["factors"])
        tables.append(table.to_pylist())
        summary.append(
            {
                "provider": provider,
                "engine_version": factor.engine_version,
                "manifest_id": run["manifest_id"],
                "report_artifact_id": report["artifact_id"],
                "report": str(path),
                "dataset_id": dataset["id"],
                "input_rows": 3200,
                "input_columns": len(dataset["panel"]["columns"]),
                "factor_rows": table.num_rows,
                "factor_columns": table.num_columns,
                "pipeline_wall_seconds": elapsed,
                "process_peak_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
                "rss_scope": "cumulative process high-water mark, includes all prior stages",
            }
        )
    import math

    if not len(tables[0]) == len(tables[1]) == 19200:
        raise RuntimeError("Unexpected factor row count")
    for reference, calculated in zip(*tables):
        for key in reference:
            if key in ("value", "rank_value") and reference[key] is not None:
                if not math.isclose(reference[key], calculated[key], rel_tol=1e-9, abs_tol=1e-10):
                    raise RuntimeError("Numerical parity failed")
            elif reference[key] != calculated[key]:
                raise RuntimeError("Metadata/null parity failed")
    (args.output / "summary.json").write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2))  # noqa: T201


if __name__ == "__main__":
    main()

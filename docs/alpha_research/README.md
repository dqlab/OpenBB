# Alpha research in the existing OpenBB V4 fork

This implementation adds `obb.alpha` and `obb.research` to the checked-out fork.
It does not replace DQ data access, collectors, databases or native market building.
All research is explicitly **Lab**. Synthetic output is not investment evidence.
See [implementation status](IMPLEMENTATION_STATUS.md) for executed checks and
[reuse audit](REUSE_AUDIT.md) for the initial checkout and protected changes.

## Install and reproduce

Run from this repository root. The constraints were resolved on CPython 3.12.3,
Linux x86_64. They pin dependencies without installing optional packages.
The system Python environment is not modified.

```bash
UV_CACHE_DIR=/tmp/openbb-alpha-uv-cache uv venv /tmp/openbb-alpha-env --python /usr/bin/python3
UV_CACHE_DIR=/tmp/openbb-alpha-uv-cache uv pip install --python /tmp/openbb-alpha-env/bin/python -r docs/alpha_research/requirements-core.txt
```

The local core, alpha/research extensions and calculation providers are editable.
`openbb-core` supplies the `openbb` import; the full PyPI `openbb` metapackage is
unnecessary. Tests use source core 1.6.13 from this OpenBB 4.7.3 checkout, not the
older system installation. No V5 migration is involved.

For isolated test settings (without altering `HOME` or existing credentials):

```bash
export PYTHONPATH="$PWD/scripts/alpha_research"
export OPENBB_ALPHA_TEST_SETTINGS=/tmp/openbb-alpha-settings
export MPLCONFIGDIR=/tmp/openbb-alpha-mpl
export MLFLOW_DISABLE_AGENT_HINT=1
/tmp/openbb-alpha-env/bin/openbb-build
/tmp/openbb-alpha-env/bin/python scripts/alpha_research/demo.py --output /tmp/openbb-alpha-demo
```

The test-only `sitecustomize.py` changes configuration paths before OpenBB loads
settings. It does not change provider discovery. Omit it for normal configured
OpenBB use. Generated interfaces reside in the editable core checkout; rebuild
after changing the installed extension set and restart the interpreter.

The demo registers 10 synthetic instruments and 320 weekday sessions, evaluates all
six factors with both providers, checks float/null/identity parity, records each
run, and exports HTML plus JSON manifests. Its weekday calendar is explicitly not
an exchange calendar. `summary.json` records versions, row/column counts, wall time
and cumulative process peak RSS. These are pipeline measurements, not isolated
engine speed claims.

For optional adapters, install either independently:

```bash
UV_CACHE_DIR=/tmp/openbb-alpha-uv-cache uv pip install --python /tmp/openbb-alpha-env/bin/python -r docs/alpha_research/requirements-qlib.txt
UV_CACHE_DIR=/tmp/openbb-alpha-uv-cache uv pip install --python /tmp/openbb-alpha-env/bin/python -r docs/alpha_research/requirements-bt.txt
/tmp/openbb-alpha-env/bin/openbb-build
```

Separate `/tmp/openbb-alpha-qlib-env` and `/tmp/openbb-alpha-bt-env` environments were
also tested with their respective requirement files. A separate minimal environment
with only core, alpha, reference and research successfully calculated factors with
all specialist libraries absent. Missing optional workflow adapters produce an
installation instruction. A calculation provider that is not installed is absent
from native provider choices; there is no fallback to another provider.

## Public commands

Set `OPENBB_ALPHA_ROOT` to an operator-owned private directory (mode 0700). No command
accepts a server file path, URL, SQL statement or executable formula.

```python
import os
os.environ["OPENBB_ALPHA_ROOT"] = "/tmp/my-alpha-artifacts"
from openbb import obb
from openbb_alpha.fixture import synthetic

snapshot = obb.alpha.register(dataset=synthetic()).results
factor_ids = [item["id"] for item in obb.alpha.catalog().results]
request = {"dataset_id": snapshot["id"], "factors": factor_ids,
           "transform": "centered_rank"}
factors = obb.alpha.compute(request=request, provider="polars_ta").results
# The identical command also accepts provider="alpha_reference".
labels = obb.alpha.labels(request={"dataset_id": snapshot["id"]}).results
evaluation = obb.alpha.evaluate(request={
    "factor_artifact_id": factors.artifact_id,
    "label_artifact_id": labels["artifact_id"],
}, provider="alphalens").results
run = obb.research.run(request={
    "compute": request, "calculation_provider": "polars_ta", "seed": 1729,
}).results
manifest = obb.research.result(manifest_id=run["manifest_id"]).results
listing = obb.research.runs(limit=20).results
report = obb.research.report(manifest_id=run["manifest_id"], format="html").results
# report["html"] is self-contained; saving/export is the caller's decision.
```

`obb.alpha.capabilities()` reports both adapter and library installation versions.
The native models are `AlphaCompute` (alpha_reference / polars_ta) and
`AlphaEvaluate` (alphalens). Their typed `request` parameters are POST bodies in
REST, for example `POST /api/v1/alpha/compute?provider=polars_ta` with the request
object itself as JSON. `alpha/register` takes the dataset object itself as its
JSON body. Research workflow POST routes also use typed bodies, except
`research/report`, whose small `manifest_id` and `format` are query parameters.
Actual TestClient requests and generated Python commands verify these interfaces.

`research.qlib_train(request=...)` accepts factor/label artifact IDs, session dates
`train_end` and `validation_end`, horizon (1/5/20), ridge alpha and seed.
`research.bt_backtest(request=...)` accepts a factor artifact ID, factor ID, top K,
rebalance interval, initial cash, fractional-share choice and cost basis points.
All optional outputs include manifests using the shared run contract.

## Calculation and timing contracts

The catalogue contains six version-1 raw factors:

| Factor | Definition | polars_ta mapping |
| --- | --- | --- |
| momentum_63_21 | P[t-21]/P[t-63]-1 | ts_delay; complete 43-price mask |
| momentum_126_21 | P[t-21]/P[t-126]-1 | ts_delay; complete 106-price mask |
| momentum_252_21 | P[t-21]/P[t-252]-1 | ts_delay; complete 232-price mask |
| reversal_5 | -(P[t]/P[t-5]-1) | ts_delay; complete 6-price mask |
| negative_realized_volatility_20 | -sqrt(252) × sample SD of 20 log returns | ts_std_dev(ddof=1, min_samples=20); 21-price mask |
| log_dollar_volume_20 | log(mean(raw_close × raw_share_volume)) for t-19..t | ts_mean(min_samples=20); raw fields only |

Polars handles sorted instrument groups, full calendar slots, masks and rank glue.
Log returns use the algebraically equivalent difference of logs to avoid intermediate
ratio overflow. The reference oracle uses only Python's standard library for
calculations; Arrow is used solely for its input/output boundary. It never imports
Polars or production factor code. The oracle/tests mentioned in the implementation
instructions were not supplied; locally implemented golden tests are distinguished
from those missing supplied tests.

Complete required windows are mandatory. Missing slots are materialized, never
compressed. Zero share volume is valid; a zero mean notional is null. Invalid,
missing, warmup and unavailable-at-close results have explicit null reasons.
The conservative availability policy requires each source observation to have
been available by its own completed-session decision. Late revisions are not
retroactively incorporated as if known earlier. Factor output availability is its
decision time. Historical eligibility is evaluated at the decision session.

Centered ranks use average tie ranks over valid eligible names at each session:
`(rank-1)/(n-1)-0.5`. With n<2 they are null; all equal values with n>=2 give zero.
Raw scores and raw null reasons remain present alongside rank outputs. No implicit
winsorization, neutralization, fill, scaling or user expression execution exists.
Parity uses float64 absolute tolerance 1e-10 / relative tolerance 1e-9; identities,
null reasons, row counts and masks must match exactly.

Labels are independent: completed t signal, entry at open t+1, exit at open t+1+h.
The 5-session label exits at t+6. No endpoint substitution or cost inclusion occurs.
Version-1 executable labels and bt explicitly support synthetic no-action prices
only. Real corporate-action/total-return execution labels are refused.

Alphalens receives already constructed labels and never recomputes forward returns
or filters them by future-return z-scores. Its frequency index represents ordinal
declared sessions and is translated back to actual session dates in outputs; this
prevents holiday/missing-session compression. Returns are simple fractional returns,
not annualized or compounded, and are not demeaned/group-adjusted. Outputs include
daily Spearman IC, IC mean/sample SD, daily-equal-weighted quantile means/spread,
rank persistence and quantile turnover. Persistence/turnover use the paired evaluation
sample. Warmup, ineligibility and sample-tail losses are counted separately from
unexpected data/join losses; the latter fail by default. Tied/insufficient quantile
cross-sections yield warnings rather than arbitrary bucket assignment. Overlapping
label significance is not implemented; no naive confidence intervals are reported.

## Saved DQ boundary

`openbb_alpha.datasets.from_dq_history(result, metadata=..., eligibility=...,
price_field=...)` converts an already fetched `obb.dq_market.history` result.
It uses actual `data`/`meta.publication_id`, `instrument_uid`, `bar_start`,
`first_published_at`, units and raw OHLC fields from the local DQ reader schema.
An explicit calendar, historical eligibility mask, source metadata and convention
remain mandatory. It accepts only complete publication-pinned daily snapshots;
pagination, unresolved revisions, intraday data, unknown volume units and adjusted
raw fields fail. Canonical IDs are retained. It does not fetch, collect, create a
canonical dataset, or open an existing database.

Existing raw-close DQ history can be registered with `price_convention="raw_close_only"`,
`open_convention="unsupported"`, `price_field=None`. The dollar-volume factor is
available; price-based factors require a separately verified consistent return
index. They do not silently interpret raw close as adjusted total return. Current
DQ history does not supply that index or certify historical universes. DQ mapping is
tested with saved synthetic records shaped like the native reader response; no
credentialed DQ service or real historical publication was queried in this task.

## Optional workflows

Qlib uses the actual `DataHandlerLP.from_df`, `DatasetH`, and CPU ridge `LinearModel`
APIs. Normalization is fitted only on complete purged training rows. Both label exit
and actual label availability must precede the validation open; validation is also
purged against the test open. Test labels never enter the Qlib handler. Held-out
label perturbation leaves coefficients, scaling and predictions unchanged. Exact
split dates, effective fitting interval, protected timestamps and purged counts are
recorded. Models are trusted hashed numeric JSON, not executable pickles or workflow
YAML. Predictions retain canonical IDs and decision timestamps. Qlib does not call
`qlib.init`, download benchmarks, start an MLflow server, or access market data.

bt consumes the saved synthetic raw-open panel and previous-session factor scores.
Top-K ties use canonical ID order; scheduled cross-sections with fewer than K valid
names move to cash. It solves equal weights after linear trade costs using drifted
pre-trade holdings, sells before buying, and uses actual bt transactions/accounting.
Integer positions are rounded down; fractional positions are explicit. Commission
and spread/slippage assumptions are separate cash fees, not a market-impact model.
Gross is a separate zero-cost bt run. Holdings, trades, gross NAV and net NAV are
separate artifacts. Turnover is absolute realized traded notional divided by
pre-trade marked NAV. Hand-calculated fee/share/cash cases, drift rebalancing and
rotation with costs cover both fractional and integer positions. No borrowing,
shorting, corporate-action simulation or live execution is supported.

## Artifacts and security scope

Panels use Parquet with a content hash, row count, schema columns and a preview of
at most 10 records. Maximum aligned input is 100,000 rows; factor output is 600,000
rows; each stored object is limited to 64 MiB and decoded Parquet to 256 MiB.
The in-memory registration body is intended for bounded fixtures/saved snapshots.
Processing uses Arrow between stages and a deliberate pandas conversion at the
specialist-library boundaries; factor production operators use Polars expressions.

Artifact IDs are SHA-256 digests of actual bytes, not filenames or `latest` aliases.
Only opaque hexadecimal IDs are accepted. Writes are atomic, reads verify hashes,
and symlinks, hardlinks and unsafe root permissions are rejected. No implicit latest
resolution is supported: supply the immutable snapshot/publication explicitly.
HTML escapes all metadata and includes no remote scripts, styles or CDNs.

Each artifact root belongs to one OS/application principal. Separate authenticated
OpenBB processes and private roots are required for different users. This is a local
single-principal workflow, not a shared multi-tenant authorization service. Network
exposure and authentication deployment were not part of this task. The same Lab-only
policy applies to Python and REST. No Lab-to-Production promotion is implemented.

Source revision and a fingerprint of relevant tracked/untracked source accompany
runs. Dirty-code reproduction requires retaining those exact files. The manifest
stores no environment variables, credentials or raw source diffs. Failed core runs
are persisted with a stage/error type and never marked complete. Optional adapter
failures propagate and never create a successful manifest.

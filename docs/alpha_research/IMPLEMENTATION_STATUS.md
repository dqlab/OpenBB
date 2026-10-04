# Alpha research implementation status

All eight milestones are **verified** for the implemented Lab/offline scope.
Authoritative instructions: `OpenBB_Implementation_Instructions.md`, read in full.
No implementation blocker remains. Explicit scope limitations and missing supplied
oracle files are listed below; these are not represented as verified real-data paths.

| Milestone | Status | Implemented and verified evidence |
| --- | --- | --- |
| AR-01 audit and dependency baseline | verified | REUSE_AUDIT.md, TESTED_VERSIONS.json, constraints-py312.txt; local core 1.6.13 in isolated CPython 3.12.3 environments |
| AR-02 shared contracts and dataset boundary | verified | Native standard_models/alpha_research.py; immutable private Parquet/JSON store; bounded registration and saved native DQ-history conversion; identity, null, zero, policy and security tests |
| AR-03 alpha and reference provider | verified | Native alpha router, catalogue/capabilities, six standard-library factors, golden fixtures and typed Python/REST provider selection |
| AR-04 polars_ta and parity | verified | Actual polars_ta operators; all six factors, masks, eligible ranks, future perturbation, suffix and warmed-chunk parity |
| AR-05 independent labels and Alphalens | verified | t+1 / t+1+h labels; actual Alphalens; perfect-IC fixture, ties, counts, loss policy, ordinal session calendar |
| AR-06 workflows/artifacts/reports | verified | run/runs/result/report; successful and failed manifests; source fingerprints; escaped HTML; final 10-instrument/320-session demo through public API with both engines |
| AR-07 Qlib/bt | verified | Separate installable adapters and separate dependency-group environments; real Qlib fit/predict with purging; real bt ledger with fee-aware post-cost weights and drift/timing reconciliation |
| AR-08 installation/regressions/handover | verified | openbb-build, restarted interpreters, Python signatures, OpenAPI and actual TestClient requests; 246 distinct test cases passed across the final suites; no final skips |

## Checkout and preservation

- Checkout: `/home/zdqclawbot/work/dev/OpenBB` (the lowercase path does not exist).
- Implementation branch: `develop`; validation HEAD before the implementation commit: `807333c376e7004f65a7783d848292a2ced845a6`.
- One worktree; origin dqlab/OpenBB, upstream OpenBB-finance/OpenBB.
- Twelve pre-existing modified collector paths plus four untracked instruction/Zone.Identifier files are preserved. Their initial tracked binary diff was saved outside the checkout and compared byte-for-byte at handover: unchanged.
- During implementation, no staging, commits, resets, cleans, stashes, pushes, merges, deployment, collector restarts or live market/broker calls were performed. The subsequent user request explicitly authorizes committing and pushing the implementation to the existing tracked branch, `origin/develop`.
- Rebuilt environment-specific `openbb/assets/reference.json` and `openbb/package/__init__.py` are present as generated changes and excluded from the implementation commit. Generated command modules are ignored by the existing repository rules. These assets describe the isolated development installation, not a production rollout.

## Source changes

Exact paths: [CHANGED_FILES.txt](CHANGED_FILES.txt). Main groups:

- `openbb_platform/core/openbb_core/provider/standard_models/alpha_research.py`: all shared version-1 contracts and native provider metamodels.
- `openbb_platform/core/openbb_core/app/static/package_builder.py`: narrowly imports typed body models flattened from provider dataclasses; scalar datetime imports remain qualified to avoid shadowing.
- `openbb_platform/core/tests/app/static/test_package_builder.py`: two corresponding generator regressions.
- `openbb_platform/extensions/alpha/`: catalogue, dataset/store boundary, fixtures, independent labels, artifact results, router and 43 acceptance/integration tests.
- `openbb_platform/extensions/research/`: native-provider orchestration, manifests, reports and optional workflow routes.
- `openbb_platform/providers/{alpha_reference,polars_ta,alphalens,qlib,bt}/`: five separately packaged real adapters.
- `scripts/alpha_research/`: public offline demo, physical-minimal-environment smoke and isolated configuration bootstrap.
- `docs/alpha_research/`: audit, tested versions, reproducible requirements/constraints, methodology, checksums and final results.

Existing DQ, DuckDB, Stooq, collection, analytical and market-building code is reused
or tested, not rewritten. No canonical data store or collector is duplicated.

## Exposed commands and providers

- `obb.alpha.catalog()`, `capabilities()`, `register(dataset=...)`.
- `obb.alpha.compute(request=..., provider="alpha_reference" | "polars_ta")`, native model `AlphaCompute`.
- `obb.alpha.labels(request=...)`.
- `obb.alpha.evaluate(request=..., provider="alphalens")`, native model `AlphaEvaluate`.
- `obb.research.run(request=...)`, `runs(limit=...)`, `result(manifest_id=...)`, `report(manifest_id=..., format="html")`.
- `obb.research.qlib_train(request=...)`, `obb.research.bt_backtest(request=...)`; optional imports are delayed and absence errors name the required package.
- REST prefix `/api/v1`; typed inputs are POST bodies. Real Python/REST factor artifact IDs, optional predictions and optional trade artifacts match.

## Dependency matrix

All new distributions are version 0.1.0. Full tested versions are in
[TESTED_VERSIONS.json](TESTED_VERSIONS.json); third-party constraints in
[constraints-py312.txt](constraints-py312.txt).

| Integration | Intended | Actually tested |
| --- | --- | --- |
| Local fork | OpenBB V4 source, no system changes | source 4.7.3 / editable core 1.6.13 / Python 3.12.3 |
| Reference | independent standard library | real numerical implementation; minimal environment without specialist libraries |
| Columnar execution | polars_ta + compatible Polars | polars-ta 0.5.17, Polars 1.44.2, PyArrow 25.0.1 |
| Evaluation | alphalens-reloaded | 0.4.6, pandas 2.3.3, NumPy 2.5.3 |
| Qlib | separately installable actual CPU model | pyqlib 0.9.7; dedicated `/tmp/openbb-alpha-qlib-env`; real fit/predict |
| bt | separately installable actual simulator | bt 1.3.0; dedicated `/tmp/openbb-alpha-bt-env`; fractional/integer and cost-rotation tests |
| DQ / DuckDB / Stooq / collection | existing packages | 0.1.5 / 1.2.1 / 1.0.0 / 0.1.0; local editable regressions |

## Exact installation and validation commands

Commands run from the checkout root. The reproducible requirement groups encode the
same editable packages installed during implementation. The initial incremental
installs and their resolved versions were recorded during the audit; no monorepo
lock was regenerated.

```bash
UV_CACHE_DIR=/tmp/openbb-alpha-uv-cache uv venv /tmp/openbb-alpha-env --python /usr/bin/python3
UV_CACHE_DIR=/tmp/openbb-alpha-uv-cache uv pip install --python /tmp/openbb-alpha-env/bin/python -r docs/alpha_research/requirements-core.txt
UV_CACHE_DIR=/tmp/openbb-alpha-uv-cache uv pip install --python /tmp/openbb-alpha-env/bin/python -r docs/alpha_research/requirements-regression.txt
UV_CACHE_DIR=/tmp/openbb-alpha-uv-cache uv pip install --python /tmp/openbb-alpha-env/bin/python -r docs/alpha_research/requirements-qlib.txt -r docs/alpha_research/requirements-bt.txt
```

The source packages were installed incrementally with `uv pip install -e` before
the equivalent locked requirement groups were written. Both dedicated optional
environments were installed from those requirement files, with `--offline` using
the populated cache, and their tests passed. Initial network requests failed in the
sandbox; normal approved package downloads succeeded. The cache is under `/tmp`.

For the commands below, the literal prefixes used were:

```bash
export PYTHONPATH="$PWD/scripts/alpha_research"
export OPENBB_ALPHA_TEST_SETTINGS=/tmp/openbb-alpha-settings
export MPLCONFIGDIR=/tmp/openbb-alpha-mpl
export MLFLOW_DISABLE_AGENT_HINT=1
```

`sitecustomize.py` only isolates OpenBB configuration paths. Neither HOME nor real
credentials are repurposed. Offline AnyIO/TestClient execution required normal
approved execution outside this sandbox: even a minimal standalone AnyIO portal
hangs inside it. No public server was started.

| Exact command (with the prefix above) | Final result |
| --- | --- |
| `/tmp/openbb-alpha-env/bin/openbb-build` | exit 0; actual generated imports and commands verified in fresh interpreters |
| `timeout 180 /tmp/openbb-alpha-env/bin/python -m pytest -c openbb_platform/extensions/alpha/pyproject.toml openbb_platform/extensions/alpha/tests -q --disable-warnings` | exit 0; 43 passed, 54.65s; real engines, optional libraries, Python and REST |
| `/tmp/openbb-alpha-env/bin/python -m pytest -c openbb_platform/core/pyproject.toml openbb_platform/core/tests/app/static/test_package_builder.py -q --disable-warnings` | exit 0; 77 passed, 11.59s |
| `/tmp/openbb-alpha-env/bin/python -m pytest -c openbb_platform/providers/dq_quant_data/pyproject.toml openbb_platform/providers/dq_quant_data/tests -q` | exit 0; 60 passed, 3.82s; mocked service responses and local fixtures |
| `OPENBB_DUCKDB_INTERFACE_TEST=1 timeout 60 /tmp/openbb-alpha-env/bin/python -m pytest -c openbb_platform/providers/duckdb/pyproject.toml openbb_platform/providers/duckdb/tests openbb_platform/providers/stooq/tests -q --disable-warnings` | exit 0; 50 passed, 7.12s; includes real installed derivatives commands against temporary DuckDB |
| `timeout 60 /tmp/openbb-alpha-env/bin/python -m pytest -c openbb_platform/extensions/collection/pyproject.toml openbb_platform/extensions/collection/tests -q` | exit 0; 16 passed, 2.52s; fake clients and temporary fixtures |
| `OPENBB_ALPHA_TEST_SETTINGS=/tmp/openbb-alpha-qlib-settings /tmp/openbb-alpha-qlib-env/bin/python -m pytest -c openbb_platform/extensions/alpha/pyproject.toml openbb_platform/extensions/alpha/tests/test_optional_qlib.py -q` | exit 0; 1 passed, 11.76s in separate Qlib-only group |
| `OPENBB_ALPHA_TEST_SETTINGS=/tmp/openbb-alpha-bt-settings /tmp/openbb-alpha-bt-env/bin/python -m pytest -c openbb_platform/extensions/alpha/pyproject.toml openbb_platform/extensions/alpha/tests/test_optional_bt.py -q --disable-warnings` | exit 0; 4 passed, 4.80s in separate bt-only group |
| `OPENBB_ALPHA_TEST_SETTINGS=/tmp/openbb-alpha-minimal-settings OPENBB_ALPHA_ROOT=/tmp/openbb-alpha-minimal-artifacts timeout 45 /tmp/openbb-alpha-minimal-env/bin/python scripts/alpha_research/minimal_smoke.py` | exit 0; all five specialist import names absent, reference command works, actionable optional errors |
| `timeout 120 /tmp/openbb-alpha-env/bin/python scripts/alpha_research/demo.py --output /tmp/openbb-alpha-demo-final` | exit 0; both engines reconciled; reports/manifests exported |

The 246-case total counts the five main suites once; dedicated optional repeats and
CLI smokes are additional evidence, not extra distinct cases. Final main suites have
no skips. Earlier DuckDB runs skipped the opt-in interface test; it was subsequently
enabled and passed. Library deprecation and intentional constant-IC warnings are
reported; no credentialed/live integration tests were run.

```bash
/tmp/openbb-alpha-env/bin/ruff check openbb_platform/extensions/alpha openbb_platform/extensions/research openbb_platform/providers/alpha_reference openbb_platform/providers/polars_ta openbb_platform/providers/alphalens openbb_platform/providers/qlib openbb_platform/providers/bt openbb_platform/core/openbb_core/provider/standard_models/alpha_research.py scripts/alpha_research --no-fix --output-format concise
```

Result: exit 0, all checks passed. The two touched legacy builder files have seven
pre-existing lint findings (2 implementation, 5 tests). Comparing codes/messages
against `git show HEAD:<path>` produced identical lists; those unrelated issues were
left unchanged. No separate static type checker was run; typed request validation,
generated signatures and OpenAPI are exercised by the integration tests.

## Final demonstration and packaging

Both runs use dataset `a3b2a0f56ea0c0ac3d3872928f4088b08bc7f17636680210c773682737006551`:
3,200 rows, 13 columns; each engine emits 19,200 factor rows, 11 columns and 18
factor/horizon evaluations. See [DEMO_RESULTS.json](DEMO_RESULTS.json) for timings
and cumulative process RSS (not isolated benchmark comparisons).

- **alpha_reference**: report `/tmp/openbb-alpha-demo-final/alpha_reference.html`; manifest `/tmp/openbb-alpha-demo-final/alpha_reference.manifest.json`.
  Manifest ID `596c44404edad6d8cad54877bfe420edbab55a4e8673ca8932eb7b9d79d19b4e`; HTML SHA-256 `3e8f98e6970a7bffc87220c4cb4b29cfd772d0b57f950de123fa03ddf1d3f99a`.
- **polars_ta**: report `/tmp/openbb-alpha-demo-final/polars_ta.html`; manifest `/tmp/openbb-alpha-demo-final/polars_ta.manifest.json`.
  Manifest ID `4dc1db90b66f64b735387b2b026208c4e02b3a5120edabc1628bbcc76683b21b`; HTML SHA-256 `487338a1c6149b2739003d1d2ccb19987330d6aa55fa971aa048b09126a1666d`.

Both reports were parsed with html5lib: 18 inline SVG plots each, no scripts, hashes
verified. Full immutable data/factor/label/evaluation artifacts reside under
`/tmp/openbb-alpha-demo-final/artifacts` (private mode 0700).

Seven actual wheels were built with the installed Hatchling backend under
`/tmp/openbb-alpha-wheels`. From each of the seven new package directories the exact
build command was `/tmp/openbb-alpha-env/bin/python -m hatchling build -t wheel -d /tmp/openbb-alpha-wheels`.
All exited 0. [WHEELS.json](WHEELS.json) records names, version 0.1.0, byte sizes and
SHA-256 checksums. They require this fork's shared core contracts. Nothing was
published or uploaded.

## Resolved issues and genuine limitations

Resolved during integration:

- Typed provider bodies initially lacked generated model imports. The small core
  fix and two regressions also prevent datetime-module shadowing in existing routes.
- Default bt rebalancing could borrow while rotating names with fees. Post-cost
  weights and sell-before-buy bt transactions now pass ledger and rotation checks.
- Existing builder/collection fixtures needed FMP/news and IBKR/yfinance validators
  installed in the isolated environment. Their final regressions pass; no provider
  calls or production collection was triggered by those installations.

Remaining limitations / unavailable inputs:

- The instructions reference supplied `reference_oracle.py` and tests, but no such
  files were provided. An independent standard-library oracle and expanded golden
  tests were implemented; they are not claimed to be the missing supplied tests.
- Data classification is Lab only. No PIT certification, historical universe
  certification or future-vintage corporate-action safety is asserted.
- Executable labels and bt require explicitly synthetic no-action prices. Real
  corporate-action labels/portfolios are refused. Raw DQ history cannot silently
  become a total-return index; price factors require an explicit verified index.
- Saved DQ conversion is tested against the actual reader's schema with synthetic
  records. No real DQ publication, credentials, broker gateway, collector workload,
  external database or live market endpoint was exercised.
- Artifact roots serve one authenticated application/OS principal. Separate private
  roots/processes are required for different users; shared multi-tenant deployment
  is not implemented or tested. No network deployment was performed.
- Evaluation omits dependence-aware confidence intervals. Quantile diagnostics are
  not strategy NAV. bt cost assumptions are linear research assumptions, not market
  impact/capacity evidence. Qlib is one CPU ridge baseline, not a model search system.
- `/tmp` environments, wheel files and demo artifacts are local/ephemeral. Source,
  locked requirements, commands, versions and checksums are retained in the checkout
  to reproduce them.

Final artifact audit: both demo manifests match current source fingerprint
`4af772acda020ee1c5fa0fda98a275e361e22a0833fb4d04e3a0a37f54145fa1`.
All seven wheel hashes and their packaged Python source match the final checkout.
`git diff --check` exited 0. The 12 protected collector-file diffs remain exactly
unchanged, and the task-created transient build lock was removed after builds ended.

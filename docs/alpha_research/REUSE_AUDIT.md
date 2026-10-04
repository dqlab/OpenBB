# Alpha research reuse audit

Baseline: `807333c376e7004f65a7783d848292a2ced845a6`, branch `develop`, one worktree at `/home/zdqclawbot/work/dev/OpenBB`. Remotes: origin dqlab/OpenBB; upstream OpenBB-finance/OpenBB. No branch switch because the worktree is dirty.

Source: OpenBB 4.7.3, core 1.6.13, Python requirement >=3.10 (DQ >=3.11). System Python 3.12.3 has OpenBB 4.7.1/core 1.6.8. New isolated environment: `/tmp/openbb-alpha-env`, with editable local core so the system installation cannot shadow it.

| Existing component | Reuse / boundary |
| --- | --- |
| dq_quant_data 0.1.5 | Preserve provider and `dq_data`/`dq_market`; thin conversion of bounded saved history results, no acquisition |
| DuckDB 1.2.1 / Stooq 1.0.0 | Preserve bounded database reads and symbol/date parsing; registration regressions |
| Collection 0.1.0 | Preserve historical/live routers; import/build smoke only, no collection commands |
| Technical 1.6.2 | Existing pandas-ta chart/indicator commands; does not provide research snapshots or interchangeable factor models |
| Quantitative 1.6.2 | Preserve statistics, performance and native dqlib interfaces |
| Econometrics 1.7.2 | Preserve regression/time-series interfaces |
| Core registry_map/provider_interface | Native discovery identifies shared fields by standard_models directory; use additive standard models, Field(kw_only=True) for typed POST bodies |

Chosen namespaces: `obb.alpha` and `obb.research`, neither exists. Shared contracts added once under core standard_models. Separate reference, polars_ta, alphalens calculation providers. Separate optional Qlib/bt packages serve lazy-loaded research workflow commands; libraries are imported only when called.

The instructions mention a supplied reference_oracle.py and tests, but neither was found in the checkout. An independent standard-library oracle and explicit golden tests will be implemented; these are not represented as supplied tests.

## Protected pre-existing paths

```text
 M openbb_platform/collectors/core/README.md
 M openbb_platform/collectors/core/src/openbb_collector_core/delivery.py
 M openbb_platform/collectors/core/src/openbb_collector_core/process.py
 M openbb_platform/collectors/core/tests/test_dispatch.py
 M openbb_platform/collectors/core/tests/test_retention.py
 M openbb_platform/collectors/historical/README.md
 M openbb_platform/collectors/historical/src/dq_historical_market_data_collector/config.py
 M openbb_platform/collectors/historical/src/dq_historical_market_data_collector/planner.py
 M openbb_platform/collectors/historical/src/dq_historical_market_data_collector/runtime.py
 M openbb_platform/collectors/historical/src/dq_historical_market_data_collector/storage.py
 M openbb_platform/collectors/historical/tests/test_historical_config_planner.py
 M openbb_platform/collectors/historical/tests/test_repeated_schedule.py
?? CODEX_IMPLEMENTATION.md
?? CODEX_IMPLEMENTATION.md:Zone.Identifier
?? OpenBB_Implementation_Instructions.md
?? OpenBB_Implementation_Instructions.md:Zone.Identifier
```

No existing database, collector configuration, credential, service, or remote state is part of this work.

## Dependency evidence

Primary references consulted: [polars_ta operators](https://polars-ta.readthedocs.io/en/latest/tdx/statistic/), [Alphalens performance API](https://alphalens.ml4trading.io/api-reference.html), [Qlib LinearModel source](https://github.com/microsoft/qlib/blob/main/qlib/contrib/model/linear.py), [bt API](https://pmorissette.github.io/bt/docs/source/overview.html). Installed source inspection and real tests determine compatibility. Exact resolved constraints and tested status are recorded alongside IMPLEMENTATION_STATUS.md.

Final intended-versus-tested dependency matrix and verification results: [IMPLEMENTATION_STATUS.md](IMPLEMENTATION_STATUS.md).

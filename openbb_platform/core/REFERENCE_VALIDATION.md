# Reference metadata validation

The reference generator preserves annotation values and metadata through generated
Python and JSON. In particular, `choices` can be either an array or `null`;
consumers should not assume that it is always an array. In a historical generated
reference, Nasdaq economic calendar `country` had `choices: []`; the final
reference generated in this validation has `choices: null`. The source of the
historical metadata is not established. The repository's reference loaders pass
both values through, and the CLI handles both as falsy. External consumers and
their compatibility with this change are unknown.

For the historical candidate validated on 2026-11-01 (builder SHA-256
`840c3a22969ba55b4d36240b953d172b9063966316e5a9065aec80023910b309`,
regression test SHA-256
`d91844ae27d4908a597a7f50af018f6e324b18a5f55a7d968ab187adc4b463b6`):

- An isolated offline core suite recorded 909 passed and 404 subtests passed,
  exit 0. An independent isolated run of the regression file recorded 12 passed,
  exit 0. These results do not establish compatibility for unknown consumers.
- Under the same installed provider and extension paths, develop, the prior PR
  head and the final candidate each generated the same 404 route names. A prior
  408-route artifact includes four additional AKShare routes, but its import
  provenance is incomplete; the cause of that difference remains unknown.
- Generated-code Ruff reported 85 findings for both develop and the final
  candidate (68 D105, 13 E501, 4 D401); their rule/file/message multisets match,
  although line positions changed. Ruff exited 1. The existing build invokes
  Ruff with `check=False`, so a successful build exit does not mean Ruff passed.
  Source Ruff also reported two pre-existing PLC0207 findings under the same
  version and conditions as the baseline.
- The locally built `openbb_core` wheel (SHA-256
  `fd33687257ab07729b4d5a6226fe15aead99d925ac67fe528db905363ef40b44`)
  built and installed offline. Its reference asset is a placeholder; a separate
  full build supplied the generated package and reference copied into the wheel
  installation for the mocked Nasdaq public-command check. That check passed
  with fixture transport; it does not validate live providers or production.

These are local observations, not CI results or production validation. The
historical 803-item positional comparison is an inventory, not proof that each
metadata change is semantically compatible.


## Develop rebase validation, 2026-11-10

Base: `ed080b94ea15440f6fcdffc7b1e969cf3d856365`, including PRs #8 and #7.
The import conflict was resolved by retaining both develop's `ContextVar` build
cache and the metadata `deepcopy` import. Provider and collector implementations
are unchanged by this PR.

An additional public SDK check found a regression missed by the original core
suite: `obb.technical.sma(DataFrame(...), length=2)` failed with
`NameError: ndarray is not defined`. The annotation normalizer now retains
develop's handling of the four native data forward references (`Data`,
`DataFrame`, `Series`, `ndarray`) inside parsed annotations. It leaves literal
values and annotation metadata intact. A regression test reproduces the failure
after Ruff removes unused imports and passes with the correction, using both
DataFrame and ndarray inputs.

Current local observations, using Python 3.12.3 and isolated settings:

- Core: **911 passed, 408 subtests passed**, zero failed or skipped. The reference
  regression file is included and also passed independently: **13 passed**.
- A fresh full build has 408 callable routes, matching the baseline route set.
  Parameter names, kinds, underlying types, defaults and return types match the
  baseline for all 408 commands. Full annotated signature strings differ where
  description whitespace is now preserved.
- Repeated reference generation is equal within a build and leaves 6,268
  provider field metadata entries unchanged. Written JSON matches the reference
  structure generated in that same build. Mocked profile/ETF commands and a
  real local SMA calculation pass. These are offline checks.
- Changed-source Ruff 0.12.12 / 0.15.22 and Black 25.1.0 pass. Generated-code
  Black passes with the builder's 122-character setting.
- **Blocked, not merged:** explicit generated-code Ruff checks exit 1 with
  **85 diagnostics** (68 D105, 13 E501, 4 D401), under both Ruff versions. The
  baseline has the same rule/file/message multiset. No lint rule was relaxed.
  A successful build exit still does not establish passing generated-code lint.
- The first core run had four home-path masking failures caused by an
  inconsistent test launcher. The corrected launcher isolates both `Path.home`
  and `expanduser`; repository assertions were retained. The final counts above
  come from the corrected environment after the native-data fix.

Current builder SHA-256: `eb58526a8e0d96fee4000c7255cf426148ae2bfef6c3139aa51392bcd91ae203`.
Current regression test SHA-256: `0b81bc3c76c34bfa98d07be8f0b53ab29a2d352dd3d711b30017ae564e69f328`.

No live provider, gateway, DQ or deployment integration was tested. The PR remains
open because the user required red checks to block merging.

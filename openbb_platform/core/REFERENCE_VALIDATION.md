# Reference metadata validation

The reference generator preserves annotation values and metadata through generated
Python and JSON. In particular, `choices` can be either an array or `null`;
consumers should not assume that it is always an array. In a historical generated
reference, Nasdaq economic calendar `country` had `choices: []`; the final
reference generated in this validation has `choices: null`. The source of the
historical metadata is not established. The repository's reference loaders pass
both values through, and the CLI handles both as falsy. External consumers and
their compatibility with this change are unknown.

For the final candidate (builder SHA-256
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

# OpenBB development

## Ownership and layout

This is the OpenBB source checkout. The sibling application workspace is
`../../app/openbb`; reusable providers, router fixes, and packaging changes belong
here, while dated research outputs and local pricing runs belong there.

- `openbb_platform/core`: common interfaces and standard models.
- `openbb_platform/providers/<provider>`: provider packages, fetchers, and tests.
- `openbb_platform/extensions`: routers and integrations.
- `desktop`: desktop application; use its own package scripts for UI changes.
- Existing workflow guidance lives in
  `openbb_platform/extensions/mcp_server/openbb_mcp_server/skills/`.
  Read `develop_extension/SKILL.md` for extension changes; existing extensions
  should not be scaffolded again.

## Provider changes

Follow the neighboring provider's QueryParams/Data/Fetcher conventions and entry
points. Preserve symbol, date, currency, adjustment, and missing-data semantics
across the standard model boundary. Use the shared OpenBB HTTP helpers where
fetchers make network requests; test parsing with saved or synthetic responses.

DuckDB database access should retain bounded queries and parameterized values;
validate identifiers separately. Stooq parsing should preserve provider-specific
symbol and date conventions. Inspect the current implementation before changing
either contract.

## Validation and releases

Use a Python environment containing the packages under test. Provider checks
from the repository root include:

```bash
python -m pytest openbb_platform/providers/duckdb/tests
python -m pytest openbb_platform/providers/stooq/tests
python -m ruff check openbb_platform/providers/duckdb
```

Choose only affected packages for an isolated change. When provider registrations,
router models, or installed extension interfaces change, follow the existing
skill's installation and `openbb-build` guidance, then check the exposed command.
Keep offline tests separate from credentialed integration checks.

Inspect the current branch and remotes before release work; do not assume a
particular upstream, fork, tag, or publication destination. Existing release
branches and uncommitted edits may be in active use. Generate artifacts locally
and record version/checksum before any requested publication. Use
`market-data-onboarding` for a new dataset contract and
`dqlib-market-calibration` for native market-building workflows.

Done means the affected checks pass, exposed behavior is documented, and missing
credentials or untested integration paths are stated. Documentation-only edits
do not require rebuilding OpenBB.

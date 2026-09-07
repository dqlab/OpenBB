# Release notes

## Unreleased — OpenBB collector architecture

- Move the historical and live engines into sibling `collectors` packages.
- Separate the `openbb-collection` router distribution from both engines.
- Add shared collector discovery, response contracts and supervised workers.
- Use explicit configuration v2 provider/model mappings through QueryExecutor.
- Move bounded historical and quote acquisition into the IBKR provider.
- Preserve CLI/import names, journal tables and readable exported record schemas;
  new configuration/request contracts receive distinct fingerprints and identities.

## 0.1.0 — 2026-09-07

Initial standalone release within dq-quant-invest-data:

- Validated instrument lists, provider routing, explicit historical ranges, six
  daily/intraday frequencies, required fields and bounded request planning.
- Pinned dqlab OpenBB integration and explicit-end historical IBKR adapter.
- Immutable raw evidence, quarantine, revision/as-of history, resumable checkpoints
  and JSONL/CSV/Parquet/SQLite/DuckDB storage.
- Continuous scheduling, session reports, catch-up, safe weekly/monthly resets
  and graceful shutdown.
- Functional, resilience, bounded-resource and real-provider acceptance tooling.

The software release does not certify unavailable provider subscriptions or
historical coverage. See the original data-platform validation report linked from the README for tested datasets and
remaining gateway acceptance conditions.

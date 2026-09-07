# Historical market-data collector

The independently installed historical engine lives beside OpenBB providers in
`openbb_platform/collectors/historical`. Its distribution and CLI remain
`dq-historical-market-data-collector`; Python imports remain
`dq_historical_market_data_collector`. The OpenBB routers are a separate
[collection extension](../../extensions/collection/README.md).

See the [collector architecture guide](../README.md) for installation, provider
selection, configuration v2 migration and package validation.

## Configuration and commands

The [packaged example](src/dq_historical_market_data_collector/example.yaml) and
[deployment example](configs/historical_market_data_collector.yaml) use
explicit provider/model pairs and query mappings. Equity and index history have
separate source models. The [derivative template](examples/ibkr_derivatives.template.yaml)
shows explicit qualified contracts; replace its placeholders before collection.

```bash
dq-historical-market-data-collector init-config --output historical.yaml
dq-historical-market-data-collector validate-config --config historical.yaml
dq-historical-market-data-collector plan --config historical.yaml --as-of 2026-09-07 --limit 20
dq-historical-market-data-collector status --config historical.yaml
# Explicitly acquire a bounded date range or start the scheduler:
dq-historical-market-data-collector once --config historical.yaml --as-of 2026-09-07
dq-historical-market-data-collector run --config historical.yaml --max-seconds 300
```

`doctor` checks installed provider/query capabilities without connecting.
`smoke --require-source SOURCE --max-seconds 180` performs bounded real-source
acceptance when explicitly run. `query --instrument ID --as-of AWARE_TIMESTAMP`
reads the revisions available at the collector observation cutoff. `once --refresh`
revisits completed chunks to observe corrections. Use `--help` for all options.

## Data and lifecycle contract

Historical ranges are start-inclusive/end-exclusive. Supported frequencies are
`1m`, `5m`, `15m`, `30m`, `1h` and `1d`. Provider hints and configured row/chunk
budgets bound each request, including overlap. Provider selection, parameters and
capabilities are validated before collection; fallback uses only the explicitly
listed secondary source IDs.

The engine preserves immutable raw responses, rejected rows and reasons, accepted
records, source revisions, reconciliation counts, calendar coverage and resumable
checkpoints. Repeated data is idempotent; corrected observations remain traceable.
`available_at` means first observed by this collector. Historical publication time
remains unknown, so a reconstructed backfill does not establish point-in-time
availability at its market event time.

An operator-supplied calendar controls gap checks, holidays and session overrides.
Without it, coverage is unknown. Overnight/DST sessions retain aware timestamp
handling. Weekly/monthly resets recycle workers and reload configuration while
preserving stored evidence, checkpoints and source pacing. A reload cannot move
storage. Configuration v2 creates new fingerprints and retains readable prior
journal/export schemas; see migration guidance before reusing a deployment.

Storage supports JSONL, CSV, Parquet, SQLite and DuckDB. Install the engine's
`parquet`/`duckdb` extras when selected. Only the collector's journal writer should
write a storage root. Relative roots resolve against the YAML location. The
[systemd template](deployment/systemd/dq-historical-market-data-collector.service.example)
uses the preserved CLI and requires deployment-specific paths/account settings.

## Real-source acceptance

Offline tests do not connect to a gateway or validate entitlements. The former
standalone validation report remains in the sibling `dq-quant-invest-data` repo
under `docs/reports/historical_market_data_collector_validation.md`; it documents
that earlier run, not an acceptance run of this refactor. Re-run bounded smoke
acceptance for the configured sources before relying on new live acquisitions.

## Delivery to the data-platform master

The optional top-level `delivery` configuration sends sealed sessions to a local
central directory or an SSH/SFTP master, and can remove acknowledged data files
while retaining the acquisition journal. Use `delivery.local_retention` to choose
immediate cleanup after verification, a duration such as `1d`, `1w`, or `1mo`, or
`forever`. See [session delivery](../SESSION_DELIVERY.md)
for configuration and the `deliver` / `delivery-status` commands.

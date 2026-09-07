# Live market-data collector

The independently installed live engine lives in `openbb_platform/collectors/live`.
Its distribution and CLI remain `dq-live-market-data-collector`; Python imports
remain `dq_live_market_data_collector`. It collects scheduled quote snapshots.
See the [architecture guide](../README.md) for installation and configuration v2.
The [collection extension](../../extensions/collection/README.md) exposes its
bounded OpenBB commands.

## Configuration and commands

Start from the [packaged example](src/dq_live_market_data_collector/example.yaml)
or [deployment example](configs/live_market_data_collector.yaml).
Every source specifies `provider`, `model`, fixed `parameters` and `parameter_map`.
The default live mapping is `symbol: symbol`. Provider timestamp, quote-field and
feed-code mappings are explicit; no vendor route chooses normalization behavior.

```bash
dq-live-market-data-collector init-config --output live.yaml
dq-live-market-data-collector validate-config --config live.yaml
dq-live-market-data-collector plan --config live.yaml --date 2026-09-07
dq-live-market-data-collector status --config live.yaml
# Collect one scheduled pass or run for a bounded interval:
dq-live-market-data-collector once --config live.yaml
dq-live-market-data-collector run --config live.yaml --max-seconds 300
```

`doctor` checks installed capabilities without connecting. `export --max-batches N`
replays pending output without requesting quotes. Use `--help` for options and
`smoke --require-source SOURCE --max-seconds 120` for explicitly initiated
real-source acceptance.

## Observation contract

The engine owns configured sessions, holiday/overnight/DST handling, polling slots,
source pacing, retries, explicit secondary-source fallback and shutdown recovery.
A shared supervised worker enforces timeout and response limits. Provider code
owns qualification and snapshot acquisition. Weekly/monthly resets recycle
workers and preserve data and pacing.

Configuration declares fields, required values, maximum age, accepted feed types,
adjustment and size units. Use `field_map`, `timestamp_field`,
`timestamp_semantics`, `timestamp_timezone`, `feed_type_field` and `feed_type_map`
for provider-specific output. For example, numeric feed codes can be configured as
`feed_type_map: {"1": live, "2": frozen, "3": delayed, "4": delayed_frozen}`.
Unknown feed status stays unknown. `requested_feed_type` is separate from actual
observed feed status; delayed requests require explicit delayed acceptance.

The bundled IBKR examples request delayed quotes and accept `live`, `delayed`,
and `delayed_frozen` observations (plus explicitly allowed `unknown` sources).
IBKR can return live data when the login has the entitlement even if delayed
mode was requested; the configured numeric callback mapping records the actual
feed type. The bounded provider resets the library's default feed-type value
before receiving callbacks so it cannot mislabel an unconfirmed quote as live.

An unavailable IBKR bid/ask is represented as null, with its original decoded
value retained in `raw_quote` in the immutable response archive. A present last
price can therefore be collected when optional bid/ask are unavailable. Valid
negative futures quotes with positive size are preserved. Required-field,
identity and crossed-quote checks still apply. Volume is retained as supplied,
with provider-defined units; delayed equity volume anomalies observed during
Gateway checks still require a separate units/decoder investigation.

Raw responses and rejected records remain traceable. The journal preserves
business observation keys, revision/content hashes, collector receive/availability
time and export checkpoints. A source timestamp becomes event time only when
its declared semantics support that interpretation. Collector receipt does not
establish earlier market availability. A changed source model, request mapping,
feed mapping or normalization contract receives a new observation identity.

JSONL, CSV, Parquet, SQLite and DuckDB exports retain the v1 record schema. Install
`parquet`/`duckdb` extras when needed. Configuration v2 changes fingerprints but
does not rewrite previous evidence or exports. Relative storage roots resolve
against the YAML file; preserve the location or set an explicit root when moving
configuration. The [systemd template](deployment/systemd/dq-live-market-data-collector.service.example)
retains the original executable and needs deployment-specific settings.

## Real-source acceptance

No gateway or paid-data request is needed for the offline suite. Connection,
subscriptions, live-versus-delayed delivery and provider coverage require a
separate bounded acceptance run. The earlier standalone report remains in the
sibling data repository at `docs/reports/live_market_data_collector_validation.md`;
it is not an acceptance result for this refactor.

## Delivery to the data-platform master

The optional top-level `delivery` configuration sends sealed sessions to a local
central directory or an SSH/SFTP master, and can remove acknowledged data files
while retaining the acquisition journal. Use `delivery.local_retention` to choose
immediate cleanup after verification, a duration such as `1d`, `1w`, or `1mo`, or
`forever`. See [session delivery](../SESSION_DELIVERY.md)
for configuration and the `deliver` / `delivery-status` commands.

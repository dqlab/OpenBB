# OpenBB collectors

Collectors are peer packages of `providers` and `extensions`. They own scheduling,
request budgets, quality checks, journals, revisions, exports and recovery.
Providers own acquisition, contract qualification, vendor parameters and limits.

```text
openbb_platform/
  core/                         OpenBB provider interfaces and QueryExecutor
  providers/<provider>/         QueryParams / Data / Fetcher implementations
  collectors/
    core/                       response contracts, discovery, dispatch, workers
    historical/                 resumable date-exclusive historical engine
    live/                       scheduled quote snapshot engine
  extensions/collection/        thin obb.collection and obb.collection.live routers
```

The dependency direction is `collection routers -> engines -> collector core ->
OpenBB QueryExecutor -> selected provider`. Providers do not import collectors.
Historical and live engines retain their separate scheduling and storage contracts.
They share the response envelope, provider dispatch, bounded process lifecycle, and
verified session delivery to the data-platform master.

| Directory | Distribution | Registration |
| --- | --- | --- |
| `core` | `openbb-collector-core` | discovers `openbb_collector_extension` |
| `historical` | `dq-historical-market-data-collector` | `historical` collector |
| `live` | `dq-live-market-data-collector` | `live` collector |
| `../extensions/collection` | `openbb-collection` | `collection` core extension |

## Install from this checkout

Use Python 3.11+ and an environment containing the selected providers. From the
repository root:

```bash
python -m pip install -e openbb_platform/core \
  -e openbb_platform/collectors/core \
  -e openbb_platform/collectors/historical \
  -e openbb_platform/collectors/live \
  -e openbb_platform/extensions/collection
# Install providers used by your configuration, for example:
python -m pip install -e openbb_platform/providers/yfinance -e openbb_platform/providers/ibkr
openbb-build
```

`openbb-build` is needed for the OpenBB SDK router registration. Standalone CLIs
use `openbb-core` and installed provider entry points directly; they do not import
the generated `obb` tree or require the equity/index router packages.
Engine imports and stored-data readers remain usable without provider imports.
Provider-aware validation, historical planning and acquisition require the selected
providers and `openbb-core`. The repository development installer also includes
these local packages on Python 3.11+ and understands their PEP 621 metadata.

## Deploy collectors on independent hosts

Use `storage.root` for each collector's working data and configure `delivery` for
the master data repository. Completed session data can move through a local mount
or SSH/SFTP after checksum verification, with durable retry state. See
[session delivery](SESSION_DELIVERY.md) for configuration, deployment, recovery,
local retention, and the master inbox contract.

## Configured provider dispatch

A source specifies an installed **provider name** and a **fetcher model name**.
There is no implicit default broker or fallback. Primary and secondary source IDs
remain the engine's explicit retry/fallback policy.

```yaml
version: 2
sources:
  daily_prices:
    provider: yfinance
    model: EquityHistorical
    adjustment: splits_only
    parameter_map:
      symbol: symbol
      start_date: start_date
      end_date: end_date
      interval: frequency
      adjustment: adjustment
    parameters:
      include_actions: true
```

`parameter_map` maps a provider query field (left) to a named collection context
value (right). `parameters` supplies fixed provider query values. A key cannot be
both fixed and mapped. Unknown context names, unknown query fields, unavailable
providers/models and invalid provider query values fail preflight validation.
Mapping never evaluates code. The query field can be named differently (for
example `ticker: symbol`). For a provider with a fixed bar interval and no query
interval field, map symbol/dates and declare `fixed_frequency: 1d`; a collection
requesting another frequency is rejected. Otherwise map the collection's
`frequency` into its interval query field. Each source has one model; use separate sources
and collections for different model contracts, such as equity and index history.

Context always includes the instrument fields and its source-specific `symbol`.
Historical context adds collection fields, `start_date`, `end_date`,
`end_datetime` (aware UTC end, including overnight coverage), `adjustment`,
`extended_hours` and `timeout`. Live context adds `snapshot_wait_seconds` and
`requested_feed_type`. The source's configured timestamp/field mappings govern
normalization; numeric or custom feed codes need an explicit `feed_type_map`.

Credentials use `credentials_env: {provider_credential: ENVIRONMENT_VARIABLE}`.
They are resolved inside the worker and filtered by OpenBB's QueryExecutor.
They are not copied into query parameters or acquisition reports.
Provider warnings are counted; warning/error text is not retained.

The selected fetcher may declare `collection_max_days` (frequency keys plus a
`default`) and `collection_overlap_days`; historical planning combines these with
configured chunk and row budgets. It may supply `close_collection()` for graceful
worker cleanup. These optional attributes introduce no dependency on collectors.
Provider-specific requirements remain in that fetcher's query validation.

## Configuration migration

Version 2 replaces historical `kind` and live `route` dispatch with explicit
`provider`, `model` and mappings. Version 1 configurations are rejected rather than
silently selecting another implementation. Start from the updated
[historical example](historical/src/dq_historical_market_data_collector/example.yaml)
or [live example](live/src/dq_live_market_data_collector/example.yaml).

| Previous source | Version 2 selection |
| --- | --- |
| Historical `kind: openbb` equities | configured provider + `EquityHistorical` |
| Historical index route | configured provider + `IndexHistorical` |
| Historical `kind: ibkr` | `provider: ibkr`, `model: MarketHistorical` |
| Live `equity.price.quote` | configured provider + `EquityQuote` |
| Live `ibkr.market_quote` | `provider: ibkr`, `model: MarketQuote` |

For bounded IBKR models, copy the explicit contract/time/feed mappings from the
examples. Vendor security-type codes, historical duration strings, qualified
identity checks and snapshot cancellation now live in the IBKR provider. The
historical adapter retains its explicit end time and overlap day; the quote
adapter retains the configured wait and read-only connection.

The distribution names, Python import names and CLI executable names of both
engines are unchanged. The routers now belong to `openbb-collection`; reinstall
all affected editable packages after moving the checkout. Existing raw files,
journal tables and exported record schemas remain readable. Configuration and
request-mapping changes create new contract fingerprints/observation identities;
old evidence and revisions are not rewritten. Relative storage paths still
resolve beside the YAML file, so retain its location or use an explicit storage
root when migrating a deployment.

## Validation

Install development dependencies and all providers referenced by test fixtures
(`yfinance`, `ibkr`, `fmp`). Run each package independently to avoid collisions
between their existing test fixture packages:

```bash
(cd openbb_platform/collectors/core && python -m pytest tests)
(cd openbb_platform/collectors/historical && python -m pytest tests)
(cd openbb_platform/collectors/live && python -m pytest tests)
(cd openbb_platform/extensions/collection && python -m pytest tests)
python -m pytest openbb_platform/providers/ibkr/tests/test_bounded_market_data.py
python -m ruff check openbb_platform/collectors openbb_platform/extensions/collection
```

Tests use synthetic data and mocked gateways. They cover arbitrary registered
provider dispatch, credentials, strict parameter mapping, process timeout/reuse,
provider-owned bounds, identity checks, scheduling, revisions, reconciliation,
retry/resume, exports and the OpenBB interface. Offline validation does not verify
network credentials, subscriptions, live feed status or provider coverage.

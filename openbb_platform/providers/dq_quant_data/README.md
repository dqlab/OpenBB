# DQ data for OpenBB

`openbb-dq-quant-data` is a combined provider and router extension. Its maintained
OpenBB source is in `dqlab/OpenBB/openbb_platform/providers/dq_quant_data`.
The database implementation remains in `dqlab/dq-quant-invest-data`.

## Architecture review

A provider and an extension are not mutually exclusive: a provider is one OpenBB
extension type. Standard financial datasets belong behind QueryParams/Data/Fetcher
classes; database governance operations need dedicated commands.

| Interface | Purpose | Backend |
| --- | --- | --- |
| `obb.equity.price.historical`, `obb.etf.historical`, `obb.index.price.historical` | Retained stock, ETF and index daily bars | Daily-bars REST API |
| `provider="dq_quant_data"` | Equity search/info, dividends, splits, filings and FRED series | Canonical REST API |
| `obb.derivatives.options.chains(provider="dq_quant_data", ...)` | Standard options-chain result with provenance | Market-store REST API |
| `obb.derivatives.futures.curve(provider="dq_quant_data", ...)` | Server-built futures prices and contract/quality metadata | Futures-curve REST API |
| `obb.dq_market.*` | Captures, publication/as-of queries, bounded histories, baskets, instruments and status | Market-store REST API |
| `obb.dq_data.*` | Fundamental facts, quality and lineage | Canonical REST API |

A provider-only conversion would obscure the governance operations; a router-only
conversion would lose standard endpoint interoperability. One distribution registers
all three entry points, avoiding a second provider name or a breaking notebook change.
No database engine or collector code is copied into OpenBB.

The migration fixes the old README's incorrect `provider="dq_quant"` example,
replaces the non-OpenBB FastAPI router registration, uses OpenBB HTTP settings,
closes silent pagination gaps, preserves missing ETF volume and zero-valued fields,
retains currency/adjustment/source provenance, supports company-name search, and fixes the FRED series fallback and result limit.

## Install from this checkout

Use the Python environment where you run OpenBB:

```bash
python -m pip install ./openbb_platform/providers/dq_quant_data
openbb-build
```

API-only use requires no local database package. For explicit local diagnostics, install
`dq-quant-invest-data>=0.1.5,<0.2` from your data-platform checkout/wheel (or use the
`local` extra when that distribution is available on your package index). The
optional import is delayed until a local command is called.

Do not install the older copy from `dq-quant-invest-data/integrations` over this
package. The distribution name and Python import paths remain compatible.

## REST options chain

```python
from openbb import obb

obb.user.credentials.dq_quant_data_api_url = "https://data-host:8016"
obb.user.credentials.dq_quant_data_api_key = "your-read-key"
# For a private CA; omit when the service uses a publicly trusted certificate.
obb.user.credentials.dq_quant_data_api_ca_file = "/path/to/server-ca.crt"

snapshot = obb.derivatives.options.chains(symbol="SPX", provider="dq_quant_data")
df = snapshot.to_df()
metadata = snapshot.extra["results_metadata"]

# Governance operations use the same REST service and credentials.
chain = obb.dq_market.chain(symbol="SPX")
print(chain.extra)
```

Both calls use the platform REST API and its governed reader, eligible Gold default, complete-capture requirement,
source units, and numeric projection. Missing values remain null. Prices become
standard OpenBB floats; the underlying store retains its decimal precision.
The source is a saved capture, potentially delayed, not a live quote request.
Pass the same `capture_id` and `publication_id` to reproduce both interfaces;
an unpinned latest query may advance between calls as ingestion continues.

Both interfaces accept `expiration`, `option_type`, `as_of`, `availability`,
`layer`, `max_rows` and `allow_incomplete`. Central availability means actual
publication time; collector availability is retrospective. Silver may contain
quality-blocked rows. Bounds fail explicitly instead of truncating a chain.

`obb.dq_market` also exposes `status`, `instruments`, `captures`, `history`, and
`basket`. These commands retain their existing names and parameters.

## Client/server boundary

OpenBB runs on the client (for example WSL). `dq-quant-invest-data` serves the
versioned REST API and owns DuckDB, Parquet, capture selection and publication.
`dq_quant_data` is the OpenBB provider identifier for this platform.

All default market reads use `/api/v1/market-store/{method}` over HTTP(S), including
`status`, `instruments`, `captures`, `chain`, `history` and `basket`. Standard stock/ETF/index daily bars use `/api/v1/market/daily-bars`, and the
standard futures curve uses `/api/v1/market/futures/curve`. Reference data and
fundamentals use the existing canonical `/api/v1` endpoints.
Clients need no database package, NAS filesystem mount or SSH command execution.
`DQ_MARKET_REMOTE_CONFIG` is no longer used. HTTP errors never fall back to files.

The chain client requests the columns needed by the OpenBB model while retaining
every contract and the original capture/publication metadata. The API serializes
stored decimals as strings before OpenBB's documented float projection. Missing
values, date offsets, completeness and explicit diagnostic flags are preserved.
The client rejects truncated or incomplete default chains. History returns its
publication-pinned cursor; it does not silently combine separate pages/captures.

An explicit `config_path=` still permits local server-side diagnostics; only that
mode imports the optional `dq-quant-invest-data` Python package. API URL/key/CA
settings never get sent as query parameters. TLS certificate validation remains
enabled, and redirects are rejected.

## Futures term structure

```python
from openbb import obb

curve = obb.derivatives.futures.curve(
    symbol="SOFR3",
    provider="dq_quant_data",
    layer="silver",  # current SOFR exports have unresolved volume units
    price_type="midpoint",
    # config_path="/path/to/market-store.yaml",
)
df = curve.to_df()
metadata = curve.extra["results_metadata"]
```

The platform REST service reads the configured `sofr_futures_quotes` central dataset, resolves qualified
IBKR identities, and sorts contracts by their source expiration/last-trade date.
`contract_month` remains a separate field: an SR3 reference month is not replaced
with its later last-trade month. Only unexpired contracts for the dataset's source
provider are included. No live gateway request or collector-file scan is made.

The default layer is **Gold**, consistent with options chains. Select **Silver**
explicitly to inspect stored rows with quality blockers. A complete Silver result
can still have `metadata["eligible"] == False`, including when collector observation
times are too far apart. It does not constitute a simultaneous exchange snapshot.

`price_type` is `midpoint` (default), `bid`, `ask`, or `last`. Midpoint requires two
finite, nonnegative, non-crossed quotes; no last-price or settlement fallback is
used. Output includes source bid/ask/last, quantity units, feed type, contract
multiplier, reference version, observation identity, and source quality flags.
`price` remains a futures price in its source quote units, not a derived yield.

Latest queries default to a 900-second observation window and a 300-second maximum
observation spread for eligibility. These use collector clocks, which do not
measure exchange-feed delay. `max_age_seconds` is bounded to 86,400, references to
1,000 contracts and candidate reads to 100,000 rows. Overflow fails explicitly.
Missing or invalid selected prices fail by default; `allow_incomplete=True` keeps
those contracts with `price=None` and reports them in metadata. No zeros are filled.

For one historical UTC observation date:

```python
historical = obb.derivatives.futures.curve(
    symbol="SOFR3", provider="dq_quant_data", layer="silver",
    date="2026-09-09",  # exact UTC observation day, not settlement marks
    as_of="2026-09-10T14:00:00Z",  # separate publication/knowledge cutoff
)
```

Dated queries default to the entire requested UTC day (or the elapsed part of that
day at `as_of`). An explicit `max_age_seconds` narrows the window ending at that
day's end or the earlier cutoff. Only one date is accepted per call. Historical
quote ages are relative to the observation-window end, not today's clock.
Contract discovery uses the current identity catalog with terms known by `as_of`;
it does not reconstruct historical trading-universe membership.

For repeatable queries, retain `as_of`, `publication_id`, root, date, layer and
price/window settings from metadata. `curve_snapshot_id`, selected observation
IDs and `reference_versions` record the result's identity and provenance.

## Daily stock, ETF and index bars

```python
stocks = obb.equity.price.historical("AAPL", start_date="2026-09-08", provider="dq_quant_data")
etfs = obb.etf.historical("SPY,QQQ,DIA", start_date="2026-09-08", provider="dq_quant_data")
indices = obb.index.price.historical("SPX,NDX,DJIA", start_date="2026-09-08", provider="dq_quant_data")
a_shares = obb.equity.price.historical("SH.600000", start_date="2026-09-01", provider="dq_quant_data")
```

These commands read retained daily snapshots through `/api/v1/market/daily-bars`.
Only `interval="1d"` is supported. Dates are inclusive, up to 20 symbols and 100,000
rows; overflow fails. `as_of` selects revisions known at the supplied UTC-offset
timestamp. Omitted dates request one year. Result metadata reports actual date
ranges, missing symbols and source snapshot IDs without inventing missing history.

The default `adjustment="source"` returns U.S. split-adjusted prices, cash-index
levels, or unadjusted A-share prices. U.S. `splits_and_dividends` and BaoStock
`forward`/`backward` conventions are explicit and distinct. Unsupported adjustments
fail. Volumes, nulls, units, quality and observation times remain visible. Use
`.extra["results_metadata"]` for provenance. Source history collected later is not
claimed to have been available before collection. ETF/index coverage starts at
the configured collection date, not at instrument inception.

## Canonical API

```python
from openbb import obb

obb.user.credentials.dq_quant_data_api_url = "http://127.0.0.1:8000"
obb.user.credentials.dq_quant_data_api_key = "your-key"
obb.equity.price.historical("AAPL", provider="dq_quant_data")
obb.dq_data.quality()
obb.dq_data.lineage(dataset="market.bars")
obb.dq_data.fundamental_facts(symbol="AAPL")
```

These API commands use OpenBB user credentials, including the routers; API keys
are not command parameters. The previous FastAPI-specific header route signatures
are replaced by supported OpenBB commands. API calls use shared HTTP helpers with
OpenBB proxy/TLS/timeout settings and reject redirects. Paged reads are bounded to
100 pages / 100,000 rows, reject repeated cursors and changing snapshots, and fail
on overflow. Narrow oversized queries. `dq_data` returns page metadata in `extra`.
The existing canonical standard-model fetchers return their typed records; use
`dq_data` for API lineage/quality. Explicit local diagnostics do not need API credentials.

The configured API URL applies to both canonical and market-store REST reads.
Use explicit `config_path=` only when intentionally opening a local store.

## Validation

```bash
python -m pytest -c openbb_platform/providers/dq_quant_data/pyproject.toml openbb_platform/providers/dq_quant_data/tests
python -m ruff check openbb_platform/providers/dq_quant_data
```

Offline fixtures cover provider mappings, router registration, HTTP settings,
pagination, source values and provenance. Credentialed API checks are separate.

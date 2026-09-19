# OpenBB DuckDB Provider

## Daily equity price compatibility (1.1.0)

The equity historical endpoint accepts the same daily price controls as the
Yahoo provider: `interval="1d"`, `adjustment="splits_only"` (default) or
`"splits_and_dividends"`, `include_actions=True`, and `extended_hours=False`.
Omitted dates default to the past year through today. Date bounds are inclusive.
Multi-symbol requests and `.to_df()` retain the standard OpenBB interface.

Store split-only OHLC in `open/high/low/close` and dividend-adjusted OHLC in
`adjusted_open/adjusted_high/adjusted_low/adjusted_close` (or `adj_*`). Adjusted
requests select these stored values without recalculating corporate actions.
Missing adjusted prices raise an error. Volume retains the source convention;
ordinary VWAP is omitted for adjusted requests. Optional `dividend` and
`split_ratio` match Yahoo's result fields; missing actions remain unknown.
`include_actions=False` removes action values from results. Historical source
and observation/availability metadata in extra columns are retained.

The deployed market-store `equity_historical` view aliases `session_date AS date`
and selects one latest stored revision per symbol/date. `limit` defaults to
100,000 rows (maximum 1,000,000); exceeding it raises rather than truncates.
The provider honors an existing adjacent `.market.lock` file when opening the
database, coordinating with the data platform's publisher. Querying a general
DuckDB file without that lock remains supported. Unsupported intraday or
extended-session requests fail validation rather than being silently ignored.

```python
from openbb import obb

prices = obb.equity.price.historical(
    symbol="AAPL,MSFT",
    start_date="2026-09-08",
    end_date="2026-09-16",
    interval="1d",
    adjustment="splits_and_dividends",
    include_actions=True,
    provider="duckdb",
).to_df()
```

This reads collected data only. Missing stocks or dates are not downloaded from
Yahoo automatically. It does not imply full historical coverage or availability
before the recorded collection time.

This extension makes historical market data in a local
[DuckDB](https://duckdb.org/) database available through standard OpenBB
endpoints. Connections are opened in read-only mode.

## Installation

Install the extension from this directory with:

```bash
pip install -e .
openbb-build
```

Once published, it can be installed with:

```bash
pip install openbb-duckdb
openbb-build
```

## Database configuration

Pass the database file on each request:

```python
from openbb import obb

result = obb.equity.price.historical(
    "AAPL,MSFT",
    provider="duckdb",
    database_path="/data/market.duckdb",
)
```

Alternatively, store it once as `duckdb_database_path` in OpenBB user settings:

```json
{
  "credentials": {
    "duckdb_database_path": "/data/market.duckdb"
  }
}
```

The per-request `database_path` takes precedence over the configured value.

## Table contract

The provider uses these default tables:

| OpenBB endpoint | Default table |
| --- | --- |
| `obb.equity.price.historical` | `equity_historical` |
| `obb.etf.historical` | `etf_historical` |
| `obb.index.price.historical` | `index_historical` |
| `obb.currency.price.historical` | `currency_historical` |
| `obb.crypto.price.historical` | `crypto_historical` |

Use the `table` parameter to select another table or view. Qualified names and
quoted identifiers are supported, for example
`table="market_data.equity_prices"`.

Every relation must contain case-insensitive `symbol` and `date` columns.
Equity and ETF relations must also contain `open`, `high`, `low`, and
`close`. Index, currency, and crypto relations require `close`. The
`volume` and `vwap` columns are optional, and additional columns are
preserved in the OpenBB result.

```sql
CREATE TABLE equity_historical (
    symbol VARCHAR NOT NULL,
    date DATE NOT NULL,
    open DOUBLE NOT NULL,
    high DOUBLE NOT NULL,
    low DOUBLE NOT NULL,
    close DOUBLE NOT NULL,
    volume BIGINT,
    vwap DOUBLE
);
```

Symbol matching is case-insensitive. Comma-separated symbols and inclusive
`start_date` and `end_date` filters are applied inside DuckDB.

## Derivatives

Install the `openbb-derivatives` router package alongside this provider and run
`openbb-build` to expose these commands:

| OpenBB endpoint | Default table | Required columns |
| --- | --- | --- |
| `obb.derivatives.options.chains` | `options_chains` | `underlying_symbol` or `underlying`, plus `eod_date, expiration, strike, option_type` |
| `obb.derivatives.futures.historical` | `futures_historical` | `symbol, date, contract_symbol, expiration, close` |
| `obb.derivatives.futures.curve` | `futures_curve` | `symbol, date, contract_symbol, expiration, price` |

These are row-oriented daily tables or views; use DATE columns for observation
and expiration dates. Options filter by `underlying_symbol`, falling back to
`underlying` if absent. Missing `underlying_symbol` and `contract_symbol` are
returned as aligned nulls; existing values are preserved, and identifiers
are not synthesized. Use `table="option_chain"` for the local collector table.
Symbol-dependent analytics may be unavailable while identifiers are missing.
Use exchange-qualified contract identifiers where needed to avoid collisions.
Stored option sides accept `C`/`P` and `call`/`put`, case-insensitively with
surrounding whitespace ignored. Output and filters normalize to `call`/`put`.
Optional standard columns include
bid/ask, last price, volume, open interest, implied volatility and Greeks for
options, and OHLC, volume and open interest for futures. Missing optional values
remain missing, not zero. Extra columns, including currency, contract multiplier,
source and availability timestamps, are retained.

The input must contain one selected revision per observation date and contract
within each underlying/root. Deduplicate upstream; do not mix intraday snapshots,
vendors or historical revisions in these daily views. Session dates and timezones
must already be normalized by ingestion. These endpoints do not perform
availability-time filtering and are not a point-in-time-safe revision store.
Keep raw revisions and entitlement/lineage records in the data platform.

Prices and currencies are not converted, adjusted or rolled. Implied volatility
must already use the standard decimal convention (0.20 means 20%). Futures
quote conventions are preserved (for example, SR3 96.3 remains 96.3, not 3.7%).
The provider does not calculate implied volatility, Greeks or interpolated curves.
Missing option DTE is derived from expiration minus the snapshot date.

Options return a standard column-oriented chain object. Omitting `date` selects
the latest stored date for the requested underlying, before expiration/side filters.
An explicit date is exact, with no fallback. Futures curves behave similarly and
also accept comma-separated dates; only matching dates are returned. Neither
snapshot endpoint proves that the upstream snapshot includes every listed contract.

Futures history accepts comma-separated symbols, inclusive date bounds, and an
optional `expiration="YYYY-MM"` filter. Symbols match the stored root or symbol;
individual contracts are retained separately, with no continuous-series construction.

All derivatives queries are read-only and bounded: `limit` defaults to 100,000
rows (maximum 1,000,000). Exceeding it raises an error instead of returning a partial
chain or curve. An empty selection raises an empty-data error.

```python
from openbb import obb

db = "/data/market.duckdb"
chain = obb.derivatives.options.chains(
    symbol="SPX",
    date="2026-09-02",
    expiration="2026-09-18",
    provider="duckdb",
    database_path=db,
)
history = obb.derivatives.futures.historical(
    symbol="SR3",
    start_date="2026-09-01",
    end_date="2026-09-02",
    expiration="2026-09",
    provider="duckdb",
    database_path=db,
)
curve = obb.derivatives.futures.curve(
    symbol="SR3",
    date="2026-09-02",
    provider="duckdb",
    database_path=db,
)
```

A curve may be exposed as a view of history when the chosen quote is appropriate:

```sql
CREATE VIEW futures_curve AS
SELECT symbol, date, contract_symbol, expiration, close AS price
FROM futures_historical;
```

Validation uses synthetic local snapshots; vendor ingestion, snapshot completeness
and credentialed/live market feeds are outside this provider's offline checks.

### BaoStock A-share prices

Version 1.2.0 adds explicit `adjustment="unadjusted"`, `"forward"`, and `"backward"`
for stored BaoStock series. The data platform exposes these through
`table="a_share.equity_historical"` with symbols such as `SH.600000` and
`SZ.000001`. Unadjusted reads require the relation's `base_adjustment` declaration;
forward/backward reads require the corresponding saved OHLC columns. Missing
prices remain null only for source-marked suspensions. Existing U.S. split-only
and split-and-dividend modes retain their contracts. BaoStock adjustment factors
are not inferred cash dividends or split events.

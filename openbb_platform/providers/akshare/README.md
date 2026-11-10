# OpenBB AKShare Provider

AKShare data provider for the OpenBB Platform, focused on mainland China
futures data (CFFEX, SHFE, DCE, CZCE, INE, GFEX) and the other source datasets
exposed by the `akshare` library.

## Installation

```bash
pip install openbb-akshare
```

## Coverage

Standard endpoint:

- `obb.futures.historical(symbol="IF0", provider="akshare")` — continuous or
  dated China futures daily bars.

Native commands (source-shaped rows, one command per AKShare interface):

- `obb.akshare.cffex_daily(date="2026-09-18")` — CFFEX daily quotes for every
  listed contract.
- `obb.akshare.futures_hist_daily_cffex(date="2026-09-18")`
- `obb.akshare.cffex_rank_table(date="2026-09-18", vars_list="IF,T")`
- `obb.akshare.futures_zh_daily_sina(symbol="IF0")`
- `obb.akshare.futures_zh_minute_sina(symbol="IF0", period="5")`
- `obb.akshare.stock_zh_index_daily(symbol="sh000300")` — daily bars for one Chinese
  index. Symbols are the lower-case Sina form (`sh000300`, `sz399001`).
- `obb.akshare.futures_main_sina(symbol="IF0", start_date=..., end_date=...)` — the
  main/continuous contract over a bounded window (source columns are Chinese).
- `obb.akshare.bond_zh_us_rate(start_date="2026-01-01")` — China and US government
  bond yields by tenor plus GDP growth (source columns are Chinese).

## Bounded option quotes

`obb.akshare.etf_option_quotes(symbols="10012535,10012536")` requests up to
50 exact eight-digit Sina option identifiers. The source-reported underlying code
does not independently establish exchange or security identity. The current
51-field parser retains expiration, option type and raw appended fields. Missing
and malformed rows remain explicit diagnostics, and source time is distinct from
fetch time. Contract discovery remains available through `etf_option_contracts`;
CFFEX models and collectors retain their existing interfaces.

The registered fetcher remains in `models.etf_options`. The older
`models.etf_option_quotes` path re-exports those same classes; both quote utility
paths share the current parser and bounded transport.

The source is a direct Sina batch HTTP request, based on the source used by
AKShare's single-symbol function; its batch protocol is not independently
verified. `raw_payload` is the GB18030-decoded assignment value, not the HTTP
wire bytes. The transport requests identity encoding and rejects all redirects
and any other content encoding, including gzip. Both encoded and decoded body
sizes are limited to 1,000,000 bytes (they are identical for identity encoding).
OpenBB Requests settings govern TLS, proxy, headers and timeout. Global auth is
accepted only when absent, a two-element string/bytes tuple, or an exact Requests
HTTPBasicAuth instance; Digest and custom auth (including Basic subclasses) fail
closed before sending. Existing response hooks are also rejected. Other OpenBB
commands retain their usual global authentication behavior;
the configured HTTP timeout overrides the 25-second default. The timeout is a
socket inactivity timeout, not a hard total deadline: a slow trickle can take
longer, and DNS and other platform behavior can also exceed it. No hard wall-clock
deadline is promised. Synthetic loopback tests do not establish upstream
compatibility; live status remains `live_not_tested`.

## Conventions

- Dates are entered as `YYYY-MM-DD` and converted to the `YYYYMMDD` form the
  source requires.
- Futures symbols are the exchange/Sina roots (`IF0`, `RB0`) or dated contracts
  (`IF2610`), upper-cased and validated before a request is made.
- Native commands return all source columns with `NaN` normalized to `null`;
  values are not unit-converted, so reported units are the provider's own.
- `date` is required on exchange-wide datasets: AKShare defaults to a hardcoded
  historical date, which would silently return stale rows.
- Native datasets keep the source's date representation (CFFEX daily data uses the
  `YYYYMMDD` string form). `to_df()` uses a column named `date` as the index by
  default; pass `index=None` to keep it as a column.

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

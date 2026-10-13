# AKShare Provider Coverage

Scope of this provider: **mainland China futures, index, and sovereign-rate**
datasets whose symbol, date, and column conventions have been modelled
explicitly. Symbol normalization, identifier handling, and the parameter
contract are per-family, so AKShare datasets outside these families stay out
of scope until their own conventions are modelled.

## Standard endpoints

| Endpoint | Provider fetcher | AKShare interface | Status |
| --- | --- | --- | --- |
| `obb.futures.historical(symbol=..., provider="akshare")` | `FuturesHistorical` | `futures_zh_daily_sina` | verified live |

`FuturesHistorical` accepts a futures root (`IF0`, `RB0`) or a dated contract
(`IF2610`). `IF0`/`RB0` are Sina **dominant-contract** series: verified on
2026-10-13 that `IF0` reproduced the CFFEX `IF2612` row exactly
(open 4183.2 / high 4257.4 / low 4151.0 / close 4217.6 / volume 78395 /
open interest 178602). Settlement is `0.0` because Sina does not publish it.

## Native commands

Source-shaped rows, one command per catalogued interface, exposed under the
`akshare` router.

| Command | AKShare interface | Parameters | Status |
| --- | --- | --- | --- |
| `cffex_daily` | `get_cffex_daily` | `date` | verified live (28 CFFEX futures rows) |
| `futures_hist_daily_cffex` | `futures_hist_daily_cffex` | `date` | verified live |
| `cffex_rank_table` | `get_cffex_rank_table` | `date`, `vars_list` | verified live (per-contract mapping flattened) |
| `futures_zh_daily_sina` | `futures_zh_daily_sina` | `symbol` | verified live |
| `futures_zh_minute_sina` | `futures_zh_minute_sina` | `symbol`, `period` | verified live |
| `stock_zh_index_daily` | `stock_zh_index_daily` | `symbol` | verified live |
| `futures_main_sina` | `futures_main_sina` | `symbol`, `start_date`, `end_date` | verified live |
| `bond_zh_us_rate` | `bond_zh_us_rate` | `start_date` | verified live |

Notes:

- **Symbol case is per-family.** Futures roots are upper-cased (`if2610` -> `IF2610`),
  but Sina index symbols are lower-case and exchange-qualified (`sh000300`), so they
  use a separate normalizer. Routing an index symbol through the futures normalizer
  would corrupt it.
- **Chinese-column sources keep their own identifier.** `futures_main_sina` and
  `bond_zh_us_rate` expose `日期` as the identifier column, matching the source
  rather than inventing an English alias.
- `get_cffex_rank_table` returns a mapping of contract to dataframe; the
  provider flattens it into one row list, each row carrying its own `symbol`.
- `date` is required on exchange-wide datasets because AKShare's own default is
  a hardcoded historical date (`20100416`); relying on it would silently return
  stale rows.
- `vars_list` is a comma-separated string (`"IF,T"`), not a list literal: OpenBB
  joins list-valued parameters into a comma-separated string before they reach
  the query model. The provider splits and validates it before calling AKShare.
- `get_cffex_daily` returns futures only. For index options (IO/HO/MO) use the
  CFFEX daily XML or a different interface.

## Date handling

`to_df()` defaults to `index="date"`, so a column literally named `date` becomes the
DataFrame index — call `.to_df(index=None)` to keep it as a column. Native datasets
preserve the source's own date representation rather than parsing it:

- `stock_zh_index_daily`, `futures_zh_daily_sina` — `datetime.date` values.
- `cffex_daily`, `futures_hist_daily_cffex` — the source string form `YYYYMMDD`.
- `futures_main_sina`, `bond_zh_us_rate` — `日期`, which is not the default index
  name, so it stays a column.

For a parsed `DatetimeIndex` on the CFFEX datasets, convert explicitly:
`pd.to_datetime(df.index, format="%Y%m%d")`.

## Not covered

- Other AKShare families (equities, funds, options, macro, news) are not modelled
  yet; each needs its own symbol and column conventions.
- `bond_zh_us_rate` fetches roughly 19 pages and emits a tqdm progress bar on
  stderr, so a full-history call takes about 25 seconds.

Cannot be verified without credentials or a brokerage connection: real-time
tick/level-2 sources and anything requiring an authenticated session.

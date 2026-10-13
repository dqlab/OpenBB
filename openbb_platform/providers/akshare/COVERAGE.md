# AKShare Provider Coverage

Scope of this provider: **mainland China futures**. Symbol normalization,
identifier handling, and the parameter contract are built around the exchange
and Sina futures conventions, so non-futures AKShare datasets are deliberately
out of scope until their own conventions are modelled.

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

Notes:

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

## Not covered

Verified as callable but intentionally excluded until their conventions are
modelled:

- `stock_zh_index_daily` — uses lower-case Sina index symbols (`sh000300`),
  which the futures symbol normalizer would corrupt.
- `futures_main_sina` — source columns are Chinese (`日期`, `开盘价`, ...).
- `bond_zh_us_rate` — Chinese columns plus a slow paginated fetch.

Cannot be verified without credentials or a brokerage connection: real-time
tick/level-2 sources and anything requiring an authenticated session.

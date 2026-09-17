# BaoStock data API coverage

This inventory covers every `query_*` function in BaoStock SDK 0.9.3:
**26 main-module exports and 14 additional SDK-module APIs**.
All rows have provider-backed OpenBB commands, validated query schemas,
paginated extraction, and source-preserving results. No dataset is hidden
behind an arbitrary method-name dispatcher.

The provider also retains the normalized equity history, index history,
stock search/profile, and index constituents commands described in [README.md](README.md).

| OpenBB command | SDK function | Parameters | SDK exposure |
| --- | --- | --- | --- |
| `obb.baostock.history_k_data_plus` | `query_history_k_data_plus` | `code` (required), `start_date`, `end_date`, `fields` (required), `frequency`, `adjustflag` | Main module |
| `obb.baostock.daily_history_k_astock` | `query_daily_history_k_AStock` | `date` | Main module |
| `obb.baostock.daily_history_k_etf` | `query_daily_history_k_ETF` | `date` | Main module |
| `obb.baostock.stock_basic` | `query_stock_basic` | `code`, `code_name` | Main module |
| `obb.baostock.trade_dates` | `query_trade_dates` | `start_date`, `end_date` | Main module |
| `obb.baostock.all_stock` | `query_all_stock` | `day` | Main module |
| `obb.baostock.stock_industry` | `query_stock_industry` | `code`, `date` | Main module |
| `obb.baostock.hs300_stocks` | `query_hs300_stocks` | `date` | Main module |
| `obb.baostock.sz50_stocks` | `query_sz50_stocks` | `date` | Main module |
| `obb.baostock.zz500_stocks` | `query_zz500_stocks` | `date` | Main module |
| `obb.baostock.dividend_data` | `query_dividend_data` | `code` (required), `year`, `year_type` | Main module |
| `obb.baostock.adjust_factor` | `query_adjust_factor` | `code` (required), `start_date`, `end_date` | Main module |
| `obb.baostock.daily_adjust_factor` | `query_daily_adjust_factor` | `date` | Main module |
| `obb.baostock.profit_data` | `query_profit_data` | `code` (required), `year`, `quarter` | Main module |
| `obb.baostock.operation_data` | `query_operation_data` | `code` (required), `year`, `quarter` | Main module |
| `obb.baostock.growth_data` | `query_growth_data` | `code` (required), `year`, `quarter` | Main module |
| `obb.baostock.balance_data` | `query_balance_data` | `code` (required), `year`, `quarter` | Main module |
| `obb.baostock.cash_flow_data` | `query_cash_flow_data` | `code` (required), `year`, `quarter` | Main module |
| `obb.baostock.dupont_data` | `query_dupont_data` | `code` (required), `year`, `quarter` | Main module |
| `obb.baostock.performance_express_report` | `query_performance_express_report` | `code` (required), `start_date`, `end_date` | Main module |
| `obb.baostock.forecast_report` | `query_forecast_report` | `code` (required), `start_date`, `end_date` | Main module |
| `obb.baostock.deposit_rate_data` | `query_deposit_rate_data` | `start_date`, `end_date` | Main module |
| `obb.baostock.loan_rate_data` | `query_loan_rate_data` | `start_date`, `end_date` | Main module |
| `obb.baostock.required_reserve_ratio_data` | `query_required_reserve_ratio_data` | `start_date`, `end_date`, `year_type` | Main module |
| `obb.baostock.money_supply_data_month` | `query_money_supply_data_month` | `start_date`, `end_date` | Main module |
| `obb.baostock.money_supply_data_year` | `query_money_supply_data_year` | `start_date`, `end_date` | Main module |
| `obb.baostock.terminated_stocks` | `query_terminated_stocks` | `date` | SDK submodule |
| `obb.baostock.suspended_stocks` | `query_suspended_stocks` | `date` | SDK submodule |
| `obb.baostock.st_stocks` | `query_st_stocks` | `date` | SDK submodule |
| `obb.baostock.starst_stocks` | `query_starst_stocks` | `date` | SDK submodule |
| `obb.baostock.stock_concept` | `query_stock_concept` | `code`, `date` | SDK submodule |
| `obb.baostock.stock_area` | `query_stock_area` | `code`, `date` | SDK submodule |
| `obb.baostock.ame_stocks` | `query_ame_stocks` | `date` | SDK submodule |
| `obb.baostock.gem_stocks` | `query_gem_stocks` | `date` | SDK submodule |
| `obb.baostock.shhk_stocks` | `query_shhk_stocks` | `date` | SDK submodule |
| `obb.baostock.szhk_stocks` | `query_szhk_stocks` | `date` | SDK submodule |
| `obb.baostock.stocks_in_risk` | `query_stocks_in_risk` | `date` | SDK submodule |
| `obb.baostock.cpi_data` | `query_cpi_data` | `start_date`, `end_date` | SDK submodule |
| `obb.baostock.ppi_data` | `query_ppi_data` | `start_date`, `end_date` | SDK submodule |
| `obb.baostock.pmi_data` | `query_pmi_data` | `start_date`, `end_date` | SDK submodule |

## Source interpretation

- The native surface preserves exact source names and values, including blank
  values and decimal strings. Monetary/rate units are source-defined.
- Profit, operation, growth, balance, cash-flow, and DuPont datasets are source
  indicator tables. In particular, balance indicators are not a full balance sheet.
- Dividend data includes proposals, announcement/registration/payment dates,
  cash distributions, and stock components. The native endpoint does not discard
  a proposal because it has no ex-dividend date yet.
- `pubDate` and `statDate` are distinct. Corporate reports, forecasts, and reserve
  ratios retain the source date-selection semantics, without assuming point-in-time availability.
- `stock_basic()` preserves every returned security type. Standard equity
  search/profile intentionally select stocks only.
- `history_k_data_plus` exposes the SDK field-selection and frequency parameters.
  Select fields supported by the requested security and interval; source validation
  errors are surfaced rather than replaced with invented values.
- Standard index constituents accept `HS300`, `SZ50`, `ZZ500`, or their supported
  exchange-qualified codes. Native membership methods expose the corresponding source calls.
- SDK-module APIs are callable but may be unavailable on the public server.
  A timeout or source rejection is not a missing adapter, and an empty date window
  is not proof that a dataset is unavailable.

Login/logout and API-key session configuration are not data APIs. The provider
manages anonymous sessions. Coverage is checked against the installed SDK in
`tests/test_native.py`; new SDK functions require an explicit catalog/schema update.

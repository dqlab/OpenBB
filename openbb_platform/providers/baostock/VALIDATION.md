# BaoStock 1.1.0 validation

Validated on 2026-09-17 in an isolated Python 3.12.3 environment using
OpenBB Core 1.6.13 and BaoStock SDK 0.9.3.

All **169 offline tests passed**. Ruff, the rebuilt OpenBB interface, and the
`openbb-baostock` 1.1.0 wheel build passed validation.

- Installed the provider and its router, ran `openbb-build`, and exercised every
  `obb.baostock` command plus the five standard integrations.
- Offline checks compare every SDK query and its complete parameter list with
  the catalog, validate source argument mapping, preserve source values, test
  empty/error responses, and exercise pagination and session cleanup/concurrency.
- Saved live source samples are replayed offline. The fixture contains at most
  two records per successful query, with query inputs, retrieval time, and total
  response count; it does not claim to contain full datasets.

## Live service results

All **26 main-module APIs completed successfully** with data or a valid empty
response for the chosen window. Across all 40 APIs, **25 returned records, five
returned empty results, and ten additional SDK-module APIs were rejected by the
server with error `10004020` (unknown message type)**.

The first sweep used a 12-second process deadline. `all_stock` and daily A-share
bars exceeded that deadline; follow-up checks with a 40-second deadline succeeded.
Daily ETF bars returned no data for 2024-01-02 but returned 1,667 rows for
2026-09-16. The table reports the final bounded samples, with dates shown explicitly.
These checks establish availability for those inputs, not completeness for every
security or historical period.

| Command under `obb.baostock` | Input window / snapshot | Result | Records |
| --- | --- | --- | ---: |
| `history_k_data_plus` | `code=sz.000651, start_date=2024-01-02, end_date=2024-01-05` | Data returned | 4 |
| `daily_history_k_astock` | `date=2026-09-16` | Data returned | 5220 |
| `daily_history_k_etf` | `date=2026-09-16` | Data returned | 1667 |
| `stock_basic` | `code=sz.000651` | Data returned | 1 |
| `trade_dates` | `start_date=2024-01-01, end_date=2024-03-31` | Data returned | 91 |
| `all_stock` | `day=2024-01-02` | Data returned | 5638 |
| `stock_industry` | `code=sz.000651, date=2024-01-02` | Data returned | 1 |
| `hs300_stocks` | `date=2024-01-02` | Data returned | 300 |
| `sz50_stocks` | `date=2024-01-02` | Data returned | 50 |
| `zz500_stocks` | `date=2024-01-02` | Data returned | 500 |
| `dividend_data` | `code=sz.000651, year=2023, year_type=report` | Data returned | 1 |
| `adjust_factor` | `code=sz.000651, start_date=2024-01-01, end_date=2024-03-31` | Successful empty response | 0 |
| `daily_adjust_factor` | `date=2024-01-02` | Data returned | 1 |
| `profit_data` | `code=sz.000651, year=2024, quarter=1` | Data returned | 1 |
| `operation_data` | `code=sz.000651, year=2024, quarter=1` | Data returned | 1 |
| `growth_data` | `code=sz.000651, year=2024, quarter=1` | Data returned | 1 |
| `balance_data` | `code=sz.000651, year=2024, quarter=1` | Data returned | 1 |
| `cash_flow_data` | `code=sz.000651, year=2024, quarter=1` | Data returned | 1 |
| `dupont_data` | `code=sz.000651, year=2024, quarter=1` | Data returned | 1 |
| `performance_express_report` | `code=sz.000651, start_date=2024-01-01, end_date=2024-03-31` | Successful empty response | 0 |
| `forecast_report` | `code=sz.000651, start_date=2024-01-01, end_date=2024-03-31` | Successful empty response | 0 |
| `deposit_rate_data` | `start_date=2015-01-01, end_date=2015-12-31` | Data returned | 5 |
| `loan_rate_data` | `start_date=2015-01-01, end_date=2015-12-31` | Data returned | 5 |
| `required_reserve_ratio_data` | `start_date=2024-01-01, end_date=2024-03-31, year_type=0` | Successful empty response | 0 |
| `money_supply_data_month` | `start_date=2024-01, end_date=2024-03` | Data returned | 3 |
| `money_supply_data_year` | `start_date=2023, end_date=2023` | Data returned | 1 |
| `terminated_stocks` | `date=2024-01-02` | Data returned | 234 |
| `suspended_stocks` | `date=2024-01-02` | Successful empty response | 0 |
| `st_stocks` | `date=2024-01-02` | Data returned | 64 |
| `starst_stocks` | `date=2024-01-02` | Data returned | 52 |
| `stock_concept` | `code=sz.000651, date=2024-01-02` | Source error 10004020 | — |
| `stock_area` | `code=sz.000651, date=2024-01-02` | Source error 10004020 | — |
| `ame_stocks` | `date=2024-01-02` | Source error 10004020 | — |
| `gem_stocks` | `date=2024-01-02` | Source error 10004020 | — |
| `shhk_stocks` | `date=2024-01-02` | Source error 10004020 | — |
| `szhk_stocks` | `date=2024-01-02` | Source error 10004020 | — |
| `stocks_in_risk` | `date=2024-01-02` | Source error 10004020 | — |
| `cpi_data` | `start_date=2024-01-01, end_date=2024-03-31` | Source error 10004020 | — |
| `ppi_data` | `start_date=2024-01-01, end_date=2024-03-31` | Source error 10004020 | — |
| `pmi_data` | `start_date=2024-01-01, end_date=2024-03-31` | Source error 10004020 | — |

The successful responses contained **13844 records** in total. Native adapters
do not filter or deduplicate source rows; all records and columns were returned.
The copied samples reconcile input and output counts exactly and preserve source
types and values. They establish parsing fidelity, not calendar completeness or
point-in-time revision history.

Rejected SDK-module APIs remain callable wrappers so a changed server capability
does not require inventing a different interface. Their original error codes are
surfaced; the provider cannot enable a message type rejected by BaoStock.

For reproducibility, see [the API inventory](COVERAGE.md),
[usage and data contracts](README.md), and `tests/live_probe.py`.

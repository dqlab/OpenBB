# OpenBB BaoStock Provider

An installable `openbb-baostock` provider using the official [BaoStock Python SDK](https://pypi.org/project/baostock/).
Version 1.1.0 exposes **all 40 data-query functions in SDK 0.9.3**, including all
26 main-module exports and 14 additional SDK-module APIs. Anonymous sessions
require no OpenBB credentials or API key; source service availability is reported
separately from wrapper coverage.

The complete source API is available under `obb.baostock`. Five normalized
commands also integrate with the usual equity and index routers. See the
[full API coverage and parameter reference](COVERAGE.md).
The [1.1.0 validation report](VALIDATION.md) lists live results for every query,
including the ten SDK-module APIs currently rejected by BaoStock's server.

From the repository root, in the Python environment that runs OpenBB:

```bash
pip install -e openbb_platform/providers/baostock
openbb-build
```

The repository's development installer also includes this provider. The standalone
provider requires `openbb-core`; install the `openbb-equity` and `openbb-index`
router packages if they are not already present in your OpenBB environment.

| OpenBB command | BaoStock source | Supported queries |
| --- | --- | --- |
| `obb.equity.price.historical` | `query_history_k_data_plus` | Daily, weekly, monthly, 5/15/30/60 minute bars |
| `obb.index.price.historical` | `query_history_k_data_plus` | Daily, weekly, monthly unadjusted bars |
| `obb.equity.search` | `query_stock_basic` | Company name, exact or partial symbol, stock listings |
| `obb.equity.profile` | `query_stock_basic` | Basic listing metadata for one or more stocks |
| `obb.index.constituents` | `query_hs300_stocks`, `query_sz50_stocks`, `query_zz500_stocks` | Dated/latest CSI 300, SSE 50, or CSI 500 membership |

```python
from openbb import obb

prices = obb.equity.price.historical(
    symbol="sh.600000,sz.000001",
    start_date="2024-01-02",
    end_date="2024-01-05",
    interval="1d",
    adjustment="unadjusted",
    provider="baostock",
)
intraday = obb.equity.price.historical(
    symbol="600000.SH",
    start_date="2024-01-02",
    end_date="2024-01-02",
    interval="5m",
    provider="baostock",
)
index = obb.index.price.historical(
    symbol="sh.000001", start_date="2024-01-02", end_date="2024-01-05",
    provider="baostock",
)
stocks = obb.equity.search(query="浦发", provider="baostock")
matches = obb.equity.search(query="6000", is_symbol=True, limit=20, provider="baostock")
profiles = obb.equity.profile(symbol="sh.600000,sz.000001", provider="baostock")
members = obb.index.constituents(symbol="HS300", date="2024-01-02", provider="baostock")
```

## Complete source API

Native commands retain every source column and its value, including financial
precision, Chinese text, security types, and publication/reporting dates:

```python
profit = obb.baostock.profit_data(code="sz.000651", year=2024, quarter=1)
cash_flow = obb.baostock.cash_flow_data(code="sz.000651", year=2024, quarter=1)
dividends = obb.baostock.dividend_data(code="sz.000651", year=2023, year_type="report")
calendar = obb.baostock.trade_dates(start_date="2024-01-01", end_date="2024-01-31")
money = obb.baostock.money_supply_data_month(start_date="2024-01", end_date="2024-03")
etfs = obb.baostock.daily_history_k_etf(date="2024-01-02")
all_security_types = obb.baostock.stock_basic()

# Select any source-supported history fields, including valuation indicators.
bars = obb.baostock.history_k_data_plus(
    code="sh.600000", fields="date,code,close,peTTM,pbMRQ",
    start_date="2024-01-02", end_date="2024-01-05",
    frequency="d", adjustflag="3",
)
frame = profit.to_df()
```

Native command names are SDK names without `query_`, in lowercase. The one input
renaming is `yearType` → `year_type`. Every SDK input remains available. Native
`code` accepts one exchange-qualified security; optional blank/omitted inputs
retain SDK defaults. No implicit full-market query is made for a missing required
code. Monthly money-supply windows use `YYYY-MM`, annual windows use `YYYY`, and
other date windows use `YYYY-MM-DD`. Invalid or reversed windows, invalid quarter
numbers, unknown inputs, and protocol delimiters fail before network access.

Native records preserve source column spelling (`pubDate`, `statDate`, `roeAvg`,
etc.) and values without numeric conversion or scaling. Blank source values stay
blank; missing columns remain absent. Extra columns added by the source are
retained. Source units differ across financial and macroeconomic series, so no
common percent or monetary multiplier is assumed. `.results` preserves source
values; OpenBB's `.to_df()` may convert a column named `date` to its usual date
index. Native successful empty responses return an empty list, and provider
failures retain the BaoStock error code. Calls read all pages without a hidden
result limit; pass explicit dates for reproducible bounded retrieval.

SDK-module APIs include listing-status/board/Stock Connect classifications and
CPI/PPI/PMI. Their presence in the SDK does not guarantee that the public server
currently serves them. These APIs remain accessible with explicit source errors.
Login/logout and `set_API_key` are session controls, not datasets, and are not
exposed as data commands. Sessions are managed by the provider.

## Normalized data contract

- History and profiles require exchange-qualified symbols: `sh.600000`,
  `sz.000001`, `600000.SH`, `600000.SS`, or `000001.SZ`. Results use uppercase
  `SH.600000`/`SZ.000001`; bare numbers are rejected because `000001` can mean
  a stock or an index. Repeated aliases in a request are fetched once.
- Dates are inclusive. The default end date is today in `Asia/Shanghai`, and the
  default start date is 365 days before the end date. Reversed ranges are rejected.
  Daily/weekly/monthly bars retain the source trading date. Intraday `date` is
  the source bar-end time with `Asia/Shanghai` timezone. Weekly/monthly periods
  are supplied by BaoStock, not resampled locally.
- `adjustment="unadjusted"` maps to BaoStock `3`, `"forward"` (前复权, qfq) to
  `2`, and `"backward"` (后复权, hfq) to `1`. The returned adjustment flag must
  match the request. OHLC fields contain that series; no additional adjustment
  or separate adjusted-close series is synthesized. Indices accept only
  `unadjusted`. Adjusted historical values can change after corporate actions.
- Equity prices and `amount` retain the security's trading currency (CNY for
  A shares); `volume` retains the source share count. Index OHLC are index
  points, with source aggregate volume and CNY trading value. No FX conversion
  or volume multiplication is applied. `pctChg` → `change_percent` and
  `turn` → `turnover_rate` are divided by 100: `0.01` means 1%.
- Empty strings become `None`, never zero. Suspended rows remain in the output;
  `trade_status=0` means suspended and `1` means normal trading. Fields unavailable
  for a frequency remain null. No gap filling, trading-calendar inference, or
  reconstruction of delisted symbols is performed.
- Stock search/profile exclude index, ETF, and other security types. Listing and
  delisting dates and current listing status are retained. Full company
  descriptions, sectors, and financial statements are unavailable from this
  basic endpoint and remain unset. Partial-symbol or empty searches download
  the source metadata list before applying the result limit.
- History is sorted by date and symbol. Duplicate `(symbol, date)` rows,
  out-of-window records, symbol mismatches, malformed rows, adjustment mismatches,
  and failed pages raise errors. An empty history/profile for any requested
  symbol raises `EmptyDataError`; searches with no matches return an empty list.
  Multi-symbol requests never silently succeed with a missing symbol.

## Sessions, availability, and lineage

The SDK uses its own TCP protocol, not HTTP, so OpenBB HTTP proxy and timeout
settings do not apply. SDK calls run in a worker thread, with a process-wide lock
covering login, all queries/pages, and logout. The socket is closed on errors.
Direct use of `baostock` by other application code is outside this lock; avoid
mixing it with this provider in the same process. The upstream SDK has no
configurable request timeout; deployments needing a hard deadline should isolate
requests in a process that can be terminated. Cancelling an async request does
not terminate the SDK thread or release its lock early.

BaoStock error codes are preserved in OpenBB errors. There are no implicit retries
or writes, so callers choose their retry policy. Provider pagination is fully
consumed, including detection of the SDK's silent full-page transport failure.

Source: BaoStock, SDK `>=0.9.3,<0.10`. The price observation key is
`(provider, symbol, interval, adjustment, date)`. BaoStock supplies neither a
publication/availability timestamp nor a revision identifier for price history
and basic metadata. Financial reports can supply `pubDate`, `statDate`, and other
publication, reporting, or event dates; native commands preserve each distinctly.
Those dates do not supply revision history or intraday availability, and date
filters must not be treated as an as-of reconstruction. These are latest-retrieved
observations, **not point-in-time-safe datasets**.
Consumers persisting data should retain request parameters, retrieval time, raw
responses, and their own immutable revision identity. This provider does not
publish a canonical dataset or authorize redistribution of the source data.

API references: [historical bars](https://www.baostock.com/mainContent?file=stockKData.md),
[basic securities](https://www.baostock.com/mainContent?file=stockBasic.md).

## Validation

Offline tests use synthetic SDK responses to verify field mapping, missing values,
adjustments, dates, pagination, session cleanup, and concurrency. Coverage tests
compare the inventory and parameters against the installed SDK, call every
native command through OpenBB, and verify lossless data and dataframe conversion.
They require no network or credentials:

```bash
python -m pytest openbb_platform/providers/baostock/tests
python -m ruff check openbb_platform/providers/baostock
```

The separately invoked live probe runs each query in a process with a hard
deadline, retains at most two source sample rows plus total count, and records
success, empty data, source errors, and timeouts separately. It refuses to
overwrite earlier samples:

```bash
python openbb_platform/providers/baostock/tests/live_probe.py \
  --output /tmp/baostock-live --timeout 15
```

Live availability depends on access to BaoStock's TCP service. A live check is
separate from these tests; source coverage and trading-calendar completeness
cannot be established by synthetic fixtures.

The initial 1.0.0 validation on 2026-09-17 used Python 3.12.3, OpenBB Core 1.6.13, and BaoStock 0.9.3
in an isolated environment. All 41 offline tests passed, including calls through
the rebuilt public OpenBB interface. Ruff and the wheel build also passed.
Anonymous live requests through that interface returned:

| Sample | Window | Rows |
| --- | --- | ---: |
| `SH.600000` daily, unadjusted | 2024-01-02 to 2024-01-05 | 4 |
| `SH.000001` index daily | 2024-01-02 to 2024-01-05 | 4 |
| Stock search / profile for 浦发银行 | Current basic metadata | 1 each |
| `SH.600000` five-minute, unadjusted | 2024-01-02 | 48 |
| `SH.600000` weekly, forward adjusted | 2024-01-01 to 2024-01-12 | 2 |
| `SH.600000` monthly, backward adjusted | 2024-01-01 to 2024-02-29 | 2 |
| `SH.000001` index weekly | 2024-01-01 to 2024-01-12 | 2 |
| `SH.000001` index monthly | 2024-01-01 to 2024-02-29 | 2 |

All 64 live history rows passed the symbol, date-window, adjustment, and duplicate
checks; no rows were rejected. These bounded checks do not establish full-market
coverage. The 15/30/60 minute variants and failure paths were tested offline.

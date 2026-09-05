# OpenBB DuckDB Provider

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

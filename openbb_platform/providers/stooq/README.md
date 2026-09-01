# OpenBB Stooq Provider

This extension integrates [Stooq](https://stooq.com/) historical market data
with the OpenBB Platform.

## Installation

Install the extension from this directory with:

```bash
pip install -e .
```

Once published, it can be installed with:

```bash
pip install openbb-stooq
```

## Credentials

Stooq requires an API key for CSV downloads. Open a Stooq historical-data page
with the `get_apikey` flag, complete the verification, and copy the `apikey`
value from the CSV download link. For example:

<https://stooq.com/q/d/?s=aapl.us&get_apikey>

Store the value as `stooq_api_key` in OpenBB user settings or set the
`STOOQ_API_KEY` environment variable.

## Endpoint coverage

- `obb.equity.price.historical`
- `obb.etf.historical`
- `obb.index.price.historical`
- `obb.currency.price.historical`

Stooq supports daily, weekly, monthly, quarterly, and yearly intervals. Equity
and ETF symbols without an exchange suffix default to the US market. Set the
`country` parameter or provide a Stooq-qualified symbol such as `VOD.UK`,
`BMW.DE`, or `7203.JP`.

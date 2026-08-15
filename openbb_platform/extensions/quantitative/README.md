# OpenBB QA Extension

This extension provides Quantitative Analysis (QA) tools for the OpenBB Platform.

Features of the QA extension include various statistical tools and models.

This extension works nicely with a companion `openbb-charting` extension

## Installation

To install the extension, run the following command in this folder:

```bash
pip install openbb-quantitative
```

Documentation available [here](https://docs.openbb.co/platform/developer_guide/contributing).

## Optional dqlib analytics

The quantitative extension includes an optional bridge to the proprietary
[dqlib Python plugin](https://github.com/dqlab/dqlibpy/releases). The validated
release is `dqlib 3.0.2`, published as
`dqlib-3.0.2-cp312-cp312-linux_x86_64.whl`.

The released wheel requires CPython 3.12 on Linux x86_64. Download the wheel
from the authenticated release page, install it into the same environment as
OpenBB, and configure the dqlib runtime license as described with the release:

```bash
python3.12 -m pip install ./dqlib-3.0.2-cp312-cp312-linux_x86_64.whl
```

Check the integration and discover the installed analytics:

```python
from openbb import obb

obb.quantitative.dqlib.status()
obb.quantitative.dqlib.functions(domain="iranalytics")
```

Run typed dqlib commands directly through OpenBB:

```python
obb.quantitative.dqlib.datetime.simple_year_fraction(
    start_date="2022-03-07",
    end_date="2023-06-07",
    day_count="ACT_365_FIXED",
)

obb.quantitative.dqlib.risk.value_at_risk(
    request={
        "profit_loss_samples": [-4.0, 1.0, -2.0, 3.0, -7.0],
        "probability": 0.99,
    }
)

obb.quantitative.dqlib.risk.expected_shortfall(
    request={
        "profit_loss_samples": [-4.0, 1.0, -2.0, 3.0, -7.0],
        "probability": 0.99,
    }
)

obb.quantitative.dqlib.interest_rate.curve_analytics(
    request={
        "as_of_date": "2026-01-02",
        "currency": "USD",
        "pillars": [
            {"date": "2027-01-02", "zero_rate": 0.02, "name": "1Y"},
            {"date": "2028-01-02", "zero_rate": 0.023, "name": "2Y"},
        ],
        "query_dates": ["2027-07-02"],
    }
)

obb.quantitative.dqlib.equity.build_volatility_surface(
    request={
        "as_of_date": "2026-01-02",
        "underlying": "SPX",
        "underlying_price": 100.0,
        "currency": "USD",
        "option_chain": [
            {
                "expiry_date": "2026-07-02",
                "strike": 90.0,
                "option_type": "PUT",
                "bid": 2.0,
                "ask": 2.2,
            },
            {
                "expiry_date": "2026-07-02",
                "strike": 100.0,
                "option_type": "CALL",
                "price": 5.1,
            },
        ],
        "discount_curve": {"flat_rate": 0.02},
        "repo_curve": {"flat_rate": 0.02},
        "dividend_curve": {"flat_rate": 0.01},
        "build_settings": {
            "smile_method": "LINEAR_SMILE_METHOD",
            "wing_strike_type": "ABSOLUTE_STRIKE",
            "lower": 50.0,
            "upper": 150.0,
        },
    }
)
```

For an end-to-end market-data workflow, see the
[SPX option-chain volatility-surface example](../../../examples/dqlib_spx_volatility_surface.py).

The typed native surface currently covers interest-rate curves, fixed-coupon
bond yield, equity volatility-surface calibration, equity and commodity
European option pricing, FX ATM strike, credit curves, VaR, and expected
shortfall. The Python packages also explicitly bind all 188 functions defined
by the installed analytics modules. See
[the dqlib public API map](DQLIB_API.md) for package ownership, response models,
tested compatibility paths, and operations that cannot honestly be exposed as
typed JSON commands in dqlib 3.0.2.

Every released domain also exposes an allowlisted `call` command. For example,
the same date calculation can be executed by function name:

```python
obb.quantitative.dqlib.datetime.call(
    function="simple_year_frac_calculator",
    args=[
        {"$datetime": "2022-03-07T00:00:00"},
        {"$datetime": "2023-06-07T00:00:00"},
        "ACT_365_FIXED",
    ],
)
```

Use `pipeline` when one dqlib operation returns a native protobuf object that
another operation consumes. References are resolved in-process, and only the
selected final outputs are converted to OpenBB response data:

```python
obb.quantitative.dqlib.pipeline(
    steps=[
        {
            "id": "pnl",
            "domain": "mktrisk",
            "function": "dqCreateProtoVector",
            "args": [[-4.0, 1.0, -2.0, 3.0, -7.0]],
        },
        {
            "id": "var",
            "domain": "mktrisk",
            "function": "calculate_value_at_risk",
            "args": [{"$ref": "pnl"}, 0.99, False],
        },
        {
            "id": "expected_shortfall",
            "domain": "mktrisk",
            "function": "calculate_expected_short_fall",
            "args": [{"$ref": "pnl"}, 0.99, False],
        },
    ],
    outputs=["var", "expected_shortfall"],
)
```

Pipeline arguments support `$ref`, `$date`, `$datetime`, and base64
`$bytes` tokens. Analytics constructors, builders, calculators, pricers, and
typed converters are exposed; private, demo, process-request, and file-loading
functions are rejected.

The released Python APIs are organized by quantitative domain:

```python
from openbb_quantitative.interest_rate.analytics import ir_single_ccy_curve_builder
from openbb_quantitative.fixed_income.analytics import vanilla_bond_pricer
from openbb_quantitative.equity.analytics import eq_european_option_pricer
```

The former `iranalytics`, `fianalytics`, `eqanalytics`, `fxanalytics`,
`cranalytics`, `cmanalytics`, and `mktrisk` imports remain compatibility shims.

Bridged domains include shared analytics, dates, markets, numerics, interest
rates, fixed income, equities, foreign exchange, credit, commodities, and
market risk. The proprietary wheel, native library, source, and license are not
stored in the OpenBB repository.

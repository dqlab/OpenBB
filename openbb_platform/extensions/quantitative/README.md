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

obb.quantitative.dqlib.mktrisk.risk_factor_change(
    values=[100.0, 102.0, 99.0, 104.0],
    change_type="RELATIVE",
)

obb.quantitative.dqlib.mktrisk.value_at_risk(
    profit_loss_samples=[-4.0, 1.0, -2.0, 3.0, -7.0],
    probability=0.99,
)
```

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

The released Python APIs remain available through lazy module namespaces:

```python
from openbb_quantitative.iranalytics import ir_single_ccy_curve_builder
from openbb_quantitative.fianalytics import vanilla_bond_pricer
from openbb_quantitative.eqanalytics import eq_european_option_pricer
```

Bridged domains include shared analytics, dates, markets, numerics, interest
rates, fixed income, equities, foreign exchange, credit, commodities, and
market risk. The proprietary wheel, native library, source, and license are not
stored in the OpenBB repository.

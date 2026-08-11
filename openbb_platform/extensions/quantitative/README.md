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

Check the integration without exposing runtime paths or license details:

```python
from openbb import obb

obb.quantitative.dqlib.status()
```

The released Python APIs are available through lazy OpenBB namespaces. Imports
remain safe when dqlib is not installed:

```python
from openbb_quantitative.iranalytics import ir_single_ccy_curve_builder
from openbb_quantitative.fianalytics import vanilla_bond_pricer
from openbb_quantitative.eqanalytics import eq_european_option_pricer
```

Bridged domains include shared analytics, dates, markets, numerics, interest
rates, fixed income, equities, foreign exchange, credit, commodities, and
market risk. The proprietary wheel, native library, source, and license are not
stored in the OpenBB repository.

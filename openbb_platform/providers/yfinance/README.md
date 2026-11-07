# OpenBB Yahoo!Finance Provider

This extension integrates the [Yahoo!Finance](https://finance.yahoo.com/) data provider into the OpenBB Platform.

## Installation

To install the extension:

```bash
pip install openbb-yfinance
```

Documentation available [here](https://docs.openbb.co/platform/developer_guide/contributing).

## Profile source evidence

The provider registry exposes `EquityProfileSource` through OpenBB's
`QueryExecutor`. It accepts one literal `symbol` and returns one envelope with
`evidence_format` set to `yfinance-profile-extraction-v1` and `payload` holding
a one-element list containing the value returned by `Ticker(symbol).get_info()`.
This is an SDK return, not a network-wire capture. Empty and null returns are
preserved; client exceptions propagate. Values outside finite JSON-native types
are rejected. The existing `EquityInfo` projection is unchanged.

This model requires an installation of the specific provider artifact that
contains it. Record the trusted artifact source, version and SHA-256, and verify
the installed provider registers `EquityProfileSource`; a version number or a
public package with the same name alone does not establish provenance. Check
the intended release destination for version collisions before publishing.

Validation status: `live_not_tested`. Offline tests use synthetic SDK returns;
they do not establish actual Yahoo service responses, SDK return shapes, or
availability. The package requires yfinance `^1.4.0` and openbb-core `^1.6.10`.

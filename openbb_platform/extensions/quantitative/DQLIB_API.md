# dqlib 3.0.2 typed integration map

This map records the public dqlib 3.0.2 Python analytics surface and the
operations used by typed OpenBB quantitative commands. It intentionally does
not reproduce proprietary source, generated runtime data, licenses, or internal
implementation details.

## Domain packages

Every function defined by the installed analytics modules has an explicit,
lazy Python binding in the corresponding OpenBB package. The native audit test
compares these declarations with the installed release rather than inferring or
fabricating names.

| OpenBB package | Installed dqlib module | Bound functions |
| --- | --- | ---: |
| `common/analytics.py` | `dqlib.analytics` | 63 |
| `interest_rate/analytics.py` | `dqlib.iranalytics` | 10 |
| `fixed_income/analytics.py` | `dqlib.fianalytics` | 10 |
| `equity/analytics.py` | `dqlib.eqanalytics` | 22 |
| `foreign_exchange/analytics.py` | `dqlib.fxanalytics` | 29 |
| `credit/analytics.py` | `dqlib.cranalytics` | 9 |
| `commodity/analytics.py` | `dqlib.cmanalytics` | 25 |
| `risk/analytics.py` | `dqlib.mktrisk` | 20 |

This is 188 explicit bindings. Each package exposes its exact names in
`PUBLIC_FUNCTIONS`. The old flat module names remain import-compatible shims.
Bindings that accept native protobuf objects are Python APIs, not misleading
JSON routes; callers can compose those objects in-process. The `/call` and
`/pipeline` commands remain available for allowlisted runtime composition.

## Executable typed operations

| OpenBB command | Public dqlib operations used | Typed result |
| --- | --- | --- |
| `interest_rate.curve_analytics` | `create_ir_yield_curve`, `get_zero_rate`, `get_discount_factor`, `get_fwd_rate` | zero, discount, and forward rates by date |
| `fixed_income.fixed_coupon_bond_ytm` | `create_fixed_cpn_bond_template`, `build_fixed_cpn_bond`, `create_flat_ir_yield_curve`, `yield_to_maturity_calculator` | yield to maturity |
| `equity.european_option` | public flat curve/surface constructors, `create_european_option`, `create_eq_mkt_data_set`, `eq_european_option_pricer` | present value and currency |
| `foreign_exchange.atm_strike` | public flat curve/surface constructors, `create_foreign_exchange_rate`, `create_fx_spot_rate`, `create_fx_mkt_conventions`, `fx_atm_strike_calculator` | ATM strike |
| `credit.curve_analytics` | `create_credit_curve`, `get_credit_spread`, `get_survival_probability` | credit spread and survival probability by date |
| `commodity.european_option` | public flat curve/surface constructors, `create_european_option`, `create_cm_mkt_data_set`, `cm_european_option_pricer` | present value and currency |
| `risk.value_at_risk` | `dqCreateProtoVector`, public `CalculateValueAtRiskInput`/`Output`, `process_request` | VaR and optional mirrored VaR |
| `risk.expected_shortfall` | `dqCreateProtoVector`, public `CalculateExpectedShortfallInput`/`Output`, `process_request` | expected shortfall and optional mirrored result |

All request dates, curve points, market inputs, option terms, probabilities,
and output shapes are validated by Pydantic models before or after the native
call. Native protobuf objects remain in process and are translated to stable
OpenBB response models.

## dqlib 3.0.2 compatibility boundaries

The released `dqlib.mktrisk` VaR and expected-shortfall wrappers refer to a
missing `ProcessRequest` symbol, and the VaR wrapper also refers to a missing
input-constructor symbol. OpenBB uses the public protobuf request classes and
`dqlib.processrequest.process_request` to execute those two registered native
services.

The released `create_flat_fx_volatility_surface` convenience wrapper calls a
public constructor with an incompatible argument list. The typed FX command
therefore composes the public generic flat surface, currency pair, and market
convention messages into the public `FxVolatilitySurface` expected by
`fx_atm_strike_calculator`.

Other market-risk wrappers that depend on missing request or response symbols
remain explicit Python bindings but do not have typed OpenBB commands. No
replacement calculation is fabricated. They remain outside the typed HTTP
surface until an installed dqlib release provides an executable public
request/response contract.

## Native regression tests

The normal test suite uses test doubles and does not require proprietary
software. On a licensed dqlib 3.0.2 Linux environment, run the real native
vertical slices and exact public-surface audit with:

```bash
OPENBB_RUN_DQLIB_NATIVE_TESTS=1 pytest -q \
  openbb_platform/extensions/quantitative/tests/test_dqlib_native.py
```

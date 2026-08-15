# dqlib 3.0.2 typed integration map

This map records the public dqlib 3.0.2 Python operations used by the typed
OpenBB quantitative commands. It intentionally does not reproduce proprietary
source, generated runtime data, licenses, or internal implementation details.

## Executable typed operations

| OpenBB command | Public dqlib operations used | Typed result |
| --- | --- | --- |
| `iranalytics.curve_analytics` | `create_ir_yield_curve`, `get_zero_rate`, `get_discount_factor`, `get_fwd_rate` | zero, discount, and forward rates by date |
| `fianalytics.fixed_coupon_bond_ytm` | `create_fixed_cpn_bond_template`, `build_fixed_cpn_bond`, `create_flat_ir_yield_curve`, `yield_to_maturity_calculator` | yield to maturity |
| `eqanalytics.european_option` | public flat curve/surface constructors, `create_european_option`, `create_eq_mkt_data_set`, `eq_european_option_pricer` | present value and currency |
| `fxanalytics.atm_strike` | public flat curve/surface constructors, `create_foreign_exchange_rate`, `create_fx_spot_rate`, `create_fx_mkt_conventions`, `fx_atm_strike_calculator` | ATM strike |
| `cranalytics.curve_analytics` | `create_credit_curve`, `get_credit_spread`, `get_survival_probability` | credit spread and survival probability by date |
| `cmanalytics.european_option` | public flat curve/surface constructors, `create_european_option`, `create_cm_mkt_data_set`, `cm_european_option_pricer` | present value and currency |
| `mktrisk.value_at_risk` | `dqCreateProtoVector`, public `CalculateValueAtRiskInput`/`Output`, `process_request` | VaR and optional mirrored VaR |
| `mktrisk.expected_shortfall` | `dqCreateProtoVector`, public `CalculateExpectedShortfallInput`/`Output`, `process_request` | expected shortfall and optional mirrored result |

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
do not have typed OpenBB commands. No replacement calculation is fabricated.
They remain outside the supported typed surface until an installed dqlib
release provides an executable public request/response contract.

## Native regression tests

The normal test suite uses test doubles and does not require proprietary
software. On a licensed dqlib 3.0.2 Linux environment, run the real native
vertical slices with:

```bash
OPENBB_RUN_DQLIB_NATIVE_TESTS=1 pytest -q \
  openbb_platform/extensions/quantitative/tests/test_dqlib_native.py
```

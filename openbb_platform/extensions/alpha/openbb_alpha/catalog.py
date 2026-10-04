"""Allowlisted factor definitions; formulas are documentation, never executable input."""

from importlib.metadata import PackageNotFoundError, version

from openbb_core.provider.standard_models.alpha_research import FactorSpec


def catalog() -> tuple[FactorSpec, ...]:
    momentum = tuple(
        FactorSpec(
            id=f"momentum_{lookback}_21",
            formula=f"P[t-21]/P[t-{lookback}]-1",
            required_fields=("price",),
            lookback=lookback,
            skip=21,
            min_observations=lookback - 20,
            window_endpoints=f"[t-{lookback},t-21]",
            units="fractional_return",
        )
        for lookback in (63, 126, 252)
    )
    return momentum + (
        FactorSpec(
            id="reversal_5",
            formula="-(P[t]/P[t-5]-1)",
            required_fields=("price",),
            lookback=5,
            min_observations=6,
            window_endpoints="[t-5,t]",
            units="negative_fractional_return",
        ),
        FactorSpec(
            id="negative_realized_volatility_20",
            formula="-sqrt(252)*sample_sd(log(P[s]/P[s-1]),s=t-19..t,ddof=1)",
            required_fields=("price",),
            lookback=20,
            min_observations=21,
            window_endpoints="[t-20,t]",
            units="negative_annualized_log_volatility",
        ),
        FactorSpec(
            id="log_dollar_volume_20",
            formula="log(mean(raw_close[s]*raw_share_volume[s],s=t-19..t))",
            required_fields=("raw_close", "raw_share_volume"),
            lookback=19,
            min_observations=20,
            window_endpoints="[t-19,t]",
            units="log_source_currency_notional; single currency and raw shares only",
        ),
    )


def select(ids):
    registry = {s.id: s for s in catalog()}
    if len(set(ids)) != len(ids) or any(f not in registry for f in ids):
        raise ValueError("unsupported or duplicate factor; use alpha.catalog()")
    return tuple(registry[f] for f in ids)


def capabilities():
    output = []
    for name, package in (
        ("alpha_reference", "openbb-alpha-reference"),
        ("polars_ta", "polars-ta"),
        ("alphalens", "alphalens-reloaded"),
        ("qlib", "pyqlib"),
        ("bt", "bt"),
    ):
        try:
            installed = version(package)
        except PackageNotFoundError:
            installed = None
        distribution = f"openbb-{name.replace('_', '-')}"
        try:
            adapter_version = version(distribution)
        except PackageNotFoundError:
            adapter_version = None
        output.append(
            {
                "provider": name,
                "library_version": installed,
                "adapter_version": adapter_version,
                "available": installed is not None and adapter_version is not None,
                "capability": "factor_v1"
                if name in ("alpha_reference", "polars_ta")
                else "evaluation"
                if name == "alphalens"
                else "research_workflow",
                "install": f"Install the local openbb-{name.replace('_', '-')} package",
                "classification": "Lab",
            }
        )
    return output

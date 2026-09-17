"""Explicit bindings to every public function defined by dqlib.analytics."""

from typing import Any

from openbb_quantitative._dqlib import domain_dir, domain_getattr
from openbb_quantitative._dqlib_api import install_public_functions
from openbb_quantitative.dqlib_domain import create_domain_router

_DOMAIN = "analytics"
router = create_domain_router(_DOMAIN, "common")

_FUNCTION_NAMES = (
    "create_asset_yield_curve",
    "create_credit_curve",
    "create_credit_curve_from_binary",
    "create_credit_curve_risk_settings",
    "create_dividend_curve",
    "create_dividend_curve_risk_settings",
    "create_flat_credit_curve",
    "create_flat_dividend_curve",
    "create_flat_ir_yield_curve",
    "create_flat_vol_curve",
    "create_flat_volatility_surface",
    "create_ir_curve_risk_settings",
    "create_ir_yield_curve",
    "create_ir_yield_curve_from_binary",
    "create_model_free_pricing_settings",
    "create_model_settings",
    "create_monte_carlo_settings",
    "create_option_quote_matrix",
    "create_pde_settings",
    "create_price_risk_settings",
    "create_price_vol_risk_settings",
    "create_pricing_settings",
    "create_proxy_option_quote_matrix",
    "create_scn_analysis_settings",
    "create_theta_risk_settings",
    "create_vol_curve",
    "create_vol_risk_settings",
    "create_vol_surf_build_settings",
    "create_volatility_smile",
    "create_volatility_surface",
    "create_volatility_surface_definition",
    "create_yield_curve",
    "get_credit_spread",
    "get_discount_factor",
    "get_fwd_rate",
    "get_survival_probability",
    "get_volatility",
    "get_zero_rate",
    "implied_vol_calculator",
    "print_term_structure_curve",
    "to_atm_type",
    "to_compounding_type",
    "to_delta_type",
    "to_dividend_type",
    "to_finite_difference_method",
    "to_ir_yield_curve_building_method",
    "to_ir_yield_curve_type",
    "to_option_quote_strike_type",
    "to_option_quote_term_type",
    "to_option_quote_value_type",
    "to_option_underlying_type",
    "to_pricing_method_name",
    "to_pricing_model_name",
    "to_risk_granularity",
    "to_scn_analysis_type",
    "to_smile_quote_type",
    "to_threading_mode",
    "to_vol_smile_method",
    "to_vol_smile_type",
    "to_vol_term_time_interp_method",
    "to_vol_termtime_extrap_method",
    "to_volatility_type",
    "to_wing_strike_type",
)

PUBLIC_FUNCTIONS = install_public_functions(globals(), _DOMAIN, _FUNCTION_NAMES)


def __getattr__(name: str) -> Any:
    """Resolve re-exported public attributes from dqlib.analytics."""
    return domain_getattr(_DOMAIN, name)


def __dir__() -> list[str]:
    """Return local and dqlib public analytics names for discovery."""
    return sorted(set(globals()) | set(domain_dir(_DOMAIN)))


__all__ = [*PUBLIC_FUNCTIONS, "PUBLIC_FUNCTIONS", "router"]  # noqa: PLE0604

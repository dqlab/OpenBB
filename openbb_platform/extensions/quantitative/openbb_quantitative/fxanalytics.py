"""Lazy bridge to dqlib foreign-exchange analytics."""

from typing import Any

from openbb_core.app.model.obbject import OBBject

from openbb_quantitative._dqlib import (
    DQLibExecutionError,
    domain_dir,
    domain_getattr,
    execute_function,
)
from openbb_quantitative._dqlib_typed import to_datetime
from openbb_quantitative.dqlib_domain import create_domain_router
from openbb_quantitative.dqlib_models import FxAtmStrikeRequest, FxAtmStrikeResult

_DOMAIN = "fxanalytics"
router = create_domain_router(_DOMAIN)


def _flat_fx_volatility_surface(
    request: FxAtmStrikeRequest, valuation_date: Any
) -> Any:
    """Build the FX surface shape required by the dqlib 3.0.2 calculator."""
    try:
        from dqlibproto import dqproto  # noqa: PLC0415

        generic_surface = execute_function(
            "analytics",
            "create_flat_volatility_surface",
            [valuation_date, request.volatility, request.currency_pair],
        )
        conventions = execute_function(
            _DOMAIN,
            "create_fx_mkt_conventions",
            [
                "ATM_FORWARD",
                "PERCENTAGE_SPOT_DELTA",
                "PERCENTAGE_SPOT_DELTA",
                "1Y",
                "RR_CALL_PUT",
                "MARKET_STRANGLE_QUOTE",
                request.currency_pair,
            ],
        )
        surface = dqproto.FxVolatilitySurface()
        surface.volatility_surface.CopyFrom(generic_surface)
        surface.currency_pair.CopyFrom(
            execute_function("market", "to_ccy_pair", [request.currency_pair])
        )
        surface.market_conventions.CopyFrom(conventions)
        return surface
    except DQLibExecutionError:
        raise
    except Exception as exc:  # pylint: disable=broad-exception-caught
        raise DQLibExecutionError(
            "dqlib could not construct the FX volatility surface."
        ) from exc


@router.command(
    methods=["POST"],
    operation_id="dqlib_fxanalytics_atm_strike",
)
def atm_strike(
    request: FxAtmStrikeRequest,
) -> OBBject[FxAtmStrikeResult]:
    """Calculate a native dqlib FX at-the-money strike from flat market data."""
    valuation_date = to_datetime(request.valuation_date)
    expiry_date = to_datetime(request.expiry_date)
    domestic_currency = request.currency_pair[3:]
    foreign_currency = request.currency_pair[:3]
    domestic_curve = execute_function(
        "analytics",
        "create_flat_ir_yield_curve",
        [valuation_date, domestic_currency, request.domestic_rate],
    )
    foreign_curve = execute_function(
        "analytics",
        "create_flat_ir_yield_curve",
        [valuation_date, foreign_currency, request.foreign_rate],
    )
    fx_rate = execute_function(
        "market",
        "create_foreign_exchange_rate",
        [request.spot, foreign_currency, domestic_currency],
    )
    spot_rate = execute_function(
        "market",
        "create_fx_spot_rate",
        [fx_rate, valuation_date, valuation_date],
    )
    surface = _flat_fx_volatility_surface(request, valuation_date)
    result = execute_function(
        _DOMAIN,
        "fx_atm_strike_calculator",
        [
            request.atm_type,
            expiry_date,
            surface,
            spot_rate,
            domestic_curve,
            foreign_curve,
        ],
    )
    return OBBject(
        results=FxAtmStrikeResult(
            strike=float(result),
            currency_pair=request.currency_pair,
            atm_type=request.atm_type,
        )
    )


def __getattr__(name: str) -> Any:
    """Resolve a public attribute from dqlib.fxanalytics."""
    return domain_getattr(_DOMAIN, name)


def __dir__() -> list[str]:
    """Return local and dqlib domain attributes for interactive discovery."""
    return sorted(set(globals()) | set(domain_dir(_DOMAIN)))

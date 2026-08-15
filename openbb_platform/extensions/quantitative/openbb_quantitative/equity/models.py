"""Typed models for equity analytics."""

from datetime import date
from typing import Literal

from pydantic import BaseModel, Field, model_validator

from openbb_quantitative.common.models import (
    Currency,
    EuropeanOptionRequest,
    EuropeanOptionResult,
)


class EquityEuropeanOptionRequest(EuropeanOptionRequest):
    """Inputs for a native dqlib equity European option valuation."""


class EqOptionChainQuote(BaseModel):
    """One equity option quote supplied to the native surface builder."""

    expiry_date: date
    strike: float = Field(gt=0)
    option_type: Literal["CALL", "PUT"]
    price: float | None = Field(default=None, ge=0)
    bid: float | None = Field(default=None, ge=0)
    ask: float | None = Field(default=None, ge=0)

    @model_validator(mode="after")
    def validate_price(self) -> "EqOptionChainQuote":
        """Require a direct price or a complete, ordered bid/ask pair."""
        has_bid_ask = self.bid is not None or self.ask is not None
        if self.price is None and (self.bid is None or self.ask is None):
            raise ValueError("Each option quote requires price or both bid and ask.")
        if has_bid_ask and (self.bid is None or self.ask is None):
            raise ValueError("Option bid and ask must be provided together.")
        if self.bid is not None and self.ask is not None and self.ask < self.bid:
            raise ValueError("Option ask cannot be below bid.")
        return self

    @property
    def market_price(self) -> float:
        """Return the direct price or bid/ask midpoint used by dqlib."""
        if self.price is not None:
            return self.price
        if self.bid is None or self.ask is None:  # pragma: no cover - validated
            raise ValueError("Option quote has no market price.")
        return (self.bid + self.ask) / 2


class EqRateCurvePillar(BaseModel):
    """One continuously compounded zero-rate pillar."""

    date: date
    rate: float
    name: str = ""


class EqRateCurveInput(BaseModel):
    """Flat or pillar-based rate curve used by equity calibration."""

    flat_rate: float | None = None
    pillars: list[EqRateCurvePillar] | None = None
    day_count: str = "ACT_365_FIXED"
    interpolation: str = "LINEAR_INTERP"
    extrapolation: str = "FLAT_EXTRAP"
    compounding: Literal["CONTINUOUS_COMPOUNDING"] = "CONTINUOUS_COMPOUNDING"

    @model_validator(mode="after")
    def validate_curve_shape(self) -> "EqRateCurveInput":
        """Require exactly one flat rate or an ordered term structure."""
        if (self.flat_rate is None) == (self.pillars is None):
            raise ValueError("Provide exactly one of flat_rate or pillars.")
        if self.pillars is not None:
            if len(self.pillars) < 2:
                raise ValueError("A term curve requires at least two pillars.")
            dates = [pillar.date for pillar in self.pillars]
            if dates != sorted(dates) or len(dates) != len(set(dates)):
                raise ValueError("Curve pillar dates must be unique and increasing.")
        return self


class EqVolatilitySurfaceBuildSettings(BaseModel):
    """Public dqlib settings for equity volatility-surface calibration."""

    smile_method: Literal[
        "LINEAR_SMILE_METHOD",
        "CUBIC_SPLINE_SMILE_METHOD",
        "SVI_SMILE_METHOD",
        "SABR_SMILE_METHOD",
        "SABR_NORMAL_SMILE_METHOD",
        "PSEUDO_DELTA_QUADRATIC_SMILE_METHOD",
    ] = "LINEAR_SMILE_METHOD"
    wing_strike_type: Literal[
        "DELTA",
        "RELATIVE_RATIO_STRIKE",
        "RELATIVE_SPREAD_STRIKE",
        "ABSOLUTE_STRIKE",
    ] = "ABSOLUTE_STRIKE"
    lower: float
    upper: float
    fixed_parameter_index: int = Field(default=0, ge=0)
    fixed_parameter_value: float = 0.0
    exercise_type: Literal["EUROPEAN", "AMERICAN", "BERMUDAN"] = "EUROPEAN"
    pricing_method: Literal[
        "ANALYTICAL",
        "ANALYTICAL_SMILE_ON",
        "PDE",
        "MONTE_CARLO",
        "BINOMIAL_TREE",
        "MOMENT_MATCHING",
    ] = "ANALYTICAL"

    @model_validator(mode="after")
    def validate_bounds(self) -> "EqVolatilitySurfaceBuildSettings":
        """Require a non-empty native wing interval."""
        if self.upper <= self.lower:
            raise ValueError("The volatility-surface upper bound must exceed lower.")
        return self


class BuildEqVolatilitySurfaceRequest(BaseModel):
    """Typed inputs for building a native dqlib equity volatility surface."""

    as_of_date: date
    option_chain: list[EqOptionChainQuote] = Field(min_length=2)
    underlying_price: float = Field(gt=0)
    discount_curve: EqRateCurveInput
    repo_curve: EqRateCurveInput | None = None
    dividend_curve: EqRateCurveInput
    build_settings: EqVolatilitySurfaceBuildSettings
    underlying: str = Field(min_length=1)
    currency: Currency = "USD"
    evaluation_strikes: list[float] | None = None

    @model_validator(mode="after")
    def validate_surface_inputs(self) -> "BuildEqVolatilitySurfaceRequest":
        """Validate dates, quote identity, grids, and curve coverage."""
        if any(quote.expiry_date <= self.as_of_date for quote in self.option_chain):
            raise ValueError("Option expiries must follow the as-of date.")
        identities = [
            (quote.expiry_date, quote.strike, quote.option_type)
            for quote in self.option_chain
        ]
        if len(identities) != len(set(identities)):
            raise ValueError("Option chain contains duplicate contract quotes.")
        expiries = {quote.expiry_date for quote in self.option_chain}
        for expiry in expiries:
            strikes = {
                quote.strike
                for quote in self.option_chain
                if quote.expiry_date == expiry
            }
            if len(strikes) < 2:
                raise ValueError("Each option expiry requires at least two strikes.")
        quoted_strikes = [quote.strike for quote in self.option_chain]
        if self.build_settings.wing_strike_type == "ABSOLUTE_STRIKE" and (
            self.build_settings.lower > min(quoted_strikes)
            or self.build_settings.upper < max(quoted_strikes)
        ):
            raise ValueError(
                "Absolute wing bounds must contain every quoted strike."
            )
        if self.evaluation_strikes is not None:
            if not self.evaluation_strikes:
                raise ValueError("evaluation_strikes cannot be empty.")
            if any(strike <= 0 for strike in self.evaluation_strikes):
                raise ValueError("Evaluation strikes must be positive.")
            if len(self.evaluation_strikes) != len(set(self.evaluation_strikes)):
                raise ValueError("Evaluation strikes must be unique.")
        for curve in (
            self.discount_curve,
            self.repo_curve,
            self.dividend_curve,
        ):
            if (
                curve is not None
                and curve.pillars is not None
                and any(
                    pillar.date <= self.as_of_date for pillar in curve.pillars
                )
            ):
                raise ValueError("Curve pillars must follow the as-of date.")
        return self

    @property
    def expiry_dates(self) -> list[date]:
        """Return the unique option expiries in native surface order."""
        return sorted({quote.expiry_date for quote in self.option_chain})

    @property
    def output_strikes(self) -> list[float]:
        """Return the requested or quoted strike evaluation grid."""
        values = self.evaluation_strikes or [
            quote.strike for quote in self.option_chain
        ]
        return sorted(set(values))


class EqVolatilitySurfacePoint(BaseModel):
    """One native calibrated volatility at an expiry and strike."""

    expiry_date: date
    strike: float
    volatility: float = Field(ge=0)


class BuildEqVolatilitySurfaceResult(BaseModel):
    """Translated dqlib equity volatility surface on a stable output grid."""

    as_of_date: date
    underlying: str
    smile_method: str
    wing_strike_type: str
    lower: float
    upper: float
    points: list[EqVolatilitySurfacePoint]


__all__ = [
    "BuildEqVolatilitySurfaceRequest",
    "BuildEqVolatilitySurfaceResult",
    "EqOptionChainQuote",
    "EqRateCurveInput",
    "EqRateCurvePillar",
    "EqVolatilitySurfaceBuildSettings",
    "EqVolatilitySurfacePoint",
    "EquityEuropeanOptionRequest",
    "EuropeanOptionResult",
]

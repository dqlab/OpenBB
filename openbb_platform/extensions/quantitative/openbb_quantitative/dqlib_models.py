"""Typed request and response models for native dqlib analytics."""

from datetime import date
from typing import Annotated, Literal

from pydantic import BaseModel, Field, model_validator

Currency = Annotated[str, Field(pattern=r"^[A-Z]{3}$")]
PayoffType = Literal["CALL", "PUT"]


class IrCurvePillar(BaseModel):
    """One zero-rate pillar used to construct a native dqlib IR curve."""

    date: date
    zero_rate: float
    name: str = ""


class IrCurveAnalyticsRequest(BaseModel):
    """Inputs for constructing and querying a native interest-rate curve."""

    as_of_date: date
    currency: Currency = "USD"
    pillars: list[IrCurvePillar] = Field(min_length=2)
    query_dates: list[date] = Field(min_length=1)
    day_count: str = "ACT_365_FIXED"
    interpolation: str = "LINEAR_INTERP"
    extrapolation: str = "FLAT_EXTRAP"
    compounding: str = "CONTINUOUS_COMPOUNDING"
    frequency: str = "ANNUAL"
    forward_tenor: float = Field(default=0.25, gt=0)
    curve_name: str = ""

    @model_validator(mode="after")
    def validate_dates(self) -> "IrCurveAnalyticsRequest":
        """Require ordered future pillars and future query dates."""
        pillar_dates = [pillar.date for pillar in self.pillars]
        if pillar_dates != sorted(pillar_dates) or len(set(pillar_dates)) != len(
            pillar_dates
        ):
            raise ValueError("IR curve pillar dates must be unique and increasing.")
        if any(value <= self.as_of_date for value in pillar_dates):
            raise ValueError("IR curve pillar dates must follow the as-of date.")
        if any(value <= self.as_of_date for value in self.query_dates):
            raise ValueError("IR curve query dates must follow the as-of date.")
        return self


class IrCurvePoint(BaseModel):
    """Native dqlib analytics at one interest-rate curve date."""

    date: date
    zero_rate: float
    discount_factor: float
    forward_rate: float


class IrCurveAnalyticsResult(BaseModel):
    """Translated native interest-rate curve analytics."""

    as_of_date: date
    currency: str
    curve_name: str = ""
    points: list[IrCurvePoint]


class FixedCouponBondYtmRequest(BaseModel):
    """Inputs for a native fixed-coupon bond yield calculation."""

    calculation_date: date
    issue_date: date
    maturity: str = Field(pattern=r"^[1-9][0-9]*[DWMY]$")
    coupon_rate: float = Field(ge=0)
    price: float = Field(gt=0)
    nominal: float = Field(default=100.0, gt=0)
    issue_price: float = Field(default=100.0, gt=0)
    currency: Currency = "USD"
    calendar: str = "USNY"
    settlement_days: int = Field(default=0, ge=0)
    frequency: str = "ANNUAL"
    day_count: str = "ACT_365_FIXED"
    price_type: Literal["CLEAN_PRICE", "DIRTY_PRICE"] = "CLEAN_PRICE"
    compounding: str = "CONTINUOUS_COMPOUNDING"
    curve_rate: float = 0.0
    instrument_name: str = "OPENBB_FIXED_COUPON_BOND"

    @model_validator(mode="after")
    def validate_calculation_date(self) -> "FixedCouponBondYtmRequest":
        """Require pricing on or after issuance."""
        if self.calculation_date < self.issue_date:
            raise ValueError("The calculation date cannot precede the issue date.")
        if not self.calendar.strip():
            raise ValueError("A dqlib calendar name is required.")
        return self


class FixedCouponBondYtmResult(BaseModel):
    """Translated native fixed-coupon bond yield result."""

    yield_to_maturity: float
    price: float
    price_type: str
    currency: str


class EuropeanOptionRequest(BaseModel):
    """Shared inputs for a flat-market European option valuation."""

    valuation_date: date
    expiry_date: date
    payoff_type: PayoffType = "CALL"
    strike: float = Field(gt=0)
    spot: float = Field(gt=0)
    volatility: float = Field(ge=0)
    discount_rate: float = 0.0
    carry_rate: float = 0.0
    nominal: float = Field(default=1.0, gt=0)
    currency: Currency = "USD"
    underlying: str = Field(min_length=1)

    @model_validator(mode="after")
    def validate_expiry(self) -> "EuropeanOptionRequest":
        """Require an expiry after the valuation date."""
        if self.expiry_date <= self.valuation_date:
            raise ValueError("The option expiry must follow the valuation date.")
        return self


class EquityEuropeanOptionRequest(EuropeanOptionRequest):
    """Inputs for a native dqlib equity European option valuation."""


class CommodityEuropeanOptionRequest(EuropeanOptionRequest):
    """Inputs for a native dqlib commodity European option valuation."""


class EuropeanOptionResult(BaseModel):
    """Translated native European option pricing result."""

    present_value: float
    cash_value: float | None = None
    currency: str


class FxAtmStrikeRequest(BaseModel):
    """Inputs for a native dqlib FX at-the-money strike calculation."""

    valuation_date: date
    expiry_date: date
    currency_pair: str = Field(pattern=r"^[A-Z]{6}$")
    spot: float = Field(gt=0)
    domestic_rate: float = 0.0
    foreign_rate: float = 0.0
    volatility: float = Field(default=0.0, ge=0)
    atm_type: Literal[
        "ATM_FORWARD",
        "ATM_SPOT",
        "ATM_DNS_PERCENTAGE",
        "ATM_DNS_PIPS",
    ] = "ATM_FORWARD"

    @model_validator(mode="after")
    def validate_expiry(self) -> "FxAtmStrikeRequest":
        """Require an expiry after the valuation date."""
        if self.expiry_date <= self.valuation_date:
            raise ValueError("The FX expiry must follow the valuation date.")
        return self


class FxAtmStrikeResult(BaseModel):
    """Translated native FX at-the-money strike result."""

    strike: float
    currency_pair: str
    atm_type: str


class CreditCurvePillar(BaseModel):
    """One hazard-rate pillar used to construct a native credit curve."""

    date: date
    hazard_rate: float = Field(ge=0)
    name: str = ""


class CreditCurveAnalyticsRequest(BaseModel):
    """Inputs for constructing and querying a native credit curve."""

    as_of_date: date
    pillars: list[CreditCurvePillar] = Field(min_length=1)
    query_dates: list[date] = Field(min_length=1)
    day_count: str = "ACT_365_FIXED"
    interpolation: str = "LINEAR_INTERP"
    extrapolation: str = "FLAT_EXTRAP"
    curve_name: str = ""

    @model_validator(mode="after")
    def validate_dates(self) -> "CreditCurveAnalyticsRequest":
        """Require ordered future pillars and future query dates."""
        pillar_dates = [pillar.date for pillar in self.pillars]
        if pillar_dates != sorted(pillar_dates) or len(set(pillar_dates)) != len(
            pillar_dates
        ):
            raise ValueError("Credit curve pillar dates must be unique and increasing.")
        if any(value <= self.as_of_date for value in pillar_dates):
            raise ValueError("Credit curve pillar dates must follow the as-of date.")
        if any(value <= self.as_of_date for value in self.query_dates):
            raise ValueError("Credit curve query dates must follow the as-of date.")
        return self


class CreditCurvePoint(BaseModel):
    """Native dqlib analytics at one credit curve date."""

    date: date
    credit_spread: float
    survival_probability: float


class CreditCurveAnalyticsResult(BaseModel):
    """Translated native credit curve analytics."""

    as_of_date: date
    curve_name: str = ""
    points: list[CreditCurvePoint]


class TailRiskRequest(BaseModel):
    """Inputs for native historical tail-risk calculations."""

    profit_loss_samples: list[float] = Field(min_length=2)
    probability: float = Field(default=0.99, gt=0, lt=1)
    antithetic: bool = False


class ValueAtRiskResult(BaseModel):
    """Translated native dqlib value-at-risk result."""

    probability: float
    value_at_risk: float
    value_at_risk_mirrored: float | None = None


class ExpectedShortfallResult(BaseModel):
    """Translated native dqlib expected-shortfall result."""

    probability: float
    expected_shortfall: float
    expected_shortfall_mirrored: float | None = None

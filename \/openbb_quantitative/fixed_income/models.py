"""Typed models for fixed-income analytics."""

from datetime import date
from typing import Literal

from pydantic import BaseModel, Field, model_validator

from openbb_quantitative.common.models import Currency


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

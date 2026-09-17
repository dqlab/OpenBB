"""Shared typed models for dqlib domain integrations."""

from datetime import date
from typing import Annotated, Literal

from pydantic import BaseModel, Field, model_validator

Currency = Annotated[str, Field(pattern=r"^[A-Z]{3}$")]
PayoffType = Literal["CALL", "PUT"]


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


class EuropeanOptionResult(BaseModel):
    """Translated native European option pricing result."""

    present_value: float
    cash_value: float | None = None
    currency: str

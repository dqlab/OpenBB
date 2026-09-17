"""Typed models for foreign-exchange analytics."""

from datetime import date
from typing import Literal

from pydantic import BaseModel, Field, model_validator


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

"""Typed models for market-risk analytics."""

from pydantic import BaseModel, Field


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

"""Typed models for interest-rate analytics."""

from datetime import date

from pydantic import BaseModel, Field, model_validator

from openbb_quantitative.common.models import Currency


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
        if pillar_dates != sorted(pillar_dates) or len(set(pillar_dates)) != len(pillar_dates):
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

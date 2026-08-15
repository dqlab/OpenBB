"""Typed models for credit analytics."""

from datetime import date

from pydantic import BaseModel, Field, model_validator


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
        if pillar_dates != sorted(pillar_dates) or len(set(pillar_dates)) != len(pillar_dates):
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

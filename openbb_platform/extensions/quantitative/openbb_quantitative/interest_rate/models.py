"""Typed models for interest-rate analytics."""

from datetime import date
from re import fullmatch
from typing import Annotated, Literal

from pydantic import BaseModel, Field, field_validator, model_validator

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


IrFrequency = Literal[
    "ANNUAL",
    "SEMIANNUAL",
    "EVERY_FOURTH_MONTH",
    "QUARTERLY",
    "BIMONTHLY",
    "MONTHLY",
    "EVERY_FOURTH_WEEK",
    "BIWEEKLY",
    "WEEKLY",
    "DAILY",
    "ONCE",
]


def _normalize_period(value: str) -> str:
    """Normalize one period supported by dqlib's public ``to_period`` API."""
    normalized = value.strip().upper()
    if normalized not in {"ON", "TN"} and not fullmatch(
        r"[1-9][0-9]*[DWMY]", normalized
    ):
        raise ValueError("Periods must be ON, TN, or a positive D/W/M/Y tenor.")
    return normalized


def _normalize_identity(value: str, label: str, *, uppercase: bool = False) -> str:
    """Strip and validate one public dqlib static-data identity."""
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{label} cannot be blank.")
    return normalized.upper() if uppercase else normalized


def _normalize_identities(values: list[str], label: str) -> list[str]:
    """Normalize a non-empty list of dqlib static-data identities."""
    return [_normalize_identity(value, label, uppercase=True) for value in values]


def _period_rank(value: str) -> int:
    """Return a monotonic rank for validating increasing public tenors."""
    if value == "ON":
        return 1
    if value == "TN":
        return 2
    multipliers = {"D": 1, "W": 7, "M": 31, "Y": 366}
    return int(value[:-1]) * multipliers[value[-1]]


class IrIborIndexTemplate(BaseModel):
    """Static IBOR index registered before native curve calibration."""

    index_name: str = Field(min_length=1)
    tenor: str
    calendars: list[str] = Field(min_length=1)
    start_delay: int = Field(default=2, ge=0)
    day_count: str = "ACT_360"
    interest_day_convention: str = "MODIFIED_FOLLOWING"
    date_roll_convention: str = "INVALID_DATE_ROLL_CONVENTION"
    ibor_type: str = "STANDARD_IBOR_INDEX"

    @field_validator("index_name")
    @classmethod
    def normalize_name(cls, value: str) -> str:
        """Use the uppercase identity stored by dqlib static data."""
        return _normalize_identity(value, "IBOR index names", uppercase=True)

    @field_validator("tenor")
    @classmethod
    def normalize_tenor(cls, value: str) -> str:
        """Validate the public dqlib period representation."""
        return _normalize_period(value)

    @field_validator("calendars")
    @classmethod
    def normalize_calendars(cls, value: list[str]) -> list[str]:
        """Normalize calendar identities used by the native index."""
        return _normalize_identities(value, "Calendar names")


class IrInstrumentTemplateBase(BaseModel):
    """Shared identity for a native interest-rate instrument template."""

    instrument_name: str = Field(min_length=1)
    calendar: str = Field(min_length=1)
    start_delay: int = Field(default=2, ge=0)
    start_convention: str = "SPOTSTART"

    @field_validator("instrument_name", "calendar")
    @classmethod
    def normalize_static_identity(cls, value: str) -> str:
        """Use uppercase static-data identities stored by dqlib."""
        return _normalize_identity(value, "IR static-data names", uppercase=True)


class IrDepositTemplate(IrInstrumentTemplateBase):
    """Public dqlib deposit template used by bootstrap pillars."""

    instrument_type: Literal["DEPOSIT"] = "DEPOSIT"
    day_count: str = "ACT_360"
    interest_day_convention: str = "MODIFIED_FOLLOWING"
    pay_day_offset: int = 0
    pay_day_convention: str = "MODIFIED_FOLLOWING"


class IrFixedLegConvention(BaseModel):
    """Fixed-leg conventions for a vanilla swap template."""

    frequency: IrFrequency = "SEMIANNUAL"
    day_count: str = "THIRTY_360"
    interest_day_convention: str = "MODIFIED_FOLLOWING"
    stub_policy: str = "INITIAL"
    broken_period_type: str = "LONG"
    pay_day_offset: int = 0
    pay_day_convention: str = "MODIFIED_FOLLOWING"
    notional_exchange: str = "INVALID_NOTIONAL_EXCHANGE"


class IrFloatingLegConvention(BaseModel):
    """Floating-leg conventions for a vanilla swap template."""

    frequency: IrFrequency = "QUARTERLY"
    fixing_frequency: IrFrequency = "QUARTERLY"
    day_count: str = "ACT_360"
    payment_discount_method: str = "NO_DISCOUNT"
    rate_calculation_method: str = "STANDARD"
    spread: bool = False
    interest_day_convention: str = "MODIFIED_FOLLOWING"
    stub_policy: str = "INITIAL"
    broken_period_type: str = "LONG"
    pay_day_offset: int = 0
    pay_day_convention: str = "MODIFIED_FOLLOWING"
    fixing_day_convention: str = "MODIFIED_PRECEDING"
    fixing_mode: str = "IN_ADVANCE"
    fixing_day_offset: int = -2
    notional_exchange: str = "INVALID_NOTIONAL_EXCHANGE"


class IrVanillaSwapTemplate(IrInstrumentTemplateBase):
    """Public dqlib fixed/floating vanilla swap template."""

    instrument_type: Literal["IR_VANILLA_SWAP"] = "IR_VANILLA_SWAP"
    reference_index: str = Field(min_length=1)
    fixing_calendars: list[str] = Field(min_length=1)
    fixed_leg: IrFixedLegConvention = Field(default_factory=IrFixedLegConvention)
    floating_leg: IrFloatingLegConvention = Field(
        default_factory=IrFloatingLegConvention
    )

    @field_validator("reference_index")
    @classmethod
    def normalize_reference_index(cls, value: str) -> str:
        """Match the uppercase IBOR index identity stored by dqlib."""
        return _normalize_identity(value, "IBOR index names", uppercase=True)

    @field_validator("fixing_calendars")
    @classmethod
    def normalize_fixing_calendars(cls, value: list[str]) -> list[str]:
        """Normalize calendar identities used by the floating leg."""
        return _normalize_identities(value, "Calendar names")


IrInstrumentTemplate = Annotated[
    IrDepositTemplate | IrVanillaSwapTemplate,
    Field(discriminator="instrument_type"),
]


class IrSingleCurrencyCurveQuote(BaseModel):
    """One deposit or vanilla-swap par quote supplied to dqlib."""

    instrument_name: str = Field(min_length=1)
    instrument_type: Literal["DEPOSIT", "IR_VANILLA_SWAP"]
    term: str
    factor: float = 1.0
    quote: float

    @field_validator("instrument_name")
    @classmethod
    def normalize_instrument_name(cls, value: str) -> str:
        """Match the uppercase native instrument-template identity."""
        return _normalize_identity(value, "Instrument names", uppercase=True)

    @field_validator("term")
    @classmethod
    def normalize_term(cls, value: str) -> str:
        """Validate the public dqlib period representation."""
        return _normalize_period(value)


class IrSingleCurrencyCurveTarget(BaseModel):
    """One target yield curve and the par-rate curve used to build it."""

    curve_name: str = Field(min_length=1)
    par_curve_name: str | None = None
    quotes: list[IrSingleCurrencyCurveQuote] = Field(min_length=2)
    discount_curves: dict[str, str] | None = None
    forward_curves: dict[str, str] = Field(default_factory=dict)
    use_on_tn_fx_swap: bool = False

    @field_validator("curve_name", "par_curve_name")
    @classmethod
    def strip_curve_name(cls, value: str | None) -> str | None:
        """Reject whitespace-only curve identities."""
        if value is None:
            return None
        normalized = value.strip()
        if not normalized:
            raise ValueError("Curve names cannot be blank.")
        return normalized

    @field_validator("discount_curves", "forward_curves")
    @classmethod
    def normalize_curve_manager(
        cls, value: dict[str, str] | None
    ) -> dict[str, str] | None:
        """Normalize manager keys and reject blank curve references."""
        if value is None:
            return None
        return {
            _normalize_identity(key, "Curve manager keys", uppercase=True): (
                _normalize_identity(curve, "Curve references")
            )
            for key, curve in value.items()
        }

    @model_validator(mode="after")
    def validate_quotes(self) -> "IrSingleCurrencyCurveTarget":
        """Require unique quotes in increasing tenor order."""
        identities = [
            (quote.instrument_type, quote.instrument_name, quote.term)
            for quote in self.quotes
        ]
        if len(identities) != len(set(identities)):
            raise ValueError("Single-currency curve quotes must be unique.")
        ranks = [_period_rank(quote.term) for quote in self.quotes]
        if ranks != sorted(ranks):
            raise ValueError("Single-currency curve quotes must be ordered by term.")
        return self

    @property
    def native_par_curve_name(self) -> str:
        """Return the explicit or target-derived native par-curve name."""
        return self.par_curve_name or self.curve_name


class IrKnownYieldCurve(BaseModel):
    """An already-known zero curve supplied to a multi-curve bootstrap."""

    curve_name: str = Field(min_length=1)
    pillars: list[IrCurvePillar] = Field(min_length=2)
    day_count: str = "ACT_365_FIXED"
    interpolation: str = "LINEAR_INTERP"
    extrapolation: str = "FLAT_EXTRAP"
    compounding: str = "CONTINUOUS_COMPOUNDING"
    frequency: IrFrequency = "ANNUAL"

    @field_validator("curve_name")
    @classmethod
    def normalize_curve_name(cls, value: str) -> str:
        """Reject blank known-curve identities."""
        return _normalize_identity(value, "Curve names")

    @model_validator(mode="after")
    def validate_pillars(self) -> "IrKnownYieldCurve":
        """Require unique, increasing known-curve pillars."""
        dates = [pillar.date for pillar in self.pillars]
        if dates != sorted(dates) or len(dates) != len(set(dates)):
            raise ValueError("Known IR curve pillars must be unique and increasing.")
        return self


class IrSingleCurrencyCurveBuildRequest(BaseModel):
    """Inputs for dqlib's native single-currency yield-curve builder."""

    as_of_date: date
    currency: Currency = "USD"
    targets: list[IrSingleCurrencyCurveTarget] = Field(min_length=1)
    instrument_templates: list[IrInstrumentTemplate] = Field(min_length=1)
    ibor_indices: list[IrIborIndexTemplate] = Field(default_factory=list)
    other_curves: list[IrKnownYieldCurve] = Field(default_factory=list)
    query_dates: list[date] = Field(min_length=1)
    day_count: str = "ACT_365_FIXED"
    compounding: str = "CONTINUOUS_COMPOUNDING"
    frequency: IrFrequency = "ANNUAL"
    forward_tenor: float = Field(default=0.25, gt=0)
    building_method: Literal[
        "BOOTSTRAPPING_METHOD",
        "GLOBAL_OPTIMIZATION_METHOD",
        "HYBRID_METHOD",
    ] = "BOOTSTRAPPING_METHOD"
    calculate_jacobian: bool = False
    shift: float = Field(default=0.0001, gt=0)
    finite_difference_method: Literal[
        "CENTRAL_DIFFERENCE_METHOD",
        "ONE_SIDE_DOWN_METHOD",
        "ONE_SIDE_UP_METHOD",
    ] = "CENTRAL_DIFFERENCE_METHOD"
    threading_mode: Literal[
        "SINGLE_THREADING_MODE",
        "MULTI_THREADING_MODE",
    ] = "SINGLE_THREADING_MODE"

    @model_validator(mode="after")
    def validate_build(self) -> "IrSingleCurrencyCurveBuildRequest":
        """Validate static identities, curve references, and requested dates."""
        target_names = [target.curve_name for target in self.targets]
        if len(target_names) != len(set(target_names)):
            raise ValueError("Target IR curve names must be unique.")
        template_identities = [
            (template.instrument_type, template.instrument_name)
            for template in self.instrument_templates
        ]
        if len(template_identities) != len(set(template_identities)):
            raise ValueError("IR instrument templates must be unique.")
        supplied_templates = set(template_identities)
        required_templates = {
            (quote.instrument_type, quote.instrument_name)
            for target in self.targets
            for quote in target.quotes
        }
        missing_templates = sorted(required_templates - supplied_templates)
        if missing_templates:
            raise ValueError(
                f"Missing IR instrument templates for quotes: {missing_templates}."
            )

        index_names = [index.index_name for index in self.ibor_indices]
        if len(index_names) != len(set(index_names)):
            raise ValueError("IBOR index names must be unique.")
        missing_indices = sorted(
            {
                template.reference_index
                for template in self.instrument_templates
                if isinstance(template, IrVanillaSwapTemplate)
            }
            - set(index_names)
        )
        if missing_indices:
            raise ValueError(
                f"Missing IBOR index definitions for swap templates: {missing_indices}."
            )

        known_names = [curve.curve_name for curve in self.other_curves]
        if len(known_names) != len(set(known_names)):
            raise ValueError("Known IR curve names must be unique.")
        available_curves = set(target_names) | set(known_names)
        for target in self.targets:
            references = set((target.discount_curves or {}).values()) | set(
                target.forward_curves.values()
            )
            unknown = sorted(references - available_curves)
            if unknown:
                raise ValueError(
                    f"Curve build settings reference unknown curves: {unknown}."
                )

        if len(self.query_dates) != len(set(self.query_dates)):
            raise ValueError("IR curve query dates must be unique.")
        if any(value <= self.as_of_date for value in self.query_dates):
            raise ValueError("IR curve query dates must follow the as-of date.")
        if any(
            pillar.date <= self.as_of_date
            for curve in self.other_curves
            for pillar in curve.pillars
        ):
            raise ValueError("Known IR curve pillars must follow the as-of date.")
        return self


class IrBuiltCurvePillar(BaseModel):
    """One native calibrated pillar from a built yield curve."""

    date: date
    name: str
    zero_rate: float


class IrCurveJacobian(BaseModel):
    """One native curve-calibration Jacobian matrix."""

    name: str
    rows: int = Field(ge=0)
    columns: int = Field(ge=0)
    values: list[float]


class IrBuiltYieldCurve(BaseModel):
    """One translated target curve from the single-currency builder."""

    curve_name: str
    currency: str
    pillars: list[IrBuiltCurvePillar]
    points: list[IrCurvePoint]
    jacobians: list[IrCurveJacobian] = Field(default_factory=list)


class IrSingleCurrencyCurveBuildResult(BaseModel):
    """Translated result from dqlib's single-currency curve builder."""

    as_of_date: date
    building_method: str
    curves: list[IrBuiltYieldCurve]


__all__ = [
    "IrBuiltCurvePillar",
    "IrBuiltYieldCurve",
    "IrCurveAnalyticsRequest",
    "IrCurveAnalyticsResult",
    "IrCurveJacobian",
    "IrCurvePillar",
    "IrCurvePoint",
    "IrDepositTemplate",
    "IrFixedLegConvention",
    "IrFloatingLegConvention",
    "IrFrequency",
    "IrIborIndexTemplate",
    "IrInstrumentTemplate",
    "IrKnownYieldCurve",
    "IrSingleCurrencyCurveBuildRequest",
    "IrSingleCurrencyCurveBuildResult",
    "IrSingleCurrencyCurveQuote",
    "IrSingleCurrencyCurveTarget",
    "IrVanillaSwapTemplate",
]

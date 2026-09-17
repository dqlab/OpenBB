"""Pydantic models for Quantitative Analysis."""

from typing import Any

from pydantic import BaseModel, Field


class TestModel(BaseModel):
    """Base model for QA tests."""

    statistic: float
    p_value: float


class NormalityModel(BaseModel):
    """Normality model."""

    kurtosis: TestModel
    skewness: TestModel
    jarque_bera: TestModel
    shapiro_wilk: TestModel
    kolmogorov_smirnov: TestModel


class ADFTestModel(TestModel):
    """Augmented Dickey-Fuller test model."""

    nlags: int
    nobs: int
    icbest: float


class KPSSTestModel(TestModel):
    """Kwiatkowski–Phillips–Schmidt–Shin test model."""

    nlags: int


class UnitRootModel(BaseModel):
    """Unit root model."""

    adf: ADFTestModel
    kpss: KPSSTestModel


class OmegaModel(BaseModel):
    """Omega model."""

    threshold: float
    omega: float


class SummaryModel(BaseModel):
    """Summary model."""

    count: int
    mean: float
    std: float
    var: float
    min: float
    max: float
    p_25: float
    p_50: float
    p_75: float


class CAPMModel(BaseModel):
    """CAPM model."""

    market_risk: float
    systematic_risk: float
    idiosyncratic_risk: float


class DQLibStatusModel(BaseModel):
    """Status of the optional dqlib analytics runtime."""

    available: bool
    supported_runtime: bool
    release_version: str
    installed_version: str | None = None
    release_url: str
    message: str


class DQLibFunctionModel(BaseModel):
    """One callable exposed from an installed dqlib analytics domain."""

    name: str
    signature: str
    description: str = ""


class DQLibCallResult(BaseModel):
    """JSON-safe result from one dqlib function call."""

    domain: str
    function: str
    result: Any


class DQLibPipelineStep(BaseModel):
    """One native-object-aware operation in a dqlib execution pipeline."""

    id: str = Field(pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")
    domain: str
    function: str
    args: list[Any] = Field(default_factory=list)
    kwargs: dict[str, Any] = Field(default_factory=dict)


class DQLibPipelineResult(BaseModel):
    """Selected JSON-safe outputs from a dqlib execution pipeline."""

    outputs: dict[str, Any]


class DQLibScalarResult(BaseModel):
    """Named JSON-safe result from a typed dqlib command."""

    value: Any

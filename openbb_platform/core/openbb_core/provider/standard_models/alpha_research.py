"""Version 1 contracts for bounded, snapshot-based alpha research.

These native metamodels deliberately live here so OpenBB V4 exposes identical
parameters for each calculation provider without modifying provider discovery.
"""

from datetime import date, datetime
from typing import Literal

from openbb_core.provider.abstract.data import Data
from openbb_core.provider.abstract.query_params import QueryParams
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator


class Contract(BaseModel):
    """Strict immutable contract; JSON never contains NaN or infinity."""

    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    schema_version: Literal["1"] = "1"


class SourceLineage(Contract):
    """Market-data provenance, separate from the calculation engine."""

    vendor: str = Field(min_length=1, max_length=100)
    access_adapter: str = Field(min_length=1, max_length=100)
    source_dataset: str = Field(min_length=1, max_length=200)
    snapshot: str = Field(min_length=1, max_length=200)


class Session(Contract):
    """Declared open and completed-session decision timestamps."""

    session: date
    open_time: AwareDatetime
    decision_time: AwareDatetime

    @model_validator(mode="after")
    def ordered(self):
        """Reject overlapping within-session timestamps."""
        if self.open_time >= self.decision_time:
            raise ValueError("session open must precede completed-session decision")
        return self


class Bar(Contract):
    """Saved canonical instrument bar with historical eligibility."""

    instrument_id: str = Field(min_length=1, max_length=100)
    session: date
    available_at: AwareDatetime
    eligible: bool
    price: float | None
    raw_close: float | None
    raw_share_volume: float | None
    open: float | None


class DatasetInput(Contract):
    """Bounded saved inputs, with an explicit historical eligibility mask."""

    instruments: tuple[str, ...] = Field(min_length=1, max_length=1000)
    calendar: tuple[Session, ...] = Field(min_length=1, max_length=5000)
    rows: tuple[Bar, ...] = Field(max_length=100000)
    calendar_name: str = Field(min_length=1, max_length=100)
    timezone: str = Field(min_length=1, max_length=100)
    currency: str = Field(min_length=1, max_length=10)
    volume_unit: Literal["raw_shares"] = "raw_shares"
    price_convention: Literal["synthetic_no_actions", "verified_total_return_index", "raw_close_only"]
    open_convention: Literal["synthetic_no_actions", "unsupported"]
    availability_policy: Literal["observed", "current_revision_only"]
    classification: Literal["Lab", "Production"] = "Lab"
    quality_flags: tuple[str, ...] = ()
    source: SourceLineage

    @model_validator(mode="after")
    def bounded_keys(self):
        """Validate calendar, natural keys, bounds and Lab eligibility."""
        from zoneinfo import ZoneInfo

        ZoneInfo(self.timezone)
        dates = [s.session for s in self.calendar]
        if dates != sorted(set(dates)) or len(set(self.instruments)) != len(self.instruments):
            raise ValueError("duplicate or unsorted calendar/instrument keys")
        if len(dates) * len(self.instruments) > 100000:
            raise ValueError("aligned panel exceeds 100000 rows")
        if any(a.decision_time >= b.open_time for a, b in zip(self.calendar, self.calendar[1:])):
            raise ValueError("calendar sessions overlap")
        keys = [(r.instrument_id, r.session) for r in self.rows]
        if len(set(keys)) != len(keys):
            raise ValueError("duplicate instrument/session key")
        if any(i not in self.instruments or d not in dates for i, d in keys):
            raise ValueError("row outside declared universe/calendar")
        if self.classification != "Lab":
            raise ValueError("v1 accepts Lab data only; no Production certification")
        if self.price_convention == "synthetic_no_actions" and self.open_convention != "synthetic_no_actions":
            raise ValueError("inconsistent synthetic convention")
        if self.open_convention == "synthetic_no_actions" and self.price_convention != "synthetic_no_actions":
            raise ValueError("real next-open labels require a verified corporate-action implementation")
        return self


class ArtifactRef(Contract):
    """Content hash, physical schema, row count and explicitly bounded preview."""

    id: str = Field(pattern=r"^[0-9a-f]{64}$")
    media_type: str
    rows: int = Field(ge=0)
    columns: tuple[str, ...] = ()
    preview: tuple[dict, ...] = Field(default=(), max_length=10)


class ResearchDatasetRef(Contract):
    """Immutable calendar-aligned Lab dataset identity and lineage."""

    id: str = Field(pattern=r"^[0-9a-f]{64}$")
    panel: ArtifactRef
    instruments: tuple[str, ...]
    calendar: tuple[Session, ...]
    calendar_name: str
    timezone: str
    start_date: date
    end_date: date
    currency: str
    volume_unit: str
    price_convention: str
    open_convention: str
    availability_policy: str
    classification: Literal["Lab"]
    quality_flags: tuple[str, ...]
    source: SourceLineage


class FactorSpec(Contract):
    """Versioned allowlisted formula and complete-window semantics."""

    id: str
    version: Literal[1] = 1
    formula: str
    required_fields: tuple[str, ...]
    lookback: int
    skip: int = 0
    min_observations: int
    window_endpoints: str
    missing_policy: Literal["complete_window"] = "complete_window"
    units: str
    direction: Literal["higher"] = "higher"
    transform: Literal["raw"] = "raw"
    neutralization: Literal["none"] = "none"
    engines: tuple[str, ...] = ("alpha_reference", "polars_ta")


class FactorObservation(Contract):
    """One raw score; engine/spec identity resides on its enclosing artifact."""

    instrument_id: str
    session: date
    decision_time: datetime
    available_at: datetime | None
    factor_id: str
    factor_version: int = 1
    value: float | None
    null_reason: str | None
    eligible: bool


class ComputeRequest(Contract):
    """Bounded factor request shared by native calculation providers."""

    dataset_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    factors: tuple[str, ...] = Field(min_length=1, max_length=6)
    transform: Literal["raw", "centered_rank"] = "raw"
    purpose: Literal["Lab"] = "Lab"


class AlphaComputeQueryParams(QueryParams):
    """Compute registered version 1 factors from an immutable Lab snapshot."""

    request: ComputeRequest = Field(description="Typed factor request.", kw_only=True)


class FactorArtifact(Contract):
    """Immutable scores with data lineage and calculation provenance."""

    dataset_id: str
    panel: ArtifactRef
    engine: str
    engine_version: str
    spec_hash: str
    factors: tuple[FactorSpec, ...]
    transform: str
    source: SourceLineage


class AlphaComputeData(Data):
    """Immutable factor result; preview is explicitly bounded."""

    artifact_id: str
    artifact: FactorArtifact


class LabelSpec(Contract):
    """Explicit next-open session timing and simple-return convention."""

    horizons: tuple[Literal[1, 5, 20], ...] = Field(default=(1, 5, 20), min_length=1, max_length=3)
    entry: Literal["next_session_open"] = "next_session_open"
    exit: Literal["entry_plus_h_sessions_open"] = "entry_plus_h_sessions_open"
    convention: Literal["synthetic_no_actions"] = "synthetic_no_actions"
    costs: Literal["excluded"] = "excluded"
    units: Literal["simple_fractional_return"] = "simple_fractional_return"


class LabelRequest(Contract):
    """Independent label generation input."""

    dataset_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    spec: LabelSpec = LabelSpec()


class LabelObservation(Contract):
    """Signal, execution endpoints, availability and nullable forward return."""

    instrument_id: str
    session: date
    signal_time: datetime
    entry_time: datetime | None
    exit_time: datetime | None
    available_at: datetime | None
    horizon: int
    value: float | None
    null_reason: str | None
    eligible: bool


class LabelArtifact(Contract):
    """Immutable independent labels and their timing specification."""

    dataset_id: str
    panel: ArtifactRef
    spec: LabelSpec


class EvaluationRequest(Contract):
    """Artifact references and explicit evaluation loss policy."""

    factor_artifact_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    label_artifact_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    quantiles: int = Field(default=5, ge=2, le=10)
    max_unexpected_loss: float = Field(default=0, ge=0, le=1)


class AlphaEvaluateQueryParams(QueryParams):
    """Evaluate existing factor and independent label artifacts."""

    request: EvaluationRequest = Field(description="Typed evaluation request.", kw_only=True)


class FactorEvaluation(Contract):
    """Convention-tagged diagnostics with coverage and methodological warnings."""

    factor_artifact_id: str
    label_artifact_id: str
    engine: str
    engine_version: str
    configuration: dict
    metrics: tuple[dict, ...]
    coverage: tuple[dict, ...]
    warnings: tuple[str, ...]


class AlphaEvaluateData(Data):
    """Convention-tagged factor diagnostics; these are not portfolio NAV."""

    artifact_id: str
    evaluation: FactorEvaluation


class RunRequest(Contract):
    """Bounded native-provider workflow composition."""

    compute: ComputeRequest
    calculation_provider: Literal["alpha_reference", "polars_ta"] = "polars_ta"
    labels: LabelSpec = LabelSpec()
    quantiles: int = Field(default=5, ge=2, le=10)
    max_unexpected_loss: float = Field(default=0, ge=0, le=1)
    seed: int = 0


class ResearchRunManifest(Contract):
    """Reproduction inputs, source fingerprints, outputs and run status."""

    run_id: str
    created_at: AwareDatetime
    status: Literal["complete", "failed"]
    inputs: dict
    code: dict
    packages: dict
    parameters: dict
    random_seeds: tuple[int, ...]
    splits: dict
    outputs: dict
    provenance: dict
    errors: tuple[str, ...] = ()


class QlibRequest(Contract):
    """Purged chronological ridge-model request using trusted artifacts."""

    factor_artifact_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    label_artifact_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    horizon: Literal[1, 5, 20] = 5
    train_end: date
    validation_end: date
    ridge_alpha: float = Field(default=1, gt=0)
    seed: int = 0


class BtRequest(Contract):
    """Explicit long-only next-open simulation and cost assumptions."""

    factor_artifact_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    factor_id: str
    top_k: int = Field(default=3, ge=1, le=100)
    rebalance_sessions: int = Field(default=5, ge=1, le=252)
    initial_cash: float = Field(default=100000, gt=0, le=1e12)
    fractional_shares: bool = True
    commission_bps: float = Field(default=1, ge=0, le=100)
    spread_slippage_bps: float = Field(default=2, ge=0, le=100)


class QlibResult(Contract):
    """Trusted numeric model artifact and out-of-sample predictions."""

    manifest_id: str
    model_id: str
    predictions: ArtifactRef
    metrics: dict
    splits: dict


class BtResult(Contract):
    """Distinct holdings, trades, gross and net artifacts."""

    manifest_id: str
    holdings: ArtifactRef
    trades: ArtifactRef
    gross: ArtifactRef
    net: ArtifactRef
    assumptions: dict

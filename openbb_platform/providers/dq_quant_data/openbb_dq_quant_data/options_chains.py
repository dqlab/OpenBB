"""Standard options-chain provider backed by the same governed local reader."""

from datetime import datetime
from typing import Any, Literal

from openbb_core.provider.abstract.annotated_result import AnnotatedResult
from openbb_core.provider.abstract.fetcher import Fetcher
from openbb_core.provider.standard_models.options_chains import OptionsChainsQueryParams
from pydantic import Field

from openbb_dq_quant_data.market import MarketOptionsChainsData, _query, project_chain


class DQOptionsChainsQueryParams(OptionsChainsQueryParams):
    """Read a complete stored capture with explicit quality and availability semantics."""

    dataset: str = Field(default="spx_option_chains", description="Stored dataset identifier.")
    config_path: str | None = Field(
        default=None, description="Local market-store YAML; otherwise DQ_MARKET_CONFIG."
    )
    capture_id: str | None = Field(default=None, description="Pin a capture identifier.")
    publication_id: str | None = Field(default=None, description="Pin an immutable publication.")
    as_of: datetime | str | None = Field(
        default=None, description="Availability cutoff with explicit UTC offset."
    )
    availability: Literal["central", "collector"] = Field(
        default="central",
        description="Central publication or retrospective collector availability.",
    )
    layer: Literal["gold", "silver"] = Field(
        default="gold", description="Gold eligible observations or Silver with quality flags."
    )
    expiration: str | None = Field(default=None, description="Exact expiry in YYYY-MM-DD format.")
    option_type: Literal["call", "put"] | None = Field(
        default=None, description="Filter calls or puts."
    )
    max_rows: int = Field(
        default=100000,
        ge=1,
        le=100000,
        description="Maximum contracts; oversized results fail instead of truncating.",
    )
    allow_incomplete: bool = Field(
        default=False, description="Explicitly permit an incomplete capture."
    )


class DQOptionsChainsFetcher(Fetcher[DQOptionsChainsQueryParams, MarketOptionsChainsData]):
    """Expose local captures on the standard OptionsChains command."""

    require_credentials = False

    @staticmethod
    def transform_query(params: dict[str, Any]) -> DQOptionsChainsQueryParams:
        """Validate provider parameters."""
        return DQOptionsChainsQueryParams(**params)

    @staticmethod
    def extract_data(
        query: DQOptionsChainsQueryParams, credentials: dict | None, **kwargs: Any
    ) -> dict:
        """Use the same reader and defaults as obb.dq_market.chain()."""
        values = query.model_dump()
        config_path = values.pop("config_path")
        return _query("chain", config_path, **values)

    @staticmethod
    def transform_data(
        query: DQOptionsChainsQueryParams, data: dict, **kwargs: Any
    ) -> AnnotatedResult[MarketOptionsChainsData]:
        """Retain capture/publication metadata and nullable aligned columns."""
        projected = project_chain(data)
        return AnnotatedResult(result=projected.results, metadata=projected.extra)

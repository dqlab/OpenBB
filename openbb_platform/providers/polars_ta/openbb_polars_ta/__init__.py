"""Native provider; heavy imports are delayed until execution."""

from importlib.metadata import version

from openbb_alpha.catalog import select
from openbb_alpha.datasets import load
from openbb_alpha.results import save_factors
from openbb_core.provider.abstract.fetcher import Fetcher
from openbb_core.provider.abstract.provider import Provider
from openbb_core.provider.standard_models.alpha_research import (
    AlphaComputeData,
    AlphaComputeQueryParams,
)


class PolarsFetcher(Fetcher[AlphaComputeQueryParams, AlphaComputeData]):
    require_credentials = False

    @staticmethod
    def transform_query(params):
        return AlphaComputeQueryParams(**params)

    @staticmethod
    def extract_data(query, credentials, **kwargs):
        from openbb_polars_ta.engine import calculate

        request = query.request
        specs = select(request.factors)
        dataset, table = load(request.dataset_id)
        if dataset.price_convention == "raw_close_only" and any(
            "price" in s.required_fields for s in specs
        ):
            raise ValueError("price factors require an explicit consistent return index")
        try:
            result = calculate(table, specs, request.transform)
        except ImportError as exc:
            raise ValueError("Install openbb-polars-ta with its polars-ta dependencies") from exc
        return save_factors(
            request,
            dataset,
            result,
            specs,
            "polars_ta",
            f"polars-ta={version('polars-ta')};polars={version('polars')}",
        )

    @staticmethod
    def transform_data(query, data, **kwargs):
        return data


provider = Provider(
    name="polars_ta",
    description="Columnar allowlisted factor expressions.",
    website="https://polars-ta.readthedocs.io",
    fetcher_dict={"AlphaCompute": PolarsFetcher},
)

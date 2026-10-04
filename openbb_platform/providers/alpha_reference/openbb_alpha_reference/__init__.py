"""Native OpenBB independent calculation provider."""

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

from openbb_alpha_reference.oracle import calculate


class ReferenceFetcher(Fetcher[AlphaComputeQueryParams, AlphaComputeData]):
    require_credentials = False

    @staticmethod
    def transform_query(params):
        return AlphaComputeQueryParams(**params)

    @staticmethod
    def extract_data(query, credentials, **kwargs):
        import pyarrow as pa

        request = query.request
        specs = select(request.factors)
        dataset, table = load(request.dataset_id)
        if dataset.price_convention == "raw_close_only" and any(
            "price" in s.required_fields for s in specs
        ):
            raise ValueError("price factors require an explicit consistent return index")
        rows = calculate(table.to_pylist(), [s.model_dump() for s in specs], request.transform)
        schema = pa.schema(
            [
                ("instrument_id", pa.string()),
                ("session", pa.string()),
                ("decision_time", pa.string()),
                ("available_at", pa.string()),
                ("factor_id", pa.string()),
                ("factor_version", pa.int64()),
                ("value", pa.float64()),
                ("null_reason", pa.string()),
                ("eligible", pa.bool_()),
                ("rank_value", pa.float64()),
                ("rank_null_reason", pa.string()),
            ]
        )
        return save_factors(
            request,
            dataset,
            pa.Table.from_pylist(rows, schema=schema),
            specs,
            "alpha_reference",
            version("openbb-alpha-reference"),
        )

    @staticmethod
    def transform_data(query, data, **kwargs):
        return data


provider = Provider(
    name="alpha_reference",
    description="Independent standard-library factor oracle.",
    website="https://github.com/dqlab/OpenBB",
    fetcher_dict={"AlphaCompute": ReferenceFetcher},
)

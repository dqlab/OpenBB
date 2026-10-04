"""Actual Alphalens evaluation of independently constructed labels."""

from openbb_core.provider.abstract.fetcher import Fetcher
from openbb_core.provider.abstract.provider import Provider
from openbb_core.provider.standard_models.alpha_research import (
    AlphaEvaluateData,
    AlphaEvaluateQueryParams,
)


class AlphalensFetcher(Fetcher[AlphaEvaluateQueryParams, AlphaEvaluateData]):
    require_credentials = False

    @staticmethod
    def transform_query(params):
        return AlphaEvaluateQueryParams(**params)

    @staticmethod
    def extract_data(query, credentials, **kwargs):
        from openbb_alphalens.evaluation import evaluate

        try:
            return evaluate(query.request)
        except ImportError as exc:
            raise ValueError("Install openbb-alphalens with alphalens-reloaded") from exc

    @staticmethod
    def transform_data(query, data, **kwargs):
        return data


provider = Provider(
    name="alphalens",
    description="Independent-label factor diagnostics.",
    website="https://alphalens.ml4trading.io",
    fetcher_dict={"AlphaEvaluate": AlphalensFetcher},
)

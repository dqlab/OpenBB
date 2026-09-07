"""Configuration-selected OpenBB fetchers, with explicit parameter mapping."""

from __future__ import annotations

import asyncio
import hashlib
import importlib.metadata
import inspect
import json
import math
import os
import warnings
from datetime import date, datetime
from functools import lru_cache
from pathlib import Path
from typing import Any

from .models import ProviderFailure, Response


@lru_cache
def executor_for(provider_name: str) -> Any:
    # Load only the selected installed provider. The generated `obb` tree and
    # unrelated providers are not required by standalone collection workers.
    from openbb_core.provider.query_executor import QueryExecutor
    from openbb_core.provider.registry import Registry

    entries = list(
        importlib.metadata.entry_points(group="openbb_provider_extension", name=provider_name)
    )
    if len(entries) != 1:
        raise ProviderFailure("provider_not_installed_or_ambiguous")
    provider = entries[0].load()
    if provider.name.lower() != provider_name:
        raise ProviderFailure("provider_registration_mismatch")
    registry = Registry()
    registry.include_provider(provider)
    return QueryExecutor(registry)


def fetcher_for(provider: str, model: str) -> Any:
    executor = executor_for(provider)
    try:
        return executor.get_fetcher(executor.get_provider(provider), model)
    except Exception as exc:
        raise ProviderFailure("unsupported_provider_model") from exc


def request_limits(provider: str, model: str, frequency: str) -> tuple[int, int]:
    """Provider-owned span/overlap hints; collection budgets may tighten them."""
    fetcher = fetcher_for(provider, model)
    limits = getattr(fetcher, "collection_max_days", {})
    days = limits.get(frequency, limits.get("default", 365))
    overlap = getattr(fetcher, "collection_overlap_days", 0)
    if not isinstance(days, int) or days < 1 or not isinstance(overlap, int) or overlap < 0:
        raise ProviderFailure("invalid_provider_limits")
    return days, overlap


def request_parameters(source: Any, context: dict[str, Any]) -> dict[str, Any]:
    """Map named collection values into the selected provider's query schema.

    Unknown parameters are rejected even when the provider's Pydantic model
    would silently discard extras. Mapping is data, never eval or template code.
    """
    model_assets = {
        "EquityQuote": {"stock", "etf"},
        "EquityHistorical": {"stock", "etf"},
        "IndexHistorical": {"index"},
    }
    if source.model in model_assets and context.get("asset_type") not in model_assets[source.model]:
        raise ProviderFailure("unsupported_model_asset_type")
    params = dict(source.parameters)
    for target, origin in source.parameter_map.items():
        if origin not in context or target in params:
            raise ProviderFailure("invalid_parameter_mapping")
        params[target] = context[origin]
    fetcher = fetcher_for(source.provider, source.model)
    query_type = fetcher.query_params_type
    if set(params) - query_type.model_fields.keys():
        raise ProviderFailure("unsupported_provider_parameters")
    try:
        # Validate without invoking transform_query, which may apply defaults
        # based on the clock. Acquisition still uses the normal QueryExecutor.
        query_type.model_validate(params)
    except Exception as exc:
        raise ProviderFailure("invalid_provider_parameters") from exc
    return params


def json_value(value: Any) -> Any:
    if hasattr(value, "model_dump"):
        return json_value(value.model_dump(mode="python"))
    if isinstance(value, dict):
        return {str(k): json_value(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_value(v) for v in value]
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, float) and not math.isfinite(value):
        return str(value)
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def package_metadata(provider: str, model: str) -> dict[str, Any]:
    fetcher = fetcher_for(provider, model)
    module = fetcher.__module__
    distributions = importlib.metadata.packages_distributions().get(module.split(".")[0], [])
    result: dict[str, Any] = {"provider": provider, "model": model}
    for name in ["openbb-core", *distributions]:
        try:
            dist = importlib.metadata.distribution(name)
            item = {"version": dist.version}
            direct = dist.read_text("direct_url.json")
            if direct:
                commit = json.loads(direct).get("vcs_info", {}).get("commit_id")
                if commit:
                    item["commit"] = commit
            result[name] = item
        except importlib.metadata.PackageNotFoundError:
            continue
    filename = inspect.getsourcefile(fetcher)
    if filename and Path(filename).is_file():
        result[module] = {"sha256": hashlib.sha256(Path(filename).read_bytes()).hexdigest()}
    return result


class ProviderDispatcher:
    """Execute the configured model through OpenBB's credential-filtering executor."""

    def __init__(self, source: dict[str, Any]) -> None:
        from pydantic import SecretStr

        self.source = source
        self.executor = executor_for(source["provider"])
        self.fetcher = fetcher_for(source["provider"], source["model"])
        self.credentials = {}
        for key, variable in source["credentials_env"].items():
            value = os.environ.get(variable)
            if not value:
                raise ProviderFailure("missing_credentials")
            self.credentials[key] = SecretStr(value)
        provider = self.executor.get_provider(source["provider"])
        if set(self.credentials) - set(provider.credentials or []):
            raise ProviderFailure("unsupported_credentials")
        try:
            self.executor.filter_credentials(
                self.credentials, provider, self.fetcher.require_credentials
            )
        except Exception as exc:
            raise ProviderFailure("missing_credentials") from exc
        self.metadata = package_metadata(source["provider"], source["model"])

    def fetch(self, params: dict[str, Any]) -> Response:
        from openbb_core.provider.abstract.annotated_result import AnnotatedResult

        with warnings.catch_warnings(record=True) as captured:
            warnings.simplefilter("always")
            result = asyncio.run(
                self.executor.execute(
                    provider_name=self.source["provider"],
                    model_name=self.source["model"],
                    params=params,
                    credentials=self.credentials,
                )
            )
        metadata = dict(self.metadata)
        if isinstance(result, AnnotatedResult):
            metadata.update(result.metadata or {})
            result = result.result
        metadata.update(provider=self.source["provider"], model=self.source["model"])
        return Response(json_value(result if result is not None else []), len(captured), metadata)

    def close(self) -> None:
        cleanup = getattr(self.fetcher, "close_collection", None)
        if cleanup:
            cleanup()

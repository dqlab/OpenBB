"""Additive source-evidence models; legacy native models are unchanged."""

import re
from datetime import date
from typing import Any

from openbb_core.provider.abstract.data import Data
from openbb_core.provider.abstract.fetcher import Fetcher
from openbb_core.provider.abstract.query_params import QueryParams
from pydantic import ConfigDict, Field

from openbb_baostock.utils.source_evidence import OPERATIONS, query_evidence


class SourceQuery(QueryParams):
    """A literal SDK parameter document, with no clock/default normalization."""

    model_config = ConfigDict(extra="forbid", strict=True)
    literal: dict[str, Any] = Field(description="Exact explicitly supplied SDK parameters.")


class SourceData(Data):
    """One opaque, honestly labelled SDK evidence envelope."""

    model_config = ConfigDict(extra="forbid")
    evidence: dict[str, Any]


def validate_literal(operation, params):
    """Validate the protocol allowlist without silently repairing parameters."""
    allowed = {
        "BaostockHistoryKDataPlus": {
            "code",
            "fields",
            "start_date",
            "end_date",
            "frequency",
            "adjustflag",
        },
        "BaostockAdjustFactor": {"code", "start_date", "end_date"},
        "BaostockStockBasic": {"code", "code_name"},
        "BaostockTradeDates": {"start_date", "end_date"},
        "BaostockDailyHistoryKETF": {"date"},
    }
    if operation not in allowed or type(params) is not dict or set(params) - allowed[operation]:
        raise ValueError("unsupported_literal_parameters")
    if any(type(v) is not str or not v or re.search(r"[\x00-\x1f]", v) for v in params.values()):
        raise ValueError("literal_string_required")
    required = allowed[operation] if operation != "BaostockStockBasic" else set()
    if required - set(params):
        raise ValueError("explicit_parameters_required")
    if "code" in params and not re.fullmatch(r"(?:sh|sz)\.[0-9]{6}", params["code"]):
        raise ValueError("literal_code_required")
    for key in ("date", "start_date", "end_date"):
        if key in params and date.fromisoformat(params[key]).isoformat() != params[key]:
            raise ValueError("literal_date_required")
    if params.get("start_date", "") > params.get("end_date", "9999"):
        raise ValueError("invalid_literal_range")
    if operation == "BaostockHistoryKDataPlus":
        if not re.fullmatch(r"[A-Za-z][A-Za-z0-9]*(,[A-Za-z][A-Za-z0-9]*)*", params["fields"]):
            raise ValueError("invalid_fields")
        if params["frequency"] not in {"d", "w", "m", "5", "15", "30", "60"} or params["adjustflag"] not in {
            "1",
            "2",
            "3",
        }:
            raise ValueError("invalid_history_protocol")
    return dict(params)


def make_source_fetcher(operation):
    """Register one explicit operation without changing native fetchers."""

    class SourceFetcher(Fetcher[SourceQuery, list[SourceData]]):
        require_credentials = False

        @staticmethod
        def transform_query(params):
            query = SourceQuery(**params)
            validate_literal(operation, query.literal)
            return query

        @staticmethod
        async def aextract_data(query, credentials, **kwargs):
            return await query_evidence(operation, query.literal, evidence_sink_v1=kwargs.get("evidence_sink_v1"))

        @staticmethod
        def transform_data(query, data, **kwargs):
            return [SourceData(evidence=data)]

    SourceFetcher.__name__ = operation + "SourceFetcher"
    SourceFetcher.__qualname__ = SourceFetcher.__name__
    globals()[SourceFetcher.__name__] = SourceFetcher
    return SourceFetcher


SOURCE_FETCHERS = {name + "Source": make_source_fetcher(name) for name in OPERATIONS}

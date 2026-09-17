"""OpenBB standard-model fetchers backed by the dq data API."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

from openbb_core.provider.abstract.fetcher import Fetcher
from openbb_core.provider.standard_models.calendar_dividend import (
    CalendarDividendData,
    CalendarDividendQueryParams,
)
from openbb_core.provider.standard_models.calendar_splits import (
    CalendarSplitsData,
    CalendarSplitsQueryParams,
)
from openbb_core.provider.standard_models.company_filings import (
    CompanyFilingsData,
    CompanyFilingsQueryParams,
)
from openbb_core.provider.standard_models.equity_historical import (
    EquityHistoricalData,
    EquityHistoricalQueryParams,
)
from openbb_core.provider.standard_models.equity_info import (
    EquityInfoData,
    EquityInfoQueryParams,
)
from openbb_core.provider.standard_models.equity_search import (
    EquitySearchData,
    EquitySearchQueryParams,
)
from openbb_core.provider.standard_models.etf_historical import (
    EtfHistoricalData,
    EtfHistoricalQueryParams,
)
from openbb_core.provider.standard_models.fred_series import (
    SeriesData,
    SeriesQueryParams,
)

from openbb_dq_quant_data.client import DQAPIClient


def _records(
    path: str,
    params: dict[str, Any],
    credentials: dict[str, str] | None,
) -> list[dict[str, Any]]:
    return DQAPIClient(credentials).get(path, params)


def _first(row: dict[str, Any], *names: str) -> Any:
    """Return the first non-null source value, retaining zero."""
    return next((row[name] for name in names if row.get(name) is not None), None)


def _provenance(row: dict[str, Any]) -> dict[str, Any]:
    """Retain supplied units, adjustment and source identity without guessing defaults."""
    names = {
        "instrument_id",
        "currency",
        "adjustment_status",
        "adjustment",
        "interval",
        "session",
        "exchange_timezone",
        "volume_unit",
        "price_unit",
        "source_provider",
        "source_dataset",
        "source_record_id",
        "ingestion_run_id",
        "snapshot_id",
        "schema_version",
        "quality_status",
        "warning_codes",
        "available_time",
        "ingested_at",
        "published_at",
    }
    return {key: value for key, value in row.items() if key in names}


def _date_value(row: dict[str, Any], *names: str) -> date | datetime | None:
    value = next((row.get(name) for name in names if row.get(name) is not None), None)
    if value is None:
        return None
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return parsed if "T" in str(value) else parsed.date()


class DQEquityHistoricalData(EquityHistoricalData):
    """Historical equity data from a canonical platform snapshot."""

    symbol: str | None = None
    adj_close: float | None = None
    dividend: float | None = None
    split_ratio: float | None = None


class DQEquityHistoricalFetcher(Fetcher[EquityHistoricalQueryParams, list[DQEquityHistoricalData]]):
    """Fetch canonical equity bars."""

    @staticmethod
    def transform_query(params: dict[str, Any]) -> EquityHistoricalQueryParams:
        return EquityHistoricalQueryParams(**params)

    @staticmethod
    def extract_data(
        query: EquityHistoricalQueryParams,
        credentials: dict[str, str] | None,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        del kwargs
        return _records(
            "/api/v1/market/bars",
            {
                "symbols": query.symbol,
                "start_date": query.start_date,
                "end_date": query.end_date,
                "limit": 10000,
            },
            credentials,
        )

    @staticmethod
    def transform_data(
        query: EquityHistoricalQueryParams,
        data: list[dict[str, Any]],
        **kwargs: Any,
    ) -> list[DQEquityHistoricalData]:
        del query, kwargs
        output = []
        for row in data:
            timestamp = _date_value(row, "date", "event_time", "timestamp")
            required = (
                timestamp,
                row.get("open"),
                row.get("high"),
                row.get("low"),
                row.get("close"),
            )
            if any(value is None for value in required):
                continue
            output.append(
                DQEquityHistoricalData(
                    **_provenance(row),
                    date=timestamp,
                    symbol=row.get("canonical_symbol") or row.get("symbol"),
                    open=float(row["open"]),
                    high=float(row["high"]),
                    low=float(row["low"]),
                    close=float(row["close"]),
                    volume=row.get("volume"),
                    vwap=row.get("vwap"),
                    adj_close=_first(row, "adjusted_close", "adj_close"),
                    dividend=row.get("dividend"),
                    split_ratio=_first(row, "split_factor", "split_ratio"),
                )
            )
        return output


class DQEtfHistoricalData(EtfHistoricalData):
    """Historical ETF data with canonical symbol lineage and source volume."""

    symbol: str | None = None
    volume: float | None = None


class DQEtfHistoricalFetcher(Fetcher[EtfHistoricalQueryParams, list[DQEtfHistoricalData]]):
    """Fetch canonical ETF bars."""

    @staticmethod
    def transform_query(params: dict[str, Any]) -> EtfHistoricalQueryParams:
        return EtfHistoricalQueryParams(**params)

    @staticmethod
    def extract_data(
        query: EtfHistoricalQueryParams,
        credentials: dict[str, str] | None,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        del kwargs
        return _records(
            "/api/v1/market/bars",
            {
                "symbols": query.symbol,
                "start_date": query.start_date,
                "end_date": query.end_date,
                "limit": 10000,
            },
            credentials,
        )

    @staticmethod
    def transform_data(
        query: EtfHistoricalQueryParams,
        data: list[dict[str, Any]],
        **kwargs: Any,
    ) -> list[DQEtfHistoricalData]:
        del query, kwargs
        return [
            DQEtfHistoricalData(
                **_provenance(row),
                date=_date_value(row, "date", "event_time", "timestamp"),
                symbol=row.get("canonical_symbol") or row.get("symbol"),
                open=float(row["open"]),
                high=float(row["high"]),
                low=float(row["low"]),
                close=float(row["close"]),
                volume=row.get("volume"),
            )
            for row in data
            if _date_value(row, "date", "event_time", "timestamp") is not None
            and all(row.get(field) is not None for field in ("open", "high", "low", "close"))
        ]


class DQEquitySearchData(EquitySearchData):
    """Instrument search result."""

    instrument_id: str | None = None
    exchange: str | None = None
    asset_class: str | None = None


class DQEquitySearchFetcher(Fetcher[EquitySearchQueryParams, list[DQEquitySearchData]]):
    """Search the canonical instrument master."""

    @staticmethod
    def transform_query(params: dict[str, Any]) -> EquitySearchQueryParams:
        return EquitySearchQueryParams(**params)

    @staticmethod
    def extract_data(
        query: EquitySearchQueryParams,
        credentials: dict[str, str] | None,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        del kwargs
        return _records(
            "/api/v1/reference/instruments",
            {"query": query.query or None} if query.is_symbol else {},
            credentials,
        )

    @staticmethod
    def transform_data(
        query: EquitySearchQueryParams,
        data: list[dict[str, Any]],
        **kwargs: Any,
    ) -> list[DQEquitySearchData]:
        del kwargs
        return [
            DQEquitySearchData(
                **_provenance(row),
                symbol=row.get("canonical_symbol") or row.get("symbol"),
                name=row.get("name"),
                exchange=row.get("exchange"),
                asset_class=row.get("asset_class"),
            )
            for row in data
            if not query.query
            or query.is_symbol
            or query.query.casefold() in str(row.get("name") or "").casefold()
            or query.query.casefold()
            in str(row.get("canonical_symbol") or row.get("symbol") or "").casefold()
        ]


class DQEquityInfoFetcher(Fetcher[EquityInfoQueryParams, list[EquityInfoData]]):
    """Fetch canonical company reference data."""

    @staticmethod
    def transform_query(params: dict[str, Any]) -> EquityInfoQueryParams:
        return EquityInfoQueryParams(**params)

    @staticmethod
    def extract_data(
        query: EquityInfoQueryParams,
        credentials: dict[str, str] | None,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        del kwargs
        return _records("/api/v1/reference/instruments", {"query": query.symbol}, credentials)

    @staticmethod
    def transform_data(
        query: EquityInfoQueryParams,
        data: list[dict[str, Any]],
        **kwargs: Any,
    ) -> list[EquityInfoData]:
        del query, kwargs
        return [
            EquityInfoData(
                **_provenance(row),
                symbol=row.get("canonical_symbol") or row.get("symbol"),
                name=row.get("name"),
                cik=row.get("cik"),
                cusip=row.get("cusip"),
                isin=row.get("isin"),
                lei=row.get("lei"),
                stock_exchange=row.get("exchange"),
                sector=row.get("sector"),
                industry_category=row.get("industry"),
                hq_country=row.get("country"),
                standardized_active=row.get("active"),
            )
            for row in data
        ]


class DQCalendarDividendFetcher(Fetcher[CalendarDividendQueryParams, list[CalendarDividendData]]):
    """Fetch canonical cash-dividend events."""

    @staticmethod
    def transform_query(params: dict[str, Any]) -> CalendarDividendQueryParams:
        return CalendarDividendQueryParams(**params)

    @staticmethod
    def extract_data(
        query: CalendarDividendQueryParams,
        credentials: dict[str, str] | None,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        del kwargs
        return _records(
            "/api/v1/corporate-actions",
            {"start_date": query.start_date, "end_date": query.end_date, "limit": 10000},
            credentials,
        )

    @staticmethod
    def transform_data(
        query: CalendarDividendQueryParams,
        data: list[dict[str, Any]],
        **kwargs: Any,
    ) -> list[CalendarDividendData]:
        del query, kwargs
        return [
            CalendarDividendData(
                **_provenance(row),
                ex_dividend_date=_date_value(row, "ex_date", "effective_date", "timestamp"),
                symbol=row.get("canonical_symbol") or row.get("symbol"),
                amount=_first(row, "value", "dividend"),
                record_date=_date_value(row, "record_date"),
                payment_date=_date_value(row, "payment_date"),
                declaration_date=_date_value(row, "declaration_date", "declared_date"),
            )
            for row in data
            if row.get("action_type") in {"cash_dividend", "dividend"}
            and _date_value(row, "ex_date", "effective_date", "timestamp") is not None
            and (row.get("canonical_symbol") or row.get("symbol"))
        ]


class DQCalendarSplitsFetcher(Fetcher[CalendarSplitsQueryParams, list[CalendarSplitsData]]):
    """Fetch canonical split events."""

    @staticmethod
    def transform_query(params: dict[str, Any]) -> CalendarSplitsQueryParams:
        return CalendarSplitsQueryParams(**params)

    @staticmethod
    def extract_data(
        query: CalendarSplitsQueryParams,
        credentials: dict[str, str] | None,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        del kwargs
        return _records(
            "/api/v1/corporate-actions",
            {"start_date": query.start_date, "end_date": query.end_date, "limit": 10000},
            credentials,
        )

    @staticmethod
    def transform_data(
        query: CalendarSplitsQueryParams,
        data: list[dict[str, Any]],
        **kwargs: Any,
    ) -> list[CalendarSplitsData]:
        del query, kwargs
        output = []
        for row in data:
            if row.get("action_type") not in {"split", "reverse_split"}:
                continue
            event_date = _date_value(row, "effective_date", "ex_date", "timestamp")
            symbol = row.get("canonical_symbol") or row.get("symbol")
            ratio = float(_first(row, "value", "split_factor") or 0)
            if event_date is None or not symbol or ratio <= 0:
                continue
            output.append(
                CalendarSplitsData(
                    **_provenance(row),
                    date=event_date,
                    symbol=symbol,
                    numerator=ratio if ratio >= 1 else 1,
                    denominator=1 if ratio >= 1 else 1 / ratio,
                )
            )
        return output


class DQCompanyFilingsData(CompanyFilingsData):
    """Company filing with canonical metadata."""

    symbol: str | None = None
    accession: str | None = None


class DQCompanyFilingsFetcher(Fetcher[CompanyFilingsQueryParams, list[DQCompanyFilingsData]]):
    """Fetch canonical filings."""

    @staticmethod
    def transform_query(params: dict[str, Any]) -> CompanyFilingsQueryParams:
        return CompanyFilingsQueryParams(**params)

    @staticmethod
    def extract_data(
        query: CompanyFilingsQueryParams,
        credentials: dict[str, str] | None,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        del kwargs
        return _records(
            "/api/v1/fundamentals/filings",
            {"symbols": query.symbol, "limit": 10000},
            credentials,
        )

    @staticmethod
    def transform_data(
        query: CompanyFilingsQueryParams,
        data: list[dict[str, Any]],
        **kwargs: Any,
    ) -> list[DQCompanyFilingsData]:
        del query, kwargs
        return [
            DQCompanyFilingsData(
                **_provenance(row),
                filing_date=_date_value(row, "filing_date", "timestamp"),
                report_type=row.get("form") or row.get("report_type"),
                report_url=row.get("filing_url") or row.get("report_url"),
                symbol=row.get("canonical_symbol") or row.get("symbol"),
                accession=row.get("accession"),
            )
            for row in data
            if _date_value(row, "filing_date", "timestamp") is not None
            and (row.get("filing_url") or row.get("report_url"))
        ]


class DQFredSeriesFetcher(Fetcher[SeriesQueryParams, list[SeriesData]]):
    """Fetch point-in-time macro observations."""

    @staticmethod
    def transform_query(params: dict[str, Any]) -> SeriesQueryParams:
        return SeriesQueryParams(**params)

    @staticmethod
    def extract_data(
        query: SeriesQueryParams,
        credentials: dict[str, str] | None,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        del kwargs
        return _records(
            "/api/v1/macro/observations",
            {
                "series_ids": query.symbol,
                "start_date": query.start_date,
                "end_date": query.end_date,
                "limit": min(query.limit or 10000, 10000),
            },
            credentials,
        )

    @staticmethod
    def transform_data(
        query: SeriesQueryParams,
        data: list[dict[str, Any]],
        **kwargs: Any,
    ) -> list[SeriesData]:
        del kwargs
        output = []
        for row in data[: query.limit] if query.limit is not None else data:
            observation_date = _date_value(row, "observation_date", "timestamp")
            series_id = row.get("series_id") or query.symbol
            if observation_date is None or not series_id:
                continue
            output.append(
                SeriesData.model_validate({"date": observation_date, series_id: row.get("value")})
            )
        return output

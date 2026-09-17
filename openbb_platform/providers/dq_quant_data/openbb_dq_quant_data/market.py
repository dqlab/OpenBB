"""OpenBB SDK access to the governed local market store."""

from datetime import datetime

from openbb_core.app.model.abstract.error import OpenBBError
from openbb_core.app.model.obbject import OBBject
from openbb_core.provider.standard_models.options_chains import OptionsChainsData
from pydantic import Field


class MarketOptionsChainsData(OptionsChainsData):
    contract_symbol: list[str | None] = Field(default_factory=list)
    instrument_uid: list[str | None] = Field(default_factory=list)
    observation_uid: list[str | None] = Field(default_factory=list)
    available_at: list[datetime | None] = Field(default_factory=list)
    event_time: list[datetime | None] = Field(default_factory=list)
    raw_hash: list[str | None] = Field(default_factory=list)
    actual_feed_type: list[str | None] = Field(default_factory=list)
    currency: list[str | None] = Field(default_factory=list)


def _query(method: str, config_path: str | None, **kwargs) -> dict:
    try:
        from dq_quant_invest_data.data.market.config import load_market_config
        from dq_quant_invest_data.data.market.reader import MarketReader

        return getattr(MarketReader(load_market_config(config_path)), method)(**kwargs)
    except ModuleNotFoundError as exc:
        if exc.name and exc.name.startswith("dq_quant_invest_data"):
            raise OpenBBError(
                "Install dq-quant-invest-data for local market-store access."
            ) from None
        raise
    except (ValueError, OSError, TimeoutError) as exc:
        detail = str(exc)
        if "/" in detail or "\\" in detail or len(detail) > 200:
            detail = "Market store is unavailable or the query is invalid."
        raise OpenBBError(detail) from None


def project_chain(result: dict) -> OBBject[MarketOptionsChainsData]:
    records = [
        {**row, "contract_symbol": row["symbol"], "available_at": row["collector_observed_at"]}
        for row in result["data"]
    ]
    fields = set(MarketOptionsChainsData.model_fields)
    chain = MarketOptionsChainsData.model_validate(
        {
            key: [r.get(key) for r in records]
            for key in fields
            if records and any(r.get(key) is not None for r in records)
        }
    )
    return OBBject(results=chain, extra=result["meta"])

"""Explicit inventory of data queries in BaoStock 0.9.3, including module APIs."""

from dataclasses import dataclass
from importlib import import_module
from typing import Any


@dataclass(frozen=True)
class Dataset:
    """A supported SDK function and its public OpenBB command."""

    method: str
    module: str
    description: str
    parameters: str
    public: bool = True

    @property
    def command(self) -> str:
        """Use the source function name without query_, normalized to lowercase."""
        return self.method.removeprefix("query_").lower()

    @property
    def model(self) -> str:
        """Return the unique provider model name."""
        return "Baostock" + "".join(part.capitalize() for part in self.command.split("_"))


DATASETS = (
    Dataset(
        "query_history_k_data_plus", "security.history", "Historical OHLCV bars with source-selected fields.", "history"
    ),
    Dataset("query_daily_history_k_AStock", "security.history", "All A-share daily bars for a trading date.", "date"),
    Dataset("query_daily_history_k_ETF", "security.history", "All ETF daily bars for a trading date.", "date"),
    Dataset(
        "query_stock_basic",
        "metadata.stock_metadata",
        "Basic metadata for all security types, including delisted listings.",
        "basic",
    ),
    Dataset("query_trade_dates", "metadata.stock_metadata", "Trading calendar with open and closed dates.", "range"),
    Dataset("query_all_stock", "metadata.stock_metadata", "Securities and trading status for a specified day.", "day"),
    Dataset(
        "query_stock_industry",
        "security.sectorinfo",
        "Industry classifications for one or all securities.",
        "classification",
    ),
    Dataset("query_hs300_stocks", "security.sectorinfo", "CSI 300 index constituents.", "date"),
    Dataset("query_sz50_stocks", "security.sectorinfo", "SSE 50 index constituents.", "date"),
    Dataset("query_zz500_stocks", "security.sectorinfo", "CSI 500 index constituents.", "date"),
    Dataset(
        "query_dividend_data",
        "evaluation.season_index",
        "Dividend proposals and distributions, including cash and stock components.",
        "dividend",
    ),
    Dataset(
        "query_adjust_factor",
        "evaluation.season_index",
        "Corporate-action price adjustment factors for one security.",
        "code_range",
    ),
    Dataset(
        "query_daily_adjust_factor",
        "evaluation.season_index",
        "Adjustment factors for all securities on one date.",
        "date",
    ),
    Dataset("query_profit_data", "evaluation.season_index", "Quarterly profitability indicators.", "quarter"),
    Dataset("query_operation_data", "evaluation.season_index", "Quarterly operating efficiency indicators.", "quarter"),
    Dataset("query_growth_data", "evaluation.season_index", "Quarterly growth indicators.", "quarter"),
    Dataset(
        "query_balance_data",
        "evaluation.season_index",
        "Quarterly solvency indicators; not a full balance sheet.",
        "quarter",
    ),
    Dataset("query_cash_flow_data", "evaluation.season_index", "Quarterly cash-flow indicators.", "quarter"),
    Dataset("query_dupont_data", "evaluation.season_index", "Quarterly DuPont decomposition.", "quarter"),
    Dataset(
        "query_performance_express_report",
        "corpreport.corp_performance",
        "Preliminary earnings reports, filtered by publication or update date.",
        "code_range",
    ),
    Dataset(
        "query_forecast_report",
        "corpreport.corp_performance",
        "Earnings forecasts, using BaoStock publication/statistical date filtering.",
        "code_range",
    ),
    Dataset("query_deposit_rate_data", "macroscopic.economic_data", "Deposit interest rates in source units.", "range"),
    Dataset("query_loan_rate_data", "macroscopic.economic_data", "Lending interest rates in source units.", "range"),
    Dataset(
        "query_required_reserve_ratio_data",
        "macroscopic.economic_data",
        "Required reserve ratios by announcement or effective date.",
        "reserve",
    ),
    Dataset(
        "query_money_supply_data_month",
        "macroscopic.economic_data",
        "Monthly money supply in source units.",
        "month_range",
    ),
    Dataset(
        "query_money_supply_data_year", "macroscopic.economic_data", "Annual money supply in source units.", "year_range"
    ),
    # These are callable SDK-module APIs, not re-exported by baostock.__init__.
    # Server availability is independent of whether a function is in the SDK.
    Dataset(
        "query_terminated_stocks", "security.sectorinfo", "Terminated stock listings (SDK-module API).", "date", False
    ),
    Dataset("query_suspended_stocks", "security.sectorinfo", "Suspended stock listings (SDK-module API).", "date", False),
    Dataset("query_st_stocks", "security.sectorinfo", "ST stock listings (SDK-module API).", "date", False),
    Dataset("query_starst_stocks", "security.sectorinfo", "*ST stock listings (SDK-module API).", "date", False),
    Dataset(
        "query_stock_concept", "security.sectorinfo", "Concept classifications (SDK-module API).", "classification", False
    ),
    Dataset(
        "query_stock_area", "security.sectorinfo", "Regional classifications (SDK-module API).", "classification", False
    ),
    Dataset("query_ame_stocks", "security.sectorinfo", "SME board stock listings (SDK-module API).", "date", False),
    Dataset("query_gem_stocks", "security.sectorinfo", "ChiNext stock listings (SDK-module API).", "date", False),
    Dataset(
        "query_shhk_stocks",
        "security.sectorinfo",
        "Shanghai-Hong Kong Stock Connect listings (SDK-module API).",
        "date",
        False,
    ),
    Dataset(
        "query_szhk_stocks",
        "security.sectorinfo",
        "Shenzhen-Hong Kong Stock Connect listings (SDK-module API).",
        "date",
        False,
    ),
    Dataset(
        "query_stocks_in_risk",
        "security.sectorinfo",
        "Risk-warning board stock listings (SDK-module API).",
        "date",
        False,
    ),
    Dataset(
        "query_cpi_data", "macroscopic.economic_data", "Consumer price index series (SDK-module API).", "range", False
    ),
    Dataset(
        "query_ppi_data", "macroscopic.economic_data", "Producer price index series (SDK-module API).", "range", False
    ),
    Dataset(
        "query_pmi_data",
        "macroscopic.economic_data",
        "Purchasing managers index series (SDK-module API).",
        "range",
        False,
    ),
)

DATASET_BY_METHOD = {dataset.method: dataset for dataset in DATASETS}


def resolve_method(method: str) -> Any:
    """Resolve only explicitly supported SDK methods, never arbitrary user paths."""
    dataset = DATASET_BY_METHOD[method]
    # Prefer public functions so upstream instrumentation and test doubles work.
    module = import_module("baostock" if dataset.public else f"baostock.{dataset.module}")
    return getattr(module, method)

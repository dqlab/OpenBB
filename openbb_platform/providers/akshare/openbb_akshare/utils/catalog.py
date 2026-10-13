"""Explicit inventory of AKShare interfaces exposed by this provider."""

from dataclasses import dataclass, field
from importlib import import_module
from typing import Any


@dataclass(frozen=True)
class Dataset:
    """A supported AKShare function and its public OpenBB command."""

    method: str
    description: str
    parameters: str
    identifiers: tuple[str, ...] = field(default=("symbol",))
    module: str = ""
    public: bool = True

    @property
    def command(self) -> str:
        """Use the source function name without the ``get_`` prefix, lower-cased."""
        return self.method.removeprefix("get_").lower()

    @property
    def model(self) -> str:
        """Return the unique provider model name."""
        return "Akshare" + "".join(part.capitalize() for part in self.command.split("_"))


DATASETS = (
    Dataset(
        "get_cffex_daily",
        "CFFEX daily quotes for every listed contract on one trading day.",
        "date",
    ),
    Dataset(
        "futures_hist_daily_cffex",
        "CFFEX historical daily quotes for every listed contract on one trading day.",
        "date",
    ),
    Dataset(
        "get_cffex_rank_table",
        "CFFEX top-20 member volume and open-interest rankings for one trading day.",
        "rank",
    ),
    Dataset(
        "futures_zh_daily_sina",
        "China futures daily bars for one continuous or dated contract.",
        "symbol",
        identifiers=("date",),
    ),
    Dataset(
        "futures_zh_minute_sina",
        "China futures intraday bars for one continuous or dated contract.",
        "symbol_period",
        identifiers=("datetime",),
    ),
    Dataset(
        "stock_zh_index_daily",
        "Daily bars for one Chinese index, addressed with a Sina index symbol.",
        "index_symbol",
        identifiers=("date",),
    ),
    Dataset(
        "futures_main_sina",
        "Daily bars for one China futures main/continuous contract over a date range.",
        "symbol_range",
        identifiers=("日期",),
    ),
    Dataset(
        "bond_zh_us_rate",
        "China and US government bond yields by tenor plus GDP growth, from a start date.",
        "start_date",
        identifiers=("日期",),
    ),
)

DATASET_BY_METHOD = {dataset.method: dataset for dataset in DATASETS}


def resolve_method(method: str) -> Any:
    """Resolve only explicitly supported AKShare methods, never arbitrary paths."""
    dataset = DATASET_BY_METHOD[method]
    # Import lazily so provider registration does not require the akshare package.
    module = import_module("akshare" if dataset.public else f"akshare.{dataset.module}")
    return getattr(module, method)

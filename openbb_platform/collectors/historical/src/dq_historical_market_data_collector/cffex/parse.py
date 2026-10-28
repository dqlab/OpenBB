"""Downstream parse of an already-captured CFFEX daily CSV.

This module runs **after** the bytes are in the immutable store, so it may reject
a body without losing it. It is deliberately narrow:

* it never adds a trading date. The exchange layout (``cffex-rtj-csv-v1``) has no
  date column, so the only date available is the one the request named. That
  provenance is recorded by the caller (``date_source="request_parameter"``) and is
  never dressed up as something the row said;
* it never converts a number. Every price, volume and amount stays the exact
  string the source sent (whitespace trimmed); numeric acceptance is
  :func:`dq_historical_market_data_collector.cffex.normalize.number`'s job;
* it never invents a unit. If the layout carries a unit column it is read
  verbatim; if it does not -- which is the case for this layout -- the payload
  simply has no units and the committed path refuses it as ``units_undetermined``
  unless configuration declared them explicitly.

The structural rules (duplicate headers, truncated rows, contract grammar,
totals/option exclusion, duplicate contracts) are the same ones the provider
adapter applies, moved downstream so that a malformed body is captured instead of
raising inside the provider.
"""

from __future__ import annotations

import csv
import re
from dataclasses import dataclass
from datetime import date
from io import StringIO

from .source import SOURCE_ENCODING, SOURCE_LAYOUT

COLUMNS = {
    "合约代码": "symbol",
    "今开盘": "open",
    "最高价": "high",
    "最低价": "low",
    "今收盘": "close",
    "成交量": "volume",
    "持仓量": "open_interest",
    "成交金额": "turnover",
    "今结算": "settle",
    "前结算": "pre_settle",
}
FUTURE = re.compile(r"^(IF|IH|IC|IM|TS|TF|TL|T)[0-9]{2}(0[1-9]|1[0-2])$")
OPTION = re.compile(r"^(IO|HO|MO)[0-9]{4}-[CP]-[0-9]+$")
SUMMARY = {"小计", "合计"}
# Optional columns: read when the layout states them, never assumed.
DATE_COLUMNS = {"日期": "date", "交易日期": "date", "trade_date": "date", "date": "date"}
UNIT_COLUMNS = {
    "price_unit": "price_unit",
    "volume_unit": "volume_unit",
    "open_interest_unit": "open_interest_unit",
    "turnover_unit": "turnover_unit",
}
UNIT_KEYS = tuple(UNIT_COLUMNS.values())
NUMERIC = ("open", "high", "low", "close", "settle", "pre_settle", "volume",
           "open_interest", "turnover")


class SourceBodyError(ValueError):
    """A captured body that is not the layout it claims to be; carries a code."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


@dataclass(frozen=True)
class ParsedDay:
    """Rows as the source stated them, plus exactly what was removed to get them."""

    rows: list[dict[str, str]]
    filtered: dict[str, int]
    units: dict[str, str]
    date_column: str | None
    header: list[str]
    layout: str = SOURCE_LAYOUT
    encoding: str = SOURCE_ENCODING
    transforms: tuple[str, ...] = ()

    def provenance(self) -> dict[str, object]:
        return {
            "layout": self.layout,
            "encoding": self.encoding,
            "date_column": self.date_column,
            "unit_source": "source" if self.units else "absent",
            "units": dict(self.units),
            "filtered": dict(self.filtered),
            "transforms": list(self.transforms),
        }


def _decode(body: bytes) -> str:
    if type(body) is not bytes:
        raise SourceBodyError("source_body_not_bytes")
    try:
        return body.decode(SOURCE_ENCODING)
    except UnicodeDecodeError as exc:
        raise SourceBodyError("source_body_undecodable") from exc


def parse_daily_csv(body: bytes, *, requested: date | None = None) -> ParsedDay:
    """Structural parse of one captured body; no date, no numbers, no units invented."""
    del requested  # the body states no date: the caller records where the date came from
    text = _decode(body)
    try:
        records = [row for row in csv.reader(StringIO(text), strict=True) if row]
    except csv.Error as exc:
        raise SourceBodyError("source_body_malformed") from exc
    if not records:
        raise SourceBodyError("source_body_empty")
    header = [column.strip().lstrip("\ufeff") for column in records[0]]
    if len(header) != len(set(header)):
        raise SourceBodyError("source_header_duplicate")
    if not set(COLUMNS).issubset(header):
        raise SourceBodyError("source_header_missing")
    body_rows = [row for row in records[1:] if row[0].strip() not in SUMMARY]
    totals = len(records) - 1 - len(body_rows)
    if any(len(row) != len(header) for row in body_rows):
        raise SourceBodyError("source_row_truncated")
    position = {name: index for index, name in enumerate(header)}
    date_column = next((name for name in header if name in DATE_COLUMNS), None)
    unit_columns = {UNIT_COLUMNS[name]: position[name] for name in header if name in UNIT_COLUMNS}
    rows: list[dict[str, str]] = []
    options = 0
    seen: set[str] = set()
    for index, record in enumerate(body_rows):
        symbol = record[position["合约代码"]].strip()
        if not FUTURE.fullmatch(symbol):
            if OPTION.fullmatch(symbol):
                options += 1
                continue
            raise SourceBodyError(f"source_contract_unexpected:{index}")
        if symbol in seen:
            raise SourceBodyError("source_duplicate_contract")
        seen.add(symbol)
        row = {name: record[position[column]].strip() for column, name in COLUMNS.items()}
        row["symbol"] = symbol
        if date_column is not None:
            row["date"] = record[position[date_column]].strip()
        for key, column_index in unit_columns.items():
            value = record[column_index].strip()
            if value:
                row[key] = value
        rows.append(row)
    if not rows:
        raise SourceBodyError("source_no_contract_rows")
    units = {
        key: next(iter({row[key] for row in rows if key in row}), None) for key in UNIT_KEYS
    }
    declared = {key: value for key, value in units.items() if value}
    transforms = ["summary_rows_excluded", "option_rows_excluded", "cell_whitespace_stripped"]
    return ParsedDay(
        rows=rows,
        filtered={"summary": totals, "option": options},
        units=declared,
        date_column=date_column,
        header=header,
        transforms=tuple(transforms),
    )


def row_source(row: dict[str, str]) -> dict[str, str]:
    """The verbatim source cells of one parsed row (used for per-row provenance)."""
    return {key: value for key, value in row.items()}

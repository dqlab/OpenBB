"""Compatibility adapter for AKShare's CFFEX daily-CSV data contract.

AKShare 1.19.1's futures_hist_daily_cffex assigns 14 column names by position,
which fails on pre-options-era files containing 15 columns. Read the same
source and map named columns instead. This is an explicit provider adapter,
not a collector-side network bypass or a fallback to another data vendor.
"""

import csv
import math
import re
from datetime import date
from io import StringIO

import pandas as pd
from openbb_core.app.model.abstract.error import OpenBBError
from openbb_core.provider.utils.helpers import make_request

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


def parse_daily_csv(text: str, trading_date: str) -> pd.DataFrame:
    """Map both historical file layouts; reject schema drift rather than guessing."""
    day = date.fromisoformat(trading_date)
    try:
        records = [row for row in csv.reader(StringIO(text), strict=True) if row]
    except csv.Error as exc:
        raise OpenBBError("CFFEX daily CSV is structurally malformed.") from exc
    if not records:
        raise OpenBBError("CFFEX daily CSV has no header.")
    header = [column.strip().lstrip("\ufeff") for column in records[0]]
    if len(header) != len(set(header)) or not set(COLUMNS).issubset(header):
        raise OpenBBError("CFFEX daily CSV is missing required columns or has duplicates.")
    # Official legacy totals omit the options-only final column. Exclude only
    # their explicit labels before enforcing complete widths on contract rows.
    body = [row for row in records[1:] if row[0].strip() not in {"小计", "合计"}]
    if any(len(row) != len(header) for row in body):
        raise OpenBBError("CFFEX daily CSV contains a truncated or oversized row.")
    # Structural checks precede pandas: pandas otherwise mangles duplicate
    # headers and pads short rows, hiding transport/schema corruption.
    frame = pd.DataFrame(body, columns=header)
    frame = frame.rename(columns=COLUMNS)
    frame["symbol"] = frame["symbol"].str.strip()
    frame = frame[~frame["symbol"].isin({"小计", "合计"})].copy()
    symbols = frame["symbol"]
    options = symbols.str.fullmatch(OPTION)
    futures = symbols.str.fullmatch(FUTURE)
    if not (options | futures).all():
        raise OpenBBError("CFFEX daily CSV contains an unexpected contract identifier.")
    frame = frame[futures].copy()
    if frame["symbol"].duplicated().any():
        raise OpenBBError("CFFEX daily CSV contains duplicate futures contracts.")
    # Source '--' denotes an absent price, never a synthetic zero price.
    for name in set(COLUMNS.values()) - {"symbol"}:
        values = frame[name].str.strip().replace({"": None, "--": None, "-": None})
        try:
            frame[name] = pd.to_numeric(values, errors="raise")
            if any(not math.isfinite(float(value)) for value in frame[name].dropna()):
                raise OpenBBError(f"CFFEX daily CSV contains nonfinite {name}.")
        except (ValueError, TypeError) as exc:
            raise OpenBBError(f"CFFEX daily CSV contains malformed {name}.") from exc
    frame["date"] = day.strftime("%Y%m%d")
    frame["variety"] = frame["symbol"].str.extract(r"^([A-Z]+)", expand=False)
    return frame[[
        "symbol", "date", "open", "high", "low", "close", "volume", "open_interest",
        "turnover", "settle", "pre_settle", "variety",
    ]].reset_index(drop=True)


def fetch_daily_csv(source_date: str) -> pd.DataFrame:
    """Fetch the AKShare daily-CSV source with OpenBB's HTTP settings and timeout."""
    if not re.fullmatch(r"[0-9]{8}", source_date):
        raise ValueError("Expected YYYYMMDD.")
    day = date(int(source_date[:4]), int(source_date[4:6]), int(source_date[6:]))
    url = (
        "http://www.cffex.com.cn/sj/hqsj/rtj/"
        f"{source_date[:6]}/{source_date[6:]}/{source_date}_1.csv"
    )
    response = make_request(url, timeout=30)
    response.raise_for_status()
    if len(response.content) > 5_000_000:
        raise OpenBBError("CFFEX daily CSV exceeds the provider byte budget.")
    return parse_daily_csv(response.content.decode("gbk"), day.isoformat())

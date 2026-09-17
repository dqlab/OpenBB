"""Build a native USD SOFR futures curve from IBKR closes with dqlib.

This connects to TWS/IB Gateway, discovers the active quarterly CME
Three-Month SOFR strip, downloads the last common completed daily close, and
calibrates a native curve with dqlib.ir_single_ccy_curve_builder.

It never calls create_ir_yield_curve, which only constructs a curve from
supplied zero rates and does not calibrate market instruments.

Run from the repository root:

    python examples/sofr3_ibkr_dqlib_curve.py \
        --port 4002 --as-of 2026-08-28 \
        --output-dir /home/zdqclawbot/work/app/openbb

For paper TWS, use port 7497 instead of 4002. In Jupyter, run
"from ib_insync import util; util.startLoop()" once before run_sofr3_curve.
"""

from __future__ import annotations

import argparse
import calendar
import json
import logging
import re
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
from pandas import DataFrame

LOGGER = logging.getLogger(__name__)
EASTERN = ZoneInfo("America/New_York")
SR3_LOCAL_SYMBOL = re.compile(r"^SR3[HMUZ]\d{1,2}$")
STANDARD_TENORS_MONTHS = {
    "1M": 1,
    "3M": 3,
    "6M": 6,
    "1Y": 12,
    "2Y": 24,
    "3Y": 36,
    "4Y": 48,
    "5Y": 60,
}


@dataclass(frozen=True)
class SofrCurveBuildResult:
    """Native curve, diagnostics, and exported artifact paths."""

    as_of_date: date
    curve: Any
    closes: DataFrame
    inputs: DataFrame
    curve_nodes: DataFrame
    standard_tenors: DataFrame
    summary: dict[str, Any]
    output_paths: dict[str, Path]


def third_wednesday(year: int, month: int) -> date:
    """Return the third Wednesday of a calendar month."""
    first_weekday, _ = calendar.monthrange(year, month)
    first_wednesday = 1 + (calendar.WEDNESDAY - first_weekday) % 7
    return date(year, month, first_wednesday + 14)


def add_months(value: date, months: int) -> date:
    """Add whole calendar months, clipping the day at month end."""
    zero_based_month = value.month - 1 + months
    year = value.year + zero_based_month // 12
    month = zero_based_month % 12 + 1
    day = min(value.day, calendar.monthrange(year, month)[1])
    return date(year, month, day)


def _as_date(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return pd.to_datetime(value, errors="raise").date()


def _next_quarter(year: int, month: int) -> tuple[int, int]:
    return (year + 1, 3) if month == 12 else (year, month + 3)


def _discover_contracts(ib: Any, requested_as_of: date, count: int) -> list[dict[str, Any]]:
    """Discover the first count wholly forward-starting quarterly SR3s."""
    from ib_insync import Future

    details = ib.reqContractDetails(Future(symbol="SOFR3", exchange="CME", currency="USD"))
    candidates: dict[str, dict[str, Any]] = {}
    for item in details:
        contract = item.contract
        local_symbol = str(contract.localSymbol or "")
        if not SR3_LOCAL_SYMBOL.fullmatch(local_symbol):
            continue
        contract_month = str(item.contractMonth or contract.lastTradeDateOrContractMonth)[:6]
        if len(contract_month) != 6 or not contract_month.isdigit():
            continue
        year, month = int(contract_month[:4]), int(contract_month[4:6])
        if month not in {3, 6, 9, 12}:
            continue
        next_year, next_month = _next_quarter(year, month)
        accrual_start = third_wednesday(year, month)
        accrual_end = third_wednesday(next_year, next_month)
        if accrual_start <= requested_as_of:
            continue
        candidates[local_symbol] = {
            "contract": contract,
            "con_id": int(contract.conId),
            "local_symbol": local_symbol,
            "contract_month": contract_month,
            "last_trade_date": str(contract.lastTradeDateOrContractMonth),
            "accrual_start": accrual_start,
            "accrual_end": accrual_end,
            "accrual_days": (accrual_end - accrual_start).days,
        }

    selected = sorted(
        candidates.values(),
        key=lambda row: (row["contract_month"], row["local_symbol"]),
    )[:count]
    if len(selected) < count:
        raise RuntimeError(
            f"IBKR returned only {len(selected)} forward quarterly SOFR3 contracts; {count} were requested."
        )
    return selected


def fetch_sofr3_closes(
    ib: Any,
    requested_as_of: date,
    contract_count: int = 20,
    lookback_days: int = 30,
) -> DataFrame:
    """Fetch the last common daily close on or before requested_as_of."""
    contracts = _discover_contracts(ib, requested_as_of, contract_count)
    end = datetime.combine(requested_as_of + timedelta(days=1), time.min, EASTERN)
    bars_by_symbol: dict[str, dict[date, Any]] = {}

    for item in contracts:
        bars = ib.reqHistoricalData(
            item["contract"],
            endDateTime=end,
            durationStr=f"{lookback_days} D",
            barSizeSetting="1 day",
            whatToShow="TRADES",
            useRTH=False,
            formatDate=2,
            keepUpToDate=False,
        )
        dated_bars = {_as_date(bar.date): bar for bar in bars if _as_date(bar.date) <= requested_as_of}
        if not dated_bars:
            raise RuntimeError(f"No completed daily bars for {item['local_symbol']} on or before {requested_as_of}.")
        bars_by_symbol[item["local_symbol"]] = dated_bars
        ib.sleep(0.20)

    common_dates = set.intersection(*(set(values) for values in bars_by_symbol.values()))
    if not common_dates:
        raise RuntimeError("Selected SOFR3 contracts have no common daily bar.")
    actual_as_of = max(common_dates)
    if actual_as_of != requested_as_of:
        LOGGER.warning("No common close on %s; using %s.", requested_as_of, actual_as_of)

    rows: list[dict[str, Any]] = []
    for item in contracts:
        bar = bars_by_symbol[item["local_symbol"]][actual_as_of]
        close = float(bar.close)
        if not 0.0 < close < 200.0:
            raise ValueError(f"Invalid {item['local_symbol']} settlement/close: {close}")
        rows.append(
            {
                "as_of_date": actual_as_of,
                "con_id": item["con_id"],
                "local_symbol": item["local_symbol"],
                "contract_month": item["contract_month"],
                "last_trade_date": item["last_trade_date"],
                "accrual_start": item["accrual_start"],
                "accrual_end": item["accrual_end"],
                "accrual_days": item["accrual_days"],
                "close": close,
                "implied_rate_pct": 100.0 - close,
                "volume": float(bar.volume),
                "bar_count": int(bar.barCount),
                "average": float(bar.average),
            }
        )
    return DataFrame(rows)


def _register_sofr3_future(row: Any, as_of_date: date) -> tuple[str, str, str]:
    """Register an exact-date daily-compounded SOFR future in dqlib."""
    import dqlib
    from dqlib.datetime import create_period
    from dqlib.irmarket import create_floating_leg_definition
    from dqlib.staticdata import create_static_data
    from dqlibproto.dqdatetime_pb2 import WEDNESDAY
    from dqlibproto.dqirmarket_pb2 import IFMC_IMM
    from dqlibproto.dqmarket_pb2 import IR_FUTURE, SPOTSTART
    from dqlibproto.dqproto import (
        dqCreateProtoInterestRateInstrumentTemplate,
        dqCreateProtoIrFutureTemplate,
        dqCreateProtoIrFutureTemplate_SettleDayRule,
        dqCreateProtoIrFutureTemplateList,
    )

    suffix = as_of_date.strftime("%Y%m%d")
    instrument_name = f"USD_SOFR_FUT_{row.local_symbol}_{suffix}"
    index_name = f"USD_SOFR_ON_{row.local_symbol}_{suffix}"
    term = f"{(row.accrual_start - as_of_date).days}D"

    if not dqlib.create_ibor_index(
        index_name,
        f"{int(row.accrual_days)}D",
        "USD",
        ["USNY"],
        0,
        day_count="ACT_360",
        interest_day_convention="UNADJUSTED",
        ibor_type="OVERNIGHT_INDEX",
    ):
        raise RuntimeError(f"dqlib failed to register index {index_name}.")

    floating_leg = create_floating_leg_definition(
        "USD",
        index_name,
        "USNY",
        ["USNY"],
        "ONCE",
        "DAILY",
        day_count="ACT_360",
        rate_calc_method="COMPOUND_AVERAGE",
        interest_day_convention="UNADJUSTED",
        pay_day_convention="UNADJUSTED",
        fixing_day_convention="UNADJUSTED",
        fixing_mode="IN_ARREAR",
        fixing_day_offset=0,
    )
    base_template = dqCreateProtoInterestRateInstrumentTemplate(
        IR_FUTURE,
        instrument_name,
        create_period(0, "DAYS"),
        [floating_leg],
        SPOTSTART,
    )
    imm_rule = dqCreateProtoIrFutureTemplate_SettleDayRule("IMM", 0, 3, WEDNESDAY, True)
    template = dqCreateProtoIrFutureTemplate(base_template, [imm_rule], IFMC_IMM)
    template_list = dqCreateProtoIrFutureTemplateList([template])
    if not create_static_data("SDT_IR_FUTURE", template_list.SerializeToString()):
        raise RuntimeError(f"dqlib failed to register future {instrument_name}.")
    return instrument_name, index_name, term


def _proto_date(value: Any) -> date:
    return date(int(value.year), int(value.month), int(value.day))


def _vector_data(value: Any) -> list[float]:
    return [float(item) for item in value.data]


def build_sofr3_curve(
    closes: DataFrame,
    *,
    convexity_adjustment_bp: float = 0.0,
    calc_jacobian: bool = True,
) -> tuple[Any, DataFrame, DataFrame, DataFrame, dict[str, Any]]:
    """Calibrate and validate a native dqlib curve from SOFR3 closes."""
    import dqlib

    required = {
        "as_of_date",
        "local_symbol",
        "con_id",
        "contract_month",
        "last_trade_date",
        "accrual_start",
        "accrual_end",
        "accrual_days",
        "close",
    }
    missing = sorted(required.difference(closes.columns))
    if missing:
        raise ValueError(f"The closes are missing columns: {', '.join(missing)}")
    if closes.empty:
        raise ValueError("At least one SOFR3 close is required.")

    inputs = closes.copy()
    for column in ("as_of_date", "accrual_start", "accrual_end"):
        inputs[column] = inputs[column].map(_as_date)
    as_of_values = inputs["as_of_date"].unique()
    if len(as_of_values) != 1:
        raise ValueError("All futures closes must have the same as-of date.")
    as_of_date = as_of_values[0]
    inputs = inputs.sort_values("accrual_start").reset_index(drop=True)

    registered = [_register_sofr3_future(row, as_of_date) for row in inputs.itertuples(index=False)]
    inputs["instrument_type"] = "IR_FUTURE"
    inputs["instrument_name"] = [item[0] for item in registered]
    inputs["index_name"] = [item[1] for item in registered]
    inputs["term"] = [item[2] for item in registered]
    inputs["futures_rate"] = (100.0 - inputs["close"].astype(float)) / 100.0

    calibration_prices = (inputs["close"].astype(float) - convexity_adjustment_bp / 100.0).tolist()
    curve_name = f"USD_SOFR3_IBKR_BUILDER_{as_of_date:%Y%m%d}"
    as_of_datetime = datetime.combine(as_of_date, time.min)
    par_curve = dqlib.create_ir_par_rate_curve(
        as_of_datetime,
        "USD",
        curve_name,
        inputs["instrument_name"].tolist(),
        inputs["instrument_type"].tolist(),
        inputs["term"].tolist(),
        [1.0] * len(inputs),
        calibration_prices,
    )
    settings = dqlib.create_ir_curve_build_settings(
        curve_name,
        {"USD": curve_name},
        {name: curve_name for name in inputs["index_name"]},
    )
    curve = dqlib.ir_single_ccy_curve_builder(
        as_of_datetime,
        [curve_name],
        [settings],
        [par_curve],
        "ACT_365_FIXED",
        "CONTINUOUS_COMPOUNDING",
        "ANNUAL",
        [],
        building_method="BOOTSTRAPPING_METHOD",
        calc_jacobian=calc_jacobian,
    )[0]

    native_curve = curve.curve.curve
    pillar_dates = [_proto_date(value) for value in native_curve.pillar_date]
    expected_pillars = inputs["accrual_end"].tolist()
    if pillar_dates != expected_pillars:
        raise RuntimeError(
            "dqlib pillars differ from official quarterly IMM end dates: "
            f"actual={pillar_dates}, expected={expected_pillars}"
        )

    query_dates = inputs["accrual_start"].tolist() + inputs["accrual_end"].tolist()
    discounts = _vector_data(dqlib.get_discount_factor(curve, query_dates))
    count = len(inputs)
    discount_start, discount_end = discounts[:count], discounts[count:]
    dqlib_implied_rate = [
        (start_df / end_df - 1.0) * 360.0 / days
        for start_df, end_df, days in zip(
            discount_start,
            discount_end,
            inputs["accrual_days"],
            strict=True,
        )
    ]
    inputs["dqlib_implied_rate"] = dqlib_implied_rate
    inputs["dqlib_repriced_price"] = [100.0 - 100.0 * rate for rate in dqlib_implied_rate]
    inputs["repricing_error_price_points"] = inputs["dqlib_repriced_price"] - inputs["close"]
    inputs["repricing_error_bp"] = (inputs["dqlib_implied_rate"] - inputs["futures_rate"]) * 10_000.0
    inputs["discount_start"] = discount_start
    inputs["discount_end"] = discount_end

    curve_nodes = DataFrame(
        {
            "as_of_date": as_of_date,
            "curve_name": curve_name,
            "contract_symbol": inputs["local_symbol"].tolist(),
            "native_pillar_name": list(native_curve.pillar_name),
            "date": pillar_dates,
            "zero_rate": _vector_data(native_curve.pillar_values),
            "discount_factor": _vector_data(dqlib.get_discount_factor(curve, pillar_dates)),
        }
    )
    standard_dates = [add_months(as_of_date, months) for months in STANDARD_TENORS_MONTHS.values()]
    standard_tenors = DataFrame(
        {
            "as_of_date": as_of_date,
            "tenor": list(STANDARD_TENORS_MONTHS),
            "date": standard_dates,
            "zero_rate": _vector_data(dqlib.get_zero_rate(curve, standard_dates)),
            "discount_factor": _vector_data(dqlib.get_discount_factor(curve, standard_dates)),
            "forward_rate_3m": _vector_data(dqlib.get_fwd_rate(curve, standard_dates, 0.25)),
        }
    )

    tenor_records = standard_tenors.copy()
    for column in ("as_of_date", "date"):
        tenor_records[column] = tenor_records[column].astype(str)
    summary = {
        "as_of_date": str(as_of_date),
        "source": "IBKR 1-day TRADES bars",
        "curve_name": curve_name,
        "builder": "dqlib.ir_single_ccy_curve_builder",
        "building_method": "BOOTSTRAPPING_METHOD",
        "create_ir_yield_curve_used": False,
        "instrument_type": "IR_FUTURE",
        "reference_index_type": "OVERNIGHT_INDEX",
        "rate_calculation_method": "COMPOUND_AVERAGE",
        "fixing_frequency": "DAILY",
        "contracts": len(inputs),
        "first_contract": str(inputs.iloc[0]["local_symbol"]),
        "last_contract": str(inputs.iloc[-1]["local_symbol"]),
        "first_pillar": str(pillar_dates[0]),
        "last_pillar": str(pillar_dates[-1]),
        "cash_anchor": None,
        "convexity_adjustment_bp": convexity_adjustment_bp,
        "max_repricing_error_price_points": float(inputs["repricing_error_price_points"].abs().max()),
        "max_repricing_error_bp": float(inputs["repricing_error_bp"].abs().max()),
        "jacobian_count": len(curve.jacobians),
        "standard_tenors": tenor_records.to_dict(orient="records"),
    }
    return curve, inputs, curve_nodes, standard_tenors, summary


def export_sofr3_curve(
    output_dir: Path | str,
    closes: DataFrame,
    curve: Any,
    inputs: DataFrame,
    curve_nodes: DataFrame,
    standard_tenors: DataFrame,
    summary: dict[str, Any],
) -> dict[str, Path]:
    """Export quotes, diagnostics, tenor points, and native protobuf."""
    directory = Path(output_dir).expanduser().resolve()
    directory.mkdir(parents=True, exist_ok=True)
    stamp = str(_as_date(inputs.iloc[0]["as_of_date"]))
    paths = {
        "closes_csv": directory / f"sofr3_ibkr_{stamp}_closes.csv",
        "inputs_csv": directory / f"sofr3_dqlib_builder_inputs_{stamp}.csv",
        "curve_csv": directory / f"sofr3_dqlib_builder_curve_{stamp}.csv",
        "standard_tenors_csv": directory / f"sofr3_dqlib_builder_standard_tenors_{stamp}.csv",
        "metadata_json": directory / f"sofr3_dqlib_builder_{stamp}.json",
        "curve_protobuf": directory / f"sofr3_dqlib_builder_curve_{stamp}.pb",
    }
    raw_columns = [
        "as_of_date",
        "con_id",
        "local_symbol",
        "contract_month",
        "last_trade_date",
        "close",
        "implied_rate_pct",
        "volume",
        "bar_count",
        "average",
    ]
    closes.to_csv(paths["closes_csv"], columns=raw_columns, index=False)
    inputs.to_csv(paths["inputs_csv"], index=False)
    curve_nodes.to_csv(paths["curve_csv"], index=False)
    standard_tenors.to_csv(paths["standard_tenors_csv"], index=False)
    paths["metadata_json"].write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    paths["curve_protobuf"].write_bytes(curve.SerializeToString())
    return paths


def run_sofr3_curve(
    *,
    host: str = "127.0.0.1",
    port: int = 4002,
    client_id: int = 91,
    as_of: date | str | None = None,
    contract_count: int = 20,
    output_dir: Path | str = Path.cwd(),
    convexity_adjustment_bp: float = 0.0,
) -> SofrCurveBuildResult:
    """Connect to IBKR, fetch closes, build, validate, and export."""
    from ib_insync import IB

    if contract_count < 1:
        raise ValueError("contract_count must be positive.")
    requested_as_of = datetime.now(EASTERN).date() - timedelta(days=1) if as_of is None else _as_date(as_of)

    ib = IB()
    try:
        ib.connect(host, port, clientId=client_id, timeout=15, readonly=True)
        if not ib.isConnected():
            raise ConnectionError(f"Could not connect to IBKR at {host}:{port}.")
        closes = fetch_sofr3_closes(ib, requested_as_of, contract_count=contract_count)
    finally:
        if ib.isConnected():
            ib.disconnect()

    curve, inputs, nodes, tenors, summary = build_sofr3_curve(
        closes,
        convexity_adjustment_bp=convexity_adjustment_bp,
        calc_jacobian=True,
    )
    paths = export_sofr3_curve(output_dir, closes, curve, inputs, nodes, tenors, summary)
    return SofrCurveBuildResult(
        as_of_date=_as_date(inputs.iloc[0]["as_of_date"]),
        curve=curve,
        closes=closes,
        inputs=inputs,
        curve_nodes=nodes,
        standard_tenors=tenors,
        summary=summary,
        output_paths=paths,
    )


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=4002)
    parser.add_argument("--client-id", type=int, default=91)
    parser.add_argument(
        "--as-of",
        help="Latest permitted close date (YYYY-MM-DD); defaults to yesterday",
    )
    parser.add_argument("--contracts", type=int, default=20)
    parser.add_argument("--output-dir", type=Path, default=Path.cwd())
    parser.add_argument(
        "--convexity-adjustment-bp",
        type=float,
        default=0.0,
        help="Rate adjustment applied before calibration; default is zero",
    )
    return parser.parse_args()


def main() -> None:
    """Command-line entry point."""
    args = _arguments()
    result = run_sofr3_curve(
        host=args.host,
        port=args.port,
        client_id=args.client_id,
        as_of=args.as_of,
        contract_count=args.contracts,
        output_dir=args.output_dir,
        convexity_adjustment_bp=args.convexity_adjustment_bp,
    )
    LOGGER.info(
        "Built %s from %d contracts (%s through %s).",
        result.summary["curve_name"],
        result.summary["contracts"],
        result.summary["first_contract"],
        result.summary["last_contract"],
    )
    LOGGER.info(
        "Maximum repricing error: %.12g price points / %.12g rate bp.",
        result.summary["max_repricing_error_price_points"],
        result.summary["max_repricing_error_bp"],
    )
    LOGGER.info("\n%s", result.standard_tenors.to_string(index=False))
    for name, path in result.output_paths.items():
        LOGGER.info("%s: %s", name, path)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    logging.getLogger("ib_insync").setLevel(logging.WARNING)
    main()

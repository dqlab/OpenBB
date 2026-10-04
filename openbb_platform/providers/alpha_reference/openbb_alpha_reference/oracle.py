"""Independent standard-library numerical oracle; no production-engine imports."""

import math
import statistics
from collections import defaultdict


def calculate(rows, definitions, transform="raw"):
    grouped = defaultdict(list)
    for row in rows:
        grouped[row["instrument_id"]].append(row)
    output = []
    for instrument, records in sorted(grouped.items()):
        records.sort(key=lambda r: r["slot"])
        for t, current in enumerate(records):
            for spec in definitions:
                factor = spec["id"]
                lookback, skip = spec["lookback"], spec["skip"]
                window = records[t - lookback : t - skip + 1] if t >= lookback else []
                value, reason = None, None
                if not current["eligible"]:
                    reason = "ineligible"
                elif t < lookback:
                    reason = "warmup"
                elif any(not r["present"] for r in window):
                    reason = "missing_session"
                elif any(not r["known"] for r in window):
                    reason = "unavailable_at_close"
                elif any(r[field] is None for r in window for field in spec["required_fields"]):
                    reason = "missing_value"
                elif any(
                    not math.isfinite(r[field])
                    or (r[field] < 0 if field == "raw_share_volume" else r[field] <= 0)
                    for r in window
                    for field in spec["required_fields"]
                ):
                    reason = "invalid_value"
                elif factor.startswith("momentum_"):
                    value = window[-1]["price"] / window[0]["price"] - 1
                elif factor == "reversal_5":
                    value = -(window[-1]["price"] / window[0]["price"] - 1)
                elif factor == "negative_realized_volatility_20":
                    returns = [
                        math.log(b["price"]) - math.log(a["price"])
                        for a, b in zip(window, window[1:], strict=False)
                    ]
                    value = -math.sqrt(252) * statistics.stdev(returns)
                elif factor == "log_dollar_volume_20":
                    mean = statistics.mean(r["raw_close"] * r["raw_share_volume"] for r in window)
                    if mean <= 0:
                        reason = "nonpositive_notional"
                    else:
                        value = math.log(mean)
                else:
                    raise ValueError("unsupported reference formula")
                if value is not None and not math.isfinite(value):
                    value, reason = None, "nonfinite_result"
                output.append(
                    {
                        "instrument_id": instrument,
                        "session": current["session"],
                        "decision_time": current["decision_time"],
                        "available_at": current["decision_time"] if value is not None else None,
                        "factor_id": factor,
                        "factor_version": 1,
                        "value": value,
                        "null_reason": reason,
                        "eligible": current["eligible"],
                        "rank_value": None,
                        "rank_null_reason": "not_requested",
                    }
                )
    if transform == "centered_rank":
        groups = defaultdict(list)
        for row in output:
            groups[(row["session"], row["factor_id"])].append(row)
        for group in groups.values():
            valid = sorted(r["value"] for r in group if r["eligible"] and r["value"] is not None)
            for row in group:
                if row["value"] is None:
                    row["rank_null_reason"] = row["null_reason"]
                elif len(valid) < 2:
                    row["rank_null_reason"] = "insufficient_cross_section"
                else:
                    value = row["value"]
                    ranks = [i + 1 for i, x in enumerate(valid) if x == value]
                    row["rank_value"] = (statistics.mean(ranks) - 1) / (len(valid) - 1) - 0.5
                    row["rank_null_reason"] = None
    return output

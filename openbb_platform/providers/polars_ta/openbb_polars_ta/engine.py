"""Columnar polars_ta execution with explicit complete-calendar masks."""

import math


def calculate(table, specs, transform="raw"):
    import polars as pl
    from polars_ta.wq.time_series import ts_delay, ts_mean, ts_std_dev

    frame = pl.from_arrow(table).sort(["instrument_id", "slot"])
    frame = frame.with_columns(
        (pl.col("price").log() - ts_delay(pl.col("price").log(), 1))
        .over("instrument_id")
        .alias("_log_return")
    )
    panels = []
    for order, spec in enumerate(specs):
        width = spec.lookback - spec.skip + 1

        def any_window(expression, window=width, skip=spec.skip):
            return (
                expression.cast(pl.Int32)
                .rolling_sum(window, min_samples=window)
                .shift(skip)
                .over("instrument_id")
                > 0
            )

        missing = any_window(pl.any_horizontal(pl.col(f).is_null() for f in spec.required_fields))
        invalid = any_window(
            pl.any_horizontal(
                (~pl.col(f).is_finite())
                | (pl.col(f) < 0 if f == "raw_share_volume" else pl.col(f) <= 0)
                for f in spec.required_fields
            )
        )
        if spec.id.startswith("momentum_"):
            value = (
                ts_delay(pl.col("price"), spec.skip) / ts_delay(pl.col("price"), spec.lookback) - 1
            ).over("instrument_id")
        elif spec.id == "reversal_5":
            value = (-(pl.col("price") / ts_delay(pl.col("price"), 5) - 1)).over("instrument_id")
        elif spec.id == "negative_realized_volatility_20":
            value = (
                -math.sqrt(252) * ts_std_dev(pl.col("_log_return"), 20, ddof=1, min_samples=20)
            ).over("instrument_id")
        elif spec.id == "log_dollar_volume_20":
            value = (
                ts_mean(pl.col("raw_close") * pl.col("raw_share_volume"), 20, min_samples=20)
                .log()
                .over("instrument_id")
            )
        else:
            raise ValueError("unsupported polars_ta formula")
        reason = (
            pl.when(~pl.col("eligible"))
            .then(pl.lit("ineligible"))
            .when(pl.col("slot") < spec.lookback)
            .then(pl.lit("warmup"))
            .when(any_window(~pl.col("present")))
            .then(pl.lit("missing_session"))
            .when(any_window(~pl.col("known")))
            .then(pl.lit("unavailable_at_close"))
            .when(missing)
            .then(pl.lit("missing_value"))
            .when(invalid)
            .then(pl.lit("invalid_value"))
        )
        if spec.id == "log_dollar_volume_20":
            reason = reason.when(value.is_infinite() & (value < 0)).then(
                pl.lit("nonpositive_notional")
            )
        reason = reason.when(~value.is_finite()).then(pl.lit("nonfinite_result")).otherwise(None)
        result = frame.with_columns(reason.alias("null_reason"), value.alias("value"))
        result = result.with_columns(
            pl.when(pl.col("null_reason").is_null())
            .then(pl.col("value"))
            .otherwise(None)
            .alias("value")
        )
        result = result.select(
            "instrument_id",
            "session",
            "decision_time",
            pl.when(pl.col("value").is_not_null())
            .then(pl.col("decision_time"))
            .otherwise(None)
            .alias("available_at"),
            pl.lit(spec.id).alias("factor_id"),
            pl.lit(1, dtype=pl.Int64).alias("factor_version"),
            "value",
            "null_reason",
            "eligible",
            pl.lit(None, dtype=pl.Float64).alias("rank_value"),
            pl.lit("not_requested").alias("rank_null_reason"),
            pl.lit(order).alias("_order"),
        )
        if transform == "centered_rank":
            n = pl.col("value").count().over("session")
            rank = pl.col("value").rank(method="average").over("session")
            result = result.with_columns(
                pl.when(n >= 2)
                .then((rank - 1) / (n - 1) - 0.5)
                .otherwise(None)
                .alias("rank_value"),
                pl.when(pl.col("value").is_null())
                .then(pl.col("null_reason"))
                .when(n < 2)
                .then(pl.lit("insufficient_cross_section"))
                .otherwise(None)
                .alias("rank_null_reason"),
            )
        panels.append(result)
    return pl.concat(panels).sort(["instrument_id", "session", "_order"]).drop("_order").to_arrow()

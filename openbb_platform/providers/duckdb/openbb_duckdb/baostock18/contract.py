"""Explicit basis and lossless literal-to-decimal rules."""

# These SQL builders use a fixed allowlist, with no user-controlled identifiers.
# ruff: noqa: S608

TRANSFORM = "baostock18-narrow-v1"
SCHEMA = "baostock18_admission"
RAW_FIELDS = (
    "date", "code", "open", "high", "low", "close", "preclose", "volume",
    "amount", "adjustflag", "turn", "tradestatus", "pctChg", "peTTM",
    "psTTM", "pcfNcfTTM", "pbMRQ", "isST",
)
REJECT_FLAGS = (
    "adjustment_mismatch", "date_anomaly", "numeric_anomaly", "ohlc_anomaly",
    "flag_anomaly", "identity_or_range_anomaly", "core_price_empty",
)
REVIEW_FLAGS = (
    "has_confirmed_partial", "source_status_warning", "calendar_unknown",
    "calendar_source_conflict",
)
DECIMALS = {
    "open": "open", "high": "high", "low": "low", "close": "close",
    "preclose": "preclose", "amount": "amount", "turnover_rate": "turn",
    "change_percent": "pctChg", "pe_ttm": "peTTM", "ps_ttm": "psTTM",
    "pcf_ttm": "pcfNcfTTM", "pb_mrq": "pbMRQ",
}


class AdmissionError(ValueError):
    """The offline input or requested consumption contract is unsatisfied."""


def require_basis(basis: str | None, fixed_qfq_basis_date=None) -> str:
    """Refuse missing, ambiguous, or fixed-date adjustment requests."""
    if fixed_qfq_basis_date is not None or basis == "forward_fixed_2026-10-15":
        raise AdmissionError(
            "Fixed 2026-10-15 forward adjustment is unavailable: flag=2 is a "
            "dynamic capture snapshot, with no certified fixed base or factors."
        )
    if not basis:
        raise AdmissionError("An explicit basis is required; missing base is never inferred.")
    if basis != "unadjusted":
        raise AdmissionError(f"Basis {basis!r} has not been admitted for consumption.")
    return basis


def quote(name: str) -> str:
    """Quote only internal, constant field names."""
    if name not in {*RAW_FIELDS, *REJECT_FLAGS, *REVIEW_FLAGS}:
        raise AdmissionError("Unknown internal column")
    return '"' + name + '"'


def validation_sql() -> str:
    """Check lexical precision before casts, so excess scale cannot be rounded."""
    checks = [
        "regexp_full_match(code, '(sh|sz|bj)[.][0-9]{6}')",
        "regexp_full_match(date, '[0-9]{4}-[0-9]{2}-[0-9]{2}')",
        "try_cast(date AS DATE) IS NOT NULL",
        "requested_adjustflag IN ('2','3')", "adjustflag=requested_adjustflag",
        "tradestatus IN ('0','1')", "\"isST\" IN ('','0','1')",
        "(volume='' OR (regexp_full_match(volume,'[0-9]+') AND "
        "try_cast(volume AS BIGINT)>=0))",
    ]
    for source in DECIMALS.values():
        col = quote(source)
        scale = 16 if source in {"turn", "pctChg"} else 18
        checks.append(
            f"({col}='' OR (regexp_full_match({col}, '[+-]?[0-9]+([.][0-9]+)?') "
            f"AND length(split_part({col},'.',2))<={scale} "
            f"AND try_cast({col} AS DECIMAL(38,18)) IS NOT NULL))"
        )
    checks.extend([
        "try_cast(open AS DECIMAL(38,18))>0",
        "try_cast(low AS DECIMAL(38,18))>0",
        'try_cast(high AS DECIMAL(38,18))>=greatest(try_cast(open AS DECIMAL(38,18)),'
        'try_cast("close" AS DECIMAL(38,18)))',
        'try_cast(low AS DECIMAL(38,18))<=least(try_cast(open AS DECIMAL(38,18)),'
        'try_cast("close" AS DECIMAL(38,18)))',
        "(amount='' OR try_cast(amount AS DECIMAL(38,18))>=0)",
    ])
    return "coalesce((" + " AND ".join(checks) + "),false)"


def classify_sql() -> str:
    """Quarantine wins over conditional source/calendar review."""
    reject = " OR ".join(f"coalesce({quote(f)},true)" for f in REJECT_FLAGS)
    review = " OR ".join(f"coalesce({quote(f)},true)" for f in REVIEW_FLAGS)
    return (
        f"CASE WHEN NOT numeric_contract OR {reject} OR coalesce(variant_count,0)<>1 "
        "OR NOT coalesce(has_admissible_source,false) THEN 'quarantine' "
        f"WHEN {review} THEN 'conditional' ELSE 'clean' END"
    )


def raw_json_sql() -> str:
    """Retain empty strings and exact provider spelling in all eighteen cells."""
    return "to_json(struct_pack(" + ",".join(f"{quote(f)} := {quote(f)}" for f in RAW_FIELDS) + "))"


def flags_json_sql() -> str:
    """Retain the candidate decisions, including unknown calendar states."""
    fields = [*REJECT_FLAGS, *REVIEW_FLAGS, "has_admissible_source", "variant_count", "numeric_contract"]
    return "to_json(struct_pack(" + ",".join(f'"{f}" := "{f}"' for f in fields) + "))"


def bar_projection_sql() -> str:
    """Use exact decimal multiplication for percent-to-ratio conversion."""
    parts = [
        "? AS snapshot_id", "value_id", "upper(code) AS symbol", "cast(date AS DATE)",
        "CASE requested_adjustflag WHEN '3' THEN 'unadjusted' "
        "ELSE 'forward_dynamic_snapshot' END",
        "requested_adjustflag", "adjustflag", "NULL::DATE",
    ]
    for target, source in DECIMALS.items():
        value = f"cast(nullif({quote(source)},'') AS DECIMAL(38,18))"
        if source in {"turn", "pctChg"}:
            value = f"cast({value} * 0.01::DECIMAL(3,2) AS DECIMAL(38,18))"
        parts.append(f'{value} AS "{target}"')
        if target == "preclose":
            parts.append("cast(nullif(volume,'') AS BIGINT)")
    parts.extend([
        "cast(tradestatus AS INTEGER)", 'CASE WHEN "isST"=\'\' THEN NULL ELSE "isST"=\'1\' END',
        "'CNY'", "'shares'", "'Asia/Shanghai'",
    ])
    return "SELECT " + ",".join(parts) + " FROM batch WHERE disposition<>'quarantine'"

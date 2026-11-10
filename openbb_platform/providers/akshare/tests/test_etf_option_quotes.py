"""Offline source-contract tests; synthetic fixtures are not live market data."""

import pytest
from openbb_akshare.models.etf_options import FIELDS, parse_batch, validate_symbols

STAMP = "2026-10-14T08:00:00+00:00"


def payload(*, extended=True):
    """Create a clearly synthetic quote payload."""
    values = ["1"] * len(FIELDS)
    for field, value in {
        "买价": "0.0229",
        "卖价": "0.0249",
        "行情时间": "2026-10-14 15:00:00",
        "标的股票": "159901",
        "期权合约简称": "synthetic option",
        "状态码": "E0",
    }.items():
        values[FIELDS.index(field)] = value
    if extended:
        values += ["M", "0.0333", "C", "2026-12-23", "70", "1", "0", "0.024"]
    return values


def assignment(values, symbol="90007393"):
    """Encode a synthetic source assignment."""
    return f'var hq_str_CON_OP_{symbol}="{",".join(values)}";\n'


@pytest.mark.parametrize("extended", [True, False])
def test_full_quote_preserves_source(extended):
    """Both explicit layouts retain source fields."""
    values = payload(extended=extended)
    row = parse_batch(assignment(values), ["90007393"], STAMP)[0]
    assert row["quote_error"] is None
    assert row["bid"] == 0.0229 and row["ask"] == 0.0249
    assert row["source_time"] == "2026-10-14T15:00:00+08:00"
    assert row["raw_extra_fields"] == values[43:]
    if extended:
        assert row["expiration"] == "2026-12-23" and row["option_type"] == "call"


@pytest.mark.parametrize("width", [1, 42, 44, 50, 52])
def test_schema_width_not_silently_truncated(width):
    """Unrecognized widths remain diagnostic rows."""
    values = (payload() + ["1"] * 52)[:width]
    row = parse_batch(assignment(values), ["90007393"], STAMP)[0]
    assert row["quote_error"] == "unexpected_quote_width"
    assert row["raw_payload"] == ",".join(values)


def test_missing_empty_and_order_accounting():
    """Every requested identifier is accounted for."""
    text = assignment(payload()) + 'var hq_str_CON_OP_10012535="";'
    rows = parse_batch(text, ["10012535", "90007393", "10012536"], STAMP)
    assert [r["symbol"] for r in rows] == ["10012535", "90007393", "10012536"]
    assert [r["quote_error"] for r in rows] == ["empty_quote", None, "missing_quote"]


@pytest.mark.parametrize(
    "text",
    [
        "<html>403</html>",
        assignment(payload()) * 2,
        assignment(payload(), symbol="10012535"),
        'var arbitrary="x";',
    ],
)
def test_untrusted_or_duplicate_response_rejected(text):
    """Reject HTML, duplicate and foreign symbol bodies."""
    with pytest.raises(ValueError):
        parse_batch(text, ["90007393"], STAMP)


@pytest.mark.parametrize("value", ["nan", "inf", "-inf", "oops"])
def test_bad_numeric_preserves_raw(value):
    """Reject invalid numbers while retaining source text."""
    values = payload()
    values[FIELDS.index("买价")] = value
    row = parse_batch(assignment(values), ["90007393"], STAMP)[0]
    assert row["quote_error"] and row["raw_fields"]["买价"] == value


@pytest.mark.parametrize("token", ["CON_OP_90007393", "90007393 ", "90007393,90007393", "", "123", "90007393;id"])
def test_identifiers_not_repaired(token):
    """Never normalize malformed source identifiers."""
    with pytest.raises(ValueError):
        validate_symbols(token)


def test_batch_limit_and_exact_identifiers():
    """Bound batch size before network access."""
    assert validate_symbols("90007393,10012535") == ["90007393", "10012535"]
    with pytest.raises(ValueError):
        validate_symbols(",".join(str(10000000 + i) for i in range(51)))


def test_absent_and_zero_not_synthetic_prices():
    """Null and zero values remain distinguishable."""
    values = payload()
    values[FIELDS.index("买价")] = "--"
    values[FIELDS.index("卖价")] = "0"
    row = parse_batch(assignment(values), ["90007393"], STAMP)[0]
    assert row["bid"] is None and row["ask"] == 0
    assert row["quote_error"] is None  # collector owns pricing-eligibility classification

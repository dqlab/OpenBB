"""Offline source-contract tests; synthetic fixtures are not live market data."""

import pytest
from openbb_akshare.utils.etf_option_quotes import FIELDS, parse_batch, validate_symbols

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
        assert "expiration" not in row and "option_type" not in row


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


def test_transport_uses_shared_helper_and_checks_bounds(monkeypatch):
    """Mock transport verifies source, status, encoding and response size."""
    from unittest.mock import Mock

    from openbb_akshare.utils import etf_option_quotes as quotes

    response = Mock(content=assignment(payload()).encode("gb18030"))
    request = Mock(return_value=response)
    monkeypatch.setattr(quotes, "make_request", request)
    assert quotes.fetch_quotes("90007393")[0]["symbol"] == "90007393"
    assert request.call_args.args[0] == "https://hq.sinajs.cn/list=CON_OP_90007393"
    assert request.call_args.kwargs["timeout"] == 25
    response.raise_for_status.assert_called_once()
    response.content = b"x" * 1_000_001
    with pytest.raises(ValueError, match="quote_response_byte_limit"):
        quotes.fetch_quotes("90007393")


def test_fetcher_round_trip_and_registration(monkeypatch):
    """The actual registered source model retains diagnostics and exact identifiers."""
    import asyncio

    from openbb_akshare import akshare_provider
    from openbb_akshare.models import etf_option_quotes as model
    from openbb_akshare.utils.catalog import DATASETS

    fetcher = model.AkshareEtfOptionQuotesFetcher
    assert akshare_provider.fetcher_dict["AkshareEtfOptionQuotes"] is fetcher
    assert all(d.command != "etf_option_quotes" and d.model != "AkshareEtfOptionQuotes" for d in DATASETS)
    query = fetcher.transform_query({"symbols": "90007393"})
    monkeypatch.setattr(model, "fetch_quotes", lambda symbols: [{"symbol": symbols, "quote_error": "missing_quote"}])
    rows = fetcher.transform_data(query, asyncio.run(fetcher.aextract_data(query, None)))
    assert rows[0].symbol == "90007393" and rows[0].quote_error == "missing_quote"
    with pytest.raises(ValueError):
        fetcher.transform_query({"symbols": "90007393,90007393"})

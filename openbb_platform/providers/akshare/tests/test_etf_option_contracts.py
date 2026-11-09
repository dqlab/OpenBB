"""Offline contract-universe fixtures; synthetic records are not market evidence."""

import json
from io import BytesIO

import pandas as pd
import pytest
from openbb_akshare.models import etf_options as contracts
from openbb_akshare.models.etf_options import FIELDS, parse_batch


class Response:
    """Minimal bounded HTTP fixture."""

    def __init__(self, value, status=200):
        self.content = value if isinstance(value, bytes) else json.dumps(value).encode()
        self.status = status

    def raise_for_status(self):
        """Surface HTTP errors without a fallback response."""
        if self.status != 200:
            raise RuntimeError(f"HTTP {self.status}")


def sse_row(**changes):
    """Named headers deliberately differ from column order."""
    row = {
        "EXPIRE_DATE": "2026-12-23",
        "SECURITY_ID": "10011809",
        "CONTRACT_ID": "510500C2612A07500",
        "CONTRACT_SYMBOL": "synthetic adjusted contract",
        "SECURITYNAMEBYID": "500ETF(510500)",
        "CALL_OR_PUT": "C",
        "EXERCISE_PRICE": "6.8760",
        "CONTRACT_UNIT": "",
        "START_DATE": "2026-09-01",
    }
    row.update(changes)
    return row


def szse_row(**changes):
    """Source-shaped current and original adjustment fields."""
    row = {
        "合约编码": "90007393",
        "合约代码": "159901C2612A003500",
        "标的证券简称(代码)": "深证100ETF(159901)",
        "合约类型": "认购",
        "行权价": "3.4990",
        "合约单位": "10003",
        "到期日": "2026-12-23",
        "交易日期": "2026-10-14",
        "合约调整": "是",
        "原行权价格": "3.5000",
        "原合约单位": "10000",
        "原合约代码": "159901C2612M003500",
    }
    row.update(changes)
    return row


def workbook(rows, columns=None):
    """Encode an offline XLSX without relying on the source MIME charset."""
    buffer = BytesIO()
    pd.DataFrame(rows, columns=columns).to_excel(buffer, index=False)
    return buffer.getvalue()


def install_sina(monkeypatch, *, months=None, categories=None, list_size=1):
    """Provide all discovery endpoints and quote records offline."""
    calls = []
    discovered = {}
    categories = list(contracts.SSE_CATEGORIES.values()) if categories is None else categories
    months = ["2026-12", "2026-12"] if months is None else months

    def request(url, **kwargs):
        calls.append((url, kwargs))
        assert kwargs["timeout"] == 25
        params = kwargs.get("params", {})
        if url.endswith("getStockName"):
            underlying = next(k for k, v in contracts.SSE_CATEGORIES.items() if v == params["cate"])
            return Response(
                {
                    "result": {
                        "status": {"code": 0},
                        "data": {
                            "stockId": underlying,
                            "cateList": categories,
                            "contractMonth": months,
                            "cateId": underlying + "C2612A07500",
                        },
                    }
                }
            )
        if url.endswith("getRemainderDay"):
            underlying = next(k for k, v in contracts.SSE_CATEGORIES.items() if v == params["cate"])
            return Response(
                {
                    "result": {
                        "status": {"code": 0},
                        "data": {
                            "stockId": underlying,
                            "expireDay": params["date"] + "-23",
                        },
                    }
                }
            )
        name = url.split("list=")[1]
        side, rest = name.removeprefix("OP_").split("_")
        underlying = rest[:6]
        symbols = []
        for _ in range(list_size):
            symbol = str(10000000 + len(discovered))
            discovered[symbol] = (underlying, "call" if side == "UP" else "put", "20" + rest[6:8] + "-" + rest[8:])
            symbols.append("CON_OP_" + symbol)
        return Response(f'var hq_str_{name}="{",".join(symbols)},";\n'.encode())

    def quotes(symbols):
        tokens = symbols.split(",")
        assert len(tokens) <= 50
        text = ""
        for symbol in tokens:
            underlying, side, month = discovered[symbol]
            values = ["1"] * 43
            values[FIELDS.index("行权价")] = "6.8760"
            values[FIELDS.index("标的股票")] = underlying
            values[FIELDS.index("行情时间")] = "2026-10-14 00:00:00"
            values[FIELDS.index("期权合约简称")] = "synthetic adjusted A"
            values += ["A", "1", "C" if side == "call" else "P", month + "-23", "1", "1", "0", "1"]
            text += f'var hq_str_CON_OP_{symbol}="{",".join(values)}";\n'
        return parse_batch(text, tokens, "2026-10-14T08:00:00+00:00")

    monkeypatch.setattr(contracts, "make_contract_request", request)
    monkeypatch.setattr(contracts, "fetch_quotes", quotes)
    return calls


@pytest.mark.parametrize("subtype,expected", [("C", "call"), ("P", "put"), ("认购", "call"), ("认沽", "put")])
def test_sse_named_headers_and_subtypes(subtype, expected):
    """Normalize explicit subtypes and map by header name."""
    row = sse_row(CALL_OR_PUT=subtype, CONTRACT_ID=None)
    result = contracts.parse_sse_official({"result": [row]})[0]
    assert result["symbol"] == "10011809"
    assert result["underlying"] == "510500" and result["option_type"] == expected
    assert result["strike"] == 6.876 and result["expiration"] == "2026-12-23"
    assert result["contract_unit"] is None and result["source_date"] is None
    assert result["source_row"] == row
    assert result["metadata_source"] == "official"


def test_szse_xlsx_retains_exact_ids_adjustments_and_date():
    """Keep original fields while using source-effective metadata."""
    row = szse_row()
    result = contracts.parse_szse_official(workbook([row]))[0]
    assert result["symbol"] == row["合约编码"]
    assert result["contract_trade_code"] == row["合约代码"]
    assert result["strike"] == 3.499 and result["contract_unit"] == 10003.0
    assert result["source_date"] == "2026-10-14"
    assert result["metadata_source"] == "official"
    assert result["source_row"]["原合约单位"] == "10000"
    assert result["source_row"]["合约调整"] == "是"


@pytest.mark.parametrize(
    "changes",
    [
        {"SECURITY_ID": "10011809 "},
        {"SECURITY_ID": "1001180"},
        {"SECURITYNAMEBYID": "510500/510050"},
        {"EXERCISE_PRICE": "nan"},
        {"EXERCISE_PRICE": "inf"},
        {"EXERCISE_PRICE": ""},
        {"CONTRACT_UNIT": "nan"},
        {"CALL_OR_PUT": "unknown"},
        {"EXPIRE_DATE": "2026-12"},
        {"EXPIRE_DATE": "2026-02-30"},
        {"EXPIRE_DATE": "2026123"},
        {"CONTRACT_ID": "510050C2612M07500"},
    ],
)
def test_official_sse_rejects_invalid_identity_or_metadata(changes):
    """Never repair source identifiers or manufacture metadata."""
    with pytest.raises(ValueError):
        contracts.parse_sse_official({"result": [sse_row(**changes)]})


@pytest.mark.parametrize("key", ["SECURITY_ID", "SECURITYNAMEBYID", "CALL_OR_PUT", "EXERCISE_PRICE", "EXPIRE_DATE"])
def test_official_sse_required_fields(key):
    """Fail closed when the official response drops required headers."""
    row = sse_row()
    row.pop(key)
    with pytest.raises(ValueError):
        contracts.parse_sse_official({"result": [row]})


@pytest.mark.parametrize(
    "changes",
    [
        {"合约编码": "90007393.0"},
        {"标的证券简称(代码)": "159901 and 159919"},
        {"交易日期": ""},
        {"到期日": "2026-12-99"},
        {"合约单位": "-inf"},
        {"行权价": "nan"},
        {"合约类型": "future"},
    ],
)
def test_szse_rejects_invalid_source_metadata(changes):
    """Reject ambiguous identities and invalid source dates/numbers."""
    with pytest.raises(ValueError):
        contracts.parse_szse_official(workbook([szse_row(**changes)]))


def test_szse_duplicate_headers_and_contracts_rejected():
    """Catch duplicate raw headers before pandas can mangle them."""
    row = szse_row()
    cols = list(row) + ["合约编码"]
    with pytest.raises(ValueError):
        contracts.parse_szse_official(workbook([row], columns=cols))
    with pytest.raises(ValueError):
        contracts.parse_szse_official(workbook([row, row]))


@pytest.mark.parametrize(
    "exchange,source,underlyings",
    [
        ("SSE", "auto", None),
        ("SZSE", "sina", None),
        ("SH", "official", None),
        ("SSE", "sina", ""),
        ("SSE", "sina", "510050,"),
        ("SSE", "sina", "510050,510050"),
        ("SSE", "sina", "510050 "),
        ("SSE", "sina", "159901"),
    ],
)
def test_explicit_source_policy_validates_before_network(monkeypatch, exchange, source, underlyings):
    """Reject unsupported policies and malformed filters before I/O."""
    monkeypatch.setattr(contracts, "make_contract_request", lambda *a, **k: pytest.fail("network called"))
    with pytest.raises(ValueError):
        contracts.fetch_contracts(exchange, source, underlyings)


def test_official_403_never_falls_back(monkeypatch):
    """A blocked official endpoint must not implicitly select Sina."""
    calls = []

    def request(url, **kwargs):
        calls.append(url)
        assert kwargs["timeout"] == 25
        return Response(b"forbidden", status=403)

    monkeypatch.setattr(contracts, "make_contract_request", request)
    with pytest.raises(RuntimeError, match="403"):
        contracts.fetch_contracts("SSE", "official")
    assert calls == [contracts.SSE_URL]


def test_sina_all_five_categories_and_duplicate_month_dedup(monkeypatch):
    """Cover all declared categories using only deduplicated source months."""
    calls = install_sina(monkeypatch)
    results = contracts.fetch_contracts("SSE", "sina")
    assert len(results) == 10
    assert {r["underlying"] for r in results} == set(contracts.SSE_CATEGORIES)
    assert len([url for url, _ in calls if "list=OP_" in url]) == 10
    for row in results:
        assert row["contract_unit"] is None and row["contract_trade_code"] is None
        assert row["strike"] == 6.876 and row["source_date"] == "2026-10-14"
        assert row["official_universe_verified"] is False
        assert row["metadata_complete"] is False
        assert row["source_row"]["discovery"]["raw_response"].endswith(";\n")
        assert row["source_row"]["expiry_validation"]["expireDay"] == "2026-12-23"
        assert row["source_row"]["quote"]["raw_extra_fields"][0] == "A"


@pytest.mark.parametrize("months", [[], ["2026-99"], ["2612"], ["2026-2"]])
def test_sina_invalid_or_missing_maturities(monkeypatch, months):
    """Never calculate a maturity when discovery is missing or invalid."""
    install_sina(monkeypatch, months=months)
    with pytest.raises(ValueError):
        contracts.fetch_contracts("SSE", "sina", "510050")


def test_sina_shrunk_category_inventory_rejected(monkeypatch):
    """Require the entire known source-category inventory."""
    install_sina(monkeypatch, categories=["50ETF"])
    with pytest.raises(ValueError):
        contracts.fetch_contracts("SSE", "sina", "510050")


@pytest.mark.parametrize("change", ["wrong_stock", "bad_status"])
def test_sina_silent_fallback_or_status_error_rejected(monkeypatch, change):
    """Catch the vendor's HTTP-200 invalid-category fallback."""
    install_sina(monkeypatch)
    original = contracts.make_contract_request

    def request(url, **kwargs):
        response = original(url, **kwargs)
        if url.endswith("getStockName"):
            value = json.loads(response.content)
            if change == "wrong_stock":
                value["result"]["data"]["stockId"] = "510050"
            else:
                value["result"]["status"]["code"] = 1
            return Response(value)
        return response

    monkeypatch.setattr(contracts, "make_contract_request", request)
    with pytest.raises(ValueError):
        contracts.fetch_contracts("SSE", "sina", "510500")


@pytest.mark.parametrize(
    "text",
    [
        "",
        'var hq_str_OP_UP_5100502612="";',
        'var hq_str_OP_UP_5100502612="CON_OP_10011809,CON_OP_10011809,";',
        'var hq_str_OP_UP_5100502612="CON_OP_10011809,garbage,";',
        'var hq_str_OP_UP_5100502612="CON_OP_10011809,";evil();',
        'var hq_str_OP_UP_5105002612="CON_OP_10011809,";',
    ],
)
def test_discovery_full_residue_duplicate_and_empty_rejection(text):
    """Parse the complete assignment, not a permissive ID substring."""
    with pytest.raises(ValueError):
        contracts.parse_discovery(text, "OP_UP_5100502612")


@pytest.mark.parametrize(
    "change",
    [
        "missing",
        "duplicate",
        "wrong_underlying",
        "wrong_type",
        "wrong_month",
        "wrong_day",
        "nonfinite",
        "legacy",
    ],
)
def test_sina_quotes_fail_closed(monkeypatch, change):
    """Reject partial quotes or metadata contradicting frozen discovery."""
    install_sina(monkeypatch)
    original = contracts.fetch_quotes

    def quotes(symbols):
        rows = original(symbols)
        if change == "missing":
            return rows[:-1]
        if change == "duplicate":
            return [rows[0], rows[0]]
        key, value = {
            "wrong_underlying": ("underlying", "510500"),
            "wrong_type": ("option_type", "put"),
            "wrong_month": ("expiration", "2027-03-23"),
            "wrong_day": ("expiration", "2026-12-24"),
            "nonfinite": ("strike", float("nan")),
            "legacy": ("source_field_count", 43),
        }[change]
        rows[0][key] = value
        return rows

    monkeypatch.setattr(contracts, "fetch_quotes", quotes)
    with pytest.raises(ValueError):
        contracts.fetch_contracts("SSE", "sina", "510050")


def test_official_filter_cannot_silently_return_empty(monkeypatch):
    """Requested underlyings must actually occur in the retrieved master."""
    monkeypatch.setattr(contracts, "make_contract_request", lambda *a, **k: Response({"result": [sse_row()]}))
    with pytest.raises(ValueError):
        contracts.fetch_contracts("SSE", "official", "510050")


def test_response_byte_budget(monkeypatch):
    """Enforce response bytes before parsing vendor content."""
    monkeypatch.setattr(
        contracts, "make_contract_request", lambda *a, **k: Response(b"x" * (contracts.JSON_BYTE_LIMIT + 1))
    )
    with pytest.raises(ValueError, match="byte_limit"):
        contracts.fetch_contracts("SSE", "official")


def test_typed_fetcher_api(monkeypatch):
    """Exercise the parent's exact typed extraction/transformation API."""
    import asyncio

    from openbb_akshare.models.etf_options import AkshareEtfOptionContractsFetcher

    monkeypatch.setattr(contracts, "make_contract_request", lambda *a, **k: Response({"result": [sse_row()]}))
    query = AkshareEtfOptionContractsFetcher.transform_query({"exchange": "SSE", "source": "official"})
    raw = asyncio.run(AkshareEtfOptionContractsFetcher.aextract_data(query, None))
    result = AkshareEtfOptionContractsFetcher.transform_data(query, raw)
    assert result[0].symbol == "10011809" and result[0].contract_unit is None


def test_full_sina_evidence_budget_and_fifty_contract_batches(monkeypatch):
    """A synthetic full-sized 640-contract response fits the worker byte budget."""
    calls = install_sina(monkeypatch, months=["2026-10", "2026-11", "2026-12", "2027-03"], list_size=16)
    original = contracts.fetch_quotes
    batches = []

    def quotes(symbols):
        batches.append(len(symbols.split(",")))
        return original(symbols)

    monkeypatch.setattr(contracts, "fetch_quotes", quotes)
    rows = contracts.fetch_contracts("SSE", "sina")
    assert len(rows) == 640 and batches == [50] * 12 + [40]
    assert len([url for url, _ in calls if "list=OP_" in url]) == 40
    encoded = json.dumps({"rows": rows, "metadata": {}, "warning_count": 0}, allow_nan=False).encode()
    assert len(encoded) < 8_000_000


@pytest.mark.parametrize("change", ["error", "missing_strike", "missing_date"])
def test_sina_metadata_requires_successful_quote_fields(monkeypatch, change):
    """Diagnostic or incomplete quotes cannot become valid master records."""
    install_sina(monkeypatch)
    original = contracts.fetch_quotes

    def quotes(symbols):
        rows = original(symbols)
        if change == "error":
            rows[0]["quote_error"] = "nonfinite_quote"
        else:
            rows[0].pop("strike" if change == "missing_strike" else "source_time")
        return rows

    monkeypatch.setattr(contracts, "fetch_quotes", quotes)
    with pytest.raises(ValueError):
        contracts.fetch_contracts("SSE", "sina", "510050")


def test_sina_retains_prior_source_date_without_capture_date_inference(monkeypatch):
    """Preserve stale source metadata dates; let collector quality decide eligibility."""
    install_sina(monkeypatch)
    original = contracts.fetch_quotes

    def quotes(symbols):
        rows = original(symbols)
        rows[0]["source_time"] = "2026-10-13T15:00:00+08:00"
        return rows

    monkeypatch.setattr(contracts, "fetch_quotes", quotes)
    rows = contracts.fetch_contracts("SSE", "sina", "510050")
    assert [row["source_date"] for row in rows] == ["2026-10-13", "2026-10-14"]


@pytest.mark.parametrize("change", ["empty", "duplicate", "wrong_expiry", "wrong_stock"])
def test_sina_discovery_and_expiry_integrity(monkeypatch, change):
    """Reject empty/cross-list-duplicate identifiers and inconsistent expiry evidence."""
    install_sina(monkeypatch)
    original = contracts.make_contract_request

    def request(url, **kwargs):
        response = original(url, **kwargs)
        if "list=OP_" in url and change in {"empty", "duplicate"}:
            name = url.split("list=")[1]
            payload = "" if change == "empty" else "CON_OP_10000000,"
            return Response(f'var hq_str_{name}="{payload}";'.encode())
        if url.endswith("getRemainderDay") and change in {"wrong_expiry", "wrong_stock"}:
            value = json.loads(response.content)
            key, replacement = ("expireDay", "2027-03-23") if change == "wrong_expiry" else ("stockId", "588000")
            value["result"]["data"][key] = replacement
            return Response(value)
        return response

    monkeypatch.setattr(contracts, "make_contract_request", request)
    with pytest.raises(ValueError):
        contracts.fetch_contracts("SSE", "sina", "510050")


def test_szse_fetch_uses_same_official_xlsx_endpoint(monkeypatch):
    """Reuse the SDK's source URL/parameters with bounded OpenBB transport."""
    calls = []

    def request(url, **kwargs):
        calls.append((url, kwargs))
        return Response(workbook([szse_row()]))

    monkeypatch.setattr(contracts, "make_contract_request", request)
    rows = contracts.fetch_contracts("SZSE", "official", "159901")
    assert len(rows) == 1
    assert calls == [
        (
            contracts.SZSE_URL,
            {
                "timeout": 25,
                "params": {"SHOWTYPE": "xlsx", "CATALOGID": "option_drhy", "TABKEY": "tab1"},
            },
        )
    ]

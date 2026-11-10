"""Offline CFFEX source contract tests; synthetic fixtures are never market data."""

import asyncio
import base64
import hashlib
import json
from copy import deepcopy

import pytest
from openbb_akshare.models.cffex_options import (
    AkshareCffexOptionExpiriesFetcher,
    AkshareCffexOptionQuotesFetcher,
)
from openbb_akshare.utils import cffex_option_quotes as source

STAMP = "2026-10-14T08:00:00+00:00"


def synthetic_payload():
    """Construct a deliberately synthetic two-strike source response."""
    return {
        "result": {
            "status": {"code": 0},
            "data": {
                "info": {
                    "expire_date": "2026-10-16",
                    "name": "沪深300指数",
                    "relate": "sh000300",
                    "exchange": "cffex",
                },
                "up": [
                    ["1", "1.2", "1.3", "1.4", "2", "3", -1.5, "3800", "io2610C3800"],
                    ["2", "2.2", "2.3", "2.4", "3", "4", 1.5, "3900", "io2610C3900"],
                ],
                "down": [
                    ["4", "4.2", "4.3", "4.4", "5", "6", -2, "io2610P3900"],
                    ["6", "6.2", "6.3", "6.4", "7", "8", 2, "io2610P3800"],
                ],
            },
        }
    }


def encode(payload):
    """Serialize synthetic payloads without rewriting source fixture bytes."""
    return json.dumps(payload, ensure_ascii=False).encode()


def synthetic_html(tokens=("io2610", "io2611")):
    """Build a synthetic discovery list with native data-value attributes."""
    return (
        '<div id="option_suffix"><ul>'
        + "".join(f'<li data-value="{token}">{token}</li>' for token in tokens)
        + "</ul></div>"
    ).encode()


def test_independent_sides_and_exact_evidence():
    """Reordered put rows must never inherit the positional call strike."""
    payload = synthetic_payload()
    raw = b" \n" + encode(payload) + b"\n"
    rows = source.parse_quotes(raw, "io2610", STAMP)
    found = {row["symbol"]: row for row in rows}
    assert len(rows) == 4
    assert found["io2610P3900"]["strike"] == 3900
    assert found["io2610P3800"]["bid"] == 6.2
    for row in rows:
        assert row["symbol"].startswith("io2610")
        assert row["product"] == "IO" and row["exchange"] == "CFFEX"
        assert row["underlying_symbol"] == "000300"
        assert row["underlying_source_symbol"] == "sh000300"
        assert row["expiration"] == "2026-10-16"
        assert row["contract_unit"] is None and row["source_time"] is None
        assert row["source_info"] == payload["result"]["data"]["info"]
        assert row["source_sha256"] == hashlib.sha256(raw).hexdigest()
        assert row["fetched_at"] == STAMP
        assert "volume" not in row
    assert rows[0]["raw_fields"] == {"side": "up", "values": payload["result"]["data"]["up"][0]}
    evidence = rows[0]["raw_evidence"]
    assert evidence["encoding"] == "base64"
    assert base64.b64decode(evidence["content_base64"]) == raw
    assert evidence["sha256"] == rows[0]["source_sha256"]
    assert "pinzhong=io2610" in evidence["url"]
    assert all("raw_evidence" not in row for row in rows[1:])


@pytest.mark.parametrize("token", [
    "IO2610", "io2613", "io2600", " io2610", "io2610 ", "io2610;id", "io2610/", "if2610",
    "io2610,ho2610", "io２６１０", "", None, 2610,
])
def test_exact_query_rejected_before_io(token, monkeypatch):
    """Never repair case, whitespace, invalid months or malicious source tokens."""
    def fail(*args, **kwargs):
        raise AssertionError("Network must not be called")

    monkeypatch.setattr(source, "make_request", fail)
    with pytest.raises(ValueError):
        AkshareCffexOptionQuotesFetcher.transform_query({"expiry_symbol": token})
    with pytest.raises(ValueError):
        source.fetch_quotes(token)


@pytest.mark.parametrize("product", ["io", "HO ", "IF", "", None])
def test_explicit_product_enum(product):
    """Discovery requires an exact uppercase supported family."""
    with pytest.raises(ValueError):
        AkshareCffexOptionExpiriesFetcher.transform_query({"product": product})


def test_no_implicit_queries_or_extra_fields():
    """Missing family/expiry and unknown query keys fail closed."""
    for fetcher in (AkshareCffexOptionExpiriesFetcher, AkshareCffexOptionQuotesFetcher):
        with pytest.raises(ValueError):
            fetcher.transform_query({})
    with pytest.raises(ValueError):
        AkshareCffexOptionQuotesFetcher.transform_query({"expiry_symbol": "io2610", "date": "2026-10-14"})


def test_discovery_order_scope_and_raw():
    """Discovery returns all displayed expiries without selecting a main contract."""
    raw = synthetic_html(("io2611", "io2610", "io2703"))
    rows = source.parse_expiries(raw, "IO", STAMP)
    assert [row["expiry_symbol"] for row in rows] == ["io2611", "io2610", "io2703"]
    assert all(row["universe_scope"] == "sina_displayed_expiries" for row in rows)
    assert base64.b64decode(rows[0]["raw_evidence"]["content_base64"]) == raw
    assert all("raw_evidence" not in row for row in rows[1:])
    assert rows[0]["raw_evidence"]["url"] == source.DISCOVERY_URLS["IO"]


@pytest.mark.parametrize("raw", [
    b"<html>blocked</html>",
    synthetic_html(()),
    synthetic_html(("io2610", "io2610")),
    synthetic_html(("ho2610",)),
    synthetic_html(("io2613",)),
    synthetic_html(("IO2610",)),
    b'<div id="option_suffix"><li data-value="io2610">io2611</li></div>',
    b'<div id="option_suffix"><li>io2610</li></div>',
    synthetic_html() + synthetic_html(),
    synthetic_html(tuple(f"io{year:02}01" for year in range(30))),
])
def test_discovery_incomplete_or_wrong_family_is_fatal(raw):
    """Malformed, duplicate, foreign or excessive discovery lists are not usable."""
    with pytest.raises(ValueError):
        source.parse_expiries(raw, "IO", STAMP)


@pytest.mark.parametrize("symbol", [
    "io2610P3800", "ho2610C3800", "io2611C3800", "IO2610C3800", "io2610C3800;id",
    "io2610Cnan", "io2610C0", "io2610C-1", "io2610C03800", " io2610C3800",
    "io2610C3800 ", "io2610c3800", "io2613C3800", "io2600C3800", None,
])
def test_bad_native_ids_fatal(symbol):
    """Preserve valid source IDs, never normalize malformed ones."""
    payload = synthetic_payload()
    payload["result"]["data"]["up"][0][-1] = symbol
    with pytest.raises(ValueError):
        source.parse_quotes(encode(payload), "io2610", STAMP)


@pytest.mark.parametrize("product", ["IO", "HO", "MO"])
@pytest.mark.parametrize("strike", ["3800.5", "3800.0", "3800.000000000000000001"])
def test_fractional_native_ids_reject_even_with_matching_strikes(product, strike):
    """Native IDs require integer suffixes, even when both sides and strike agree."""
    payload = synthetic_payload()
    data = payload["result"]["data"]
    expiry = product.lower() + "2610"
    data["info"]["relate"], data["info"]["name"] = source.PRODUCT_INFO[product]
    for side in ("up", "down"):
        for fields in data[side]:
            fields[-1] = fields[-1].replace("io2610", expiry).replace("3800", strike)
    data["up"][0][7] = strike
    with pytest.raises(ValueError, match="invalid_or_foreign_native_id"):
        source.parse_quotes(encode(payload), expiry, STAMP)


@pytest.mark.parametrize("value", ["3800.5", "3800.000000000000000001", 0, -3800, "nan", "inf"])
def test_source_strike_must_equal_positive_integer_native_suffix(value):
    """Reject fractional, nonpositive and nonfinite source strikes, without repair."""
    payload = synthetic_payload()
    payload["result"]["data"]["up"][0][7] = value
    with pytest.raises(ValueError):
        source.parse_quotes(encode(payload), "io2610", STAMP)


@pytest.mark.parametrize("strike", ["3800.5", "3800.0"])
def test_fractional_put_id_rejects_before_membership_check(strike):
    """The put-only native suffix must be integer even after valid calls parse."""
    payload = synthetic_payload()
    payload["result"]["data"]["down"][1][-1] = "io2610P" + strike
    with pytest.raises(ValueError, match="invalid_or_foreign_native_id"):
        source.parse_quotes(encode(payload), "io2610", STAMP)


@pytest.mark.parametrize("value", ["3800", "3800.0", 3800, 3800.0])
def test_integer_source_strike_representations_preserved(value):
    """The source number may be decimal-formatted; the native ID may not."""
    payload = synthetic_payload()
    payload["result"]["data"]["up"][0][7] = value
    row = source.parse_quotes(encode(payload), "io2610", STAMP)[0]
    assert row["symbol"] == "io2610C3800" and row["strike"] == 3800
    assert row["raw_fields"]["values"][7] == value


@pytest.mark.parametrize("mutation", [
    "duplicate", "unmatched", "empty", "width_up", "width_down", "strike", "no_info",
    "no_up", "no_down", "status", "row_type", "budget",
])
def test_structural_failures_reject_entire_response(mutation):
    """Truncation, identity and membership failures must not leak partial rows."""
    payload = synthetic_payload()
    data = payload["result"]["data"]
    if mutation == "duplicate":
        data["up"].append(deepcopy(data["up"][0]))
    elif mutation == "unmatched":
        data["down"][0][-1] = "io2610P4000"
    elif mutation == "empty":
        data["down"] = []
    elif mutation == "width_up":
        data["up"][0].append("extra")
    elif mutation == "width_down":
        data["down"][0].pop(0)
    elif mutation == "strike":
        data["up"][0][7] = "3900"
    elif mutation == "no_info":
        del data["info"]
    elif mutation == "no_up":
        del data["up"]
    elif mutation == "no_down":
        del data["down"]
    elif mutation == "status":
        payload["result"]["status"]["code"] = 1
    elif mutation == "row_type":
        data["up"][0] = "not an array"
    elif mutation == "budget":
        data["up"] *= 1001
    with pytest.raises(ValueError):
        source.parse_quotes(encode(payload), "io2610", STAMP)


@pytest.mark.parametrize("field,value", [
    ("relate", "sh000016"), ("relate", "000300"), ("name", "上证50指数"),
    ("expire_date", "2026-11-20"), ("expire_date", "2025-10-16"),
    ("expire_date", "2026-10-32"), ("expire_date", "20261016"),
    ("exchange", "sse"), ("name", None), ("expire_date", None), ("relate", None),
])
def test_source_metadata_mismatch(field, value):
    """Expiration and validated source underlying must agree with the request."""
    payload = synthetic_payload()
    payload["result"]["data"]["info"][field] = value
    with pytest.raises(ValueError):
        source.parse_quotes(encode(payload), "io2610", STAMP)


@pytest.mark.parametrize("value", ["nan", "inf", "-inf", "oops", True, [], {}, float("nan"), float("inf"), " 1.2", -1])
@pytest.mark.parametrize("index", [0, 1, 2, 3, 4, 5])
def test_bad_numeric_fatal(index, value):
    """All quote fields fail closed on malformed, negative or nonfinite numbers."""
    payload = synthetic_payload()
    payload["result"]["data"]["up"][0][index] = value
    with pytest.raises(ValueError):
        source.parse_quotes(encode(payload), "io2610", STAMP)


@pytest.mark.parametrize("index,value", [(0, "1.5"), (4, "2.5"), (5, "3.5"), (6, "nan"), (7, "--")])
def test_quantity_change_and_strike_validation(index, value):
    """Counts are integral, change finite when present, strike always positive."""
    payload = synthetic_payload()
    payload["result"]["data"]["up"][0][index] = value
    with pytest.raises(ValueError):
        source.parse_quotes(encode(payload), "io2610", STAMP)


@pytest.mark.parametrize("missing", [None, "", "-", "--"])
def test_missing_zero_and_no_event_clock(missing):
    """Keep absence and zero distinct, and do not turn acquisition into event time."""
    payload = synthetic_payload()
    values = payload["result"]["data"]["up"][0]
    values[1], values[3] = missing, "0"
    row = source.parse_quotes(encode(payload), "io2610", STAMP)[0]
    assert row["bid"] is None and row["ask"] == 0
    assert "missing_bid" in row["quote_diagnostics"]
    assert "zero_ask" in row["quote_diagnostics"]
    assert "source_time_unavailable" in row["quote_diagnostics"]
    assert row["source_time"] is None
    assert row["contract_unit"] is None


@pytest.mark.parametrize("raw", [
    b"{}", b"null", b"[]", b"<html>WAF</html>", b'{"result":{},"result":{}}', b"\xff", b"x" * 1_000_001,
])
def test_bad_envelopes_and_byte_budget(raw):
    """Reject malformed envelopes, duplicate JSON keys and oversized payloads."""
    with pytest.raises(ValueError):
        source.parse_quotes(raw, "io2610", STAMP)


class FakeResponse:
    """Synthetic streaming HTTP response, not a network market sample."""

    def __init__(self, body, status=200):
        self.body = body
        self.status_code = status
        self.headers = {}
        self.closed = False

    def raise_for_status(self):
        """Mirror status failure before parsing."""
        if self.status_code >= 400:
            raise RuntimeError("HTTP failure")

    def iter_content(self, chunk_size):
        """Yield bounded source chunks."""
        for start in range(0, len(self.body), chunk_size):
            yield self.body[start:start + chunk_size]

    def close(self):
        """Expose cleanup for assertions."""
        self.closed = True


def test_transport_literal_params_budget_and_close(monkeypatch):
    """Use the shared helper, literal source identity, timeout and streamed byte cap."""
    calls = []
    response = FakeResponse(encode(synthetic_payload()))

    def request(url, **kwargs):
        calls.append((url, kwargs))
        return response

    monkeypatch.setattr(source, "make_request", request)
    assert len(source.fetch_quotes("io2610")) == 4
    assert calls[0][0] == source.QUOTE_URL
    assert calls[0][1]["params"] == {"type": "futures", "product": "io", "exchange": "cffex", "pinzhong": "io2610"}
    assert calls[0][1]["timeout"] == 30 and calls[0][1]["stream"] is True
    assert response.closed
    for status, body in [(503, b"bad"), (204, b""), (200, b"x" * 1_000_001)]:
        response = FakeResponse(body, status)
        with pytest.raises((ValueError, RuntimeError)):
            source.fetch_quotes("io2610")
        assert response.closed


def test_fetcher_models_and_off_event_loop(monkeypatch):
    """Both fetchers validate data and run blocking acquisition on a worker thread."""
    import threading

    main_thread = threading.get_ident()
    threads = []

    def quotes(expiry):
        threads.append(threading.get_ident())
        return source.parse_quotes(encode(synthetic_payload()), expiry, STAMP)

    def expiries(product):
        threads.append(threading.get_ident())
        return source.parse_expiries(synthetic_html(), product, STAMP)

    import openbb_akshare.models.cffex_options as models

    monkeypatch.setattr(models, "fetch_quotes", quotes)
    monkeypatch.setattr(models, "fetch_expiries", expiries)
    for fetcher, params in [
        (AkshareCffexOptionQuotesFetcher, {"expiry_symbol": "io2610"}),
        (AkshareCffexOptionExpiriesFetcher, {"product": "IO"}),
    ]:
        query = fetcher.transform_query(params)
        data = asyncio.run(fetcher.aextract_data(query, None))
        typed = fetcher.transform_data(query, data)
        assert typed[0].model_dump()["source_sha256"] == data[0]["source_sha256"]
        assert not fetcher.require_credentials
    assert all(thread != main_thread for thread in threads)


def test_strike_identity_not_rounded_into_a_false_match():
    """Finite float rounding cannot prove equality of distinct native strike tokens."""
    payload = synthetic_payload()
    payload["result"]["data"]["up"][0][-1] = "io2610C3800.000000000000000001"
    with pytest.raises(ValueError):
        source.parse_quotes(encode(payload), "io2610", STAMP)
    payload["result"]["data"]["up"][0][-1] = "io2610C3800"
    payload["result"]["data"]["up"][0][7] = "3800.000000000000000001"
    with pytest.raises(ValueError):
        source.parse_quotes(encode(payload), "io2610", STAMP)


def test_nonfinite_nested_metadata_rejected():
    """A JSON exponent overflow is invalid even outside the normalized quote fields."""
    raw = encode(synthetic_payload()).replace(b'"cffex"', b'"cffex", "unexpected": 1e1000')
    with pytest.raises(ValueError):
        source.parse_quotes(raw, "io2610", STAMP)


# Original complete wire bytes from cffex_quote_probe_20261015/receipt.json.
# zlib/base64 merely packages the immutable samples; SHA-256 verifies each one.
NATIVE_QUOTES = [
    ("io2610", "461fa7f0481288286e204bf1335cf0dc1e27f7c0af4bacb29d4684bcd14d547f",
     "eNqNV9tuGzkM/Zd5jmWJ1DWv/YF9r4Ng4DiboIkdxA42RdF/X1Ijaqii9W6BZuSZ4ZnDQ4qkfkzvh/PHy2W6/TGdL/Pl48yr/enh"
     "MN3anzfTw3yZ+c7H23T79evkppvJO2+8tXWVZAVobF3VN8p0s3HOlHwzYa63n08Qnf1Sf93dLEAYBQiTM9BW2Fb8BgADgQmegcIA"
     "FBYgYCNiFBfzzg1BA/nEQNFYpCdlYFTsCgQxNDcg2mYOySvXgmOgYlJhoIFRCatrQMosjACsQAKY3IFyA8qkkbeaUf0lQC5mMUoC"
     "5EgjvwJlAgJvgmOgMAApRg5cY+RAXHMQGjg7HwqrjRQJz6EdKDlFKZX2db2InU9KgWGSSYlhBkKuEcKaMj131sWqDwTLdHw2UPjR"
     "QAeETvUsdV3WBXS3ED0DRTAADDQQAsmiwAI0L0QUucZFHEahPA3sFg50sNFBzx+X2IgmkgiM5DyBbbJliIEINiKREnWyzWS95pbT"
     "zMRmz0zywsQPTLwwWWB8M/cDHD9Dn9Pij0dGGcj4RoYjOUmy/EoKWJYU2ecNBMIIA5MgISoDxsio+pMTZTArEgYSQULjBoARyNUM"
     "CDnSj01gkDiwiLK58QqI93Wbs2YVYaARg0q1P/lRA4+WL7RTuNwMLFJjka6RiNWd3LRIA4nUSCwG9hcAWDkAlgYwlF3fy669QoFr"
     "2xQ5zyvAwEDK7TUXAoczJ1zshyrrpcqWaxIsTUHsh++X/6FAqhJa6ytAGIpq6EUVrsTRLb3CtTCGoQgGZ3+bCiNUqVu0LBSGqhWk"
     "al31ge1rOrP9UGbqr7s76smnf45LN74WTagOLdmwIPy1dl/8T0PE0VK2QbwiX6zl1jb1xFQ32D9VNuZJulpSNmjTMAStvStVvl2r"
     "aYr0CpehZjo20W7Zup0g1Hpt2VU32LbPcvnzzdc+88i1NufIqZKUqeRIbSjSH6U9Fmks1eNIcrkAJqIyV1MNDTCt0qrF2pGodTFv"
     "T11KAYAS20s/9NIP+yLUehUp2RxNTEE5AKpF5ygSx/ZhWqydHgM3pEyzV17tURFw2EcW7PMh9hGL/xcu/iGY5BWEEsHlHqzcB7qs"
     "JzKMliC8NQVWCK+iDyAjFKAMToB6XoWYSQkiq5X0epzLkmzQ+UAXxS2Fl5YUoKC0CEoLBPEbUeKJGNWM6iBxDtPWcApCscAUpd9k"
     "SUvMGqLwCOcUg6h08CCv+u69x6jndqwpaUAjKAK+E/A9EXzWCJRTnJMGlZBJcQggOz94iUXApMZZV5MSTVIckuIQKAANQa2STktX"
     "67fJZUXQx47YJ/JoRYeIWR87HLU5bxSDrBjEKPs59hSIebDnCY02u0rIogfoLkKi1GwjNBFYXWD7ZLLalvpwkfqZIGVJqpS1vQPa"
     "ErkOvA0g6HqYEzS22UtEcy76UEHbn45dyt6pXC4xNQIlS1UuxSoFMhWYYEpc7UHXYxv7qG6TRNHZYpUPwNMnZdIKIf3v+fh44jPp"
     "4fPt+f1wT0dUOqxS24K4cXZTO9TD/J0OskT/OL/yw91H3M8z/X14pKJkaYE20d+Q+HPvh5cF4/xErJam+36YXy6H+9/b0/PD5/5p"
     "Pv7ND/ePj4dPuvW2v395Pn6jO0+Xy9v5drfdbR+fj/NxfzBnupr96dXsj7stY58vp/233ZZuvc3H77utfHq3Pe7N+eny+jL9pH//"
     "AmDTJC0="
    ),
    ("ho2610", "2d258408f678ac3e790f916f0b9e9a00b7caa9b0d9d2414ebd35b8d69711eada",
     "eNqNVttu2zAM/Rc/J4qou/K6H9h7XRSe6yxBUyeIHaxF0X8fKVm21LXpCjSibPPwiKIO9VZduuF6HKvtWzWMzXgdyGpPj1215e+r"
     "6rEZG3pyPVfbu7sKqlUlrGaC82AZpqLlFDPRwn/pq9XaMWNwqsPT/UkY4D/C7H51F74SQjMXfcSMI4HxYFEkaRAHOFOKgHQBpCMQ"
     "fQZIIxKCmRBYNxEKX5C1BskkUTIFJcMzpJkICDuRA+EzJENLA8OAKJmCkskoOT4tA40I6CBDUQFGAANHWSz42CxFMtHJjQgjCUoa"
     "hzhyomMLOnaiY/GZndikNaVRKfoBicPacCZoIwsybiIjNYVLuZnHSIXeaQmSQDzTllAKKm6ionA/Kz7tVUpMGmlByppIBSF8QcSn"
     "XZKywCixwkuhVADBTBEVX1DxExXnbsFQzgR4Kj+NXCTPuYQZYUQX/gEijgRvAXc5uOvCXX/vDpooaCcjABTx4Wb8uATvaQneTf4F"
     "AdDf+0PYcGujvyjii/9YP4T6VFN8UcQXeqnwr7ZAiVCeEP1lEZ9m9/eoTac/fVClW0BCBzQRgCLCz0KGvnIEFYrAlY7pjN/0DEKh"
     "9FQ9yTcJjfyk+HkSBxHUSpZ0k7Ko7NAt43Sko2hhyWJ5LK42C+qSJqUancYA6xy5qihJs7devGVaZm64WfOF8kjBoU6JxT/Jhwmn"
     "ITGdDZVpPSjSMowvVeafqzz4WZvVnC6V6i3MqOpQwo1cIHwu7xg6NQo9WzmEcXhuABNlMgSd96zUn4RQ/1ihZ5EFgvkZYNaL0DyN"
     "To0SA6sPVlyFoD1kymYIWR7w+CYE76fAeKTybkcyGNQ8+UPGQM4MpEoVITEdCwM6tWvOICMAOQGfGgD+phLiIqsFOu04XwpplozY"
     "/FIwpedGtLRwioAEsEiy+CKLryFtv8aoEUkvjTukkFMClkKaJePQ7050nelezodL94C3G7znVIILg/cM7OsV3Xhe8Q6EyeubZ3pZ"
     "X1XHm/rqfrWYhfpqJLf4qy1Fu3THCDHsMZM8AFy65jh2D5+64+vupd03/W961+523Qs+OrcPx0P/hE/243getvWm3uwOfdO3HRtw"
     "ZO3pmbV9vSHoYTy1T/UGH52b/rXepMj1pm/ZsB+fj9U7/v0FYF0HFQ=="
    ),
    ("mo2610", "0efeba27f84768c3b89cae419fbad939f89b4b12a9ecb023aaffadb70d709d3e",
     "eNp9l81SIzkMx9+lz9DY8odkrvsCe59QVDaEhRpIqEmoZWpq3n0lt+WWqSEcaKXb/rUs/W2pf00/9qf3l/N0+2s6nbfn95NYu+PD"
     "frp1v6+mh+15K3fe36bbb98mP11N6HDOzonl3UyLBWV21ZIRAGm6uvZxTv5qylDvvx4he/dX/XV3tZCyo3l5miPMcbF8aPTlXQKC"
     "GSI/CQMoGFDyvjmSZPBiQWpIGUFeQOxklpcNoGhAEXybFD02ZIRsPEoITOJhlJiUBlIypAChORLYEdes3JiVFCspzYhMygMpGxKE"
     "2OaLlT9ZNUqETApuLrI6HEhoSD5FfX9Sn4xVfxVxKuS5SMRpQFFDgcST2vvZoNGI1b8SmBN5vYE5ZeCUxskyOGi8Ne3dCEHkkIGH"
     "XSeWHIocLKj+ElBM4rrmy5MuSA0BkcsiyoxVlKxcC/IaJJRXqhKbHz37gim5Lgw5a1FkP2BU20lGgqaqXxf3ZG9MhZIoGzmKhTGD"
     "srErWwLkm4pUl3qtSQCoFFYiDppG1TTJy1zzQDerXgVRfCZBMJYThYOeUfUcoM6KbbZeW5pgUaDoJjs2ByWjKjkuDPjEiD1B2SXJ"
     "dAhzEE8GFaOqGMMFSj1SiuhF3Bi0i2Rj+hVAlBsRYAEMokUV7TLetXkwXL2s0fssyr1OTkRnGaR6BX9pFaJBoLJ4QYNQSYUK5QJB"
     "ch4w4wIYJEoq0UgXAFWiPN4vhEGdpOqM+IcIKKFUQkDSQAzyJJVnyBcYqR4kkOPixSBNUmnCJUGEJR/AnnLgmDFIk/oh6+IFN+o6"
     "fXaLrGjQJfXTlS4QsvgYY3QajEGa9dfdHZfa43+HWmTxD3vWN8nJsxScbDYumZQV83cvrGk4LPJwDVVfrkjdcevUYKZ+dWxJpsCT"
     "JJQrWwrr7F4/a9LatKT1uF1rCFAORp6M6+RkJ/d2YjXsukM9ZLitoLICNIlUxaKF0hjQz7oIkVf+KWxoHYj63tVQB0QiKcnOTSZy"
     "ZKoraqgwqNNqxEUBjp1MMCOsAD1WamYgat6gl/fePEhyUpZjgSt2WFewVkFZptPqaa2o55MEkX1mhK8FXhHethtFlyxWi16JpnEJ"
     "KJtBOrY1kWj7ukC98pJ2eKE407EEQHaXz/pMK8J2dJF0eKSiuSj2pIckCECrBrS9XCLdiKn7kwqaWEASQYAJhO3gMmnoc19FLtnO"
     "L8AZBTcHvyJs68Zlp3mMHYHsgmmVgxQNz9S0ImzPRiyi1mAV7QNpRPhCdVsUkw4rzNIXYqxiW9EsZy033t6EstjG0WXft2bpKmXb"
     "QDzIYcz995rSXvH8Uhi7BHzpPHBkhOFLPaznvO4Rsur0sHrCetFkhMETlkaFxDWmZPXpQ4Y+s2gH6qN35rTw0rGUeY0pWX36uCIi"
     "kCLSiJDzjjffelhSHNrxlZFK0SjwOJNd5H1P9RNBEVajPqei787YYyCfaOtOKbxb85zW3JLVqMfcv7jI9dzS4AXxPkErMBq+Kyiv"
     "80i/5DjHFpGlF82z2W298j0fHo/ykbn/eHv+sb/nb07++uRzC/K1d9e1ZXvY/uQvUxbFYfsqDzfvcQ8Pm3f6Z8cydG7zzlUM+X+S"
     "T0b+qH1ZIKcnfkgJ6r3ty3l//wWAB+w/dk/bw7/ydPf4uP/gW2+7+5fnw3e+83Q+v51uNzebm8fnw/aw288nvs674+u8O2xuBH46"
     "H3ffNzd86217+Lm50Xdvbg67+fR0fn2ZfvPf/7F8Ha4="
    ),
 ]


@pytest.mark.parametrize("expiry,digest,compressed", NATIVE_QUOTES)
def test_saved_native_quote_bytes(expiry, digest, compressed):
    """Real recorded IO/HO/MO packets satisfy the parser without the SDK's zip join."""
    import zlib

    raw = zlib.decompress(base64.b64decode(compressed))
    assert hashlib.sha256(raw).hexdigest() == digest
    payload = json.loads(raw)
    rows = source.parse_quotes(raw, expiry, STAMP)
    data = payload["result"]["data"]
    assert len(rows) == len(data["up"]) + len(data["down"])
    assert {r["symbol"] for r in rows} == {r[-1] for side in ("up", "down") for r in data[side]}
    assert all(row["source_sha256"] == digest for row in rows)
    assert base64.b64decode(rows[0]["raw_evidence"]["content_base64"]) == raw
    query = AkshareCffexOptionQuotesFetcher.transform_query({"expiry_symbol": expiry})
    typed = AkshareCffexOptionQuotesFetcher.transform_data(query, rows)
    assert typed[0].symbol == data["up"][0][-1]
    assert typed[0].source_time is None and typed[0].contract_unit is None

# Verbatim option_suffix fragments from the same saved native discovery pages.
NATIVE_DISCOVERY = [
    ("IO", """<div class="js-select" id="option_suffix" data-value="io2610">
                            <span class="selected">io2610</span>
                            <ul style="display: none">
                                                                    <li class="active"  data-value="io2610">io2610</li>
                                                                    <li  data-value="io2611">io2611</li>
                                                                    <li  data-value="io2612">io2612</li>
                                                                    <li  data-value="io2703">io2703</li>
                                                                    <li  data-value="io2709">io2709</li>
                                                                    <li  data-value="io2706">io2706</li>
                                                            </ul>
                        </div>"""),
    ("HO", """<div class="js-select" id="option_suffix" data-value="ho2610">
                            <span class="selected">ho2610</span>
                            <ul style="display: none">
                                                                    <li class="active"  data-value="ho2610">ho2610</li>
                                                                    <li  data-value="ho2612">ho2612</li>
                                                                    <li  data-value="ho2611">ho2611</li>
                                                                    <li  data-value="ho2703">ho2703</li>
                                                                    <li  data-value="ho2709">ho2709</li>
                                                                    <li  data-value="ho2706">ho2706</li>
                                                            </ul>
                        </div>"""),
    ("MO", """<div class="js-select" id="option_suffix" data-value="mo2610">
                            <span class="selected">mo2610</span>
                            <ul style="display: none">
                                                                    <li class="active"  data-value="mo2610">mo2610</li>
                                                                    <li  data-value="mo2612">mo2612</li>
                                                                    <li  data-value="mo2611">mo2611</li>
                                                                    <li  data-value="mo2703">mo2703</li>
                                                                    <li  data-value="mo2706">mo2706</li>
                                                                    <li  data-value="mo2709">mo2709</li>
                                                            </ul>
                        </div>"""),
 ]


@pytest.mark.parametrize("product,html", NATIVE_DISCOVERY)
def test_saved_native_discovery_fragments(product, html):
    """Preserve the displayed list, including non-sorted quarter-expiry order."""
    raw = html.encode("ascii")
    rows = source.parse_expiries(raw, product, STAMP)
    assert len(rows) == 6
    assert {row["expiry_symbol"] for row in rows} == {product.lower() + month for month in (
        "2610", "2611", "2612", "2703", "2706", "2709",
    )}
    assert rows[0]["expiry_symbol"] == product.lower() + "2610"
    assert base64.b64decode(rows[0]["raw_evidence"]["content_base64"]) == raw


def test_expiry_transport_params_and_cleanup(monkeypatch):
    """Discovery passes an empty mapping, not None, to the shared requests helper."""
    response = FakeResponse(synthetic_html())
    calls = []

    def request(url, **kwargs):
        calls.append((url, kwargs))
        assert kwargs["params"] == {}  # get_requests_session updates Session.params.
        return response

    monkeypatch.setattr(source, "make_request", request)
    assert len(source.fetch_expiries("IO")) == 2
    assert calls[0][0] == source.DISCOVERY_URLS["IO"]
    assert calls[0][1]["timeout"] == 30
    assert response.closed

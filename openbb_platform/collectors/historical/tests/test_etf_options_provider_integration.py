"""Exercise installed option registrations with synthetic source responses only."""

import hashlib
import json
import runpy
from pathlib import Path

import pytest
from openbb_akshare.models import etf_options
from openbb_akshare.utils import cffex_option_quotes, etf_option_quotes
from openbb_collector_core.providers import ProviderDispatcher

from .test_etf_options import TransportResponse, quote_body, sse_master_body


def dispatcher(model):
    return ProviderDispatcher(
        {"provider": "akshare", "model": model, "credentials_env": {}}
    )


def test_etf_models_dispatch_through_installed_provider(monkeypatch):
    """Core dispatch preserves master terms, quote clocks and explicit missing symbols."""
    calls = []

    def master(url, **kwargs):
        calls.append(("master", url, kwargs))
        return TransportResponse(sse_master_body())

    def quotes(url, **kwargs):
        calls.append(("quotes", url, kwargs))
        return quote_body("10000001")

    monkeypatch.setattr(etf_options, "make_contract_request", master)
    monkeypatch.setattr(etf_option_quotes, "_read_quote_body", quotes)
    contracts = dispatcher("AkshareEtfOptionContracts").fetch(
        {"exchange": "SSE", "source": "official", "underlyings": "510050"}
    )
    quotes = dispatcher("AkshareEtfOptionQuotes").fetch(
        {"symbols": "10000001,10000002"}
    )

    assert contracts.rows[0]["contract_unit"] == 10000
    assert contracts.rows[0]["source_date"] == "2026-10-14"
    assert contracts.rows[0]["source_row"]["SECURITY_ID"] == "10000001"
    assert [row["symbol"] for row in quotes.rows] == ["10000001", "10000002"]
    assert quotes.rows[0]["source_time"] == "2026-10-14T15:00:00+08:00"
    assert quotes.rows[0]["bid"] == 0.1
    assert quotes.rows[1]["quote_error"] == "missing_quote"
    assert quotes.rows[1]["bid"] is None
    assert contracts.metadata["model"] == "AkshareEtfOptionContracts"
    assert (
        quotes.metadata["openbb_akshare.models.etf_options"]["sha256"]
        == hashlib.sha256(Path(etf_options.__file__).read_bytes()).hexdigest()
    )
    assert [call[0] for call in calls] == ["master", "quotes"]


def test_cffex_option_models_dispatch_without_changing_daily_collector(monkeypatch):
    """Both new models retain their native identity and unknown event clock/unit."""
    tests = Path(__file__).resolve().parents[3] / "providers/akshare/tests"
    fixtures = runpy.run_path(str(tests / "test_cffex_option_quotes.py"))
    payload = fixtures["synthetic_payload"]()
    html = fixtures["synthetic_html"]()
    calls = []

    class StreamingResponse(TransportResponse):
        def iter_content(self, chunk_size):
            yield self.content

        def close(self):
            pass

    def request(url, **kwargs):
        calls.append((url, kwargs))
        body = (
            json.dumps(payload).encode()
            if url == cffex_option_quotes.QUOTE_URL
            else html
        )
        return StreamingResponse(body)

    monkeypatch.setattr(cffex_option_quotes, "make_request", request)
    expiries = dispatcher("AkshareCffexOptionExpiries").fetch({"product": "IO"})
    quotes = dispatcher("AkshareCffexOptionQuotes").fetch({"expiry_symbol": "io2610"})

    assert [row["expiry_symbol"] for row in expiries.rows] == ["io2610", "io2611"]
    assert {row["symbol"] for row in quotes.rows} == {
        "io2610C3800",
        "io2610C3900",
        "io2610P3800",
        "io2610P3900",
    }
    assert all(row["source_time"] is None for row in quotes.rows)
    assert all(row["contract_unit"] is None for row in quotes.rows)
    assert calls[1][1]["params"] == {
        "type": "futures",
        "product": "io",
        "exchange": "cffex",
        "pinzhong": "io2610",
    }
    assert len(calls) == 2


@pytest.mark.parametrize(
    "model,params",
    [
        ("AkshareEtfOptionContracts", {"exchange": "SSE", "underlyings": "510050 "}),
        ("AkshareEtfOptionQuotes", {"symbols": "10000001,10000001"}),
        ("AkshareCffexOptionExpiries", {"product": "IF"}),
        ("AkshareCffexOptionQuotes", {"expiry_symbol": "IO2610"}),
    ],
)
def test_dispatch_validation_precedes_transport(monkeypatch, model, params):
    def forbidden(*_args, **_kwargs):
        pytest.fail("invalid option queries must not reach source transport")

    monkeypatch.setattr(etf_options, "make_contract_request", forbidden)
    monkeypatch.setattr(etf_option_quotes, "_read_quote_body", forbidden)
    monkeypatch.setattr(cffex_option_quotes, "make_request", forbidden)
    with pytest.raises(Exception, match="(validation|Expected|Invalid)"):
        dispatcher(model).fetch(params)

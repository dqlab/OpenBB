"""Offline synthetic fixtures: no live provider acceptance is claimed."""

from datetime import datetime
from threading import Event

import pytest
from openbb_collector_core.models import ProviderFailure, Response

from dq_historical_market_data_collector.etf_options.config import Config, TradingCalendar
from dq_historical_market_data_collector.etf_options.quality import DataError, contracts, quotes
from dq_historical_market_data_collector.etf_options.runtime import Collector
from dq_historical_market_data_collector.etf_options.storage import Journal, LockBusy

NOW = datetime.fromisoformat("2026-10-14T15:20:00+08:00")
DAY = NOW.date()

# The committed path refuses a capture whose numbers nobody stated the units of
# (``units_undetermined``), so the fixture declares them once for the deployment --
# the same shape a shipped YAML must carry.  No assertion below changed: what
# changed is that a fixture without a unit declaration no longer commits.
UNITS = {
    "price_unit": "CNY_per_contract_share",
    "size_unit": "contract",
    "volume_unit": "contract",
    "turnover_unit": "CNY",
}


def contract(symbol="10000001", **extra):
    result = dict(
        symbol=symbol,
        exchange="SSE",
        underlying="510050",
        option_type="call",
        strike=2.5,
        expiration="2026-10-28",
        contract_unit=None,
        contract_trade_code=None,
        metadata_source="sina",
        source_date=None,
        source_row={},
    )
    result.update(extra)
    return result


def quote(symbol="10000001", **extra):
    result = dict(
        symbol=symbol,
        source_time="2026-10-14T15:00:00+08:00",
        bid=0.1,
        ask=0.2,
        bid_size=10,
        ask_size=20,
        last=0.15,
        volume=100,
        turnover=1000,
        open_interest=300,
        underlying="510050",
        strike=2.5,
        contract_name="synthetic",
        source_status="ok",
        raw_fields={},
        quote_error=None,
    )
    result.update(extra)
    return result


class FakeClient:
    def __init__(self, metadata=None, baskets=None, stop=None):
        self.metadata = metadata or [contract()]
        self.baskets = list(baskets or [[quote()]])
        self.calls = []
        self.stop = stop

    def fetch(self, source_id, source, request):
        self.calls.append((source.model, request))
        if source.model == "AkshareEtfOptionContracts":
            return Response(self.metadata)
        if source.model == "AkshareToolTradeDateHistSina":
            return Response(
                [
                    {"trade_date": "2026-10-13"},
                    {"trade_date": "2026-10-14"},
                    {"trade_date": "2026-10-15"},
                ]
            )
        rows = self.baskets.pop(0)
        if isinstance(rows, BaseException):
            raise rows
        if self.stop:
            self.stop.set()
        return Response(rows)

    def close(self):
        pass


def config(tmp_path, **extra):
    values = dict(
        root=tmp_path / "state",
        calendar_file=tmp_path / "calendar.json",
        exchanges={"SSE": {"source": "sina"}},
        limits={"retries": 0, "retry_delay_seconds": 0.0, "min_interval_seconds": 0.1},
        units=dict(UNITS),
    )
    values.update(extra)
    return Config(**values)


def calendar():
    return TradingCalendar(
        coverage_start="2026-10-13",
        coverage_end="2026-10-15",
        trading_dates=["2026-10-13", "2026-10-14", "2026-10-15"],
        source="synthetic offline",
    )


def run(cfg, journal, client, **extra):
    return Collector(cfg, journal, calendar(), client, clock=lambda: NOW, **extra).collect()


def test_unknown_multiplier_and_diagnostic_coverage():
    metadata = contracts([contract()], "SSE", "sina", DAY)
    assert metadata[0]["contract_unit"] is None
    assert "unknown_contract_unit" in metadata[0]["quality_flags"]
    rows = quotes([quote(bid=0, ask=None, last=0)], metadata, DAY, NOW, NOW)
    assert len(rows) == 1 and not rows[0]["pricing_eligible"]
    assert rows[0]["bid"] == 0 and rows[0]["ask"] is None
    assert "zero_bid" in rows[0]["quality_flags"]


@pytest.mark.parametrize(
    "extra,code",
    [
        ({"source_time": "2026-10-13T15:00:00+08:00"}, "quote_date_mismatch"),
        ({"source_time": None}, "missing_or_invalid_quote_time"),
        ({"source_time": "2026-10-14T25:00:00"}, "missing_or_invalid_quote_time"),
        ({"source_time": "2026-10-14"}, "missing_or_invalid_quote_time"),
        ({"source_time": "2026-10-15T00:00:00+08:00"}, "quote_date_mismatch"),
        ({"source_time": "2026-10-14T15:21:00+08:00"}, "quote_future_time"),
        ({"underlying": "510300"}, "underlying_mismatch"),
        ({"strike": 2.6}, "strike_mismatch"),
        ({"last": float("inf")}, "nonfinite_numeric"),
        ({"quote_error": "empty"}, "quote_error"),
    ],
)
def test_quote_quality_fail_closed(extra, code):
    with pytest.raises(DataError, match=code):
        quotes([quote(**extra)], contracts([contract()], "SSE", "sina", DAY), DAY, NOW, NOW)


@pytest.mark.parametrize(
    "stamp,flags",
    [
        ("2026-10-14T14:56:00+08:00", {"preclose_quote", "stale_quote"}),
        ("2026-10-14T00:00:00+08:00", {"unknown_quote_time"}),
    ],
)
def test_same_day_early_quotes_retained_not_pricing_eligible(stamp, flags):
    metadata = contracts([contract(contract_unit=10000)], "SSE", "sina", DAY)
    source_row = quote(source_time=stamp, raw_fields={"quote_time": stamp})
    row = quotes([source_row], metadata, DAY, NOW, NOW)[0]
    assert row["source_time"] == stamp and row["raw_fields"] == source_row["raw_fields"]
    assert flags.issubset(row["quality_flags"])
    assert not row["pricing_eligible"]
    assert source_row == quote(source_time=stamp, raw_fields={"quote_time": stamp})
    # Checkpoint/final-universe validation must not duplicate flags or promote stale rows.
    assert quotes([row], metadata, DAY, NOW, NOW) == [row]


@pytest.mark.parametrize("unit,eligible", [(10000, True), (None, False)])
def test_same_day_close_pricing_requires_known_unit(unit, eligible):
    metadata = contracts([contract(contract_unit=unit)], "SSE", "sina", DAY)
    row = quotes([quote()], metadata, DAY, NOW, NOW)[0]
    assert row["pricing_eligible"] is eligible
    assert row["source_time"] == "2026-10-14T15:00:00+08:00"
    assert not {"preclose_quote", "stale_quote", "unknown_quote_time"}.intersection(
        row["quality_flags"]
    )
    if unit is None:
        assert "unknown_contract_unit" in row["quality_flags"]


@pytest.mark.parametrize(
    "last,flag,eligible",
    [(None, "missing_last", True), (0, "zero_last", True), (-0.1, "negative_last", False)],
)
def test_bid_ask_pricing_does_not_require_a_last_trade(last, flag, eligible):
    metadata = contracts([contract(contract_unit=10000)], "SSE", "sina", DAY)
    row = quotes([quote(last=last)], metadata, DAY, NOW, NOW)[0]
    assert row["last"] == last and flag in row["quality_flags"]
    assert row["pricing_eligible"] is eligible


@pytest.mark.parametrize(
    "extra",
    [{"bid": None}, {"ask": 0}, {"bid_size": None}, {"ask_size": 0}, {"bid": 0.3}],
)
def test_known_unit_does_not_bypass_bid_ask_pricing_gates(extra):
    metadata = contracts([contract(contract_unit=10000)], "SSE", "sina", DAY)
    assert not quotes([quote(**extra)], metadata, DAY, NOW, NOW)[0]["pricing_eligible"]


@pytest.mark.parametrize(
    "extra,code",
    [
        ({"underlying": "510300"}, "underlying_mismatch"),
        ({"strike": 2.6}, "strike_mismatch"),
        ({"last": float("nan")}, "nonfinite_numeric"),
    ],
)
def test_preclose_diagnostic_does_not_bypass_hard_validation(extra, code):
    metadata = contracts([contract(contract_unit=10000)], "SSE", "sina", DAY)
    with pytest.raises(DataError, match=code):
        quotes([quote(source_time="2026-10-14T14:56:00", **extra)], metadata, DAY, NOW, NOW)


@pytest.mark.parametrize(
    "extra,code",
    [
        ({"metadata_source": "official"}, "metadata_source_mismatch"),
        ({"source_date": "2026-10-13"}, "metadata_date_mismatch"),
        ({"symbol": "1000001"}, "invalid_identity"),
    ],
)
def test_metadata_identity_and_source_date_remain_fail_closed(extra, code):
    with pytest.raises(DataError, match=code):
        contracts([contract(**extra)], "SSE", "sina", DAY)


def test_crossed_is_retained_diagnostic():
    row = quotes([quote(bid=0.3)], contracts([contract()], "SSE", "sina", DAY), DAY, NOW, NOW)[0]
    assert not row["pricing_eligible"] and "crossed_market" in row["quality_flags"]


def test_idempotent_acquisitions_and_revisions(tmp_path):
    cfg = config(tmp_path)
    with Journal(cfg.root) as journal:
        first = run(cfg, journal, FakeClient())
        second = run(cfg, journal, FakeClient())
        assert first["status"] == second["status"] == "complete"
        assert journal.db.execute("SELECT count(*) FROM revisions").fetchone()[0] == 1
        assert journal.db.execute("SELECT count(*) FROM current_quotes").fetchone()[0] == 1
        assert journal.db.execute("SELECT count(*) FROM captures").fetchone()[0] == 2
        assert journal.db.execute("SELECT count(*) FROM attempts").fetchone()[0] == 4


def test_partial_retains_good_snapshot_and_raw(tmp_path):
    cfg = config(tmp_path)
    with Journal(cfg.root) as journal:
        assert run(cfg, journal, FakeClient())["status"] == "complete"
        result = run(cfg, journal, FakeClient(baskets=[[quote(source_time="2026-10-13T15:00:00")]]))
        assert result["status"] == "failed"
        assert journal.db.execute("SELECT count(*) FROM current_quotes").fetchone()[0] == 1
        assert journal.db.execute("SELECT count(*) FROM revisions").fetchone()[0] == 1
        assert (
            journal.db.execute(
                "SELECT count(*) FROM attempts WHERE raw_path IS NOT NULL"
            ).fetchone()[0]
            == 4
        )


def test_smaller_universe_never_overwrites(tmp_path):
    cfg = config(tmp_path)
    with Journal(cfg.root) as journal:
        first = FakeClient([contract(), contract("10000002")], [[quote(), quote("10000002")]])
        assert run(cfg, journal, first)["status"] == "complete"
        result = run(cfg, journal, FakeClient())
        assert result["status"] == "failed" and "universe_shrink" in str(result["errors"])
        assert journal.db.execute("SELECT count(*) FROM current_quotes").fetchone()[0] == 2


def test_missing_batch_is_nonzero_and_retry_bounded(tmp_path):
    cfg = config(tmp_path, limits={"retries": 1, "retry_delay_seconds": 0.0})
    with Journal(cfg.root) as journal:
        client = FakeClient(baskets=[[], []])
        result = run(cfg, journal, client)
        assert result["status"] == "failed" and result["provider_calls"] == 3
        assert journal.db.execute("SELECT count(*) FROM current_quotes").fetchone()[0] == 0


def test_timeout_retry_then_success(tmp_path):
    cfg = config(tmp_path, limits={"retries": 1, "retry_delay_seconds": 0.0})
    with Journal(cfg.root) as journal:
        result = run(cfg, journal, FakeClient(baskets=[ProviderFailure("timeout"), [quote()]]))
        assert result["status"] == "complete" and result["provider_calls"] == 3
        assert (
            journal.db.execute("SELECT count(*) FROM attempts WHERE status='failed'").fetchone()[0]
            == 1
        )


def test_lock_and_recovery(tmp_path):
    cfg = config(tmp_path)
    with Journal(cfg.root) as journal:
        with pytest.raises(LockBusy):
            with Journal(cfg.root):
                pass
        journal.start_run({"status": "running", "command": "collect"})
    with Journal(cfg.root) as journal:
        assert journal.db.execute("SELECT status FROM runs").fetchone()[0] == "interrupted"


def test_cancel_and_no_empty_success(tmp_path):
    cfg = config(tmp_path)
    with Journal(cfg.root) as journal:
        stop = Event()
        result = run(cfg, journal, FakeClient(stop=stop), stop=stop)
        assert result["status"] == "interrupted"
        assert journal.db.execute("SELECT count(*) FROM current_quotes").fetchone()[0] == 0


def test_no_historical_live_backfill(tmp_path):
    cfg = config(tmp_path)
    with Journal(cfg.root) as journal:
        collector = Collector(cfg, journal, calendar(), FakeClient(), clock=lambda: NOW)
        with pytest.raises(ValueError, match="live_source_date_only"):
            collector.collect(DAY.replace(day=13))


def test_fail_closed_calendar_and_current_cutoff(tmp_path):
    cfg = config(tmp_path)
    with Journal(cfg.root) as journal:
        unknown = TradingCalendar(
            coverage_start="2026-10-13",
            coverage_end="2026-10-13",
            trading_dates=["2026-10-13"],
            source="synthetic",
        )
        with pytest.raises(ValueError, match="calendar_coverage"):
            Collector(cfg, journal, unknown, FakeClient(), clock=lambda: NOW).collect()
        with pytest.raises(ValueError, match="capture_before_cutoff"):
            Collector(
                cfg, journal, calendar(), FakeClient(), clock=lambda: NOW.replace(hour=15, minute=9)
            ).collect()


# ---------------------------------------------------------------------------
# Task 5B-4 -- raw-only split: literal transport capture, bound delivery
#
# The tests above are the pre-existing suite for the business predicates and the
# legacy report/checkpoint contract.  They are unchanged on purpose: the rules did
# not change, they moved.  The tests below exercise the boundary that moved them
# out of acquisition.  Nothing here weakens an older assertion.
# ---------------------------------------------------------------------------

import contextlib  # noqa: E402
import hashlib  # noqa: E402
import json  # noqa: E402

_PROVIDER_FIELDS = []


def provider_fields():
    if not _PROVIDER_FIELDS:
        from openbb_akshare.models.etf_options import FIELDS

        _PROVIDER_FIELDS.extend(FIELDS)
    return _PROVIDER_FIELDS


def quote_line(symbol="10000001", **overrides):
    """One 51-field Sina payload line, built from the provider's own field names."""
    values = {name: "0" for name in provider_fields()}
    values.update(
        {
            "买量": "10",
            "买价": "0.1",
            "最新价": "0.15",
            "卖价": "0.2",
            "卖量": "20",
            "持仓量": "300",
            "行权价": "2.5",
            "行情时间": "2026-10-14 15:00:00",
            "主力合约标识": "-",
            "状态码": "0",
            "标的证券类型": "EBS",
            "标的股票": "510050",
            "期权合约简称": symbol,
            "成交量": "100",
            "成交额": "1000",
        }
    )
    values.update(overrides)
    return ",".join(values[name] for name in provider_fields()) + ",A,0,C,2026-10-28,0,0,0,0"


def quote_body(*symbols, **overrides):
    lines = [
        f'var hq_str_CON_OP_{symbol}="{quote_line(symbol, **overrides)}";' for symbol in symbols
    ]
    return ("\n".join(lines) + "\n").encode("gb18030")


def sse_master_body(
    *, symbol="10000001", strike="2.5", expiration="2026-10-28", trade_date="2026-10-14"
):
    return json.dumps(
        {
            "result": [
                {
                    "SECURITY_ID": symbol,
                    "SECURITYNAMEBYID": "50ETF(510050)",
                    "CALL_OR_PUT": "C",
                    "EXERCISE_PRICE": strike,
                    "EXPIRE_DATE": expiration,
                    "CONTRACT_UNIT": "10000",
                    "CONTRACT_ID": "510050C2610M0250",
                    "TRADE_DATE": trade_date,
                }
            ]
        }
    ).encode()


class TransportResponse:
    def __init__(self, body, status=200, media_type="application/octet-stream"):
        self.content = body
        self.status_code = status
        self.headers = {"Content-Type": media_type}

    def raise_for_status(self):
        if not 200 <= self.status_code < 300:
            raise ValueError("http_error")


class TransportStand:
    """Stands in for the provider's own transport helper and records every call."""

    def __init__(self, master, quotes, status=200):
        self.master = master
        self.quotes = quotes if isinstance(quotes, list) else [quotes]
        self.status = status
        self.calls = []

    def __call__(self, url, **kwargs):
        self.calls.append((url, kwargs.get("params")))
        if "sinajs" in url:
            body = self.quotes.pop(0) if len(self.quotes) > 1 else self.quotes[0]
            return TransportResponse(body, self.status, "text/plain; charset=gb18030")
        return TransportResponse(self.master, self.status, "application/json")


def official_config(tmp_path, **extra):
    return config(
        tmp_path,
        exchanges={"SSE": {"source": "official", "underlyings": ["510050"]}},
        **extra,
    )


@contextlib.contextmanager
def literal_lane(tmp_path, monkeypatch, stand, **extra):
    """A fully literal lane: one official master document and one quote batch."""
    import openbb_akshare.models.etf_options as contracts_module
    import openbb_akshare.models.etf_options as quotes_module

    monkeypatch.setattr(contracts_module, "make_contract_request", stand)
    monkeypatch.setattr(quotes_module, "make_quote_request", stand)
    cfg = official_config(tmp_path, **extra)
    with Journal(cfg.root) as journal:
        report = Collector(cfg, journal, calendar(), clock=lambda: NOW).collect()
        yield cfg, journal, report


def capture_bodies(root):
    return sorted(path for path in (root / "evidence").rglob("body.bin"))


def documents(root, kind=None):
    found = []
    for path in sorted((root / "raw").rglob("*.json")):
        try:
            value = json.loads(path.read_text())
        except ValueError:
            continue
        if isinstance(value, dict) and value.get("version") == "etf-raw-delivery-v3":
            if kind is None or value.get("kind") == kind:
                found.append((path, value))
    return found


def test_raw_literal_quote_bytes_are_captured_before_any_parser(tmp_path, monkeypatch):
    from dq_historical_market_data_collector.etf_options.runtime import read_delivery

    body = quote_body("10000001")
    stand = TransportStand(sse_master_body(), body)
    with literal_lane(tmp_path, monkeypatch, stand) as (cfg, journal, report):
        assert report["status"] == "complete" and report["row_count"] == 1
        # The stored bytes are the transport bytes, byte for byte.
        assert body in {path.read_bytes() for path in capture_bodies(cfg.root)}
        ((path, document),) = documents(cfg.root, "quote-batch")
        assert document["capture"]["evidence_kind"] == "raw-bytes"
        assert document["capture"]["transformation"] is None
        assert document["capture"]["sha256"] == hashlib.sha256(body).hexdigest()
        assert document["capture"]["media_type"].startswith("text/plain")
        # A delivery carries no HTTP status: the capture has no slot for one, so a
        # status here would be an unverifiable claim.  The status is a run
        # observation, recorded on the attempt row instead.
        assert "http_status" not in document
        assert (
            journal.db.execute(
                "SELECT http_status FROM attempts WHERE stage='quote-batch'"
            ).fetchone()[0]
            == 200
        )
        # The trade date of a quote row is the requested date, and says so.
        assert document["provenance"]["date_source"] == "request_parameter"
        assert document["provenance"]["date_column"] is None
        assert document["provenance"]["unit_source"] == "configuration"
        # The row is not the bytes: the reading decoded, split, labelled,
        # converted text to floats and parsed the source time.  The delivery says
        # so, and the reader re-derives the same list.
        assert document["provenance"]["transforms"] == [
            "gb18030_decode",
            "assignment_extracted_per_symbol",
            "csv_split_on_comma",
            "field_labels_applied_43",
            "numeric_text_to_float",
            "source_time_parsed_to_iso8601",
            "underlying_and_status_extracted",
            "option_type_expiration_from_appended_tail",
        ]
        assert document["provenance"]["unit_labels"] == UNITS
        assert document["provenance"]["parse_error"] is None
        assert [row["source_row"] for row in document["row_provenance"]] == [0]
        assert (
            document["row_sha256"]
            == [document["row_provenance"][0]["source_sha256"]]
            == [
                hashlib.sha256(
                    json.dumps(
                        document["rows"][0],
                        sort_keys=True,
                        separators=(",", ":"),
                        ensure_ascii=False,
                    ).encode()
                ).hexdigest()
            ]
        )
        row = document["rows"][0]
        assert row["bid"] == 0.1 and row["ask"] == 0.2 and row["strike"] == 2.5
        assert row["source_time"] == "2026-10-14T15:00:00+08:00"
        # And the reader re-derives them from the capture, not from the envelope.
        again = read_delivery(path, cfg.root / "evidence")
        assert again["rows"] == document["rows"] and again["rederived"] is True
        assert again["capture"]["sha256"] == document["capture"]["sha256"]


def test_raw_master_document_records_its_own_date_source(tmp_path, monkeypatch):
    stand = TransportStand(sse_master_body(), quote_body("10000001"))
    with literal_lane(tmp_path, monkeypatch, stand) as (cfg, journal, _report):
        ((_, master),) = documents(cfg.root, "sse-official-master")
        assert master["provenance"]["date_source"] == "row"
        assert master["provenance"]["date_column"] == "source_date"
        assert master["rows"][0]["source_date"] == DAY.isoformat()
        assert master["rows"][0]["metadata_source"] == "official"
        # The transport URLs are the provider's own, spelled the same way.
        assert stand.calls[0][0] == "https://query.sse.com.cn/commonQuery.do"
        assert stand.calls[0][1]["sqlId"] == "SSE_ZQPZ_YSP_GGQQZSXT_XXPL_DRHY_SEARCH_L"
        assert stand.calls[1][0].startswith("https://hq.sinajs.cn/list=CON_OP_10000001")


def test_raw_quote_url_equals_the_provider_url(monkeypatch):
    """The lane's URL template is proven equal to the provider's own."""
    import openbb_akshare.models.etf_options as provider
    import openbb_akshare.utils.etf_option_quotes as transport

    from dq_historical_market_data_collector.etf_options import source as lane_source

    recorded = {}

    def stand(url):
        recorded["url"] = url
        return quote_body("10000001")

    monkeypatch.setattr(transport, "_read_quote_body", stand)
    provider.fetch_quotes("10000001")
    assert recorded["url"] == lane_source.quote_batch_url(["10000001"])


MALFORMED_BODIES = [
    pytest.param(b"", id="empty"),
    pytest.param(b'var hq_str_CON_OP_10000001="0,0,0"\n', id="truncated_width"),
    pytest.param(b"<html>blocked</html>\n", id="unrecognized"),
    pytest.param(b'var hq_str_CON_OP_99999999="x";\n', id="unexpected_symbol"),
    pytest.param(b'var hq_str_CON_OP_10000001="\xff\xfe\x80";\n', id="undecodable_or_broken"),
]


@pytest.mark.parametrize("body", MALFORMED_BODIES)
def test_raw_malformed_bodies_are_captured_byte_identical_and_rejected(tmp_path, monkeypatch, body):
    stand = TransportStand(sse_master_body(), body)
    with literal_lane(tmp_path, monkeypatch, stand) as (cfg, journal, report):
        assert report["status"] == "failed"
        assert body in {path.read_bytes() for path in capture_bodies(cfg.root)}, (
            "the malformed body must survive byte-identically"
        )
        ((_, document),) = documents(cfg.root, "quote-batch")
        typed = document["provenance"]["parse_error"]
        if typed is not None:
            # A structural refusal: no rows, and the reader agrees on the code.
            assert document["rows"] == [] and document["row_sha256"] == []
        else:
            # A structurally readable body the business rules refuse: the raw
            # diagnostic row is delivered, with its own error field intact.
            assert document["rows"] and document["rows"][0].get("quote_error")
        # Captured, delivered, refused -- and nothing committed.
        assert journal.db.execute("SELECT count(*) FROM current_quotes").fetchone()[0] == 0
        codes = {error.get("code") for error in report["errors"]}
        assert codes - {None}, "a typed refusal must be reported"
        assert "complete" not in {report["status"]}


def test_raw_capture_is_written_before_any_business_predicate(tmp_path, monkeypatch):
    from dq_historical_market_data_collector.etf_options import runtime as lane
    from dq_historical_market_data_collector.etf_options.quality import DataError

    def refuse(*_args, **_kwargs):
        raise DataError("predicate_ran_early")

    monkeypatch.setattr(lane, "contracts", refuse)
    stand = TransportStand(sse_master_body(), quote_body("10000001"))
    with literal_lane(tmp_path, monkeypatch, stand) as (cfg, journal, report):
        assert report["status"] == "failed"
        assert any(error.get("code") == "predicate_ran_early" for error in report["errors"])
        # The master's bytes and their delivery exist although the predicate threw.
        assert capture_bodies(cfg.root), "the body was stored before the predicate refused"
        ((_, master),) = documents(cfg.root, "sse-official-master")
        assert master["rows"][0]["symbol"] == "10000001"


def test_raw_delivery_refuses_rows_the_capture_does_not_support(tmp_path, monkeypatch):
    from dq_historical_market_data_collector.etf_options.runtime import (
        EvidenceStateError,
        read_delivery,
    )
    from dq_historical_market_data_collector.etf_options.storage import digest

    stand = TransportStand(sse_master_body(), quote_body("10000001"))
    with literal_lane(tmp_path, monkeypatch, stand) as (cfg, journal, report):
        assert report["status"] == "complete"
        ((original, clean),) = documents(cfg.root, "quote-batch")

        def mutate(name, change):
            path = original.parent / f"{name}.json"
            payload = json.loads(json.dumps(clean))
            change(payload)
            path.write_text(json.dumps(payload, sort_keys=True))
            return path

        def reseal(payload):
            payload["row_sha256"] = [digest(row) for row in payload["rows"]]
            for item, sha in zip(payload["row_provenance"], payload["row_sha256"], strict=True):
                item["source_sha256"] = sha

        def close_only(payload):
            payload["rows"][0]["bid"] = 999.0

        def symbol_only(payload):
            payload["rows"][0]["symbol"] = "10000002"

        cases = [
            ("row_digest", mutate("m1", close_only), "delivery_row_digest"),
            ("symbol_digest", mutate("m2", symbol_only), "delivery_row_digest"),
            (
                "resealed_row",
                mutate("m3", lambda p: (close_only(p), reseal(p))),
                "delivery_capture_mismatch",
            ),
            (
                "resealed_symbol",
                mutate("m4", lambda p: (symbol_only(p), reseal(p))),
                "delivery_capture_mismatch",
            ),
            (
                "forged_sha",
                mutate("m5", lambda p: p["capture"].__setitem__("sha256", "0" * 64)),
                "delivery_capture_mismatch",
            ),
        ]
        for name, path, code in cases:
            before = path.read_bytes()
            with pytest.raises(EvidenceStateError) as failure:
                read_delivery(path, cfg.root / "evidence")
            assert failure.value.code == code, (name, failure.value.code)
            assert path.read_bytes() == before, "a refused read must not rewrite the delivery"
        with pytest.raises(EvidenceStateError, match="delivery_unbound"):
            read_delivery(original)
        # The sealed row is what the capture says, even when the envelope lies.
        resealed = json.loads((original.parent / "m3.json").read_text())
        assert resealed["rows"][0]["bid"] == 999.0
        assert read_delivery(original, cfg.root / "evidence")["rows"][0]["bid"] == 0.1


def test_raw_units_undetermined_is_refused_not_labelled(tmp_path, monkeypatch):
    """A scale-consistent body with no declared unit never commits."""
    body = quote_body("10000001", **{"买价": "1.0", "卖价": "2.0", "最新价": "1.5"})
    stand = TransportStand(sse_master_body(), body)
    with literal_lane(tmp_path, monkeypatch, stand, units={}) as (cfg, journal, report):
        assert report["status"] == "failed"
        assert {"code": "units_undetermined"} in report["errors"]
        assert journal.db.execute("SELECT count(*) FROM current_quotes").fetchone()[0] == 0
        assert journal.db.execute("SELECT count(*) FROM revisions").fetchone()[0] == 0
        # The evidence is still there, and it is honestly labelled.
        ((_, document),) = documents(cfg.root, "quote-batch")
        assert document["provenance"]["unit_source"] == "absent"
        assert document["rows"][0]["bid"] == 1.0
        assert "provider_defined" not in json.dumps(document)


def test_raw_oversize_response_is_refused_before_storage(tmp_path, monkeypatch):
    # The body itself is over the declared bound, so it never becomes evidence.
    oversize = b"x" * 4000 + b"\n"
    stand = TransportStand(sse_master_body(), oversize)
    with literal_lane(
        tmp_path, monkeypatch, stand, limits={"retries": 0, "max_response_bytes": 1024}
    ) as (cfg, journal, report):
        assert report["status"] == "failed"
        assert any("response_limit" in json.dumps(error) for error in report["errors"])
        # The oversize quote body was never stored; the small master body was.
        assert len(capture_bodies(cfg.root)) == 1
        assert documents(cfg.root, "quote-batch") == []


def test_raw_replay_of_a_captured_generation_calls_no_source(tmp_path, monkeypatch):
    from dq_historical_market_data_collector.etf_options import runtime as lane

    stand = TransportStand(sse_master_body(), quote_body("10000001"))
    with literal_lane(tmp_path, monkeypatch, stand) as (cfg, journal, report):
        assert report["status"] == "complete"
        sent = len(stand.calls)
        collector = lane.Collector(cfg, journal, calendar(), clock=lambda: NOW)
        collector.token = "replay-session"
        entry = collector._entry(DAY, "SSE", "AkshareEtfOptionQuotes", {"symbols": "10000001"}, 0)
        first = lane.acquire(cfg.root / "evidence", [entry], client=stand)
        assert first[0]["source_calls"] == 1
        journal.record_verdict(first[0]["locator"], "accepted", None)
        again = lane.acquire(cfg.root / "evidence", [entry], client=stand)
        assert again[0]["locator"] == first[0]["locator"]
        assert again[0]["source_calls"] == 0
        assert len(stand.calls) == sent + 1


def test_raw_cancellation_leaves_a_durable_veto(tmp_path, monkeypatch):
    from dq_historical_market_data_collector.etf_options import runtime as lane

    stand = TransportStand(sse_master_body(), quote_body("10000001"))
    with literal_lane(tmp_path, monkeypatch, stand) as (cfg, journal, _report):
        collector = lane.Collector(cfg, journal, calendar(), clock=lambda: NOW)
        collector.token = "cancel-session"
        entry = collector._entry(DAY, "SSE", "AkshareEtfOptionQuotes", {"symbols": "10000001"}, 0)
        stop = Event()
        stop.set()
        cancelled = lane.acquire(cfg.root / "evidence", [entry], client=stand, stop=stop)
        assert cancelled[0]["reason"] == "cancelled"
        before = len(stand.calls)
        # The veto is durable: the same generation replays as cancelled, zero calls.
        replay = lane.acquire(cfg.root / "evidence", [entry], client=stand)
        assert replay[0]["reason"] == "cancelled" and replay[0]["source_calls"] == 0
        assert len(stand.calls) == before
        # A new attempt identity is the only way the source may be called again.
        next_entry = collector._entry(
            DAY, "SSE", "AkshareEtfOptionQuotes", {"symbols": "10000001"}, 1
        )
        fresh = lane.acquire(cfg.root / "evidence", [next_entry], client=stand)
        assert fresh[0]["locator"] != replay[0]["locator"]


def test_raw_truncated_missing_and_sha_disagreeing_evidence_is_refused(tmp_path, monkeypatch):
    from dq_historical_market_data_collector.etf_options.runtime import (
        EvidenceStateError,
        read_delivery,
    )

    stand = TransportStand(sse_master_body(), quote_body("10000001"))
    with literal_lane(tmp_path, monkeypatch, stand) as (cfg, journal, _report):
        ((delivery, _document),) = documents(cfg.root, "quote-batch")
        locator = journal.db.execute(
            "SELECT evidence_locator FROM attempts WHERE stage='quote-batch'"
        ).fetchone()[0]
        folder = cfg.root / "evidence" / locator / "responses" / "000000"
        body_path, receipt_path = folder / "body.bin", folder / "receipt.json"
        original_body, original_receipt = body_path.read_bytes(), receipt_path.read_text()

        body_path.write_bytes(original_body[:-5])
        with pytest.raises(EvidenceStateError) as failure:
            read_delivery(delivery, cfg.root / "evidence")
        assert failure.value.code in {"body_checksum", "evidence_state_unreadable"}
        body_path.write_bytes(original_body)

        receipt_path.unlink()
        with pytest.raises(EvidenceStateError) as failure:
            read_delivery(delivery, cfg.root / "evidence")
        assert failure.value.code in {
            "missing_or_nonregular_evidence",
            "evidence_state_unreadable",
        }
        receipt_path.write_text(original_receipt)

        receipt = json.loads(original_receipt)
        receipt["sha256"] = "1" * 64
        receipt_path.write_text(json.dumps(receipt, sort_keys=True, separators=(",", ":")) + "\n")
        with pytest.raises(EvidenceStateError) as failure:
            read_delivery(delivery, cfg.root / "evidence")
        assert failure.value.code in {
            "body_checksum",
            "noncanonical_json",
            "evidence_state_unreadable",
        }
        receipt_path.write_text(original_receipt)
        assert read_delivery(delivery, cfg.root / "evidence")["rows"][0]["bid"] == 0.1


def test_raw_aggregate_master_is_labelled_and_has_no_v2_delivery(tmp_path):
    """The Sina master cannot be one literal body; the capture label says so."""
    from dq_historical_market_data_collector.etf_options.runtime import (
        AGGREGATE_LOSS,
        EvidenceStateError,
        read_aggregate_delivery,
    )

    cfg = config(tmp_path)
    with Journal(cfg.root) as journal:
        report = Collector(cfg, journal, calendar(), FakeClient(), clock=lambda: NOW).collect()
        assert report["status"] == "complete"
        attempt = journal.db.execute(
            "SELECT id FROM attempts WHERE model='AkshareEtfOptionContracts'"
        ).fetchone()[0]
        document = journal.read_attempt_document(attempt)
        assert document.get("version") != "etf-raw-delivery-v3"
        locator = journal.db.execute(
            "SELECT evidence_locator FROM attempts WHERE id=?", (attempt,)
        ).fetchone()[0]
        receipt = json.loads(
            (cfg.root / "evidence" / locator / "responses/000000/receipt.json").read_text()
        )
        assert receipt["evidence_kind"] == "provider-transformed"
        assert receipt["transformation"] == "openbb-process-response-json-v1"
        assert documents(cfg.root, "provider-envelope") == []
        # The archived envelope labels itself: a reader does not have to follow a
        # pointer to learn what it holds, or what is gone.
        label = document["provenance"]
        assert label["evidence_kind"] == "provider-transformed"
        assert label["transformation"] == "openbb-process-response-json-v1"
        assert label["date_source"] == "row"
        assert label["http_status_available"] is False and document["http_status"] is None
        assert label["lost"] == AGGREGATE_LOSS
        assert "per_get_bodies" in label["lost"]
        assert label["capture"] == locator and label["sha256"] == receipt["sha256"]
        assert label["parse_error"] is None
        path = (
            cfg.root
            / journal.db.execute("SELECT raw_path FROM attempts WHERE id=?", (attempt,)).fetchone()[
                0
            ]
        )
        read = read_aggregate_delivery(path, cfg.root / "evidence")
        assert read["rederived"] is True and read["rows"] == document["rows"]
        # The label is not editorial: claiming an observable status, or another
        # evidence kind, is refused typed.
        for name, change, code in [
            (
                "status_claimed",
                lambda p: p["provenance"].__setitem__("http_status_available", True),
                "aggregate_status_claimed",
            ),
            (
                "kind",
                lambda p: p["provenance"].__setitem__("evidence_kind", "raw-bytes"),
                "invalid_delivery",
            ),
            (
                "loss",
                lambda p: p["provenance"]["lost"].pop("per_get_bodies"),
                "invalid_delivery",
            ),
            (
                "rows",
                lambda p: p["rows"][0].__setitem__("strike", 9.99),
                "delivery_capture_mismatch",
            ),
        ]:
            payload = json.loads(json.dumps(document))
            change(payload)
            copy = tmp_path / f"label_{name}.json"
            copy.write_text(json.dumps(payload, sort_keys=True, allow_nan=True))
            before = copy.read_bytes()
            with pytest.raises(EvidenceStateError) as failure:
                read_aggregate_delivery(copy, cfg.root / "evidence")
            assert failure.value.code == code, (name, failure.value.code)
            assert copy.read_bytes() == before


# ---------------------------------------------------------------------------
# acquisition-only task: a literal plan in, document bytes out, nothing else
# ---------------------------------------------------------------------------

ACQUIRE_SOURCE = {
    "provider": "akshare",
    "model": "AkshareEtfOptionQuotes",
    "max_response_bytes": 1_000_000,
    "timeout_seconds": 15,
}


def acquire_plan_document(*requests, **source):
    return {
        "version": "etf-acquire-plan-v1",
        "source": {**ACQUIRE_SOURCE, **source},
        "requests": list(requests),
    }


#: A literal identity block (``etf-acquire-plan-v2``'s ``identity``).  Any 64-hex
#: fingerprint is structurally valid here; only the collector can say whether it is
#: *its* fingerprint, which is what ``handoff_fingerprint_mismatch`` refuses.
ACQUIRE_IDENTITY = {
    "source_id": "etf-options",
    "date": "2026-10-14",
    "fingerprint": "0" * 64,
    "token": "plan-token",
    "attempt": 0,
}


def plan_identity(**extra):
    return {**ACQUIRE_IDENTITY, **extra}


def quote_request(request_id="q1", symbols="10000001"):
    return {"request_id": request_id, "kind": "quote-batch", "params": {"symbols": symbols}}


def master_request(request_id="m1", exchange="SSE"):
    return {
        "request_id": request_id,
        "kind": "sse-official-master" if exchange == "SSE" else "szse-official-master",
        "model": "AkshareEtfOptionContracts",
        "params": {"exchange": exchange, "source": "official"},
    }


def transport_modules(monkeypatch, stand):
    import openbb_akshare.models.etf_options as contracts_module
    import openbb_akshare.models.etf_options as quotes_module

    monkeypatch.setattr(contracts_module, "make_contract_request", stand)
    monkeypatch.setattr(quotes_module, "make_quote_request", stand)
    return quotes_module


def test_raw_acquire_only_task_takes_a_literal_plan_and_writes_only_evidence(tmp_path, monkeypatch):
    """The lane's acquisition step, without configuration, journal or parsing."""
    from dq_historical_market_data_collector.etf_options import acquire

    body = quote_body("10000001")
    stand = TransportStand(sse_master_body(), body)
    transport_modules(monkeypatch, stand)
    root = tmp_path / "evidence"
    plan = acquire_plan_document(quote_request())

    results = acquire.acquire_plan(root, plan)
    assert [item["status"] for item in results] == ["captured"]
    assert results[0]["complete"] is True and results[0]["source_calls"] == 1
    assert results[0]["http_status"] == 200
    assert results[0]["evidence_kind"] == "raw-bytes"
    assert results[0]["transformation"] is None
    assert results[0]["size"] == len(body)
    assert results[0]["sha256"] == hashlib.sha256(body).hexdigest()
    # The bytes on disk are the transport bytes, and the transport was called once.
    (body_path,) = sorted(root.rglob("body.bin"))
    assert body_path.read_bytes() == body
    assert len(stand.calls) == 1 and stand.calls[0][0].endswith("CON_OP_10000001")
    # Nothing else was written: no journal database, no delivery, no parsed row.
    written = {path.name for path in root.rglob("*") if path.is_file()}
    assert written <= {
        ".lock",
        "task.json",
        "receipt.json",
        "terminal.json",
        "durable.json",
        "body.bin",
        # The per-request HTTP status, persisted beside the receipts it describes,
        # the manifest that chains it into the capture's own integrity chain, and
        # the append-only log outside the locator that anchors the manifest.
        "status.json",
        "acquire_manifest.json",
        "acquisition-log.jsonl",
    }
    assert not list(root.rglob("*.sqlite3"))
    assert all(
        "etf-raw-delivery" not in path.read_text() and '"bid"' not in path.read_text()
        for path in root.rglob("*.json")
    )
    # The re-read verifies the capture it just wrote, and the entry point is
    # idempotent: the same literal plan replays with zero source calls.
    assert acquire.verify(root, results)[0]["verified"] is True
    again = acquire.acquire_plan(root, plan)
    assert again[0]["locator"] == results[0]["locator"]
    assert again[0]["source_calls"] == 0 and len(stand.calls) == 1


def test_raw_acquire_only_cli_runs_without_a_config_file(tmp_path, monkeypatch, capsys):
    """`etf_options acquire --plan ... --root ...`: no config, no journal, exits."""
    from dq_historical_market_data_collector.etf_options import acquire, cli

    body = quote_body("10000001")
    stand = TransportStand(sse_master_body(), body)
    transport_modules(monkeypatch, stand)
    plan_path = tmp_path / "plan.json"
    plan_path.write_text(json.dumps(acquire_plan_document(quote_request(), master_request())))
    root = tmp_path / "evidence"

    # No --config is accepted or required, and a config file is not read.
    assert cli.main(["acquire", "--plan", str(plan_path), "--root", str(root)]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["command"] == "acquire" and payload["status"] == "complete"
    assert payload["verified_all"] is True and len(payload["results"]) == 2
    assert {item["kind"] for item in payload["results"]} == {"quote-batch", "sse-official-master"}
    assert not (tmp_path / "config.yaml").exists()
    # The same plan through the module entry point addresses the same captures.
    before = len(stand.calls)
    assert acquire.main(["--plan", str(plan_path), "--root", str(root)]) == 0
    assert len(stand.calls) == before == 2

    # A plan may not name a lane the client cannot address as one document: the
    # aggregate Sina master and the unknown kinds are refused, typed, before any
    # source call or any file.
    refusals = [
        (
            acquire_plan_document(
                {**master_request(), "params": {"exchange": "SSE", "source": "sina"}}
            ),
            "invalid_request_params",
        ),
        (
            acquire_plan_document({"request_id": "x", "kind": "provider-envelope", "params": {}}),
            "unknown_document_kind",
        ),
        (acquire_plan_document(quote_request(symbols="10000001,10000001")), "duplicate_symbols"),
        ({**acquire_plan_document(quote_request()), "extra": 1}, "invalid_plan"),
        (
            {**acquire_plan_document(quote_request()), "version": "etf-acquire-plan-v3"},
            "unsupported_plan_version",
        ),
        # A v2 plan must carry an identity block, and an identity-bearing plan must
        # state each request's exchange: the shared identity cannot be guessed.
        (
            {**acquire_plan_document(quote_request()), "version": "etf-acquire-plan-v2"},
            "invalid_plan",
        ),
        (
            {
                **acquire_plan_document(quote_request()),
                "version": "etf-acquire-plan-v2",
                "identity": {**plan_identity(), "fingerprint": "not-a-hash"},
            },
            "invalid_identity",
        ),
        (
            {
                **acquire_plan_document(quote_request()),
                "version": "etf-acquire-plan-v2",
                "identity": plan_identity(),
            },
            "invalid_identity",
        ),
        (
            {
                **acquire_plan_document(
                    {**quote_request(), "exchange": "SSE", "task_id": "0" * 64}
                ),
                "version": "etf-acquire-plan-v2",
                "identity": plan_identity(),
            },
            "ambiguous_identity",
        ),
    ]
    fresh = tmp_path / "refused"
    for plan, code in refusals:
        with pytest.raises(acquire.PlanError) as failure:
            acquire.acquire_plan(fresh, plan)
        assert failure.value.code == code, (code, failure.value.code)
    assert not fresh.exists() and len(stand.calls) == before


def test_raw_delivery_labels_are_rederived_from_the_capture(tmp_path, monkeypatch):
    """Every label a delivery states is re-derived, and a rewrite is refused typed."""
    from dq_historical_market_data_collector.etf_options.runtime import (
        EvidenceStateError,
        read_delivery,
    )

    stand = TransportStand(sse_master_body(), quote_body("10000001"))
    with literal_lane(tmp_path, monkeypatch, stand) as (cfg, journal, report):
        assert report["status"] == "complete"
        ((original, clean),) = documents(cfg.root, "quote-batch")

        def mutate(name, change):
            path = original.parent / f"label_{name}.json"
            payload = json.loads(json.dumps(clean))
            change(payload)
            path.write_text(json.dumps(payload, sort_keys=True))
            return path

        cases = [
            # A status is not merely unbound now: the schema has no such key.
            ("status", lambda p: p.__setitem__("http_status", 503), "invalid_delivery", None),
            (
                "size",
                lambda p: p["capture"].__setitem__("size", p["capture"]["size"] + 1),
                "delivery_label_mismatch",
                "capture.size",
            ),
            (
                "media_type",
                lambda p: p["capture"].__setitem__("media_type", "application/json"),
                "delivery_label_mismatch",
                "capture.media_type",
            ),
            (
                "date_source",
                lambda p: p["provenance"].__setitem__("date_source", "row"),
                "delivery_label_mismatch",
                "provenance.date_source",
            ),
            (
                "transforms",
                lambda p: p["provenance"]["transforms"].pop(),
                "delivery_label_mismatch",
                "provenance.transforms",
            ),
            (
                "row_count",
                lambda p: p["provenance"].__setitem__("row_count", 2),
                "delivery_label_mismatch",
                "provenance.row_count",
            ),
            (
                "unit_source",
                lambda p: p["provenance"].__setitem__("unit_source", "absent"),
                "delivery_label_mismatch",
                None,
            ),
            (
                "unit_labels",
                lambda p: p["provenance"]["unit_labels"].__setitem__("size_unit", None),
                "delivery_label_mismatch",
                None,
            ),
            (
                "captured_at",
                lambda p: p.__setitem__("captured_at", "2026-10-14T07:19:00.000000Z"),
                "delivery_label_mismatch",
                "captured_at",
            ),
            (
                "request",
                lambda p: p["request"].__setitem__("symbols", "10000002"),
                "delivery_label_mismatch",
                "request.symbols",
            ),
            (
                "source",
                lambda p: p["source"].__setitem__("model", "AkshareEtfOptionContracts"),
                "delivery_label_mismatch",
                "source",
            ),
        ]
        for name, change, code, field in cases:
            path = mutate(name, change)
            before = path.read_bytes()
            with pytest.raises(EvidenceStateError) as failure:
                read_delivery(path, cfg.root / "evidence")
            assert failure.value.code == code, (name, failure.value.code)
            if field is not None:
                assert failure.value.detail.get("field") == field, (name, failure.value.detail)
            assert path.read_bytes() == before, (name, "a refused read must not rewrite anything")
        # The untouched delivery still reads, and still says what the bytes say.
        clean_read = read_delivery(original, cfg.root / "evidence")
        assert clean_read["rows"][0]["bid"] == 0.1
        assert clean_read["provenance"]["transforms"][4] == "numeric_text_to_float"


def test_raw_status_acquire_persists_each_transport_status_as_evidence(
    tmp_path, monkeypatch, capsys
):
    """A 503 body and a 200 body are distinguishable in acquire-only evidence.

    The status is persisted beside the receipts it describes (``status.json``),
    it is read back on replay, and it decides the terminal: a captured 503 is
    ``http_error``, never ``complete`` -- while its bytes are still evidence.
    """
    from dq_historical_market_data_collector.etf_options import acquire, capture_status

    page = b"<html><body>503 upstream unavailable</body></html>"
    plan_path = tmp_path / "plan.json"
    plan_path.write_text(json.dumps(acquire_plan_document(quote_request())))
    bad_root, good_root = tmp_path / "bad", tmp_path / "good"

    refused = TransportStand(sse_master_body(), page, status=503)
    transport_modules(monkeypatch, refused)
    assert acquire.main(["--plan", str(plan_path), "--root", str(bad_root)]) == 1
    failed = json.loads(capsys.readouterr().out)
    (entry,) = failed["results"]
    assert failed["status"] == "failed" and failed["terminals"] == {"http_error": 1}
    assert failed["non_2xx"] == [{"request_id": "q1", "http_status": 503}]
    # The capture is finalized and the bytes are kept: this is evidence of a
    # refusal, not a lost response, and it is still not a completed acquisition.
    assert entry["status"] == "captured" and entry["complete"] is True
    assert entry["terminal"] == "http_error" and entry["http_status"] == 503
    assert failed["verified_all"] is True
    locator = bad_root / entry["locator"]
    assert (locator / "responses/000000/body.bin").read_bytes() == page
    assert entry["sha256"] == hashlib.sha256(page).hexdigest()
    # The status is on disk, bound to that receipt, and it is the only difference.
    record = json.loads((locator / capture_status.NAME).read_bytes())
    assert record["version"] == capture_status.VERSION
    assert record["statuses"] == [{"index": 0, "sha256": entry["sha256"], "http_status": 503}]
    assert record["task_id"] == entry["task_id"]

    accepted = TransportStand(sse_master_body(), page, status=200)
    transport_modules(monkeypatch, accepted)
    assert acquire.main(["--plan", str(plan_path), "--root", str(good_root)]) == 0
    ok = json.loads(capsys.readouterr().out)
    assert ok["status"] == "complete" and ok["terminals"] == {"captured": 1}
    assert ok["non_2xx"] == []
    good = json.loads((good_root / ok["results"][0]["locator"] / capture_status.NAME).read_bytes())
    # Same bytes, same size, same sha -- and the two captures still say which
    # status produced them, which is exactly what a console field could not.
    assert good["statuses"][0]["sha256"] == ok["results"][0]["sha256"]
    assert good["statuses"][0]["http_status"] == 200
    assert record["statuses"][0]["http_status"] != good["statuses"][0]["http_status"]

    # Replay: the same plan calls nothing and *still* reports the 503, because the
    # status is read from the capture rather than from this invocation's counter.
    calls = len(refused.calls)
    assert acquire.main(["--plan", str(plan_path), "--root", str(bad_root)]) == 1
    replayed = json.loads(capsys.readouterr().out)
    assert len(refused.calls) == calls
    assert replayed["results"][0]["source_calls"] == 0
    assert replayed["results"][0]["terminal"] == "http_error"
    assert replayed["results"][0]["http_status"] == 503
    # A status record only describes the capture it was written for: bound to the
    # task digest and to each receipt sha, so another capture's record is a typed
    # refusal rather than a status that can be transplanted.
    master_plan = tmp_path / "master-plan.json"
    master_plan.write_text(json.dumps(acquire_plan_document(master_request())))
    transport_modules(monkeypatch, refused)
    assert acquire.main(["--plan", str(master_plan), "--root", str(bad_root)]) == 1
    (master,) = json.loads(capsys.readouterr().out)["results"]
    with pytest.raises(capture_status.StatusError) as failure:
        capture_status.read(bad_root, master["locator"], acquire_read_capture(bad_root, entry))
    assert str(failure.value) == "status_binding"
    # A missing record is not "no status observed": it is a typed refusal.
    (locator / capture_status.NAME).unlink()
    with pytest.raises(capture_status.StatusError) as missing:
        capture_status.read(bad_root, entry["locator"], acquire_read_capture(bad_root, entry))
    assert str(missing.value) == "missing_status"


def status_capture(base, monkeypatch, status=503):
    """One real acquire capture taken under a known transport status.

    Returns ``(root, entry, plan_path)``: the evidence root, the single result row
    (its ``locator`` is the capture directory) and the literal plan that addresses
    it, so a test can tamper with the store and then replay through the CLI.
    """
    from dq_historical_market_data_collector.etf_options import acquire

    base.mkdir(parents=True)
    page = b"<html><body>503 upstream unavailable</body></html>"
    plan_path = base / "plan.json"
    plan = acquire_plan_document(quote_request())
    plan_path.write_text(json.dumps(plan))
    root = base / "evidence"
    transport_modules(monkeypatch, TransportStand(sse_master_body(), page, status=status))
    (entry,) = acquire.acquire_plan(root, plan)
    return root, entry, plan_path


def capture_files(locator, *, beside=None):
    """Every file of a capture except the record under test, with its bytes."""
    return {
        path.relative_to(locator): path.read_bytes()
        for path in locator.rglob("*")
        if path.is_file() and path.name != beside
    }


def test_raw_status_single_field_rewrite_is_refused(tmp_path, monkeypatch, capsys):
    """The review's attack: 503 -> 200, one field, every other file untouched.

    The record still binds (same task digest, task id, generation, same receipt
    sha256), so identity checks pass -- which is exactly why the capture's own
    integrity manifest is what has to refuse it.
    """
    from dq_historical_market_data_collector.etf_options import (
        acquire,
        capture_manifest,
        capture_status,
    )

    root, entry, plan_path = status_capture(tmp_path / "case", monkeypatch)
    locator = root / entry["locator"]
    status_file = locator / capture_status.NAME
    original = status_file.read_bytes()
    others = capture_files(locator, beside=capture_status.NAME)
    assert capture_manifest.NAME in {path.name for path in locator.iterdir()}

    forged = original.replace(b'"http_status":503', b'"http_status":200')
    # Only that one field: same length, same everything else on disk.
    assert forged != original and len(forged) == len(original)
    status_file.write_bytes(forged)
    assert capture_files(locator, beside=capture_status.NAME) == others

    state = acquire_read_capture(root, entry)
    with pytest.raises(capture_status.StatusError) as failure:
        capture_status.read(root, entry["locator"], state)
    assert str(failure.value) == "status_binding"
    with pytest.raises(capture_manifest.ManifestError) as direct:
        capture_manifest.check(root, entry["locator"], state, status_name=capture_status.NAME)
    assert str(direct.value) == "status_binding"

    # The CLI reaches the same verdict from the bytes: verify is false, exit 1.
    assert acquire.main(["--plan", str(plan_path), "--root", str(root)]) == 1
    refused = json.loads(capsys.readouterr().out)
    assert refused["status"] == "failed" and refused["verified_all"] is False
    assert [item["reason"] for item in refused["verified"]] == ["status_binding"]
    # A refused read repairs nothing.
    assert status_file.read_bytes() == forged
    assert capture_files(locator, beside=capture_status.NAME) == others


def test_raw_status_wholesale_replacement_is_refused(tmp_path, monkeypatch, capsys):
    """A whole record replaced by another capture's is refused, not adopted.

    The two captures are the same plan, so the task digest, task id, generation
    and receipt sha256 are identical between them; the identity binding cannot
    tell the 200 record from the 503 one.  The manifest can, because the status
    bytes it hashed are not the status bytes on disk.
    """
    from dq_historical_market_data_collector.etf_options import (
        acquire,
        capture_manifest,
        capture_status,
    )

    refused_root, refused, plan_path = status_capture(tmp_path / "refused", monkeypatch, status=503)
    accepted_root, accepted, _ = status_capture(tmp_path / "accepted", monkeypatch, status=200)
    assert refused["locator"] == accepted["locator"]
    assert refused["sha256"] == accepted["sha256"]
    refused_file = refused_root / refused["locator"] / capture_status.NAME
    accepted_file = accepted_root / accepted["locator"] / capture_status.NAME
    refused_record = json.loads(refused_file.read_bytes())
    accepted_record = json.loads(accepted_file.read_bytes())
    # Identity is identical; the status is the only difference.
    for key in ("version", "task_sha256", "task_id", "generation"):
        assert refused_record[key] == accepted_record[key]
    assert refused_record["statuses"] == [{**accepted_record["statuses"][0], "http_status": 503}]

    manifest = refused_root / refused["locator"] / capture_manifest.NAME
    before = manifest.read_bytes()
    refused_file.write_bytes(accepted_file.read_bytes())
    with pytest.raises(capture_status.StatusError) as failure:
        capture_status.read(
            refused_root, refused["locator"], acquire_read_capture(refused_root, refused)
        )
    assert str(failure.value) == "status_binding"
    assert manifest.read_bytes() == before

    assert acquire.main(["--plan", str(plan_path), "--root", str(refused_root)]) == 1
    output = json.loads(capsys.readouterr().out)
    assert output["verified_all"] is False
    assert [item["reason"] for item in output["verified"]] == ["status_binding"]


def test_raw_status_manifest_edit_is_refused(tmp_path, monkeypatch, capsys):
    """A manifest edit is refused: a hash field, or the key set itself."""
    from dq_historical_market_data_collector.etf_options import (
        acquire,
        capture_manifest,
        capture_status,
    )

    root, entry, plan_path = status_capture(tmp_path / "case", monkeypatch)
    locator = root / entry["locator"]
    manifest = locator / capture_manifest.NAME
    original = manifest.read_bytes()
    record = json.loads(original)
    state = acquire_read_capture(root, entry)

    # (a) one hash field re-stated; nothing else on disk moves.
    forged = {**record, "status_sha256": "0" * 64}
    manifest.write_bytes((capture_manifest.canonical(forged) + "\n").encode())
    with pytest.raises(capture_status.StatusError) as failure:
        capture_status.read(root, entry["locator"], state)
    assert str(failure.value) == "status_binding"

    # (b) an extra key: the exact key set is part of what a manifest is.
    extended = {**record, "note": "not part of the chain"}
    manifest.write_bytes((capture_manifest.canonical(extended) + "\n").encode())
    with pytest.raises(capture_status.StatusError) as failure:
        capture_status.read(root, entry["locator"], state)
    assert str(failure.value) == "invalid_manifest"

    # (c) non-canonical bytes for the same object are refused before any hashing.
    manifest.write_bytes(json.dumps(record, indent=2, sort_keys=True).encode())
    with pytest.raises(capture_status.StatusError) as failure:
        capture_status.read(root, entry["locator"], state)
    assert str(failure.value) == "noncanonical_manifest"

    # The untouched manifest verifies again: the refusals above changed no state.
    manifest.write_bytes(original)
    assert capture_status.read(root, entry["locator"], state)["statuses"][0]["http_status"] == 503

    manifest.write_bytes((capture_manifest.canonical(forged) + "\n").encode())
    assert acquire.main(["--plan", str(plan_path), "--root", str(root)]) == 1
    output = json.loads(capsys.readouterr().out)
    assert output["verified_all"] is False
    assert [item["reason"] for item in output["verified"]] == ["status_binding"]


def test_raw_status_manifest_deletion_is_refused(tmp_path, monkeypatch, capsys):
    """Deleting the chain is not "unbound": it is a typed refusal, exit 1."""
    from dq_historical_market_data_collector.etf_options import (
        acquire,
        capture_manifest,
        capture_status,
    )

    root, entry, plan_path = status_capture(tmp_path / "case", monkeypatch)
    locator = root / entry["locator"]
    manifest = locator / capture_manifest.NAME
    status_file = locator / capture_status.NAME
    others = capture_files(locator, beside=capture_manifest.NAME)
    before = status_file.read_bytes()
    manifest.unlink()

    with pytest.raises(capture_status.StatusError) as failure:
        capture_status.read(root, entry["locator"], acquire_read_capture(root, entry))
    assert str(failure.value) == "missing_manifest"
    # Nothing else moved: this is a refusal about the chain, not a lost capture.
    assert status_file.read_bytes() == before
    assert capture_files(locator, beside=capture_manifest.NAME) == others

    assert acquire.main(["--plan", str(plan_path), "--root", str(root)]) == 1
    output = json.loads(capsys.readouterr().out)
    assert output["status"] == "failed" and output["verified_all"] is False
    assert [item["reason"] for item in output["verified"]] == ["missing_manifest"]


def log_lines(root):
    """The acquisition log of one evidence root, as parsed lines."""
    from dq_historical_market_data_collector.etf_options import capture_log

    content = capture_log.path(root).read_bytes()
    return [json.loads(line) for line in content.splitlines()]


def test_raw_status_log_anchor_refuses_status_and_manifest_rewrite(tmp_path, monkeypatch, capsys):
    """The two-file forgery the previous round accepted is now a typed refusal.

    ``{status.json, acquire_manifest.json}`` is a *consistent* capture: the
    manifest restates the hash of the bytes just written.  What it cannot restate
    is the digest the acquisition log recorded for that manifest when the capture
    was finalized -- a third file, outside the capture locator.
    """
    from dq_historical_market_data_collector.etf_options import (
        acquire,
        capture_log,
        capture_manifest,
        capture_status,
    )

    root, entry, plan_path = status_capture(tmp_path / "case", monkeypatch)
    directory = root / entry["locator"]
    status_file = directory / capture_status.NAME
    manifest_file = directory / capture_manifest.NAME
    pristine = capture_files(directory)
    before_log = capture_log.path(root).read_bytes()

    # The logged anchor is this capture's manifest, and it lives outside the tree.
    (line,) = log_lines(root)
    assert line["sequence"] == 0 and line["locator"] == entry["locator"]
    assert line["manifest_sha256"] == hashlib.sha256(manifest_file.read_bytes()).hexdigest()
    assert not capture_log.path(root).is_relative_to(directory)

    forged_status = status_file.read_bytes().replace(b'"http_status":503', b'"http_status":200')
    status_file.write_bytes(forged_status)
    record = json.loads(manifest_file.read_bytes())
    forged_manifest = {
        **record,
        "status_sha256": hashlib.sha256(forged_status).hexdigest(),
    }
    manifest_file.write_bytes((capture_manifest.canonical(forged_manifest) + "\n").encode())
    # Exactly two files rewritten, the other six byte-identical, and the forgery is
    # internally consistent: the old chain alone could not see it.
    after_write = capture_files(directory)
    changed = {str(name) for name in pristine if pristine[name] != after_write[name]}
    assert changed == {"status.json", "acquire_manifest.json"}
    assert len(after_write) == 8

    with pytest.raises(capture_status.StatusError) as failure:
        capture_status.read(root, entry["locator"], acquire_read_capture(root, entry))
    assert str(failure.value) == "acquisition_log_mismatch"
    assert capture_log.path(root).read_bytes() == before_log
    assert capture_files(directory) == after_write

    verdict = acquire.verify(root, [entry])[0]
    assert verdict["verified"] is False and verdict["reason"] == "acquisition_log_mismatch"
    assert acquire.main(["--plan", str(plan_path), "--root", str(root)]) == 1
    output = json.loads(capsys.readouterr().out)
    assert output["verified_all"] is False
    assert [item["reason"] for item in output["verified"]] == ["acquisition_log_mismatch"]


def test_raw_status_log_anchor_refuses_an_edited_line(tmp_path, monkeypatch):
    """A log line that no longer states this manifest's digest is refused."""
    from dq_historical_market_data_collector.etf_options import (
        capture_log,
        capture_manifest,
        capture_status,
    )

    root, entry, _plan = status_capture(tmp_path / "case", monkeypatch)
    original = capture_log.path(root).read_bytes()
    (line,) = log_lines(root)

    for name, digest in (("zeroed", "0" * 64), ("swapped", "1" * 64)):
        forged = {**line, "manifest_sha256": digest}
        capture_log.path(root).write_bytes((capture_log.canonical(forged) + "\n").encode())
        with pytest.raises(capture_status.StatusError) as failure:
            capture_status.read(root, entry["locator"], acquire_read_capture(root, entry))
        assert str(failure.value) == "acquisition_log_mismatch", name

    # An added key, or a re-indented line, is a refusal about the log itself.
    capture_log.path(root).write_bytes(
        (capture_log.canonical({**line, "note": "not anchored"}) + "\n").encode()
    )
    with pytest.raises(capture_status.StatusError) as failure:
        capture_status.read(root, entry["locator"], acquire_read_capture(root, entry))
    assert str(failure.value) == "acquisition_log_mismatch"

    capture_log.path(root).write_bytes(original)
    assert (
        capture_status.read(root, entry["locator"], acquire_read_capture(root, entry))["statuses"][
            0
        ]["http_status"]
        == 503
    )
    assert capture_manifest.NAME in {path.name for path in (root / entry["locator"]).iterdir()}


def test_raw_status_log_anchor_makes_truncation_and_gaps_visible(tmp_path, monkeypatch):
    """Two captures: a missing line, and a sequence gap, are both refusals.

    Dropping the last line is invisible to sequence arithmetic -- the remaining
    lines are still ``0..n-1`` -- but not to the capture that reads: its line is
    gone (``acquisition_log_missing``).  Dropping a line from the middle leaves a
    gap, which refuses *every* capture in the store (``acquisition_log_mismatch``).
    """
    from dq_historical_market_data_collector.etf_options import (
        acquire,
        capture_log,
        capture_status,
    )

    root, first, _plan = status_capture(tmp_path / "case", monkeypatch)
    second = acquire.acquire_plan(root, acquire_plan_document(master_request()))[0]
    assert first["locator"] != second["locator"]
    lines = log_lines(root)
    assert [line["sequence"] for line in lines] == [0, 1]
    assert [line["locator"] for line in lines] == [first["locator"], second["locator"]]
    original = capture_log.path(root).read_bytes()

    # Tail truncation: the surviving line is still sequence 0, so the log parses,
    # and the capture whose line is gone refuses.
    capture_log.path(root).write_bytes((capture_log.canonical(lines[0]) + "\n").encode())
    with pytest.raises(capture_status.StatusError) as failure:
        capture_status.read(root, second["locator"], acquire_read_capture(root, second))
    assert str(failure.value) == "acquisition_log_missing"
    assert (
        capture_status.read(root, first["locator"], acquire_read_capture(root, first))["statuses"][
            0
        ]["http_status"]
        == 503
    )

    # Middle deletion: the survivor says sequence 1 where the first line must be
    # sequence 0.  That is a gap in the store, not a fact about one capture.
    capture_log.path(root).write_bytes((capture_log.canonical(lines[1]) + "\n").encode())
    for entry in (first, second):
        with pytest.raises(capture_status.StatusError) as failure:
            capture_status.read(root, entry["locator"], acquire_read_capture(root, entry))
        assert str(failure.value) == "acquisition_log_mismatch"

    # Deleting the log is a missing anchor, never "unbound".
    capture_log.path(root).unlink()
    with pytest.raises(capture_status.StatusError) as failure:
        capture_status.read(root, first["locator"], acquire_read_capture(root, first))
    assert str(failure.value) == "acquisition_log_missing"

    capture_log.path(root).write_bytes(original)
    for entry in (first, second):
        assert capture_status.read(root, entry["locator"], acquire_read_capture(root, entry))


def test_raw_status_log_anchor_is_append_only_across_replays(tmp_path, monkeypatch):
    """A replay appends nothing: the log is one line per finalized capture."""
    from dq_historical_market_data_collector.etf_options import (
        acquire,
        capture_log,
        capture_manifest,
    )

    root, entry, plan_path = status_capture(tmp_path / "case", monkeypatch)
    before = capture_log.path(root).read_bytes()
    assert len(log_lines(root)) == 1
    assert acquire.main(["--plan", str(plan_path), "--root", str(root)]) == 1
    assert capture_log.path(root).read_bytes() == before
    manifest_file = root / entry["locator"] / capture_manifest.NAME
    (line,) = log_lines(root)
    assert line["manifest_sha256"] == hashlib.sha256(manifest_file.read_bytes()).hexdigest()


def test_raw_status_deleted_record_is_refused_without_being_reinstalled(
    tmp_path, monkeypatch, capsys
):
    """A deleted status record is refused: it is not re-installed as a null record."""
    from dq_historical_market_data_collector.etf_options import acquire, capture_status

    root, entry, plan_path = status_capture(tmp_path / "case", monkeypatch)
    directory = root / entry["locator"]
    status_file = directory / capture_status.NAME
    status_file.unlink()
    before = {
        path.relative_to(directory): path.read_bytes()
        for path in directory.rglob("*")
        if path.is_file()
    }

    assert acquire.main(["--plan", str(plan_path), "--root", str(root)]) == 1
    output = json.loads(capsys.readouterr().out)
    assert output["status"] == "failed" and output["verified_all"] is False
    assert [item["reason"] for item in output["verified"]] == ["missing_status"]
    # The refusal wrote nothing: no fresh all-null record, no re-anchored manifest.
    assert not status_file.exists()
    assert {
        path.relative_to(directory): path.read_bytes()
        for path in directory.rglob("*")
        if path.is_file()
    } == before


def test_raw_status_core_chain_tamper_is_a_verdict_not_an_exception(tmp_path, monkeypatch):
    """``verify`` answers with a typed reason for a tamper it is handed results for."""
    import json as _json

    from dq_historical_market_data_collector.etf_options import acquire

    root, entry, _plan = status_capture(tmp_path / "case", monkeypatch)
    assert acquire.verify(root, [entry])[0]["verified"] is True

    for name, mutate in (
        (
            "terminal",
            lambda path: path.write_bytes(
                path.read_bytes().replace(
                    _json.loads(path.read_bytes())["finalized_at"].encode(),
                    b"2020-01-01T00:00:00.000000Z",
                )
            ),
        ),
        (
            "receipt",
            lambda path: path.write_bytes(
                path.read_bytes().replace(b'"media_type":', b'"media_type":"x",')
            ),
        ),
    ):
        target = (
            root / entry["locator"] / "terminal.json"
            if name == "terminal"
            else root / entry["locator"] / "responses/000000/receipt.json"
        )
        stored = target.read_bytes()
        mutate(target)
        verdict = acquire.verify(root, [entry])[0]
        assert verdict["verified"] is False, name
        assert isinstance(verdict["reason"], str) and verdict["reason"], name
        assert verdict["reason"] != "acquisition_refused", name
        target.write_bytes(stored)

    assert acquire.verify(root, [entry])[0]["verified"] is True


def test_raw_status_read_requires_the_capture_state(tmp_path, monkeypatch):
    """``read(root, locator)`` cannot skip the identity binding and the chain."""
    from dq_historical_market_data_collector.etf_options import capture_status

    root, entry, _plan = status_capture(tmp_path / "case", monkeypatch)
    with pytest.raises(TypeError):
        capture_status.read(root, entry["locator"])  # type: ignore[call-arg]


def acquire_read_capture(root, entry):
    from openbb_collector_core.raw_acquisition import read_capture

    return read_capture(root, entry["task_id"], entry["generation"])


def test_raw_delivery_labels_bind_the_formerly_trusted_keys(tmp_path, monkeypatch):
    """Every key the round-2 review measured as trusted is bound or shape-checked.

    The nine were ``request.extra_key``, ``metadata``, ``warning_count``,
    ``observed_at``, ``evidence.entry_id``/``plan_sha256``/``status`` and
    ``row_provenance[i].date_source``/``unit_source``.  Eight are rebuilt from the
    capture and compared; ``observed_at`` is the one no byte can state, so it is
    refused unless it is an offset-bearing instant and is disclosed as such.
    """
    from dq_historical_market_data_collector.etf_options.runtime import (
        EvidenceStateError,
        read_delivery,
    )

    stand = TransportStand(sse_master_body(), quote_body("10000001"))
    with literal_lane(tmp_path, monkeypatch, stand) as (cfg, journal, report):
        assert report["status"] == "complete"
        ((original, clean),) = documents(cfg.root, "quote-batch")

        def mutate(name, change):
            path = original.parent / f"bound_{name}.json"
            payload = json.loads(json.dumps(clean))
            change(payload)
            path.write_text(json.dumps(payload, sort_keys=True))
            return path

        cases = [
            (
                "request_extra_key",
                lambda p: p["request"].__setitem__("extra_key", 1),
                "delivery_label_mismatch",
                "request",
            ),
            (
                "request_exchange",
                lambda p: p["request"].__setitem__("exchange", "HKEX"),
                "delivery_label_mismatch",
                "request",
            ),
            (
                "metadata",
                lambda p: p.__setitem__("metadata", {"note": "invented"}),
                "delivery_label_mismatch",
                "metadata",
            ),
            (
                "warning_count",
                lambda p: p.__setitem__("warning_count", 3),
                "delivery_label_mismatch",
                "warning_count",
            ),
            (
                "observed_at",
                lambda p: p.__setitem__("observed_at", "yesterday"),
                "invalid_delivery",
                None,
            ),
            (
                "evidence_entry_id",
                lambda p: p["evidence"].__setitem__("entry_id", "0" * 64),
                "delivery_label_mismatch",
                "evidence.entry_id",
            ),
            (
                "evidence_plan_sha256",
                lambda p: p["evidence"].__setitem__("plan_sha256", "0" * 64),
                "delivery_label_mismatch",
                "evidence.plan_sha256",
            ),
            (
                "evidence_status",
                lambda p: p["evidence"].__setitem__("status", "partial"),
                "delivery_label_mismatch",
                "evidence.status",
            ),
            (
                "evidence_complete",
                lambda p: p["evidence"].__setitem__("complete", False),
                "delivery_label_mismatch",
                "evidence.complete",
            ),
            (
                "row_date_source",
                lambda p: p["row_provenance"][0].__setitem__("date_source", "row"),
                "delivery_label_mismatch",
                "row_provenance",
            ),
            (
                "row_unit_source",
                lambda p: p["row_provenance"][0].__setitem__("unit_source", "absent"),
                "delivery_label_mismatch",
                "row_provenance",
            ),
            (
                "row_extra_key",
                lambda p: p["row_provenance"][0].__setitem__("note", "invented"),
                "delivery_label_mismatch",
                "row_provenance",
            ),
        ]
        for name, change, code, field in cases:
            path = mutate(name, change)
            before = path.read_bytes()
            with pytest.raises(EvidenceStateError) as failure:
                read_delivery(path, cfg.root / "evidence")
            assert failure.value.code == code, (name, failure.value.code)
            if field is not None:
                assert failure.value.detail.get("field") == field, (name, failure.value.detail)
            assert path.read_bytes() == before, (name, "a refused read must not rewrite anything")
        # The real delivery still reads, and its per-row labels agree with the
        # top-level provenance they are copies of.
        read = read_delivery(original, cfg.root / "evidence")
        assert {row["date_source"] for row in read["row_provenance"]} == {
            read["provenance"]["date_source"]
        }
        assert {row["unit_source"] for row in read["row_provenance"]} == {
            read["provenance"]["unit_source"]
        }


def test_raw_handoff_acquire_evidence_is_reused_by_collect_with_no_source_call(
    tmp_path, monkeypatch
):
    """The two halves share one identity: a completed acquisition is consumed.

    ``acquisition_plan`` writes the identity block the collector itself would use,
    ``acquire`` captures under it, and ``collect --acquire-plan`` (here: ``handoff``)
    finds that locator through its own ``existing_state`` and delivers from it with
    no second source call.
    """
    from dq_historical_market_data_collector.etf_options import acquire, runtime
    from dq_historical_market_data_collector.etf_options.config import ConfigurationError

    body = quote_body("10000001")
    stand = TransportStand(sse_master_body(), body)
    transport_modules(monkeypatch, stand)
    cfg = official_config(tmp_path)
    plan = runtime.acquisition_plan(cfg, NOW.date())
    # Only what may be fetched before any call: one master per exchange.  A quote
    # block's membership is a function of the master's content, so no plan can
    # name it, and the acquisition task may not read content.
    assert [entry["kind"] for entry in plan["requests"]] == ["sse-official-master"]
    assert plan["identity"]["fingerprint"] == runtime.digest(cfg.fingerprint())
    assert plan["identity"]["date"] == NOW.date().isoformat()

    results = acquire.acquire_plan(cfg.evidence_root, plan)
    (written,) = {item["locator"] for item in results}
    assert results[0]["terminal"] == "captured" and len(stand.calls) == 1

    with Journal(cfg.root) as journal:
        report = Collector(
            cfg, journal, calendar(), clock=lambda: NOW, handoff=plan["identity"]
        ).collect()
        assert report["status"] == "complete" and report["row_count"] == 1
        # One more call, and it is the quote block: the master was not refetched.
        assert len(stand.calls) == 2 and "sinajs" in stand.calls[1][0]
        assert report["provider_calls"] == 1
        # The attempt row names the very locator the acquisition task wrote.
        assert (
            journal.db.execute(
                "SELECT evidence_locator FROM attempts WHERE stage='sse-official-master'"
            ).fetchone()[0]
            == written
        )
        # A second run on the same plan calls nothing at all.
        again = Collector(
            cfg, journal, calendar(), clock=lambda: NOW, handoff=plan["identity"]
        ).collect()
        assert again["status"] == "complete" and again["provider_calls"] == 0
        assert len(stand.calls) == 2

    # A plan may cover two exchanges of one model.  The request id repeats (it is
    # the *task's* id, and each entry becomes its own task), while the tasks and
    # their locators stay distinct.
    two = config(
        tmp_path,
        exchanges={
            "SSE": {"source": "official", "underlyings": ["510050"]},
            "SZSE": {"source": "official", "underlyings": ["159901"]},
        },
    )
    both = acquire.validate_plan(runtime.acquisition_plan(two, NOW.date()))
    tasks = [
        acquire.task_of(entry["source"], entry, both["identity"]) for entry in both["requests"]
    ]
    assert {entry["request_id"] for entry in both["requests"]} == {
        f"{two.source_id}:AkshareEtfOptionContracts"
    }
    assert len({task["task_id"] for task in tasks}) == 2
    assert len({task["generation"] for task in tasks}) == 1

    # A plan that names another configuration or another day is refused, not adopted.
    with pytest.raises(ConfigurationError) as mismatch:
        Collector(cfg, None, calendar(), handoff={**plan["identity"], "fingerprint": "f" * 64})
    assert mismatch.value.code == "handoff_fingerprint_mismatch"
    with Journal(cfg.root) as journal:
        with pytest.raises(ConfigurationError) as wrong_day:
            Collector(
                cfg,
                journal,
                calendar(),
                clock=lambda: NOW,
                handoff={**plan["identity"], "date": "2026-10-13"},
            ).collect()
    assert wrong_day.value.code == "handoff_date_mismatch"

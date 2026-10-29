"""SDK-boundary unit tests for source evidence; no vendor connections."""

import asyncio
import json
import math
import struct

import pytest
from openbb_baostock.models.history_source import SOURCE_FETCHERS, validate_literal
from openbb_baostock.utils import (
    helpers,
    source_evidence as evidence,
)


class Result:
    """SDK-shaped synthetic result, including deliberately irregular rows."""

    error_code = "0"
    error_msg = "SECRET"
    fields = ["close", "close"]
    data = [["1.2300", "", "extra"], ["short"]]


@pytest.fixture
def sdk(monkeypatch):
    """Replace only SDK entry points and track cleanup."""
    calls = []

    class Socket:
        def close(self):
            calls.append("close")

    def login():
        helpers.context.default_socket = Socket()
        calls.append("login")
        return Result()

    def logout():
        calls.append("logout")
        return Result()

    monkeypatch.setattr(helpers.bs, "login", login)
    monkeypatch.setattr(helpers.bs, "logout", logout)
    monkeypatch.setattr(helpers, "resolve_method", lambda method: lambda **kw: Result())
    return calls


def test_fields_rows_and_exact_literal_binding(sdk):
    """Retain lists instead of coercing columns into a dictionary."""
    result = evidence._query("BaostockStockBasic", {"code": "sh.600000"})
    assert result["status"] == "complete"
    assert result["pages"] == [dict(error_code="0", fields=["close", "close"], rows=[["1.2300", "", "extra"], ["short"]])]
    assert result["params"] == {"code": "sh.600000"}
    assert sdk == ["login", "logout", "close"] and helpers.context.default_socket is None


def test_pagination_exception_keeps_prefix(sdk, monkeypatch):
    """Retain both the prefix and changed SDK state before an exception."""

    class Page(Result):
        data = [["prefix", "1.2300"]] * helpers.BAOSTOCK_PER_PAGE_COUNT

        def next(self):
            self.error_code = "503"
            self.data = [["received_failure"]]
            raise OSError("SECRET")

    monkeypatch.setattr(helpers, "resolve_method", lambda method: lambda **kw: Page())
    result = evidence._query("BaostockStockBasic", {})
    assert result["status"] == "partial" and result["reason"] == "pagination_exception"
    assert result["pages"][1]["rows"] == [["received_failure"]]
    assert result["pages"][1]["error_code"] == "503"
    assert "SECRET" not in str(result)
    assert sdk[-1] == "close"


def test_silent_full_page_not_success(sdk, monkeypatch):
    """Treat the SDK silent full-page failure as incomplete."""

    class Page(Result):
        data = [["prefix"]] * helpers.BAOSTOCK_PER_PAGE_COUNT

        def next(self):
            return False

    monkeypatch.setattr(helpers, "resolve_method", lambda method: lambda **kw: Page())
    result = evidence._query("BaostockStockBasic", {})
    assert result["status"] == "partial" and result["reason"] == "unconfirmed_full_page"


def test_budget_stops_before_next_page(sdk, monkeypatch):
    """Prove the next source call is not entered after admission fails."""

    class Page(Result):
        data = [["prefix"]] * helpers.BAOSTOCK_PER_PAGE_COUNT

        def next(self):
            pytest.fail("next page not admitted")

    monkeypatch.setattr(helpers, "resolve_method", lambda method: lambda **kw: Page())
    monkeypatch.setattr(evidence, "EVIDENCE_BYTES", evidence.PAGE_BYTES)
    result = evidence._query("BaostockStockBasic", {})
    assert result["status"] == "partial" and result["reason"] == "evidence_limit"
    assert result["pages"][0]["rows"][0] == ["prefix"]


def test_success_empty_final_page(sdk, monkeypatch):
    """Preserve the empty page that confirms exhaustion."""

    class Page(Result):
        data = [["prefix"]] * helpers.BAOSTOCK_PER_PAGE_COUNT

        def next(self):
            self.data = []
            return False

    monkeypatch.setattr(helpers, "resolve_method", lambda method: lambda **kw: Page())
    result = evidence._query("BaostockStockBasic", {})
    assert result["status"] == "complete" and result["pages"][-1]["rows"] == []


@pytest.mark.parametrize("phase", ["login", "query", "logout", "close"])
def test_cleanup_errors_and_no_exception_text(sdk, monkeypatch, phase):
    """Close the socket and retain source evidence on lifecycle failures."""

    def fail(*a, **k):
        raise OSError("SECRET")

    if phase == "query":
        monkeypatch.setattr(helpers, "resolve_method", lambda method: fail)
    elif phase == "close":
        original = helpers.bs.login

        def login():
            result = original()
            helpers.context.default_socket.close = fail
            return result

        monkeypatch.setattr(helpers.bs, "login", login)
    else:
        monkeypatch.setattr(helpers.bs, phase, fail)
    result = evidence._query("BaostockStockBasic", {})
    assert helpers.context.default_socket is None and "SECRET" not in str(result)
    if phase in ("login", "query"):
        assert result["status"] == "failed" and result["pages"] == []
    else:
        assert result["cleanup"] != "ok" and result["pages"]


def test_reference_registered_dispatch_and_failure(sdk, monkeypatch):
    """Run both reference models through the actual registered executor."""
    from openbb_baostock import baostock_provider
    from openbb_core.provider.query_executor import QueryExecutor
    from openbb_core.provider.registry import Registry

    class Failure(Result):
        error_code = "503"

    monkeypatch.setattr(helpers, "resolve_method", lambda method: lambda **kw: Failure())
    registry = Registry()
    registry.include_provider(baostock_provider)
    executor = QueryExecutor(registry)
    for operation, params in [
        ("BaostockStockBasic", {"code": "sh.600000"}),
        ("BaostockTradeDates", {"start_date": "2024-01-01", "end_date": "2024-01-03"}),
    ]:
        result = asyncio.run(
            executor.execute(
                provider_name="baostock",
                model_name=operation + "Source",
                params={"literal": params},
                credentials={},
            )
        )
        env = result[0].evidence
        assert env["status"] == "failed" and env["error_code"] == "503" and env["params"] == params
    assert len(SOURCE_FETCHERS) == 5


def test_stream_sink_validation_and_failure_provenance(sdk):
    """Invalid capability precedes login; callback failure is not SDK failure."""
    with pytest.raises(ValueError, match="invalid_evidence_sink_v1"):
        asyncio.run(evidence.query_evidence("BaostockStockBasic", {}, evidence_sink_v1=lambda: None))
    assert sdk == []
    error = OSError("PRIVATE_STORAGE_EXCEPTION")

    class Sink(evidence.EvidenceSinkV1):
        def source_call(self, kind):
            return True

        def observe(self, envelope, metadata):
            raise error

    with pytest.raises(OSError) as caught:
        asyncio.run(evidence.query_evidence("BaostockStockBasic", {}, evidence_sink_v1=Sink()))
    assert caught.value is error
    assert sdk == ["login", "logout", "close"]


def test_rejects_nonliteral_or_unsupported_protocol():
    """Reject rather than normalize unsupported literal protocol values."""
    for params in [
        {"code": "SH.600000"},
        {"code": False},
        {"code": " sh.600000"},
        {"mystery": "x"},
    ]:
        with pytest.raises(ValueError):
            validate_literal("BaostockStockBasic", params)
    with pytest.raises(ValueError):
        validate_literal("BaostockProfitData", {})


@pytest.mark.parametrize("code", [503.25, False, 0.0, 0, True, {"unexpected": 1}])
def test_error_code_is_evidence_not_success_coercion(sdk, monkeypatch, code):
    """Keep ordinary JSON values/types; only exact str '0' succeeds."""
    result = Result()
    result.error_code = code
    monkeypatch.setattr(helpers, "resolve_method", lambda method: lambda **kw: result)
    envelope = evidence._query("BaostockStockBasic", {})
    assert envelope["version"] == evidence.VERSION
    assert envelope["status"] == "failed"
    decoded = evidence.decode_evidence(json.loads(json.dumps(envelope, allow_nan=False)))
    for actual in (decoded["error_code"], decoded["pages"][0]["error_code"]):
        assert type(actual) is type(code) and actual == code
    assert decoded["pages"][0]["rows"] == result.data


@pytest.mark.parametrize("bits", ["7ff8000000001234", "fff8000000001234", "7ff0000000000000", "fff0000000000000"])
def test_nonfinite_values_round_trip_without_marker_collision(sdk, monkeypatch, bits):
    """Preserve binary64 bits and neighbouring literal marker-shaped JSON."""
    special = struct.unpack("!d", bytes.fromhex(bits))[0]
    result = Result()
    result.error_code = special
    result.fields = [special, "$float64", {"path": ["rows", 0], "type": "float64"}]
    result.data = [
        ["GOOD_RECEIVED_PREFIX"],
        [special, "BAD_SCALAR_RECEIVED"],
        ["$float64", {"value_types": [{"type": "float64"}]}],
    ]
    monkeypatch.setattr(helpers, "resolve_method", lambda method: lambda **kw: result)
    envelope = evidence._query("BaostockStockBasic", {})
    assert envelope["version"] == evidence.EXTENDED_VERSION
    assert envelope["status"] == "failed"
    saved = json.dumps(envelope, allow_nan=False)
    decoded = evidence.decode_evidence(json.loads(saved))
    for actual in (
        decoded["error_code"],
        decoded["pages"][0]["error_code"],
        decoded["pages"][0]["fields"][0],
        decoded["pages"][0]["rows"][1][0],
    ):
        assert type(actual) is float and not math.isfinite(actual)
        assert struct.pack("!d", actual).hex() == bits
    assert decoded["pages"][0]["rows"][0] == ["GOOD_RECEIVED_PREFIX"]
    assert decoded["pages"][0]["rows"][1][1] == "BAD_SCALAR_RECEIVED"
    assert decoded["pages"][0]["rows"][2] == result.data[2]
    assert decoded["pages"][0]["fields"][1:] == result.fields[1:]
    assert json.dumps(envelope, allow_nan=False) == saved  # decode never mutates bytes


def test_unrepresentable_value_does_not_erase_sibling_rows(sdk, monkeypatch):
    """Opaque objects have an authored unknown marker, never repr/attributes."""

    class Opaque:
        def __repr__(self):
            pytest.fail("must not serialize repr")

        def __deepcopy__(self, memo):
            pytest.fail("must not invoke arbitrary copy")

    result = Result()
    result.data = [["BEFORE"], [Opaque(), "SIBLING"], ["AFTER"]]
    monkeypatch.setattr(helpers, "resolve_method", lambda method: lambda **kw: result)
    envelope = evidence._query("BaostockStockBasic", {})
    assert envelope["reason"] == "sdk_value_unrepresentable"
    assert envelope["status"] == "partial"
    decoded = evidence.decode_evidence(json.loads(json.dumps(envelope, allow_nan=False)))
    assert decoded["pages"][0]["rows"] == [["BEFORE"], [evidence._UNREPRESENTABLE, "SIBLING"], ["AFTER"]]


@pytest.mark.parametrize("mutation", ["duplicate", "wrong_marker", "status_path", "finite_bits", "negative_index"])
def test_extended_decoder_refuses_ambiguous_sidecars(sdk, monkeypatch, mutation):
    """Refuse forged typing metadata instead of decoding a different source."""
    result = Result()
    result.data = [[float("nan")]]
    monkeypatch.setattr(helpers, "resolve_method", lambda method: lambda **kw: result)
    env = evidence._query("BaostockStockBasic", {})
    item = env["value_types"][0]
    if mutation == "duplicate":
        env["value_types"].append(dict(item))
    elif mutation == "wrong_marker":
        env["pages"][0]["rows"][0][0] = "literal"
    elif mutation == "status_path":
        item["path"] = ["status"]
    elif mutation == "finite_bits":
        item["bits"] = "0000000000000000"
    else:
        item["path"] = ["pages", -1, "rows", 0, 0]
    with pytest.raises(ValueError):
        evidence.decode_evidence(env)


def _assert_source_tree(actual, expected):
    """Compare exact built-in types and float bits, including NaN payloads."""
    assert type(actual) is type(expected)
    if type(expected) is float:
        assert struct.pack("!d", actual) == struct.pack("!d", expected)
    elif type(expected) is dict:
        assert list(actual) == list(expected)
        for key in expected:
            _assert_source_tree(actual[key], expected[key])
    elif type(expected) is list:
        assert len(actual) == len(expected)
        for left, right in zip(actual, expected, strict=True):
            _assert_source_tree(left, right)
    else:
        assert actual == expected


@pytest.mark.parametrize("root", ["error_code", "page_code", "fields", "rows", "sdk_page"])
def test_codec_supported_subtrees_bounded_compositions(root):
    """Every declared source root accepts scalar/list/dict compositions alike."""
    for bits in ("fff8000000001234", "7ff0000000000001", "7ff0000000000000", "fff0000000000000"):
        special = struct.unpack("!d", bytes.fromhex(bits))[0]
        leaves = [special, "$float64", "$unrepresentable", False, 0, 0.0, -0.0, None, "", [], {}]
        trees = [special, leaves, {"detail": leaves}]
        for _ in range(3):
            trees.append({"status": [trees[-1]], "params": special, "binding": {"": special}})
            trees.append([trees[-1], special])
        trees.append(evidence._copy_value({"detail": [object(), special]}))
        for tree in trees:
            env = dict(version=evidence.VERSION, status="failed", error_code="0", pages=[{}], params={})
            target, key = (env, root) if root == "error_code" else (
                env["pages"][0], "error_code" if root == "page_code" else root
            )
            target[key] = tree
            encoded = b"".join(evidence.encoded_chunks(env))
            wire = json.loads(encoded)
            decoded = evidence.decode_evidence(wire)
            actual = decoded[key] if root == "error_code" else decoded["pages"][0][key]
            _assert_source_tree(actual, tree)
            assert b"".join(evidence.encoded_chunks(env)) == encoded
            assert json.loads(encoded) == wire  # reader never mutates its input


@pytest.mark.parametrize("phase", ["login", "query"])
@pytest.mark.parametrize("shape", ["dict", "list"])
def test_nested_error_code_sdk_round_trip(sdk, monkeypatch, phase, shape):
    """Login and query codes share the same source-value subtree domain."""
    special = struct.unpack("!d", bytes.fromhex("fff8000000001234"))[0]
    result = Result()
    result.error_code = {"detail": [special]} if shape == "dict" else [special]
    if phase == "login":
        monkeypatch.setattr(helpers.bs, "login", lambda: result)
    else:
        monkeypatch.setattr(helpers, "resolve_method", lambda method: lambda **kw: result)
    wire = evidence._query("BaostockStockBasic", {})
    decoded = evidence.decode_evidence(json.loads(json.dumps(wire, allow_nan=False)))
    assert decoded["status"] == "failed"
    _assert_source_tree(decoded["error_code"], result.error_code)
    if phase == "query":
        _assert_source_tree(decoded["pages"][0]["error_code"], result.error_code)
        _assert_source_tree(decoded["pages"][0]["rows"], result.data)
    else:
        assert decoded["pages"] == []


@pytest.mark.parametrize(
    "path",
    [
        ["status"], ["params", "code"], ["binding", "code"], ["operation"],
        ["reason"], ["cleanup"], ["representation"], ["value_types", 0, "bits"],
        ["pages", 0, "status"], ["pages", 0, "params", "code"],
        ["pages", 0, "binding", "code"], ["pages", True, "rows", 0],
        ["pages", -1, "rows", 0], ["error_code", True],
        ["error_code", -1], ["error_code", 1.0], ("error_code", 0),
    ],
)
def test_codec_refuses_paths_outside_source_subtrees(path):
    """New error-code descendants do not authorize metadata or coerced indices."""
    env = dict(
        version=evidence.EXTENDED_VERSION, error_code=["$float64"],
        status="$float64", operation="$float64", reason="$float64", cleanup="$float64",
        representation="$float64", params={"code": "$float64"}, binding={"code": "$float64"},
        pages=[dict(rows=["$float64"], status="$float64", params={"code": "$float64"},
                    binding={"code": "$float64"})],
        value_types=[dict(path=path, type="float64", bits="7ff0000000000000")],
    )
    with pytest.raises(ValueError):
        evidence.decode_evidence(env)


@pytest.mark.parametrize(
    "damage", ["duplicate", "ancestor_first", "descendant_first", "integer_key", "marker",
               "finite", "uppercase", "short", "nonhex", "bits_type", "extra_key"]
)
def test_nested_code_sidecar_conflicts_still_refuse(damage):
    """Strict marker, bits, keys and path conflicts remain fail-closed."""
    env = evidence._wire(dict(version=evidence.VERSION, status="failed", pages=[],
                              error_code={"detail": [float("inf")]}))
    item = env["value_types"][0]
    if damage == "duplicate":
        env["value_types"].append(dict(item))
    elif damage in ("ancestor_first", "descendant_first"):
        ancestor = dict(item, path=["error_code", "detail"])
        env["value_types"].insert(0 if damage == "ancestor_first" else 1, ancestor)
    elif damage == "integer_key":
        env["error_code"] = {0: "$float64"}
        item["path"] = ["error_code", 0]
    elif damage == "marker":
        env["error_code"]["detail"][0] = "not-a-marker"
    elif damage == "extra_key":
        item["unknown"] = 1
    else:
        item["bits"] = dict(finite="0000000000000000", uppercase="7FF0000000000000",
                            short="7ff0", nonhex="x" * 16, bits_type=7)[damage]
    with pytest.raises(ValueError):
        evidence.decode_evidence(env)


def test_daily_etf_literal_and_sdk_evidence(sdk, monkeypatch):
    """The daily operation requires a literal date and preserves SDK pages."""
    calls = []
    def resolve(method):
        calls.append(method)
        return lambda **kwargs: Result()
    monkeypatch.setattr(helpers, "resolve_method", resolve)
    params = {"date": "2026-09-16"}
    assert validate_literal("BaostockDailyHistoryKETF", params) == params
    result = evidence._query("BaostockDailyHistoryKETF", params)
    assert calls == ["query_daily_history_k_ETF"]
    assert result["params"] == params and result["pages"][0]["rows"] == Result.data
    for bad in ({}, {"date": "2026-02-30"}, {"date": "2026-09-16", "extra": "x"}):
        with pytest.raises(ValueError):
            validate_literal("BaostockDailyHistoryKETF", bad)

@pytest.mark.parametrize("count,expected", [(1999, "complete"), (2000, "complete"),
                                             (2001, "complete"), (20000, "partial"),
                                             (20001, "partial")])
def test_daily_etf_single_response_cap(sdk, monkeypatch, count, expected):
    result = Result()
    result.data = [["x"]] * count
    result.next = lambda: pytest.fail("ETF operation must not paginate")
    monkeypatch.setattr(helpers, "resolve_method", lambda method: lambda **kw: result)
    env = evidence._query("BaostockDailyHistoryKETF", {"date": "2024-01-02"})
    assert env["status"] == expected
    assert len(env["pages"][0]["rows"]) == count
    class Sink(evidence.EvidenceSinkV1):
        def __init__(self): self.pages = []
        def source_call(self, kind):
            assert kind != "next"
            return True
        def observe(self, envelope, metadata):
            self.pages.append(envelope)
            return True
    sink = Sink()
    summary = evidence._query_stream("BaostockDailyHistoryKETF", {"date": "2024-01-02"}, sink)
    assert summary["status"] == expected
    assert summary["next_calls"] == 0 and len(sink.pages[0]["pages"][0]["rows"]) == count

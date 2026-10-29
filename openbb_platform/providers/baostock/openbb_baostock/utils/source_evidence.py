"""Opt-in SDK representations, before legacy validation/projection.

Not wire bytes. Error messages and session credentials are deliberately absent.
Each page is a snapshot of fields/data, not a zip into a lossy row dictionary.
"""

import asyncio
import json
import math
import struct
from contextlib import contextmanager
from copy import deepcopy
from threading import local
from typing import Any

from openbb_baostock.utils import helpers

VERSION = "baostock-source-evidence-v1"
OPERATIONS = {
    "BaostockHistoryKDataPlus": "query_history_k_data_plus",
    "BaostockAdjustFactor": "query_adjust_factor",
    "BaostockStockBasic": "query_stock_basic",
    "BaostockTradeDates": "query_trade_dates",
    "BaostockDailyHistoryKETF": "query_daily_history_k_ETF",
}
# Reserve one whole maximum admitted page before advancing the SDK. The SDK
# has no enforceable wire limit; an individual oversize result remains subject
# to the unchanged killable 50 MB ProcessClient boundary.
PAGE_BYTES = 8_000_000
EVIDENCE_BYTES = 24_000_000
MAX_PAGES = 128


EXTENDED_VERSION = "baostock-source-evidence-v2"
_UNREPRESENTABLE = object()


def _copy_value(value, ancestors=()):
    """Copy only SDK value trees, never invoke arbitrary repr/copy hooks."""
    kind = type(value)
    if kind is int and value.bit_length() > 4096:
        return _UNREPRESENTABLE
    if kind in (str, int, float, bool, type(None)):
        return value
    # Leave envelope/sidecar depth below the unchanged raw-v1 JSON depth cap.
    if id(value) in ancestors or len(ancestors) >= 16:
        return _UNREPRESENTABLE
    ancestors = (*ancestors, id(value))
    if kind is list:
        return [_copy_value(item, ancestors) for item in value]
    if kind is dict and all(type(key) is str for key in value):
        return {key: _copy_value(item, ancestors) for key, item in value.items()}
    return _UNREPRESENTABLE


def _encoding(value):
    """Path sidecar disambiguates authored markers from literal SDK strings."""
    types = []

    def visit(item, path):
        if type(item) is float and not math.isfinite(item):
            types.append(dict(path=path, type="float64", bits=struct.pack("!d", item).hex()))
            return "$float64"
        if item is _UNREPRESENTABLE:
            types.append(dict(path=path, type="unrepresentable"))
            return "$unrepresentable"
        if type(item) is list:
            return [visit(child, [*path, index]) for index, child in enumerate(item)]
        if type(item) is dict:
            return {key: visit(child, [*path, key]) for key, child in item.items()}
        return item

    return visit(value, []), types


def _wire(envelope):
    value, types = _encoding(envelope)
    if types:
        value["version"] = EXTENDED_VERSION
        value["value_types"] = types
        if any(item["type"] == "unrepresentable" for item in types):
            value["status"] = "partial" if value["pages"] else "failed"
            value["reason"] = "sdk_value_unrepresentable"
    return value


def decode_evidence(envelope):
    """Decode v2 nonfinite binary64 values; v1 bodies remain read-only compatible.

    An unsupported object is an explicit unknown sentinel, not a reconstructed
    object. No object attributes, credentials or exception messages are exposed.
    """
    value = deepcopy(envelope)
    if value.get("version") == VERSION:
        return value
    if value.get("version") != EXTENDED_VERSION:
        raise ValueError("unsupported_source_evidence_version")
    types = value.pop("value_types", None)
    if type(types) is not list or not types:
        raise ValueError("invalid_source_value_types")
    seen = set()
    for item in types:
        if type(item) is not dict or item.get("type") not in ("float64", "unrepresentable"):
            raise ValueError("invalid_source_value_type")
        expected = {"path", "type", "bits"} if item["type"] == "float64" else {"path", "type"}
        path = item.get("path")
        if set(item) != expected or type(path) is not list or not path:
            raise ValueError("invalid_source_value_path")
        # Only source value locations are encodable; status/binding remain plain.
        if not (
            path[0] == "error_code"
            or (
                len(path) >= 3
                and path[0] == "pages"
                and type(path[1]) is int
                and path[2] in ("error_code", "fields", "rows", "sdk_page")
            )
        ):
            raise ValueError("invalid_source_value_path")
        if any(type(part) not in (str, int) for part in path):
            raise ValueError("invalid_source_value_path")
        identity = tuple(path)
        if identity in seen:
            raise ValueError("duplicate_source_value_path")
        seen.add(identity)
        parent = value
        try:
            for part in path[:-1]:
                if not (
                    (type(parent) is list and type(part) is int and 0 <= part < len(parent))
                    or (type(parent) is dict and type(part) is str)
                ):
                    raise ValueError("invalid_source_value_path")
                parent = parent[part]
            last = path[-1]
            if not (
                (type(parent) is list and type(last) is int and 0 <= last < len(parent))
                or (type(parent) is dict and type(last) is str)
            ):
                raise ValueError("invalid_source_value_path")
            if parent[last] != "$" + item["type"]:
                raise ValueError("source_value_marker_conflict")
            decoded = _UNREPRESENTABLE
            if item["type"] == "float64":
                bits = item["bits"]
                if type(bits) is not str or len(bits) != 16 or any(c not in "0123456789abcdef" for c in bits):
                    raise ValueError("invalid_source_float_bits")
                decoded = struct.unpack("!d", bytes.fromhex(bits))[0]
                if math.isfinite(decoded):
                    raise ValueError("invalid_source_float_bits")
            parent[last] = decoded
        except (KeyError, IndexError, TypeError) as exc:
            raise ValueError("invalid_source_value_path") from exc
    return value


def _success(code):
    return type(code) is str and code == "0"


def _code(result):
    return _copy_value(getattr(result, "error_code", None))


def _snapshot(result):
    return {
        "error_code": _code(result),
        "fields": _copy_value(getattr(result, "fields", None)),
        "rows": _copy_value(getattr(result, "data", None)),
    }


class SourceSession:
    """One provider-owned SDK session; a transport fact, never evidence.

    Only `ok`/`error_code` describe what the source answered. `cleanup` records
    the session's own teardown, which happens after the enclosed calls returned
    and therefore can never be attributed to a single page observation.
    """

    def __init__(self):
        self.ok = False
        self.error_code: Any = None
        self.reason = "login_failed"
        self.authenticated = False
        self.login_calls = 0
        self.logout_calls = 0
        self.cleanup = "ok"


# The SDK keeps one process-global socket. While a session owns it, an enclosed
# query must not take `helpers._SESSION_LOCK` (a non-reentrant Lock), log in
# again, or tear the connection down. The activation marker is thread-local, so
# a query on any other thread keeps the unchanged single-request path.
_ACTIVE = local()


def active_session():
    """Return the session this thread is currently inside, or None."""
    return getattr(_ACTIVE, "session", None)


@contextmanager
def _no_lock():
    """Stand-in for the session lock an inner call must not re-acquire."""
    yield


@contextmanager
def source_session():
    """Open one BaoStock session and reuse it for the enclosed calls.

    Yields `(True, None)` once the session is usable and `(False, error_code)`
    when the source refused the login: a refusal is a value, never an exception.
    Nothing logs in again, logs out or closes the socket until this scope exits.
    """
    if active_session() is not None:
        # Re-entering would deadlock on the non-reentrant session lock.
        raise ValueError("nested_source_session")
    session = SourceSession()
    helpers._SESSION_LOCK.acquire()
    _ACTIVE.session = session
    try:
        session.login_calls += 1
        try:
            login = helpers.bs.login()
        except Exception:
            # No code was returned: the caller learns "not usable" without a
            # fabricated source status.
            session.reason = "login_exception"
        else:
            session.error_code = _code(login)
            session.ok = _success(session.error_code)
            session.authenticated = session.ok
            session.reason = "ok" if session.ok else "login_failed"
        try:
            yield session.ok, (None if session.ok else session.error_code)
        finally:
            try:
                if session.authenticated:
                    session.logout_calls += 1
                    try:
                        if not _success(_code(helpers.bs.logout())):
                            session.cleanup = "logout_failed"
                    except Exception:
                        session.cleanup = "logout_failed"
            finally:
                connection = getattr(helpers.context, "default_socket", None)
                try:
                    if connection is not None:
                        connection.close()
                except Exception:
                    session.cleanup = "socket_close_failed"
                finally:
                    helpers.context.default_socket = None
    finally:
        _ACTIVE.session = None
        helpers._SESSION_LOCK.release()


def _query_values(operation, params, session=None):
    session = active_session() if session is None else session
    method = OPERATIONS[operation]
    envelope: dict[str, Any] = dict(
        version=VERSION,
        operation=operation,
        params=deepcopy(params),
        representation="baostock-sdk-pages-not-wire",
        status="failed",
        error_code=None,
        reason="no_result",
        pages=[],
        cleanup="ok",
    )
    with (helpers._SESSION_LOCK if session is None else _no_lock()):
        logged_in = False
        try:
            if session is None:
                login = helpers.bs.login()
                envelope["error_code"] = _code(login)
                if not _success(envelope["error_code"]):
                    envelope["reason"] = "login_failed"
                    return envelope
                logged_in = True
            elif not session.ok:
                # A refused session never asks the source anything; the caller's
                # own envelope reports the refusal verbatim.
                envelope["error_code"] = session.error_code
                envelope["reason"] = session.reason
                return envelope
            result = helpers.resolve_method(method)(**params)
            if result is None:
                return envelope
            used = 0
            while True:
                page = _snapshot(result)
                size = len(json.dumps(_encoding(page), allow_nan=False).encode())
                envelope["pages"].append(page)
                used += size
                envelope["error_code"] = page["error_code"]
                if not _success(page["error_code"]):
                    envelope["reason"] = "source_error"
                    break
                rows = page["rows"]
                if type(rows) is not list or type(page["fields"]) is not list:
                    envelope["reason"] = "sdk_shape"
                    break
                # Retain duplicate fields and irregular rows verbatim. This is
                # protocol completion, never a business quality decision.
                if operation == "BaostockDailyHistoryKETF":
                    if len(rows) < 20000:
                        envelope.update(status="complete", reason="exhausted")
                    else:
                        envelope["reason"] = "single_response_cap"
                    break
                if len(rows) != helpers.BAOSTOCK_PER_PAGE_COUNT:
                    envelope.update(status="complete", reason="exhausted")
                    break
                if size > PAGE_BYTES or used + PAGE_BYTES > EVIDENCE_BYTES or len(envelope["pages"]) >= MAX_PAGES:
                    envelope["reason"] = "evidence_limit"
                    break
                # BaoStock ResultData.next only requests the next page once the
                # current page has been consumed. No get_row_data projection.
                result.cur_row_num = len(rows)
                previous = _encoding(page)
                try:
                    more = result.next()
                except Exception:
                    changed = _snapshot(result)
                    if _encoding(changed) != previous:
                        envelope["pages"].append(changed)
                    envelope["error_code"] = changed["error_code"]
                    envelope["reason"] = "pagination_exception"
                    break
                if not more and _encoding(_snapshot(result)) == previous:
                    envelope["reason"] = "unconfirmed_full_page"
                    break
                # Even a False next() can carry a received failure page or the
                # successful empty last page; capture that state on this loop.
            if envelope["status"] != "complete" and any(
                _success(p["error_code"]) and p["rows"] for p in envelope["pages"]
            ):
                envelope["status"] = "partial"
        except Exception:
            # No SDK result was returned: never manufacture a response body.
            # The envelope itself honestly records absence, without exception text.
            envelope["reason"] = "sdk_exception"
            if envelope["pages"]:
                envelope["status"] = "partial"
        finally:
            # An enclosing session owns login/logout/socket teardown; closing it
            # here would break every later call in the same session.
            if session is None:
                try:
                    if logged_in and not _success(_code(helpers.bs.logout())):
                        envelope["cleanup"] = "logout_failed"
                except Exception:
                    envelope["cleanup"] = "logout_failed"
                finally:
                    connection = getattr(helpers.context, "default_socket", None)
                    try:
                        if connection is not None:
                            connection.close()
                    except Exception:
                        envelope["cleanup"] = "socket_close_failed"
                    finally:
                        helpers.context.default_socket = None
    return envelope


def _query(operation, params, session=None):
    return _wire(_query_values(operation, params, session))


async def query_evidence(operation, params, evidence_sink_v1=None):
    """Serialize the SDK session; the caller owns the hard deadline."""
    if operation not in OPERATIONS:
        raise ValueError("unsupported_source_operation")
    # The SDK runs on a helper thread, so the session capability is carried
    # across that boundary explicitly instead of relying on thread-local state.
    session = active_session()
    if evidence_sink_v1 is not None:
        if not isinstance(evidence_sink_v1, EvidenceSinkV1):
            raise ValueError("invalid_evidence_sink_v1")
        return await asyncio.to_thread(
            _query_stream, operation, params, evidence_sink_v1, session
        )
    return await asyncio.to_thread(_query, operation, params, session)


class EvidenceSinkV1:
    """Trusted in-process capability, never accepted as a literal SDK parameter."""

    def source_call(self, kind):
        """Authorize the next source effect; false means stop without calling."""
        raise NotImplementedError

    def observe(self, envelope, metadata):
        """Durably capture one SDK state; return continuation permission."""
        raise NotImplementedError


def encoded_chunks(envelope):
    """Encode the same v1/v2 sidecar codec incrementally without truncation.

    Large strings use 4096-character slices; a token is at most 49152 bytes.
    Source list/dict nodes are copied, but string payloads are not duplicated.
    """
    value = _wire(envelope)

    def emit(item):
        if type(item) is str:
            yield b'"'
            for index in range(0, len(item), 4096):
                yield json.dumps(item[index : index + 4096], ensure_ascii=True)[1:-1].encode("ascii")
            yield b'"'
        elif type(item) is list:
            yield b"["
            for index, child in enumerate(item):
                if index:
                    yield b","
                yield from emit(child)
            yield b"]"
        elif type(item) is dict:
            yield b"{"
            for index, (key, child) in enumerate(item.items()):
                if index:
                    yield b","
                yield from emit(key)
                yield b":"
                yield from emit(child)
            yield b"}"
        else:
            yield json.dumps(item, allow_nan=False, separators=(",", ":")).encode("ascii")

    yield from emit(value)


def _login_observation(operation, params, code, reason):
    """One failed login is a received SDK state, never a manufactured body."""
    return dict(
        version=VERSION,
        operation=operation,
        params=params,
        representation="baostock-sdk-pages-not-wire",
        status="failed",
        error_code=code,
        reason=reason,
        pages=[],
        cleanup="ok",
    )


def _stream_login(session, sink, operation, params, summary):
    """Admit one live login; a refusal is recorded as received evidence.

    A session-owned login already happened, so only its verdict is consumed
    here. Nothing is logged out: this call never owns the connection.
    """
    import time

    if session is None:
        if not sink.source_call("login"):
            summary.update(status="cancelled", reason="cancelled")
            return False
        try:
            login = helpers.bs.login()
        except Exception:
            summary["reason"] = "login_exception"
            return False
        code, reason = _code(login), "login_failed"
        if _success(code):
            return True
    else:
        if session.ok:
            return True
        code, reason = session.error_code, session.reason
    sink.observe(
        _login_observation(operation, params, code, reason),
        dict(trigger="login_return", previous_state_sha256=None, received_ns=time.time_ns()),
    )
    summary.update(reason=reason, pages=1)
    return False


def _stream_teardown(session, summary, logged_in):
    """Close a connection this call owns; an enclosing session owns its own."""
    if session is not None:
        return "ok"
    try:
        if logged_in and not _success(_code(helpers.bs.logout())):
            summary["cleanup"] = "logout_failed"
    except Exception:
        summary["cleanup"] = "logout_failed"
    finally:
        connection = getattr(helpers.context, "default_socket", None)
        try:
            if connection is not None:
                connection.close()
        except Exception:
            summary["cleanup"] = "socket_close_failed"
        finally:
            helpers.context.default_socket = None
    return summary["cleanup"]


def _query_stream(operation, params, sink, session=None):
    """One live session; no next until the collector's durable page ACK.

    Only SDK calls are caught as SDK errors. Sink/IPC/durability failures escape
    unchanged through cleanup. The summary never carries source-sized values.
    """
    import hashlib
    import time

    summary = dict(
        version="baostock-page-summary-v1",
        status="failed",
        reason="no_result",
        pages=0,
        query_calls=0,
        next_calls=0,
        cleanup="ok",
        unchanged_sdk_state=False,
    )
    logged_in = False
    primary = None
    session = active_session() if session is None else session
    with (helpers._SESSION_LOCK if session is None else _no_lock()):
        try:
            if not _stream_login(session, sink, operation, params, summary):
                return summary
            logged_in = True
            if not sink.source_call("query"):
                summary.update(status="cancelled", reason="cancelled")
                return summary
            summary["query_calls"] += 1
            try:
                result = helpers.resolve_method(OPERATIONS[operation])(**params)
            except Exception:
                summary["reason"] = "query_exception"
                return summary
            received_ns = time.time_ns()
            if result is None:
                return summary
            previous = None
            trigger = "query_return"
            next_failed = False
            more = True
            while True:
                page = _snapshot(result)
                page["sdk_page"] = _copy_value(getattr(result, "cur_page_num", None))
                env = dict(
                    version=VERSION,
                    operation=operation,
                    params=params,
                    representation="baostock-sdk-pages-not-wire",
                    status="partial",
                    error_code=page["error_code"],
                    reason="page_observation",
                    pages=[page],
                    cleanup="ok",
                )
                state_hash = hashlib.sha256()
                for chunk in encoded_chunks(env):
                    state_hash.update(chunk)
                current = state_hash.hexdigest()
                if previous == current and (next_failed or not more):
                    summary.update(
                        status="partial",
                        reason="pagination_exception" if next_failed else "unconfirmed_full_page",
                        unchanged_sdk_state=True,
                    )
                    break
                proceed = sink.observe(
                    env, dict(trigger=trigger, previous_state_sha256=previous, received_ns=received_ns)
                )
                summary["pages"] += 1
                previous = current
                if next_failed:
                    summary.update(status="partial", reason="pagination_exception")
                    break
                if not _success(page["error_code"]):
                    summary.update(status="failed" if summary["pages"] == 1 else "partial", reason="source_error")
                    break
                if type(page["rows"]) is not list or type(page["fields"]) is not list:
                    summary.update(status="partial", reason="sdk_shape")
                    break
                if any(item["type"] == "unrepresentable" for item in _encoding(page)[1]):
                    summary.update(status="partial", reason="sdk_value_unrepresentable")
                    break
                if operation == "BaostockDailyHistoryKETF":
                    if len(page["rows"]) < 20000:
                        summary.update(status="complete", reason="exhausted")
                    else:
                        summary.update(status="partial", reason="single_response_cap")
                    break
                if len(page["rows"]) != helpers.BAOSTOCK_PER_PAGE_COUNT:
                    summary.update(status="complete", reason="exhausted")
                    break
                if not proceed:
                    summary.update(status="partial", reason="collector_stop")
                    break
                if not sink.source_call("next"):
                    summary.update(status="partial", reason="collector_stop")
                    break
                result.cur_row_num = len(page["rows"])
                del page, env
                summary["next_calls"] += 1
                trigger = "next_return"
                try:
                    more = result.next()
                except Exception:
                    next_failed = True
                    trigger = "next_raise"
                received_ns = time.time_ns()
        except BaseException as exc:
            primary = exc
            raise
        finally:
            # A session owns login/logout/socket teardown; an in-session task
            # must not disconnect it from under the tasks that follow.
            _stream_teardown(session, summary, logged_in)
            if primary is not None and summary["cleanup"] != "ok":
                primary.add_note("baostock_cleanup_failed")
    return summary

# Keep acquisition-only imports free of provider/parsing/journal initialization.
# pylint: disable=import-outside-toplevel

"""Structural decoding of captured ETF option bodies.  No business verdicts.

Everything here runs **after** the bytes are already in private evidence.  A body
that cannot be read produces a typed :class:`ParseError`; it never deletes the
bytes, and it never decides whether the day is acceptable.  Strike/expiry
semantics, contract identity, quote quality, snapshot shrink and expiry coverage
live in :mod:`.quality` and are applied by the caller afterwards.

The provider's *pure* parsers are reused (they take text/bytes and return rows) so
this lane decodes the same way the provider does.  The provider's *transport and
orchestration* are not reused: those bytes are captured first, by
:mod:`.source`.
"""

from __future__ import annotations

from typing import Any

QUOTE_KIND = "quote-batch"
SSE_MASTER_KIND = "sse-official-master"
SZSE_MASTER_KIND = "szse-official-master"
PROVIDER_ENVELOPE_KIND = "provider-envelope"

# What the structural reading did to the captured bytes before a row was delivered.
#
# These lists are *claims about this module's behaviour* and the delivery reader
# re-derives them from the document kind, so a lane cannot describe a numeric
# conversion it did not do -- nor omit one it did.  Named steps, not adjectives:
#
# * ``gb18030_decode`` -- the quote layout is GB18030 text, decoded strictly;
# * ``assignment_extracted_per_symbol`` -- ``var hq_str_CON_OP_<symbol>="...";``;
# * ``csv_split_on_comma`` -- the payload is one comma-separated record;
# * ``field_labels_applied_43`` -- the first 43 values are mapped onto ``FIELDS``
#   (the 43/51 width is enforced; a new width fails closed);
# * ``numeric_text_to_float`` -- every number in the row is a float already, so
#   ``quality.number`` re-floats a float and is not the conversion point;
# * ``source_time_parsed_to_iso8601`` -- ``行情时间`` becomes ``+08:00`` ISO text;
# * ``option_type_expiration_from_appended_tail`` -- on a 51-wide record the
#   appended tail supplies call/put and the expiry date.
TRANSFORMS = {
    QUOTE_KIND: [
        "gb18030_decode",
        "assignment_extracted_per_symbol",
        "csv_split_on_comma",
        "field_labels_applied_43",
        "numeric_text_to_float",
        "source_time_parsed_to_iso8601",
        "underlying_and_status_extracted",
        "option_type_expiration_from_appended_tail",
    ],
    SSE_MASTER_KIND: [
        "utf8_json_decode",
        "sse_header_mapping",
        "identifier_width_and_scope_checked",
        "strike_and_unit_text_to_float",
        "dates_normalized_to_iso8601",
        "metadata_source_stamped",
    ],
    SZSE_MASTER_KIND: [
        "xlsx_workbook_decoded",
        "header_row_mapping",
        "identifier_width_and_scope_checked",
        "strike_and_unit_text_to_float",
        "dates_normalized_to_iso8601",
        "metadata_source_stamped",
    ],
    PROVIDER_ENVELOPE_KIND: ["provider_response_json"],
}
# Where the trade date of a delivered row comes from.  A quote row states its own
# ``source_time`` but not the session it belongs to: the session is the requested
# date.  An official master row states ``source_date`` itself.  A provider
# envelope (the aggregate Sina master and the calendar) carries rows whose date
# column was decoded by the provider, so it is row-derived -- not, as this table
# said in round 1, a request parameter.
DATE_SOURCE = {
    QUOTE_KIND: "request_parameter",
    SSE_MASTER_KIND: "row",
    SZSE_MASTER_KIND: "row",
    PROVIDER_ENVELOPE_KIND: "row",
}


class ParseError(ValueError):
    """A typed structural refusal.  The captured bytes stay on disk."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


def _safe(exc: BaseException, fallback: str) -> str:
    text = str(exc)
    if isinstance(exc, ValueError) and text.isidentifier() and len(text) <= 64:
        return text
    return fallback


def _text(body: bytes, code: str) -> str:
    try:
        return body.decode("gb18030", errors="strict")
    except UnicodeDecodeError as exc:
        raise ParseError(code) from exc


def _utf8(body: bytes, code: str) -> str:
    try:
        return body.decode("utf-8", errors="strict")
    except UnicodeDecodeError as exc:
        raise ParseError(code) from exc


def quote_rows(
    body: bytes, symbols: list[str], captured_at: str
) -> list[dict[str, Any]]:
    """Decode one Sina quote batch.  Missing symbols become diagnostic rows."""
    from openbb_akshare.models.etf_options import parse_batch

    text = _text(body, "undecodable_quote_body")
    try:
        rows = parse_batch(text, symbols, captured_at)
    except (ValueError, IndexError) as exc:
        raise ParseError(_safe(exc, "malformed_quote_body")) from exc
    return rows


def sse_master_rows(body: bytes) -> list[dict[str, Any]]:
    """Decode the SSE official contract JSON document."""
    import json

    from openbb_akshare.models.etf_options import parse_sse_official

    text = _utf8(body, "undecodable_master_body")
    try:
        value = json.loads(text)
    except ValueError as exc:
        raise ParseError("malformed_master_json") from exc
    if not isinstance(value, dict):
        raise ParseError("invalid_contract_json")
    try:
        return parse_sse_official(value)
    except (ValueError, TypeError) as exc:
        raise ParseError(_safe(exc, "malformed_contract_master")) from exc


def szse_master_rows(body: bytes) -> list[dict[str, Any]]:
    """Decode the SZSE official contract workbook (binary; never decoded as text)."""
    from openbb_akshare.models.etf_options import parse_szse_official

    try:
        return parse_szse_official(body)
    except (ValueError, TypeError, KeyError) as exc:
        raise ParseError(_safe(exc, "malformed_contract_master")) from exc


def provider_rows(body: bytes) -> list[dict[str, Any]]:
    """Decode the provider's own response envelope (the aggregate lanes).

    Only used where the source genuinely cannot be addressed as one document: the
    Sina SSE master and the trade-date calendar.  It is labelled
    ``provider-transformed`` and never described as source bytes.
    """
    import json

    text = _utf8(body, "undecodable_envelope")
    try:
        value = json.loads(text)
    except ValueError as exc:
        raise ParseError("malformed_envelope_json") from exc
    if not isinstance(value, dict) or not isinstance(value.get("rows"), list):
        raise ParseError("invalid_envelope")
    return [row for row in value["rows"] if isinstance(row, dict)]


def rows_of(
    body: bytes, kind: str, params: dict[str, Any], captured_at: str
) -> list[dict[str, Any]]:
    """One dispatcher, one return type: the rows a captured body literally yields."""
    if kind == QUOTE_KIND:
        symbols = [item for item in str(params.get("symbols", "")).split(",") if item]
        return quote_rows(body, symbols, captured_at)
    if kind == SSE_MASTER_KIND:
        return sse_master_rows(body)
    if kind == SZSE_MASTER_KIND:
        return szse_master_rows(body)
    if kind == PROVIDER_ENVELOPE_KIND:
        return provider_rows(body)
    raise ParseError("unknown_document_kind")


UNIT_KEYS = ("price_unit", "size_unit", "volume_unit", "turnover_unit")


def provenance(
    kind: str,
    rows: list[dict[str, Any]],
    unit_source: str,
    unit_labels: dict | None = None,
) -> dict[str, Any]:
    """Facts about the reading, never about acceptance.

    ``unit_source``/``unit_labels`` are the one pair here that the bytes cannot
    state: they come from the deployment's ``units:`` block.  ``read_delivery``
    therefore re-derives everything else from the capture and only checks that
    this pair agrees with itself (labels present ⇔ not ``absent``).
    """
    labels = dict(unit_labels or {})
    return {
        "kind": kind,
        "date_source": DATE_SOURCE[kind],
        "date_column": "source_date" if kind != QUOTE_KIND else None,
        "unit_source": unit_source,
        "unit_labels": {key: labels.get(key) for key in UNIT_KEYS},
        "transforms": list(TRANSFORMS[kind]),
        "row_count": len(rows),
    }

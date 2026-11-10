"""Offline acceptance against a caller-supplied, real hash-verified NAS sample.

These tests never download fixtures or generate replacement source records.
"""

import json
import os
from pathlib import Path

import pytest

from openbb_collector_core.baostock_postprocess import (
    FIELDS,
    load_manifest,
    materialize,
    parse,
    parse_capture,
    sha,
)


@pytest.fixture(scope="module")
def evidence():
    configured = os.environ.get("BAOSTOCK18_REAL_SAMPLE")
    if not configured:
        pytest.skip("Set BAOSTOCK18_REAL_SAMPLE to a verified real sample output root")
    return Path(configured)


@pytest.fixture(scope="module")
def candidate(evidence, tmp_path_factory):
    """Exercise this checkout against immutable real inputs in a fresh directory."""
    output = tmp_path_factory.mktemp("baostock18-candidate")
    parse(evidence / "sample_manifest.jsonl", evidence / "raw", output)
    summary = json.loads((output / "parse_summary.json").read_text())
    assert summary["parse_complete"] is True
    assert summary.get("failed_captures", 0) == 0
    materialize(output, evidence / "evidence", buckets=4)
    return output


@pytest.fixture(scope="module")
def catalog(candidate):
    import duckdb

    with duckdb.connect(str(candidate / "candidate.duckdb"), read_only=True) as con:
        yield con


def test_real_conservation_and_literal_dedup(catalog):
    raw = catalog.execute("SELECT count(*) FROM raw_observations").fetchone()[0]
    variants, observed = catalog.execute(
        "SELECT count(*),sum(observation_count) FROM literal18_variants"
    ).fetchone()
    lineage = catalog.execute("SELECT count(*) FROM row_lineage").fetchone()[0]
    assert raw == observed == lineage
    assert variants < raw
    fields = ",".join('"' + f + '"' for f in FIELDS)
    independent = catalog.execute(
        "SELECT count(*) FROM (SELECT DISTINCT "
        + fields
        + ",requested_adjustflag FROM raw_observations)"
    ).fetchone()[0]
    assert independent == variants
    # Same business identity with different literals must survive DISTINCT.
    assert (
        catalog.execute(
            "SELECT count(*) FROM (SELECT code,date,requested_adjustflag,count(*) n "
            "FROM literal18_variants GROUP BY ALL HAVING n>1)"
        ).fetchone()[0]
        == catalog.execute(
            "SELECT count(*) FROM (SELECT DISTINCT code,date,requested_adjustflag FROM conflicts)"
        ).fetchone()[0]
    )


def test_real_partial_and_source_anomalies_retained(catalog):
    assert (
        catalog.execute(
            "SELECT count(*) FROM capture_inventory WHERE confirmed_partial_data "
            "AND terminal_status='partial' AND NOT complete"
        ).fetchone()[0]
        == 6
    )
    expected = catalog.execute(
        "SELECT sum(rows) FROM capture_inventory WHERE confirmed_partial_data"
    ).fetchone()[0]
    assert (
        catalog.execute(
            "SELECT count(*) FROM row_lineage WHERE confirmed_partial_data AND NOT complete"
        ).fetchone()[0]
        == expected
    )
    assert (
        catalog.execute(
            "SELECT count(*) FROM literal18_variants WHERE adjustment_mismatch"
        ).fetchone()[0]
        > 0
    )
    assert (
        catalog.execute("SELECT count(*) FROM literal18_variants WHERE turn=''").fetchone()[0] > 0
    )
    assert (
        catalog.execute(
            "SELECT count(*) FROM equity_forward_dynamic WHERE source_adjustflag<>'2' "
            "OR fixed_qfq_basis_date IS NOT NULL"
        ).fetchone()[0]
        == 0
    )


def test_real_row_traceback(evidence, catalog):
    rows = (
        catalog.execute(
            "SELECT v.*, l.capture_id AS origin_capture,l.capture_page,l.sdk_page,"
            "l.page_row_index,l.source_body_sha256,c.relative "
            "FROM literal18_variants v JOIN row_lineage l USING (value_id) "
            "JOIN capture_inventory c ON l.capture_id=c.capture_id "
            "WHERE l.confirmed_partial_data ORDER BY v.code,v.date LIMIT 8"
        )
        .fetchdf()
        .to_dict("records")
    )
    import hashlib

    for row in rows:
        path = evidence / "raw" / row["relative"] / f"page-{row['capture_page']:03d}.body"
        encoded = path.read_bytes()
        assert hashlib.sha256(encoded).hexdigest() == row["source_body_sha256"]
        env = json.loads(encoded)
        page = next(p for p in env["pages"] if str(p["sdk_page"]) == row["sdk_page"])
        assert page["rows"][row["page_row_index"]] == [row[f] for f in FIELDS]


def test_wrong_external_anchor_refused(evidence):
    record = load_manifest(evidence / "sample_manifest.jsonl")[0]
    invalid = dict(record, anchor="0" * 64)
    with pytest.raises(ValueError, match="page_capture_anchor"):
        parse_capture(evidence / "raw", invalid)


def test_existing_openbb_fetcher_real_query(candidate):
    from openbb_duckdb.models.equity_historical import DuckDBEquityHistoricalFetcher as fetcher

    for table, adjustment in [
        ("equity_unadjusted", "unadjusted"),
        ("equity_forward_dynamic", "forward"),
    ]:
        query = fetcher.transform_query(
            {
                "symbol": "sh.600000",
                "start_date": "2026-10-13",
                "end_date": "2026-10-15",
                "database_path": str(candidate / "candidate.duckdb"),
                "table": table,
                "adjustment": adjustment,
            }
        )
        result = fetcher.transform_data(query, fetcher.extract_data(query, credentials={}))
        assert len(result) == 3
        assert all(r.symbol == "SH.600000" and r.close > 0 for r in result)


def test_captured_calendar_uncertainty_is_not_silently_cleared(catalog):
    expected = catalog.execute(
        "SELECT count(*) FROM literal18_variants v JOIN calendar_review k "
        "ON v.code=k.code AND v.date=k.date AND v.requested_adjustflag=k.adjustflag "
        "WHERE k.new_status<>'independent_session_present'"
    ).fetchone()[0]
    assert expected > 0
    actual = catalog.execute(
        "SELECT count(*) FROM literal18_variants WHERE calendar_unknown"
    ).fetchone()[0]
    assert actual == expected
    assert (
        catalog.execute(
            "SELECT count(*) FROM literal18_variants v JOIN calendar_review k "
            "ON v.code=k.code AND v.date=k.date AND v.requested_adjustflag=k.adjustflag "
            "WHERE k.new_status='independent_session_present' AND v.calendar_unknown"
        ).fetchone()[0]
        == 0
    )


def test_failed_duplicate_does_not_veto_confirmed_partial_data(catalog, candidate):
    assert (
        catalog.execute(
            "SELECT count(*) FROM literal18_variants WHERE source_status_warning "
            "AND has_confirmed_partial AND has_admissible_source"
        ).fetchone()[0]
        > 0
    )
    assert (
        catalog.execute(
            "SELECT count(*) FROM literal18_variants WHERE has_confirmed_partial "
            "AND NOT has_admissible_source"
        ).fetchone()[0]
        == 0
    )
    from openbb_duckdb.models.equity_historical import DuckDBEquityHistoricalFetcher as fetcher

    query = fetcher.transform_query(
        {
            "symbol": "sh.601600",
            "start_date": "2007-04-30",
            "end_date": "2007-04-30",
            "database_path": str(candidate / "candidate.duckdb"),
            "table": "equity_unadjusted",
            "adjustment": "unadjusted",
            "limit": 10,
        }
    )
    result = fetcher.transform_data(query, fetcher.extract_data(query, credentials={}))
    assert len(result) == 1
    assert result[0].model_dump()["has_confirmed_partial"] is True
    assert result[0].model_dump()["source_status_warning"] is True


def test_real_resume_preserves_committed_artifacts(evidence, candidate):
    paths = [candidate / "candidate.duckdb", candidate / "parse_contract.json"]
    for directory in ("parsed", "checkpoints", "variants", "lineage", "quality"):
        paths.extend(sorted((candidate / directory).iterdir()))
    before = {path: sha(path) for path in paths}
    parse(evidence / "sample_manifest.jsonl", evidence / "raw", candidate)
    materialize(candidate, evidence / "evidence", buckets=4)
    assert {path: sha(path) for path in paths} == before

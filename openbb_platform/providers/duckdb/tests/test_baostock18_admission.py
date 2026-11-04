"""Offline synthetic fixtures test transactions and actual consumption semantics."""

# Synthetic SQL interpolates only internal fixture field names and schema.
# ruff: noqa: S608

import json
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import duckdb
import pytest
from openbb_duckdb.baostock18 import import_candidate, prepare_manifest, read_bars, set_visible
from openbb_duckdb.baostock18.contract import REJECT_FLAGS, REVIEW_FLAGS, SCHEMA, AdmissionError


def make_candidate(path, *, rows=1, quarantined=0, conditional=False, close="10.1000"):
    """Real DuckDB relations with the candidate contract and source evidence."""
    flags = ",".join(f'false AS "{f}"' for f in (*REJECT_FLAGS, *REVIEW_FLAGS))
    with duckdb.connect(str(path)) as c:
        c.execute("CREATE TABLE candidate_contract AS SELECT 'test-contract' AS contract")
        c.execute(
            "CREATE TABLE literal18_variants AS SELECT "
            "cast(DATE '2000-01-01'+cast(i AS INTEGER) AS VARCHAR) AS date,"
            "'sh.600000' AS code,'10.0000' AS open,'12' AS high,'9' AS low,? AS close,"
            "'9.8' AS preclose,'100' AS volume,'1000.23' AS amount,'3' AS adjustflag,"
            "'1.2500' AS turn,'1' AS tradestatus,'-0.123456' AS pctChg,'-10' AS peTTM,"
            "'2' AS psTTM,'3' AS pcfNcfTTM,'4' AS pbMRQ,'' AS isST,"
            "'3' AS requested_adjustflag,lpad(cast(i AS VARCHAR),64,'0') AS value_id,"
            "1::BIGINT AS variant_count,true AS has_admissible_source," + flags +
            " FROM range(?) r(i)", [close, rows + quarantined],
        )
        c.execute("UPDATE literal18_variants SET adjustment_mismatch=true,requested_adjustflag='2' "
                  "WHERE cast(value_id AS BIGINT)>=?", [rows])
        if conditional:
            c.execute("UPDATE literal18_variants SET has_confirmed_partial=true,source_status_warning=true")
        c.execute(
            "CREATE TABLE row_lineage AS SELECT value_id,'capture' AS capture_id,repeat('a',64) AS capture_sha256,"
            "repeat('b',64) AS source_body_sha256,repeat('c',64) AS source_receipt_sha256,"
            "0::INTEGER AS capture_page,'' AS sdk_page,cast(row_number() over()-1 AS INTEGER) AS page_row_index,"
            "cast(row_number() over()-1 AS INTEGER) AS capture_row_index,? AS complete,"
            "? AS confirmed_partial_data,true AS source_ok FROM literal18_variants", [not conditional, conditional],
        )
        c.execute(
            "CREATE TABLE capture_inventory AS SELECT 'capture' AS capture_id,'raw/capture.json.gz' AS relative,"
            "? AS historical_status,? AS terminal_status,123::BIGINT AS authority_received_ns",
            ["failed" if conditional else "success", "partial" if conditional else "complete"],
        )
    return prepare_manifest(path, end_date="2300-01-01")


def query(path, sql, args=None):
    """Inspect committed state with a separate read-only connection."""
    with duckdb.connect(str(path), read_only=True) as c:
        return c.execute(sql, args or []).fetchall()


def read(path, **kwargs):
    """Query the explicit fixture symbol/date scope."""
    return read_bars(path, symbols=["SH.600000"], start_date=date(1990, 1, 1),
                     end_date=date(2300, 1, 1), **kwargs)


def test_idempotent_replay_and_exact_values(tmp_path):
    """Replaying never duplicates rows or changes a visible revision."""
    manifest = make_candidate(tmp_path / "input.duckdb")
    target = tmp_path / "output.duckdb"
    first = import_candidate(manifest, target)
    assert first["rows"] == first["clean"] == 1
    assert read(target, basis="unadjusted") == []
    second = import_candidate(manifest, target)
    assert second["new_batches"] == 0 and second["rows"] == first["rows"]
    event = set_visible(target, first["snapshot_id"], basis="unadjusted", expected_clean_rows=1)
    assert not set_visible(target, first["snapshot_id"], basis="unadjusted", expected_clean_rows=1)["changed"]
    result = read(target, basis="unadjusted")[0]
    assert result["close"] == Decimal("10.1000")
    assert result["turnover_rate"] == Decimal("0.0125")
    assert result["change_percent"] == Decimal("-0.00123456")
    assert result["source_available_at"] is None and result["is_st"] is None
    assert json.loads(result["raw18_json"])["isST"] == ""
    assert result["known_at"].isoformat() == event["known_at"]


def test_interrupt_rolls_back_whole_batch_then_resume(tmp_path):
    """Interrupt batch two after its writes; only the first batch survives."""
    manifest = make_candidate(tmp_path / "input.duckdb", rows=50_001)
    target = tmp_path / "output.duckdb"

    def interrupt(connection, number):
        if number == 2:
            assert connection.execute(f"SELECT count(*) FROM {SCHEMA}.literal_version").fetchone()[0] == 50_001
            raise KeyboardInterrupt("simulated interruption after bars/lineage, before receipt")

    with pytest.raises(KeyboardInterrupt):
        import_candidate(manifest, target, fault_hook=interrupt)
    for table in ("literal_version", "row_lineage", "bar_version"):
        assert query(target, f"SELECT count(*) FROM {SCHEMA}.{table}") == [(50_000,)]
    assert query(target, f"SELECT count(*) FROM {SCHEMA}.batch_receipt") == [(1,)]
    assert query(target, f"SELECT state FROM {SCHEMA}.snapshot") == [("staging",)]
    with pytest.raises(AdmissionError, match="ready"):
        set_visible(target, query(target, f"SELECT snapshot_id FROM {SCHEMA}.snapshot")[0][0],
                    basis="unadjusted", expected_clean_rows=50_001)
    assert read(target, basis="unadjusted") == []
    resumed = import_candidate(manifest, target)
    assert resumed["new_batches"] == 1 and resumed["rows"] == 50_001 and resumed["batches"] == 2


def test_partial_warning_and_terminal_status_preserved(tmp_path):
    """Original failed partial status remains traceable and unconsumable."""
    manifest = make_candidate(tmp_path / "input.duckdb", conditional=True)
    target = tmp_path / "output.duckdb"
    receipt = import_candidate(manifest, target)
    assert receipt["conditional"] == 1 and receipt["clean"] == 0
    assert query(target, f"SELECT complete,confirmed_partial_data,historical_status,terminal_status "
                 f"FROM {SCHEMA}.row_lineage") == [(False, True, "failed", "partial")]
    flags = json.loads(query(target, f"SELECT flags_json FROM {SCHEMA}.literal_version")[0][0])
    assert flags["has_confirmed_partial"] and flags["source_status_warning"]
    assert query(target, f"SELECT count(*) FROM {SCHEMA}.quality") == [(2,)]
    assert read(target, basis="unadjusted") == []


def test_15838_quarantined_versions_never_consumed(tmp_path):
    """A rejected cohort stays outside both typed bars and consumption."""
    manifest = make_candidate(tmp_path / "input.duckdb", quarantined=15_838)
    target = tmp_path / "output.duckdb"
    receipt = import_candidate(manifest, target)
    assert receipt["rows"] == 15_839 and receipt["quarantine"] == 15_838
    set_visible(target, receipt["snapshot_id"], basis="unadjusted", expected_clean_rows=1)
    assert len(read(target, basis="unadjusted")) == 1
    assert query(target, f"SELECT count(*) FROM {SCHEMA}.consumable") == [(1,)]
    assert query(target, f"SELECT count(*) FROM {SCHEMA}.bar_version") == [(1,)]


@pytest.mark.parametrize("kwargs,match", [
    ({}, "explicit basis"), ({"basis": "forward"}, "not been admitted"),
    ({"basis": "forward_dynamic_snapshot"}, "not been admitted"),
    ({"basis": "backward"}, "not been admitted"),
    ({"basis": "unadjusted", "fixed_qfq_basis_date": date(2026, 10, 15)}, "Fixed 2026-10-15"),
])
def test_missing_wrong_or_fixed_basis_rejected_before_open(tmp_path, kwargs, match):
    """Ambiguous adjustment requests fail before any database access."""
    with pytest.raises(AdmissionError, match=match):
        read(tmp_path / "nonexistent.duckdb", **kwargs)


def test_asof_correction_withdrawal_and_no_fallback(tmp_path):
    """Correction and withdrawal respect past visibility, including date removal."""
    before = datetime.now(timezone.utc) - timedelta(days=1)
    one = make_candidate(tmp_path / "one.duckdb", rows=2)
    two = make_candidate(tmp_path / "two.duckdb", close="11")
    target = tmp_path / "output.duckdb"
    r1 = import_candidate(one, target)
    e1 = set_visible(target, r1["snapshot_id"], basis="unadjusted", expected_clean_rows=2)
    r2 = import_candidate(two, target)
    e2 = set_visible(target, r2["snapshot_id"], basis="unadjusted", expected_clean_rows=1)
    assert read(target, basis="unadjusted", as_of=before) == []
    at_one = read(target, basis="unadjusted", as_of=datetime.fromisoformat(e1["known_at"]))
    at_two = read(target, basis="unadjusted", as_of=datetime.fromisoformat(e2["known_at"]))
    assert len(at_one) == 2 and at_one[0]["close"] == Decimal("10.1")
    assert len(at_two) == 1 and at_two[0]["close"] == Decimal("11")
    set_visible(target, None, basis="unadjusted")
    assert read(target, basis="unadjusted") == []
    assert len(read(target, basis="unadjusted", as_of=datetime.fromisoformat(e1["known_at"]))) == 2


def test_hash_mismatch_and_foreign_catalog_refused(tmp_path):
    """Refuse changed inputs and inspect a foreign catalog without modifying it."""
    manifest = make_candidate(tmp_path / "input.duckdb")
    foreign = tmp_path / "foreign.duckdb"
    with duckdb.connect(str(foreign)) as c:
        c.execute("CREATE SCHEMA a_share")
        c.execute("CREATE TABLE a_share.bar_revision (i INTEGER)")
    with pytest.raises(AdmissionError, match="other datasets"):
        import_candidate(manifest, foreign)
    assert query(foreign, "SHOW ALL TABLES")[0][1:3] == ("a_share", "bar_revision")
    with duckdb.connect(str(tmp_path / "input.duckdb")) as c:
        c.execute("UPDATE literal18_variants SET close='11'")
    with pytest.raises(AdmissionError, match="hash mismatch"):
        import_candidate(manifest, tmp_path / "never_created.duckdb")
    assert not (tmp_path / "never_created.duckdb").exists()


def test_explicit_rehearsal_receipt_preserves_foreign_data(tmp_path):
    """A receipt-bound clone can migrate, resume and expose only rehearsal data."""
    manifest = make_candidate(tmp_path / "input.duckdb")
    target = tmp_path / "rehearsal.duckdb"
    with duckdb.connect(str(target)) as c:
        c.execute("CREATE SCHEMA a_share")
        c.execute("CREATE TABLE a_share.bar_revision AS SELECT 42 AS sentinel")
    stat = target.stat()
    receipt = tmp_path / "clone.json"
    binding = {
        "format": "baostock18-rehearsal-clone-v1",
        "purpose": "migration rehearsal only; NOT a production backup",
        "production_admitted": False, "target_path": str(target),
        "source_path": str(tmp_path / "production.duckdb"),
        "target_identity": [stat.st_dev, stat.st_ino],
        "baseline_tables": [["a_share", "bar_revision"]],
    }
    receipt.write_text(json.dumps(binding))
    first = import_candidate(manifest, target, rehearsal_receipt=receipt)
    assert import_candidate(manifest, target, rehearsal_receipt=receipt)["new_batches"] == 0
    set_visible(target, first["snapshot_id"], basis="unadjusted",
                expected_clean_rows=1, rehearsal_receipt=receipt)
    assert len(read(target, basis="unadjusted")) == 1
    assert query(target, "SELECT * FROM a_share.bar_revision") == [(42,)]
    with pytest.raises(AdmissionError, match="other datasets"):
        import_candidate(manifest, target)
    for key, value, error in [
        ("source_path", str(target), "authorize"),
        ("source_identity", binding["target_identity"], "identity"),
        ("target_identity", [0, 0], "identity"),
        ("baseline_tables", [["a_share", "other_table"]], "inventory"),
        ("production_admitted", True, "Invalid"),
    ]:
        receipt.write_text(json.dumps({**binding, key: value}))
        with pytest.raises(AdmissionError, match=error):
            import_candidate(manifest, target, rehearsal_receipt=receipt)
    assert query(target, "SELECT * FROM a_share.bar_revision") == [(42,)]
    receipt.write_text(json.dumps(binding))
    with duckdb.connect(str(target)) as c:
        c.execute("DROP SCHEMA baostock18_admission CASCADE")
        c.execute("DROP TABLE a_share.bar_revision")
    with pytest.raises(AdmissionError, match="inventory"):
        import_candidate(manifest, target, rehearsal_receipt=receipt)
    assert query(target, "SELECT schema_name FROM duckdb_tables() WHERE NOT internal") == []


def test_rehearsal_receipt_cannot_initialize_missing_clone_or_bypass_policy(tmp_path):
    """Fail before opening a writer when the clone is missing or has a policy."""
    manifest = make_candidate(tmp_path / "input.duckdb")
    target = tmp_path / "rehearsal.duckdb"
    receipt = tmp_path / "clone.json"
    receipt.write_text(json.dumps({
        "format": "baostock18-rehearsal-clone-v1",
        "purpose": "migration rehearsal only; NOT a production backup",
        "production_admitted": False, "target_path": str(target),
        "source_path": str(tmp_path / "production.duckdb"),
    }))
    with pytest.raises(AdmissionError, match="authorize"):
        import_candidate(manifest, target, rehearsal_receipt=receipt)
    assert not target.exists()
    target.with_name(target.name + ".access-windows").mkdir()
    with pytest.raises(AdmissionError, match="policy"):
        import_candidate(manifest, target, rehearsal_receipt=receipt)
    assert not target.exists()


def test_numeric_precision_missing_lineage_and_limits(tmp_path):
    """Do not round excess scale or silently discard missing provenance."""
    input_path = tmp_path / "input.duckdb"
    make_candidate(input_path, rows=2)
    with duckdb.connect(str(input_path)) as c:
        c.execute("UPDATE literal18_variants SET turn='1.12345678901234567' WHERE value_id LIKE '%0'")
    manifest = prepare_manifest(input_path)
    target = tmp_path / "out.duckdb"
    receipt = import_candidate(manifest, target)
    assert receipt["quarantine"] == 1
    set_visible(target, receipt["snapshot_id"], basis="unadjusted", expected_clean_rows=1)
    with pytest.raises(AdmissionError, match="explicit timezone"):
        read(target, basis="unadjusted", as_of=datetime(2026, 1, 1))
    with duckdb.connect(str(input_path)) as c:
        c.execute("DELETE FROM row_lineage")
    manifest = prepare_manifest(input_path)
    with pytest.raises(AdmissionError, match="Missing row lineage"):
        import_candidate(manifest, tmp_path / "missing.duckdb")


def test_manifest_binds_external_parquet_and_refuses_omission(tmp_path):
    """The tiny DuckDB catalog hash alone is not a data fingerprint."""
    candidate = tmp_path / "candidate.duckdb"
    parquet = tmp_path / "literal.parquet"
    make_candidate(candidate)
    with duckdb.connect(str(candidate)) as c:
        c.execute("COPY literal18_variants TO ? (FORMAT PARQUET)", [str(parquet)])
        c.execute("DROP TABLE literal18_variants")
        # Fixture path is escaped; SQL identifiers remain constant.
        c.execute("CREATE VIEW literal18_variants AS SELECT * FROM read_parquet('" +
                  str(parquet).replace("'", "''") + "')")
    manifest = prepare_manifest(candidate)
    assert {item["path"] for item in manifest["files"]} == {str(candidate), str(parquet)}
    omitted = {**manifest, "files": [x for x in manifest["files"] if x["path"] != str(parquet)]}
    with pytest.raises(AdmissionError, match="omits"):
        import_candidate(omitted, tmp_path / "omitted.duckdb")
    assert not (tmp_path / "omitted.duckdb").exists()
    result = import_candidate(manifest, tmp_path / "correct.duckdb")
    assert result["clean"] == 1
    with parquet.open("ab") as stream:
        stream.write(b"changed dependency")
    with pytest.raises(AdmissionError, match="hash mismatch"):
        import_candidate(manifest, tmp_path / "changed.duckdb")


def test_visibility_count_reader_bound_and_migration_checksum(tmp_path):
    """Wrong acceptance counts, overlarge queries and changed DDL fail closed."""
    manifest = make_candidate(tmp_path / "input.duckdb", rows=2)
    target = tmp_path / "output.duckdb"
    result = import_candidate(manifest, target)
    with pytest.raises(AdmissionError, match="clean count"):
        set_visible(target, result["snapshot_id"], basis="unadjusted", expected_clean_rows=3)
    assert read(target, basis="unadjusted") == []
    set_visible(target, result["snapshot_id"], basis="unadjusted", expected_clean_rows=2)
    with pytest.raises(AdmissionError, match="exceeds limit"):
        read(target, basis="unadjusted", limit=1)
    with pytest.raises(AdmissionError, match="not been admitted"):
        set_visible(target, result["snapshot_id"], basis="forward_dynamic_snapshot", expected_clean_rows=2)
    with duckdb.connect(str(target)) as c:
        c.execute(f"UPDATE {SCHEMA}.schema_version SET sha256='changed'")
    with pytest.raises(AdmissionError, match="checksum"):
        import_candidate(manifest, target)

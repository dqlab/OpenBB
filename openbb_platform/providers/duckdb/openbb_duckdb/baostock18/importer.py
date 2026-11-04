"""Resumable 50k--100k transactions into a dedicated offline catalog."""

# Only internal constant schema/columns and '?' placeholder counts enter SQL text.
# ruff: noqa: S608

import hashlib
import time
from pathlib import Path

from openbb_duckdb.baostock18.contract import (
    REJECT_FLAGS,
    REVIEW_FLAGS,
    SCHEMA,
    TRANSFORM,
    AdmissionError,
    bar_projection_sql,
    classify_sql,
    flags_json_sql,
    raw_json_sql,
    require_basis,
    validation_sql,
)
from openbb_duckdb.baostock18.manifest import (
    dependency_paths,
    encoded,
    source_connection,
    verify_manifest,
    verify_unchanged,
)
from openbb_duckdb.baostock18.store import offline_writer, transaction, utcnow


def _selection(scope):
    predicates = ["date BETWEEN ? AND ?"]
    params = [scope["start_date"], scope["end_date"]]
    symbols = scope["symbols"]
    if symbols:
        predicates.append("code IN (" + ",".join("?" for _ in symbols) + ")")
        params.extend(symbols)
    return " AND ".join(predicates), params


def _receipt(connection, snapshot_id, new_batches):
    row = connection.execute(
        f"SELECT coalesce(sum(rows),0),coalesce(sum(clean),0),coalesce(sum(conditional),0),"
        f"coalesce(sum(quarantine),0),coalesce(sum(lineage_rows),0),count(*) "
        f"FROM {SCHEMA}.batch_receipt WHERE snapshot_id=?", [snapshot_id],
    ).fetchone()
    return dict(zip(
        ("rows", "clean", "conditional", "quarantine", "lineage_rows", "batches"), row,
    ), snapshot_id=snapshot_id, new_batches=new_batches, production_admitted=False)


def _write_batch(target, source, table, snapshot_id, number, fault_hook):
    started = time.monotonic()
    target.register("incoming", table)
    source.register("selected_ids", table.select(["value_id"]))
    # A second cursor streams input while this cursor retrieves matching lineage.
    lineage = source.execute(
        "SELECT l.*, i.relative AS source_path, i.historical_status, i.terminal_status, "
        "i.authority_received_ns, to_json(l) AS lineage_json, to_json(i) AS capture_json "
        "FROM row_lineage l JOIN selected_ids b USING(value_id) "
        "JOIN capture_inventory i USING(capture_id)"
    ).fetch_arrow_table()
    target.register("incoming_lineage", lineage)
    target.execute("DROP TABLE IF EXISTS batch")
    target.execute(
        "CREATE TEMP TABLE batch AS SELECT *," + classify_sql() + " AS disposition FROM "
        "(SELECT *," + validation_sql() + " AS numeric_contract FROM incoming)"
    )
    counts = dict(target.execute("SELECT disposition,count(*) FROM batch GROUP BY 1").fetchall())
    n = table.num_rows
    ids = table.column("value_id").to_pylist()
    if len(set(ids)) != n:
        raise AdmissionError("Duplicate candidate value_id")
    digest = hashlib.sha256("\n".join(ids).encode()).hexdigest()
    # Even rejected literals retain their source evidence. A missing inventory
    # join must fail rather than silently lose provenance or counts.
    missing = target.execute(
        "SELECT count(*) FROM batch b WHERE NOT EXISTS "
        "(SELECT 1 FROM incoming_lineage l WHERE l.value_id=b.value_id)"
    ).fetchone()[0]
    if missing:
        raise AdmissionError(f"Missing row lineage/capture inventory for {missing} literals")
    bad_clean = target.execute(
        "SELECT count(*) FROM batch b WHERE disposition='clean' AND NOT EXISTS "
        "(SELECT 1 FROM incoming_lineage l WHERE l.value_id=b.value_id "
        "AND l.source_ok AND l.complete AND l.historical_status='success')"
    ).fetchone()[0]
    if bad_clean:
        raise AdmissionError("Clean literals lack a successful complete source")
    with transaction(target):
        target.execute(
            f"INSERT INTO {SCHEMA}.literal_version SELECT ?,value_id,upper(code),date,"
            f"requested_adjustflag,adjustflag,{raw_json_sql()},{flags_json_sql()},disposition FROM batch",
            [snapshot_id],
        )
        target.execute(
            f"INSERT INTO {SCHEMA}.row_lineage SELECT ?,value_id,capture_id,capture_sha256,"
            "capture_page,page_row_index,capture_row_index,source_body_sha256,source_receipt_sha256,"
            "source_path,source_ok,complete,confirmed_partial_data,historical_status,terminal_status,"
            "authority_received_ns,NULL,NULL,lineage_json,capture_json FROM incoming_lineage",
            [snapshot_id],
        )
        rules = [(flag, f'coalesce("{flag}",true)') for flag in (*REJECT_FLAGS, *REVIEW_FLAGS)]
        rules.extend([
            ("conflicting_variants", "variant_count<>1"),
            ("no_admissible_source", "NOT coalesce(has_admissible_source,false)"),
            ("numeric_contract", "NOT numeric_contract"),
        ])
        for code, predicate in rules:
            target.execute(
                f"INSERT INTO {SCHEMA}.quality SELECT ?,value_id,?,?,disposition,{flags_json_sql()} "
                f"FROM batch WHERE {predicate}", [snapshot_id, TRANSFORM, code],
            )
        target.execute(f"INSERT INTO {SCHEMA}.bar_version " + bar_projection_sql(), [snapshot_id])
        if fault_hook:
            fault_hook(target, number)
        target.execute(
            f"INSERT INTO {SCHEMA}.batch_receipt VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            [snapshot_id, number, ids[-1], n, counts.get("clean", 0),
             counts.get("conditional", 0), counts.get("quarantine", 0),
             lineage.num_rows, digest, utcnow(), time.monotonic() - started],
        )
    target.unregister("incoming")
    target.unregister("incoming_lineage")
    source.unregister("selected_ids")


def import_candidate(manifest: dict, target, *, batch_size=50_000, fault_hook=None) -> dict:
    """Stage all selected literals, lineage, quality and narrow bars atomically per batch.

    No visibility pointer is changed here. A crash leaves committed batches
    resumable and the snapshot invisible. Replaying an identical manifest is a
    no-op; a changed input must use a newly verified manifest/snapshot identity.
    """
    if not 50_000 <= batch_size <= 100_000:
        raise AdmissionError("batch_size must be between 50,000 and 100,000")
    target = Path(target).expanduser().resolve()
    if target == Path(manifest["candidate"]).resolve() or str(target) in {x["path"] for x in manifest["files"]}:
        raise AdmissionError("Target must not overwrite an input")
    signatures = verify_manifest(manifest)
    snapshot_id = hashlib.sha256(encoded(manifest).encode()).hexdigest()
    selection, params = _selection(manifest["scope"])
    with source_connection(Path(manifest["candidate"]), target.parent / (target.name + ".source-spill")) as source:
        bound_paths = {Path(item["path"]).resolve() for item in manifest["files"]}
        if not dependency_paths(source).issubset(bound_paths):
            raise AdmissionError("Manifest omits a candidate Parquet dependency")
        if source.execute("SELECT contract FROM candidate_contract").fetchall() != [(manifest["contract_id"],)]:
            raise AdmissionError("Candidate contract identity changed")
        expected = source.execute(
            "SELECT count(*) FROM literal18_variants WHERE " + selection, params,
        ).fetchone()[0]
        with offline_writer(target) as connection:
            existing = connection.execute(
                f"SELECT expected_rows,state FROM {SCHEMA}.snapshot WHERE snapshot_id=?", [snapshot_id],
            ).fetchone()
            if existing and existing[0] != expected:
                raise AdmissionError("Frozen candidate row count changed")
            if existing and existing[1] == "ready":
                verify_unchanged(signatures)
                return _receipt(connection, snapshot_id, 0)
            if not existing:
                connection.execute(
                    f"INSERT INTO {SCHEMA}.snapshot VALUES (?,?,?,?,?,?,NULL,?,'staging',?,NULL)",
                    [snapshot_id, manifest["contract_id"], snapshot_id, encoded(manifest),
                     TRANSFORM, utcnow(), manifest["availability_basis"], expected],
                )
            last = connection.execute(
                f"SELECT batch_number,last_value_id FROM {SCHEMA}.batch_receipt "
                "WHERE snapshot_id=? ORDER BY batch_number DESC LIMIT 1", [snapshot_id],
            ).fetchone()
            number, last_id = last if last else (0, "")
            stream = source.cursor()
            new_batches = 0
            try:
                batches = stream.execute(
                    "SELECT * FROM literal18_variants WHERE " + selection + " AND value_id>? ORDER BY value_id",
                    [*params, last_id],
                ).fetch_record_batch(batch_size)
                for table in batches:
                    number += 1
                    _write_batch(connection, source, table, snapshot_id, number, fault_hook)
                    new_batches += 1
            finally:
                stream.close()
            verify_unchanged(signatures)
            receipt = _receipt(connection, snapshot_id, new_batches)
            actual = connection.execute(
                f"SELECT count(*) FROM {SCHEMA}.literal_version WHERE snapshot_id=?", [snapshot_id],
            ).fetchone()[0]
            if actual != expected or receipt["rows"] != expected:
                raise AdmissionError("Input != committed literals != batch receipts")
            connection.execute(
                f"UPDATE {SCHEMA}.snapshot SET state='ready',completed_at=? WHERE snapshot_id=?",
                [utcnow(), snapshot_id],
            )
            return receipt


def set_visible(target, snapshot_id: str | None, *, basis: str,
                expected_clean_rows: int | None = None) -> dict:
    """Approve/retract an OFFLINE pointer in one short transaction, keeping history.

    A full snapshot replaces the previous scope for this basis; there is no
    fallback to older dates/symbols. None records an explicit soft withdrawal.
    """
    require_basis(basis)
    with offline_writer(target) as connection, transaction(connection):
        current = connection.execute(
            f"SELECT snapshot_id,event_id FROM {SCHEMA}.dataset_pointer WHERE adjustment_basis=?", [basis],
        ).fetchone()
        now = utcnow()
        if snapshot_id is not None:
            state = connection.execute(
                f"SELECT state FROM {SCHEMA}.snapshot WHERE snapshot_id=?", [snapshot_id],
            ).fetchone()
            if state != ("ready",):
                raise AdmissionError("Only a fully reconciled ready snapshot can be made visible")
            count = connection.execute(
                f"SELECT count(*) FROM {SCHEMA}.bar_version b JOIN {SCHEMA}.literal_version l "
                "USING(snapshot_id,value_id) WHERE b.snapshot_id=? AND adjustment_basis=? "
                "AND disposition='clean'", [snapshot_id, basis],
            ).fetchone()[0]
            if count == 0 or expected_clean_rows != count:
                raise AdmissionError(f"Expected clean count must match actual nonzero count ({count})")
            connection.execute(
                f"INSERT INTO {SCHEMA}.basis_admission VALUES (?,?,?,?) ON CONFLICT DO NOTHING",
                [snapshot_id, basis, now, count],
            )
        if current and current[0] == snapshot_id:
            return {"snapshot_id": snapshot_id, "event_id": current[1], "changed": False}
        event, previous_time = connection.execute(
            f"SELECT coalesce(max(event_id),0)+1,max(known_at) FROM {SCHEMA}.visibility_event"
        ).fetchone()
        if previous_time and now <= previous_time:
            raise AdmissionError("Visibility clock must increase; retry after clock correction")
        connection.execute(
            f"INSERT INTO {SCHEMA}.visibility_event VALUES (?,?,?,?)", [event, basis, snapshot_id, now],
        )
        connection.execute(
            f"INSERT INTO {SCHEMA}.dataset_pointer VALUES (?,?,?) ON CONFLICT(adjustment_basis) "
            "DO UPDATE SET snapshot_id=excluded.snapshot_id,event_id=excluded.event_id", [basis, snapshot_id, event],
        )
        return {"snapshot_id": snapshot_id, "event_id": event, "known_at": now.isoformat(), "changed": True}

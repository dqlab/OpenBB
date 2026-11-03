"""Offline, resumable BaoStock literal-18 candidate materialization.

No SDK execution, delivery acknowledgement, acquisition-state mutation or central
publication. Inputs are copied immutable capture directories and anchored manifests.
Heavy dependencies (duckdb, pyarrow) are optional and imported only when used.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import time
from collections import Counter
from pathlib import Path

from .baostock_page_capture import digest, encode, read_capture, read_page
from .delivery import file_hash, local_path

FIELDS = (
    "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,"
    "tradestatus,pctChg,peTTM,psTTM,pcfNcfTTM,pbMRQ,isST"
).split(",")
EXTRA = [
    "requested_adjustflag",
    "capture_id",
    "capture_sha256",
    "source_body_sha256",
    "source_receipt_sha256",
    "capture_page",
    "sdk_page",
    "page_row_index",
    "capture_row_index",
    "complete",
    "confirmed_partial_data",
    "source_ok",
]
VERSION = "baostock-literal18-candidate-v1"


def sha(path):
    return file_hash(Path(path), 100_000_000_000)[0]


def write_json(path, value):
    """Commit checkpoint only after dependent files have been committed."""
    path = Path(path)
    temp = path.with_suffix(path.suffix + ".part")
    with temp.open("wb") as stream:
        stream.write(encode(value))
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temp, path)


def load_manifest(path):
    records = [json.loads(line) for line in Path(path).read_text().splitlines()]
    names = [r["relative"] for r in records]
    if len(names) != len(set(names)):
        raise ValueError("duplicate_capture_manifest")
    return records


def parse_capture(root, record):
    """Use the existing native page/fragment/terminal verifier, including partials."""
    directory = local_path(Path(root), record["relative"])
    authority = json.loads((directory / "authority.json").read_bytes())
    task = record.get("task", authority["task"])
    proof = read_capture(directory.parent.parent, task, expected=record["anchor"])
    if proof["terminal_sha256"] != record["terminal_anchor"]:
        raise ValueError("terminal_anchor_mismatch")
    params = task["requests"][0]["params"]["literal"]
    if params != record["params"] or params["fields"].split(",") != FIELDS:
        raise ValueError("literal_contract_mismatch")
    # When saved per-file hashes exist, verify those independently as well.
    for item in record.get("expected_files", []):
        if file_hash(local_path(directory, item["name"]), item["bytes"]) != (
            item["sha256"],
            item["bytes"],
        ):
            raise ValueError("saved_file_hash_mismatch")
    columns = {name: [] for name in FIELDS + EXTRA}
    row_hash = hashlib.sha256()
    dates = set()
    count = 0
    for page in proof["pages"]:
        env = read_page(directory, page["page"])
        for source_page in env["pages"]:
            data = source_page["rows"]
            if data and source_page["fields"] != FIELDS:
                raise ValueError("nonempty_source_field_order")
            for index, row in enumerate(data):
                if type(row) is not list or len(row) != 18 or any(type(v) is not str for v in row):
                    raise ValueError("literal_row_shape")
                # Invalid business values are retained and labelled later.
                for field, value in zip(FIELDS, row, strict=True):
                    columns[field].append(value)
                additions = [
                    params["adjustflag"],
                    directory.name,
                    proof["capture_sha256"],
                    page["receipt"]["sha256"],
                    page["receipt_sha256"],
                    page["page"],
                    str(source_page["sdk_page"]),
                    index,
                    count,
                    proof["complete"],
                    bool(record.get("confirmed_partial_data")),
                    page["source_ok"] and source_page["error_code"] == "0",
                ]
                for field, value in zip(EXTRA, additions, strict=True):
                    columns[field].append(value)
                count += 1
                dates.add(row[0])
                row_hash.update(encode(row))
    if "expected_rows" in record and count != record["expected_rows"]:
        raise ValueError("independent_row_count")
    if "expected_rows_sha256" in record and row_hash.hexdigest() != record["expected_rows_sha256"]:
        raise ValueError("independent_literal_rows_hash")
    dates_sha = hashlib.sha256(("\n".join(sorted(dates)) + "\n").encode()).hexdigest()
    if "expected_dates_sha256" in record and dates_sha != record["expected_dates_sha256"]:
        raise ValueError("independent_dates_hash")
    inventory = {
        **{k: v for k, v in record.items() if k not in {"task", "expected_files"}},
        "capture_id": directory.name,
        "verified": True,
        "rows": count,
        "rows_sha256": row_hash.hexdigest(),
        "dates_sha256": dates_sha,
        "first_date": min(dates) if dates else None,
        "last_date": max(dates) if dates else None,
        "complete": proof["complete"],
        "terminal_status": (proof["terminal"] or {}).get("status"),
        "pending": proof["pending"],
        "authority_received_ns": authority["received_ns"],
        "availability_basis": "capture_start_only; row publication time unknown",
    }
    return columns, inventory


def parse(manifest, raw_root, output, batch_size=64, start_index=0, stop_index=None):
    """Bounded batches; failures retain their raw directory and a quarantine record."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    for name in ("parsed", "checkpoints", "failures"):
        (output / name).mkdir(exist_ok=True)
    records = load_manifest(manifest)
    contract = {"version": VERSION, "manifest_sha256": sha(manifest), "batch_size": batch_size}
    contract_path = output / "parse_contract.json"
    if contract_path.exists() and json.loads(contract_path.read_text()) != contract:
        raise ValueError("output_contract_conflict")
    write_json(contract_path, contract)
    types = {n: pa.string() for n in FIELDS + EXTRA}
    for n in ("capture_page", "page_row_index", "capture_row_index"):
        types[n] = pa.int32()
    for n in ("complete", "confirmed_partial_data", "source_ok"):
        types[n] = pa.bool_()
    schema = pa.schema(list(types.items()))
    totals = Counter()
    started = time.monotonic()
    stop_index = len(records) if stop_index is None else min(stop_index, len(records))
    if batch_size < 1 or start_index < 0 or start_index % batch_size:
        raise ValueError("invalid_batch_bounds")
    if stop_index < len(records) and stop_index % batch_size:
        raise ValueError("unaligned_stop_index")
    for index in range(start_index, stop_index, batch_size):
        checkpoint = output / "checkpoints" / f"{index:06d}.json"
        destination = output / "parsed" / f"{index:06d}.parquet"
        part = records[index : index + batch_size]
        selection_sha = digest(part)
        if checkpoint.exists():
            saved = json.loads(checkpoint.read_text())
            if (
                saved["selection_sha256"] != selection_sha
                or sha(destination) != saved["parquet_sha256"]
            ):
                raise ValueError("checkpoint_conflict")
            totals.update(saved["totals"])
            continue
        begin = time.monotonic()
        columns = {n: [] for n in FIELDS + EXTRA}
        inventories = []
        counts = Counter()
        for record in part:
            try:
                values, inventory = parse_capture(raw_root, record)
                for n in columns:
                    columns[n].extend(values[n])
                counts["verified_captures"] += 1
                counts["rows"] += inventory["rows"]
                counts["confirmed_partial_captures"] += bool(record.get("confirmed_partial_data"))
                counts["confirmed_partial_rows"] += (
                    inventory["rows"] if record.get("confirmed_partial_data") else 0
                )
                inventories.append(inventory)
            except Exception as exc:  # Failure is sealed and reconciled, never silently dropped.
                failure = {
                    "relative": record["relative"],
                    "anchor": record["anchor"],
                    "verified": False,
                    "error": type(exc).__name__ + ":" + str(exc),
                    "raw_retained_at": str(Path(raw_root) / record["relative"]),
                }
                write_json(
                    output / "failures" / (record["relative"].split("/")[-1] + ".json"), failure
                )
                inventories.append(failure)
                counts["failed_captures"] += 1
        table = pa.table(columns, schema=schema)
        temp = destination.with_suffix(".part")
        pq.write_table(table, temp, compression="zstd", row_group_size=65536)
        os.replace(temp, destination)
        readback = pq.read_table(destination)
        if not readback.equals(table) or readback.schema != schema:
            raise ValueError("parquet_literal_readback")
        saved = {
            "selection_sha256": selection_sha,
            "parquet_sha256": sha(destination),
            "totals": dict(counts),
            "captures": inventories,
            "seconds": round(time.monotonic() - begin, 3),
            "bytes": destination.stat().st_size,
        }
        write_json(checkpoint, saved)
        totals.update(counts)
        print(
            json.dumps(
                {
                    "phase": "parse",
                    "batch": index,
                    "totals": dict(totals),
                    "seconds": round(time.monotonic() - started, 3),
                }
            ),
            flush=True,
        )
    all_totals = Counter()
    for checkpoint in (output / "checkpoints").glob("*.json"):
        all_totals.update(json.loads(checkpoint.read_text())["totals"])
    processed = all_totals["verified_captures"] + all_totals["failed_captures"]
    write_json(
        output / "parse_summary.json",
        {
            **dict(all_totals),
            "expected_captures": len(records),
            "parse_complete": processed == len(records),
            "seconds_this_invocation": time.monotonic() - started,
        },
    )


def sql_string(value):
    return "'" + str(value).replace("'", "''") + "'"


def materialize(output, evidence, buckets=32):
    """Exact literal grouping, many-to-one lineage and isolated DuckDB views."""
    import duckdb

    output = Path(output).resolve()
    evidence = Path(evidence)
    for name in ("variants", "lineage", "quality", "scratch"):
        (output / name).mkdir(exist_ok=True)
    con = duckdb.connect(":memory:")
    con.execute("SET threads=2")
    con.execute("SET memory_limit='3GB'")
    con.execute("SET temp_directory=" + sql_string(output / "scratch"))
    con.execute(
        "CREATE VIEW parsed AS SELECT * FROM read_parquet("
        + sql_string(output / "parsed/*.parquet")
        + ")"
    )
    # Stable source-code buckets bound memory and allow independently committed retries.
    bucket_expr = "try_cast(right(code, 6) AS INTEGER) % " + str(buckets)
    fields = ",".join('"' + f + '"' for f in FIELDS)
    literal_key = "sha256(to_json(list_value(requested_adjustflag," + fields + ")))"
    valid_date = (
        "try_cast(u.date AS DATE) IS NOT NULL AND cast(try_cast(u.date AS DATE) AS VARCHAR)=u.date"
    )
    numeric_bad = " OR ".join(
        f'(u."{f}"<>\'\' AND (try_cast(u."{f}" AS DOUBLE) IS NULL '
        f'OR NOT isfinite(try_cast(u."{f}" AS DOUBLE))))'
        for f in FIELDS
        if f not in {"date", "code"}
    )
    ohlc_bad = " OR ".join(
        [
            "try_cast(low AS DOUBLE)>try_cast(high AS DOUBLE)",
            "try_cast(open AS DOUBLE)<try_cast(low AS DOUBLE)",
            "try_cast(open AS DOUBLE)>try_cast(high AS DOUBLE)",
            "try_cast(close AS DOUBLE)<try_cast(low AS DOUBLE)",
            "try_cast(close AS DOUBLE)>try_cast(high AS DOUBLE)",
            "try_cast(low AS DOUBLE)<0",
            "try_cast(volume AS DOUBLE)<0",
            "try_cast(amount AS DOUBLE)<0",
        ]
    )
    checkpoints = [
        json.loads(p.read_text()) for p in sorted((output / "checkpoints").glob("*.json"))
    ]
    captures = [r for c in checkpoints for r in c["captures"] if r["verified"]]
    write_json(output / "capture_inventory.json", captures)
    con.execute(
        "CREATE TABLE captures AS SELECT * FROM read_json_auto(?)",
        [str(output / "capture_inventory.json")],
    )
    con.execute(
        "CREATE VIEW enriched AS SELECT p.*, c.params.code AS requested_code, "
        "cast(c.params.start_date AS VARCHAR) AS requested_start, "
        "cast(c.params.end_date AS VARCHAR) AS requested_end "
        "FROM parsed p JOIN captures c USING (capture_id)"
    )
    con.execute(
        "CREATE TABLE unknowns AS SELECT code,date,adjustflag,new_status FROM "
        "read_csv_auto(?,all_varchar=true)",
        [str(evidence / "prior_calendar_unknown_review958.csv")],
    )
    con.execute(
        "CREATE TABLE calendar_conflicts AS SELECT DISTINCT code,date,adjustflag "
        "FROM read_csv_auto(?,all_varchar=true)",
        [str(evidence / "prior_calendar_conflict_source_rows.csv")],
    )
    start = time.monotonic()
    for bucket in range(buckets):
        ticket = output / "quality" / f"bucket-{bucket:02d}.json"
        dest = output / "variants" / f"bucket-{bucket:02d}.parquet"
        lin = output / "lineage" / f"bucket-{bucket:02d}.parquet"
        binding = {
            "parse_contract_sha256": sha(output / "parse_contract.json"),
            "buckets": buckets,
            "bucket": bucket,
            "version": VERSION,
        }
        if ticket.exists():
            saved = json.loads(ticket.read_text())
            if (
                saved["binding"] != binding
                or sha(dest) != saved["variants_sha256"]
                or sha(lin) != saved["lineage_sha256"]
            ):
                raise ValueError("materialize_checkpoint_conflict")
            continue
        begin = time.monotonic()
        # Copy the partition into a bounded temporary table once, not a global DISTINCT.
        con.execute(
            "CREATE OR REPLACE TEMP TABLE batch AS SELECT *, " + literal_key + " AS value_id "
            "FROM enriched WHERE coalesce(" + bucket_expr + ",-1)=?",
            [bucket],
        )
        con.execute(
            "CREATE OR REPLACE TEMP TABLE unique_values AS SELECT "
            + fields
            + ",requested_adjustflag,value_id,"
            "count(*) AS observation_count,count(DISTINCT capture_id) AS capture_count,"
            "bool_or(complete) AS has_complete,bool_or(confirmed_partial_data) "
            "AS has_confirmed_partial,"
            "bool_and(source_ok) AS all_source_ok,"
            "bool_or(code<>requested_code OR date<requested_start OR "
            "date>requested_end) AS identity_or_range_anomaly "
            "FROM batch GROUP BY ALL"
        )
        # Group all original strings, never cast/round to decide equality.
        con.execute(
            "CREATE OR REPLACE TEMP TABLE variants AS SELECT u.*, "
            "count(*) OVER (PARTITION BY u.code,u.date,u.requested_adjustflag) AS variant_count, "
            "NOT (" + valid_date + ") AS date_anomaly, (" + numeric_bad + ") AS numeric_anomaly, "
            "coalesce((" + ohlc_bad + "),false) AS ohlc_anomaly, "
            "u.adjustflag<>u.requested_adjustflag AS adjustment_mismatch, "
            "(open='' OR high='' OR low='' OR close='' OR preclose='') AS core_price_empty, "
            "(tradestatus NOT IN ('0','1') OR isST NOT IN ('0','1') OR "
            "u.adjustflag NOT IN ('1','2','3')) AS flag_anomaly, "
            "coalesce(k.new_status LIKE 'calendar_unknown%',false) AS calendar_unknown, "
            "(cc.code IS NOT NULL) AS calendar_source_conflict "
            "FROM unique_values u LEFT JOIN unknowns k ON u.code=k.code AND u.date=k.date "
            "AND u.requested_adjustflag=k.adjustflag LEFT JOIN calendar_conflicts cc "
            "ON u.code=cc.code AND u.date=cc.date AND u.requested_adjustflag=cc.adjustflag"
        )
        if con.execute("SELECT count(*)-count(DISTINCT value_id) FROM variants").fetchone()[0]:
            raise ValueError("literal_hash_collision_or_quality_join_duplicate")

        def copy(query, path):
            temp = path.with_suffix(".part")
            con.execute(
                "COPY ("
                + query
                + ") TO "
                + sql_string(temp)
                + " (FORMAT PARQUET, COMPRESSION ZSTD)"
            )
            os.replace(temp, path)

        copy("SELECT * FROM variants ORDER BY code,date,requested_adjustflag,value_id", dest)
        copy(
            "SELECT value_id,capture_id,capture_sha256,source_body_sha256,source_receipt_sha256,"
            "capture_page,sdk_page,page_row_index,capture_row_index,complete,"
            "confirmed_partial_data,source_ok FROM batch",
            lin,
        )
        n = con.execute("SELECT count(*) FROM batch").fetchone()[0]
        v, obs = con.execute(
            "SELECT count(*),coalesce(sum(observation_count),0) FROM variants"
        ).fetchone()
        back = con.execute("SELECT count(*) FROM read_parquet(?)", [str(lin)]).fetchone()[0]
        if n != obs or n != back:
            raise ValueError("lineage_conservation")
        stats = dict(
            zip(
                [
                    d[0]
                    for d in con.execute(
                        "SELECT count(*) AS variants, "
                        "count(*) FILTER (WHERE variant_count>1) AS conflict_variants, "
                        "count(*) FILTER (WHERE adjustment_mismatch) AS adjustment_mismatches, "
                        "count(*) FILTER (WHERE calendar_unknown) AS calendar_unknown_variants, "
                        "count(*) FILTER (WHERE core_price_empty) AS core_price_empty_variants, "
                        "count(*) FILTER (WHERE numeric_anomaly OR ohlc_anomaly "
                        "OR flag_anomaly OR date_anomaly OR "
                        "identity_or_range_anomaly) AS invalid_variants "
                        "FROM variants"
                    ).description
                ],
                con.fetchone(),
                strict=True,
            )
        )
        saved = {
            "binding": binding,
            "input_rows": n,
            "unique_rows": v,
            "exact_duplicates": n - v,
            "stats": stats,
            "seconds": round(time.monotonic() - begin, 3),
            "variants_sha256": sha(dest),
            "lineage_sha256": sha(lin),
        }
        write_json(ticket, saved)
        print(
            json.dumps(
                {
                    "phase": "materialize",
                    "bucket": bucket,
                    **{
                        k: saved[k]
                        for k in ["input_rows", "unique_rows", "exact_duplicates", "seconds"]
                    },
                }
            ),
            flush=True,
        )
    invalid = con.execute(
        "SELECT count(*) FROM enriched WHERE coalesce(" + bucket_expr + ",-1)<0"
    ).fetchone()[0]
    if invalid:
        raise ValueError("unbucketed_source_codes_retained_in_parsed:" + str(invalid))
    con.close()
    build_catalog(output, evidence)
    write_json(
        output / "materialize_summary.json",
        {"seconds_this_invocation": time.monotonic() - start, "buckets": buckets},
    )


def build_catalog(output, evidence):
    """Create only a new isolated catalog using existing provider-compatible columns."""
    import duckdb

    output = Path(output)
    target = output / "candidate.duckdb"
    contract = digest(
        {
            "version": VERSION,
            "output": str(output),
            "parse_contract_sha256": sha(output / "parse_contract.json"),
        }
    )
    if target.exists():
        with duckdb.connect(str(target), read_only=True) as existing:
            if existing.execute("SELECT contract FROM candidate_contract").fetchone() != (
                contract,
            ):
                raise ValueError("catalog_contract_conflict")
        return
    temp = target.with_suffix(".part")
    if temp.exists():
        raise ValueError("catalog_partial_exists; retain_for_investigation")
    con = duckdb.connect(str(temp))
    con.execute("CREATE TABLE candidate_contract AS SELECT ? AS contract", [contract])
    for view, pattern in [
        ("raw_observations", "parsed/*.parquet"),
        ("literal18_variants", "variants/*.parquet"),
        ("row_lineage", "lineage/*.parquet"),
    ]:
        con.execute(
            "CREATE VIEW "
            + view
            + " AS SELECT * FROM read_parquet("
            + sql_string(output / pattern)
            + ")"
        )
    con.execute(
        "CREATE TABLE capture_inventory AS SELECT * FROM read_json_auto(?)",
        [str(output / "capture_inventory.json")],
    )
    for table, name in [
        ("early_missing_review", "prior_missing_combination_review564.csv"),
        ("calendar_review", "prior_calendar_unknown_review958.csv"),
        ("plan_scope", "prior_plan_scope_and_coverage11124.csv"),
        ("confirmed_partial_segments", "prior_segments18.csv"),
    ]:
        con.execute(
            "CREATE TABLE " + table + " AS SELECT * FROM read_csv_auto(?,all_varchar=true)",
            [str(Path(evidence) / name)],
        )
    con.execute("CREATE VIEW conflicts AS SELECT * FROM literal18_variants WHERE variant_count>1")
    con.execute(
        "CREATE VIEW quarantine AS SELECT * FROM literal18_variants WHERE "
        "variant_count>1 OR adjustment_mismatch OR date_anomaly OR numeric_anomaly OR ohlc_anomaly "
        "OR flag_anomaly OR identity_or_range_anomaly OR core_price_empty OR NOT all_source_ok "
        "OR NOT (has_complete OR has_confirmed_partial)"
    )
    for flag, view, basis in [
        ("3", "equity_unadjusted", "unadjusted"),
        ("2", "equity_forward_dynamic", "forward_dynamic_snapshot"),
    ]:
        projection = ["upper(code) AS symbol", "cast(date AS DATE) AS date"]
        projection += [
            f'try_cast("{f}" AS DOUBLE) AS "{f}"'
            for f in ["open", "high", "low", "close", "volume"]
        ]
        projection += [
            f'try_cast("{f}" AS DOUBLE) AS "forward_{f}"'
            for f in ["open", "high", "low", "close", "preclose"]
        ]
        projection += [
            "try_cast(preclose AS DOUBLE) AS prev_close",
            "cast(tradestatus AS INTEGER) AS trade_status",
            sql_string(basis) + " AS base_adjustment",
            "value_id",
            "requested_adjustflag",
            "adjustflag AS source_adjustflag",
            "has_confirmed_partial",
            "calendar_unknown",
            "calendar_source_conflict",
            "NULL::DATE AS fixed_qfq_basis_date",
            "'CNY' AS currency",
            "'Asia/Shanghai' AS session_timezone",
        ]
        con.execute(
            "CREATE VIEW "
            + view
            + " AS SELECT "
            + ",".join(projection)
            + " FROM literal18_variants WHERE "
            "requested_adjustflag="
            + sql_string(flag)
            + " AND adjustflag=requested_adjustflag AND variant_count=1 "
            "AND (has_complete OR has_confirmed_partial) AND all_source_ok "
            "AND NOT (date_anomaly OR numeric_anomaly OR ohlc_anomaly OR "
            "flag_anomaly OR identity_or_range_anomaly OR core_price_empty)"
        )
    con.execute("CHECKPOINT")
    con.close()
    os.replace(temp, target)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    sub = p.add_subparsers(dest="command", required=True)
    a = sub.add_parser("parse")
    a.add_argument("--manifest", required=True)
    a.add_argument("--raw-root", required=True)
    a.add_argument("--output", required=True)
    a.add_argument("--batch-size", type=int, default=64)
    a.add_argument("--start-index", type=int, default=0)
    a.add_argument("--stop-index", type=int)
    a = sub.add_parser("materialize")
    a.add_argument("--output", required=True)
    a.add_argument("--evidence", required=True)
    a.add_argument("--buckets", type=int, default=32)
    args = vars(p.parse_args())
    command = args.pop("command")
    if command == "parse":
        parse(args.pop("manifest"), args.pop("raw_root"), args.pop("output"), **args)
    else:
        materialize(**args)


if __name__ == "__main__":
    main()

"""Independent full-catalog reference audit with a shared, read-only catalog lock."""

import argparse
import json
import signal
import sqlite3
import time
import traceback
from pathlib import Path

import duckdb
from openbb_duckdb.baostock18.contract import AdmissionError
from openbb_duckdb.baostock18.driver import reference_contract
from openbb_duckdb.baostock18.production import (
    READ_CONFIG,
    fingerprint,
    load_json,
    market_read_lock,
    no_wal,
    write_json,
)
from openbb_duckdb.baostock18.reference_backup import bounded_sql, enumerate_references, stable_hash
from openbb_duckdb.baostock18.store import utcnow


def full_audit(task, output, *, max_seconds=43_200):  # noqa: PLR0912, PLR0915
    """Audit every catalog reference, retaining evidence in a new output directory.

    Only the evidence directory is writable. The catalog connection and shared
    lock close after projection, before hashing payloads. A busy writer fails
    immediately; the operator can start a new audit later. No admission changes.
    """
    if not 0 < max_seconds <= 43_200:
        raise AdmissionError("Audit deadline must be 0 < seconds <= 43200")
    task, output = Path(task), Path(output)
    config = load_json(task / "verification-config.json")
    verified = load_json(task / "verification/receipt.json")
    if load_json(task / "RUN_RECEIPT.json").get("status") != "PASS":
        raise AdmissionError("A completed visibility receipt is required for this audit")
    target = Path(config["target"])
    output.mkdir(mode=0o700, parents=True, exist_ok=False)
    started = time.monotonic()
    deadline = started + max_seconds
    result = dict(
        format="baostock18-full-catalog-reference-audit-v1", status="RUNNING",
        read_only=True, catalog_lock_mode="LOCK_SH|LOCK_NB", started_at=utcnow().isoformat(),
        files=0, bytes=0, processed_files=0, catalog_enumeration_complete=False,
        missing=0, mismatch=0, changed=0, unreadable=0, conflicting_catalog_records=0,
        missing_hash_measured_files=0, examples=[],
    )
    write_json(output / "started.json", result)

    def difference(kind, path, error):
        result[kind] += 1
        if len(result["examples"]) < 1000:
            result["examples"].append(dict(kind=kind, path=str(path), error=str(error)))

    try:
        with sqlite3.connect(output / "references.sqlite") as ledger:
            ledger.execute("CREATE TABLE expected(path TEXT PRIMARY KEY,size INTEGER,sha256 TEXT,seen INTEGER)")

            def mark(source, digest, size, root):
                name = str(source)
                old = ledger.execute("SELECT size,sha256 FROM expected WHERE path=?", [name]).fetchone()
                if old is None:
                    ledger.execute("INSERT INTO expected VALUES (?,?,?,1)", [name, size, digest])
                    result["files"] += 1
                else:
                    if ((size is not None and old[0] is not None and size != old[0])
                            or (digest is not None and old[1] is not None and digest != old[1])):
                        difference("conflicting_catalog_records", name, "catalog records disagree")
                    ledger.execute("UPDATE expected SET size=coalesce(size,?),sha256=coalesce(sha256,?) "
                                   "WHERE path=?", [size, digest, name])

            with market_read_lock(target):
                no_wal(target)
                with (duckdb.connect(str(target), read_only=True,
                                     config=dict(READ_CONFIG, temp_directory=str(output / "spill"))) as conn,
                      bounded_sql(conn, deadline)):
                    projection_started = time.monotonic()
                    observed = fingerprint(target)
                    print(json.dumps(dict(phase="catalog_projection", catalog_lock_mode=result["catalog_lock_mode"],  # noqa: T201
                                          started_at=utcnow().isoformat())), flush=True)
                    contract = reference_contract(
                        conn, observed, reference_root=config["reference_root"],
                        admission_root=config["admission_root"], json_root=config["json_root"],
                        capture_inventory=verified["reference_verification"],
                    )
                    write_json(output / "reference-contract.json", contract)
                    result["projections"] = enumerate_references(conn, contract, ledger, observed, deadline, record=mark)
                    result["catalog_projection_seconds"] = time.monotonic() - projection_started
                    result["catalog_enumeration_complete"] = True
            result["catalog_connection_closed_before_payload_hashing"] = True
            result["files"] = ledger.execute("SELECT count(*) FROM expected").fetchone()[0]
            ledger.commit()
            with (output / "file-results.jsonl").open("x") as stream:
                reported = time.monotonic()
                for name, expected_size, expected_hash in ledger.execute(
                        "SELECT path,size,sha256 FROM expected ORDER BY path"):
                    item = dict(path=name, expected_size=expected_size, expected_sha256=expected_hash, status="PASS")
                    try:
                        size, digest = stable_hash(name, deadline)
                        result["bytes"] += size
                        item.update(size=size, sha256=digest)
                        if ((expected_size is not None and expected_size != size)
                                or (expected_hash is not None and expected_hash != digest)):
                            difference("mismatch", name, "size or recorded hash differs")
                            item["status"] = "MISMATCH"
                        if expected_hash is None:
                            result["missing_hash_measured_files"] += 1
                    except Exception as exc:  # Each failed file remains independently visible in the report.
                        kind = ("missing" if not Path(name).exists()
                                else ("changed" if "changed" in str(exc).lower() else "unreadable"))
                        difference(kind, name, exc)
                        item.update(status="FAIL", error=repr(exc))
                    result["processed_files"] += 1
                    stream.write(json.dumps(item) + "\n")
                    if time.monotonic() - reported >= 30:
                        print(json.dumps(dict(  # noqa: T201
                            {k: result[k] for k in ("processed_files", "files", "bytes", "missing", "mismatch",
                                                   "changed", "unreadable")}, seconds=time.monotonic() - started)),
                            flush=True)
                        reported = time.monotonic()
                    if time.monotonic() > deadline:
                        raise AdmissionError("Full audit deadline exceeded")
                stream.flush()
            result["status"] = "PASS" if not any(result[k] for k in (
                "missing", "mismatch", "changed", "unreadable", "conflicting_catalog_records")) else "DIFFERENCES"
    except BaseException as exc:
        result.update(status="INTERRUPTED" if isinstance(exc, (KeyboardInterrupt, SystemExit)) else "FAIL",
                      error=repr(exc), traceback=traceback.format_exc())
    finally:
        result.update(seconds=time.monotonic() - started, completed_at=utcnow().isoformat())
        write_json(output / "REPORT.json", result)
        print(json.dumps({k: v for k, v in result.items()  # noqa: T201
                          if k not in ("projections", "examples", "traceback")}), flush=True)
    return result


def main():
    """Run a separate read-only audit; SIGINT/SIGTERM unwind and retain a report."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--task", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--max-seconds", type=float, default=43_200)
    args = parser.parse_args()

    def interrupt(signum, frame):
        raise KeyboardInterrupt(f"Audit interrupted by signal {signum}")

    signal.signal(signal.SIGTERM, interrupt)
    result = full_audit(args.task, args.output, max_seconds=args.max_seconds)
    raise SystemExit(0 if result["status"] == "PASS" else (130 if result["status"] == "INTERRUPTED" else 1))


if __name__ == "__main__":
    main()

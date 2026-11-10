"""One explicit CLI chain for copied and production catalogs.

Run with ``python -m openbb_duckdb.baostock18.driver --config /absolute/run.json``.
All deployment paths, approvals, bounds and times come from the run packet.
"""

# All interpolated identifiers are fixed admission names or validated inventory.
# ruff: noqa: S608, S603, T201
import argparse
import fcntl
import hashlib
import json
import os
import resource
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import duckdb
from openbb_duckdb.baostock18.contract import SCHEMA, AdmissionError
from openbb_duckdb.baostock18.manifest import encoded, source_connection
from openbb_duckdb.baostock18.production import (
    A_SHARE_TABLES,
    READ_CONFIG,
    VISIBILITY_MODES,
    fingerprint,
    inventory,
    load_json,
    market_lock,
    no_wal,
    production_path,
    timestamp,
    write_json,
)
from openbb_duckdb.baostock18.reference_backup import (
    CATALOG_ONLY,
    PATH_COLUMN,
    PROVENANCE_COLUMNS,
    RECORDED_SCOPE,
    REDUCED_SCOPE,
    REFERENCE_SCOPES,
    bounded_sql,
    check_deadline,
    directory_files,
    freeze_reference_inventory,
    json_roots,
    stable_hash,
)
from openbb_duckdb.baostock18.store import transaction, utcnow


def precheck_input_references(config, *, destination=None):
    """Freeze every regular member of each candidate capture directory.

    This reads only the frozen candidate and capture bytes, never the target DB.
    The persisted per-file SHA256 inventory also binds staged catalog backups.
    """
    started = time.monotonic()
    deadline = started + config["max_verify_seconds"]
    root = Path(config["admission_root"])
    manifest = load_json(config["manifest"])
    result = dict(
        capture_records=0,
        regular_files=0,
        directories=0,
        bytes=0,
        missing=0,
        changed=0,
        invalid=0,
        unreadable=0,
        nonregular=0,
        samples=[],
    )
    frozen = []
    files_seen = set()
    file_sizes = {}
    from openbb_duckdb.baostock18.recorded_references import Sampler

    sampler = Sampler()
    try:
        if not root.is_absolute() or root.resolve(strict=True) != root or not root.is_dir():
            raise AdmissionError("Admission root must be a canonical absolute directory")
        with (
            source_connection(Path(manifest["candidate"]), Path(config["output"]).parent / "precheck-spill") as c,
            bounded_sql(c, deadline),
        ):
            cursor = c.execute("SELECT relative,count(*) FROM capture_inventory GROUP BY relative ORDER BY relative")
            reported = started
            while rows := cursor.fetchmany(512):
                for value, records in rows:
                    check_deadline(deadline)
                    if not isinstance(value, str) or not value or "://" in value or any(x in value for x in "*?["):
                        raise AdmissionError("Invalid capture directory locator")
                    path = Path(value) if Path(value).is_absolute() else root / value
                    result["capture_records"] += records
                    members = directory_files(path, root, deadline)
                    result["directories"] += records
                    files = []
                    for member in members:
                        size, digest = stable_hash(member, deadline)
                        files.append({"path": str(member), "size": size, "sha256": digest})
                        if member not in files_seen:
                            files_seen.add(member)
                            result["regular_files"] += 1
                            result["bytes"] += size
                            file_sizes[str(member)] = size
                            sampler.add(member, root)
                    if directory_files(path, root, deadline) != members:
                        raise AdmissionError(f"Capture directory changed while hashing: {path}")
                    frozen.append({"directory": str(path), "files": files})
                    if time.monotonic() - reported >= 15:
                        print(
                            json.dumps(
                                {
                                    "stage": "capture-directory-hashing",
                                    "seconds": time.monotonic() - started,
                                    **{k: result[k] for k in ("directories", "regular_files", "bytes")},
                                }
                            ),
                            flush=True,
                        )
                        reported = time.monotonic()
        result["status"] = "passed"
    except (AdmissionError, OSError) as exc:
        message = str(exc)
        result.update(status="failed", error=message)
        if isinstance(exc, FileNotFoundError) or isinstance(exc.__cause__, FileNotFoundError):
            kind = "missing"
        elif "changed" in message:
            kind = "changed"
        elif "Nonregular" in message:
            kind = "nonregular"
        elif isinstance(exc, PermissionError) or isinstance(exc.__cause__, PermissionError):
            kind = "unreadable"
        else:
            kind = "invalid"
        result[kind] += 1
        result["samples"].append({"error": message, "kind": kind})
    if destination is not None:
        with Path(destination).open("x") as stream:
            for item in frozen:
                stream.write(json.dumps(item) + "\n")
            stream.flush()
            os.fsync(stream.fileno())
        result["inventory"] = str(destination)
        if result["status"] == "passed":
            result["inventory_sha256"] = stable_hash(destination, deadline)[1]
    result.update(
        read_only=True,
        target_catalog_connections=0,
        root=str(root),
        resolved_admission_root=str(root.resolve()),
        seconds=time.monotonic() - started,
    )
    result["bytes_per_second"] = result["bytes"] / result["seconds"]
    if config.get("backup_scope") == RECORDED_SCOPE:
        from openbb_duckdb.baostock18.recorded_references import SAMPLE_SEED

        selected = sampler.selected()
        result.update(mode=RECORDED_SCOPE, recorded_hash_comparisons=0,
                      recomputed_files=result["regular_files"], recomputed_bytes=result["bytes"],
                      missing_hash_recomputed_files=result["regular_files"],
                      sample_recomputed_files=len(selected), sample_paths=sorted(selected),
                      sample_recomputed_bytes=sum(file_sizes[p] for p in selected),
                      sample_seed=SAMPLE_SEED, unread_bytes=0, size_mismatch=0, hash_mismatch=0,
                      hash_origin="candidate locators carry no byte hash; all member bytes recomputed")
    return result


def reference_contract(connection, observed, *, reference_root, admission_root, json_root, capture_inventory=None):
    """Normalize column triples before either serialization or in-memory validation."""
    columns = [
        list(row)
        for row in connection.execute(
            "SELECT c.table_schema,c.table_name,c.column_name FROM information_schema.columns c "
            "JOIN duckdb_tables() t ON t.schema_name=c.table_schema AND t.table_name=c.table_name "
            "WHERE NOT t.internal ORDER BY 1,2,3"
        ).fetchall()
    ]
    rules, provenance = [], []
    for schema, table, column in columns:
        if not PATH_COLUMN.search(column):
            continue
        if column in PROVENANCE_COLUMNS:
            provenance.append(
                {
                    "column": [schema, table, column],
                    "reason": "Logical identity or original location; archived files are projected separately",
                }
            )
            continue
        names = {n for s, t, n in columns if (s, t) == (schema, table)}
        digest = {
            "storage_key": "sha256",
            "manifest_key": "manifest_hash",
            "evidence_key": "evidence_hash",
            "payload_key": "evidence_hash",
            "raw_key": "raw_sha256",
        }.get(column)
        if digest not in names:
            digest = None
        if (schema, table, column) == ("market_data", "financedatabase_snapshot", "raw_key"):
            digest = "source_sha256"
        rules.append(
            {
                "schema": schema,
                "table": table,
                "path_column": column,
                "hash_column": digest,
                "size_column": next((n for n in (column.removesuffix("_key") + "_size",
                                                   "size_bytes", "byte_size", "bytes", "size") if n in names), None),
                "root": admission_root if schema == SCHEMA else reference_root,
            }
        )
        if (schema, table, column) == (SCHEMA, "row_lineage", "source_path"):
            if capture_inventory is None:
                raise AdmissionError("Capture directory inventory required for admission lineage")
            rules[-1].update(
                kind="capture_directory",
                inventory=capture_inventory["inventory"],
                inventory_sha256=capture_inventory["inventory_sha256"],
            )
    return {
        "format": "baostock18-catalog-reference-contract-v1",
        "catalog_fingerprint": observed,
        "columns": columns,
        "rules": rules,
        "provenance_columns": provenance,
        "json_root": json_root,
    }


class Driver:
    """No test/production branch: every command uses the production CLI gates."""

    def __init__(self, config):
        self.config = config
        self.visibility = config.get("visibility", "immediate")
        if self.visibility not in VISIBILITY_MODES:
            raise AdmissionError("visibility must be immediate or deferred")
        self.target = production_path(config["target"])
        self.root = Path(config["output"])
        if not self.root.is_absolute() or self.root.resolve() != self.root or self.root.exists():
            raise AdmissionError("Driver output must be a new canonical absolute directory")
        for key in ("manifest", "baseline", "reference_root", "admission_root"):
            p = Path(config[key])
            if not p.is_absolute() or p.resolve(strict=True) != p:
                raise AdmissionError(f"Explicit canonical {key} required")
        json_roots(config["json_root"])
        timestamp(config["window_started_at"])
        self.scope = config.get("backup_scope", REDUCED_SCOPE)
        if self.scope not in (*REFERENCE_SCOPES, CATALOG_ONLY):
            raise AdmissionError("Unknown driver backup scope")
        if (self.visibility == "deferred") != (self.scope == CATALOG_ONLY):
            raise AdmissionError("visibility deferred requires backup_scope catalog_only; immediate forbids it")
        self.cap = config["max_backup_seconds"]
        if not isinstance(self.cap, (int, float)) or not 0 < self.cap <= 14400:
            raise AdmissionError("Driver backup cap must be positive and <=14400")
        if config["approved_batch_size"] not in range(50000, 100001):
            raise AdmissionError("Invalid approved batch size")
        manifest = load_json(config["manifest"])
        if hashlib.sha256(encoded(manifest).encode()).hexdigest() != config["approved_manifest_hash"]:
            raise AdmissionError("Driver manifest differs from approval")
        self.root.mkdir(mode=0o700)
        self.commands = []
        self.started = time.monotonic()
        self.before_visibility = None
        self.published = False

    def cli(self, command, label, *extra, common=()):
        """Run the installed CLI and retain output, errors and measured duration."""
        argv = [sys.executable, "-B", "-m", "openbb_duckdb.baostock18", command, *common, *map(str, extra)]
        if command != "read":
            argv += ["--output", str(self.root / (label + ".json"))]
        started = time.monotonic()
        usage_before = resource.getrusage(resource.RUSAGE_CHILDREN)
        # Stream stdout/stderr to durable evidence without retaining large results in memory.
        with (self.root / (label + ".stdout")).open("x") as out, (self.root / (label + ".stderr")).open("x") as err:
            process = subprocess.run(argv, stdout=out, stderr=err, check=False)
        usage = resource.getrusage(resource.RUSAGE_CHILDREN)
        record = {"argv": argv, "returncode": process.returncode, "seconds": time.monotonic() - started,
                  "children_peak_rss_kib": usage.ru_maxrss,
                  "read_blocks": usage.ru_inblock - usage_before.ru_inblock,
                  "write_blocks": usage.ru_oublock - usage_before.ru_oublock}
        write_json(self.root / (label + "-command.json"), record)
        self.commands.append(record)
        print(json.dumps({"stage": label, **record}), flush=True)
        if process.returncode:
            raise AdmissionError(f"CLI {label} failed: {(self.root / (label + '.stderr')).read_text()[-3000:]}")
        # DuckDB progress output may share stdout; production receipts are authoritative.
        result = load_json(self.root / (label + (".stdout" if command == "read" else ".json")))
        return result

    def checkpoint(self, label):
        """Prepare the explicit window checkpoint under existing ordered locks."""
        started = time.monotonic()
        fd = os.open(self.target.with_suffix(".publisher.lock"), os.O_RDONLY | os.O_NOFOLLOW)
        with os.fdopen(fd, "rb") as publisher:
            fcntl.flock(publisher.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            with market_lock(self.target):
                with duckdb.connect(str(self.target), config=READ_CONFIG) as c:
                    c.execute("CHECKPOINT")
                no_wal(self.target)
                value = {
                    "format": "baostock18-checkpoint-v1",
                    "target": str(self.target),
                    "window_started_at": self.config["window_started_at"],
                    "checkpoint_completed": True,
                    "completed_at": utcnow().isoformat(),
                    "catalog_fingerprint": fingerprint(self.target),
                }
                write_json(self.root / (label + "-checkpoint.json"), value)
        write_json(self.root / (label + "-checkpoint-metrics.json"), {"seconds": time.monotonic() - started})

    def prepare(self, label, *, staged=False, checkpoint_receipt=None):
        """Freeze references and run the same bounded backup/restore CLI."""
        if checkpoint_receipt is None:
            self.checkpoint(label)
        else:
            # Deferred verification is read-only, including its preparation.
            write_json(self.root / (label + "-checkpoint.json"), load_json(checkpoint_receipt))
        fp = self.root / (label + "-fingerprint.json")
        self.cli(
            "production-fingerprint",
            label + "-fingerprint",
            "--production-target",
            self.target,
            "--window-started-at",
            self.config["window_started_at"],
        )
        common = [
            "--production-target",
            str(self.target),
            "--expected-catalog-fingerprint",
            str(fp),
            "--window-started-at",
            self.config["window_started_at"],
            "--baseline",
            self.config["baseline"],
            "--approved-manifest-hash",
            self.config["approved_manifest_hash"],
            "--approved-batch-size",
            str(self.config["approved_batch_size"]),
            "--visibility",
            self.visibility,
        ]
        if self.scope == CATALOG_ONLY:
            result = self.cli(
                "production-backup", label + "-backup",
                "--checkpoint-receipt", self.root / (label + "-checkpoint.json"),
                "--destination", self.root / (label + "-backup"),
                "--backup-scope", CATALOG_ONLY, "--max-backup-seconds", self.cap,
                common=common,
            )
            if (result["restore_check"]["status"] != "passed"
                    or result["reference_verification"] != "deferred_to_visibility_verify"):
                raise AdmissionError("catalog_only backup/restore reconciliation failed")
            return common + ["--backup-receipt", str(self.root / (label + "-backup/receipt.json"))]
        started = time.monotonic()
        with market_lock(self.target):
            no_wal(self.target)
            observed = fingerprint(self.target)
            if observed != load_json(fp)["fingerprint"]:
                raise AdmissionError("Catalog changed before reference freeze")
            with (
                duckdb.connect(str(self.target), read_only=True, config=READ_CONFIG) as c,
                bounded_sql(c, started + self.config["max_freeze_seconds"]),
            ):
                contract = reference_contract(
                    c,
                    observed,
                    reference_root=self.config["reference_root"],
                    admission_root=self.config["admission_root"],
                    json_root=self.config["json_root"],
                    capture_inventory=self.capture_inventory,
                )
                cp = self.root / (label + "-reference-contract.json")
                ip = self.root / (label + "-references.jsonl")
                write_json(cp, contract)
                # Read back the same persisted JSON supplied to production-backup.
                frozen = freeze_reference_inventory(
                    c, load_json(cp), ip, observed, started + self.config["max_freeze_seconds"], scope=self.scope
                )
        write_json(self.root / (label + "-freeze.json"), {"seconds": time.monotonic() - started, **frozen})
        dependencies = [arg for p in self.config.get("metadata", []) for arg in ("--dependency", p)]
        result = self.cli(
            "production-backup",
            label + "-backup",
            "--checkpoint-receipt",
            self.root / (label + "-checkpoint.json"),
            "--destination",
            self.root / (label + "-backup"),
            "--backup-scope",
            self.scope,
            "--reference-manifest",
            ip,
            "--reference-contract",
            cp,
            "--max-backup-seconds",
            self.cap,
            "--restore-admission-state",
            "staged_unpublished" if staged else "baseline",
            *dependencies,
            common=common,
        )
        if (
            result["restore_check"]["status"] != "passed"
            or result["reference_verification"]["status"] != "passed"
            or result["reference_verification"]["missing"]
            or result["reference_verification"]["mismatch"]
        ):
            raise AdmissionError("Backup/restore/reference reconciliation failed")
        return common + ["--backup-receipt", str(self.root / (label + "-backup") / "receipt.json")]

    def inspect(self, label, *, published):
        """Check visibility, quarantine, batch timing and the foreign baseline."""
        started = time.monotonic()
        result = {}
        deadline = started + self.config["max_verify_seconds"]
        with (
            market_lock(self.target),
            duckdb.connect(str(self.target), read_only=True, config=READ_CONFIG) as c,
            bounded_sql(c, deadline),
        ):
            baseline = load_json(self.config["baseline"])
            result["a_share_counts"] = {
                t: c.execute(f'SELECT count(*) FROM a_share."{t}"').fetchone()[0] for t in A_SHARE_TABLES
            }
            if inventory(c) != sorted(baseline["tables"]) or result["a_share_counts"] != baseline["a_share_counts"]:
                raise AdmissionError("Foreign inventory/a_share baseline changed")
            result["visibility_counts"] = {
                t: c.execute(f"SELECT count(*) FROM {SCHEMA}.{t}").fetchone()[0]
                for t in ("basis_admission", "visibility_event", "dataset_pointer")
            }
            result["visible_rows"] = c.execute(f"SELECT count(*) FROM {SCHEMA}.consumable").fetchone()[0]
            result["quarantine_visible"] = c.execute(
                f"SELECT count(*) FROM {SCHEMA}.consumable b JOIN {SCHEMA}.literal_version l "
                "USING(snapshot_id,value_id) WHERE l.disposition='quarantine'"
            ).fetchone()[0]
            if (
                result["quarantine_visible"]
                or result["visible_rows"] != (self.config["expected_clean_rows"] if published else 0)
                or result["visibility_counts"] != {k: int(published) for k in result["visibility_counts"]}
            ):
                raise AdmissionError("Visibility/quarantine count mismatch")
            sample = c.execute(
                f"SELECT symbol,date FROM {SCHEMA}.bar_version WHERE snapshot_id=? LIMIT 1",
                [self.config["approved_manifest_hash"]],
            ).fetchone()
            result["batch_seconds"] = c.execute(
                f"SELECT batch_number,rows,clean,quarantine,elapsed_seconds FROM {SCHEMA}.batch_receipt "
                "WHERE snapshot_id=? ORDER BY batch_number",
                [self.config["approved_manifest_hash"]],
            ).fetchall()
            result["sample"] = sample
        result["seconds"] = time.monotonic() - started
        write_json(self.root / (label + ".json"), result)
        return sample

    def reader(self, label, sample, *, published, as_of=None):
        """Execute a bounded real reader CLI and check its visibility result."""
        symbol, day = sample
        args = [
            "--database",
            self.target,
            "--symbols",
            symbol,
            "--start-date",
            str(day),
            "--end-date",
            str(day),
            "--basis",
            "unadjusted",
            "--limit",
            self.config["reader_limit"],
        ]
        if as_of:
            args.extend(["--as-of", as_of])
        rows = self.cli("read", label, *args)
        if len(rows) != int(published):
            raise AdmissionError("Bounded reader/as-of mismatch")
        return rows

    def withdraw(self):
        """Compensate only this run's committed pointer after a post-switch failure.

        This emergency transaction grants no admission, retains historical events,
        checks the exact pre-switch foreign baseline, and never rewrites inputs.
        """
        if self.before_visibility is None:
            return
        with market_lock(self.target), duckdb.connect(str(self.target), config=READ_CONFIG) as c:
            current = c.execute(
                f"SELECT snapshot_id FROM {SCHEMA}.dataset_pointer WHERE adjustment_basis=?", ["unadjusted"]
            ).fetchone()
            if current != (self.config["approved_manifest_hash"],):
                return
            with transaction(c):
                event = c.execute(f"SELECT coalesce(max(event_id),0)+1 FROM {SCHEMA}.visibility_event").fetchone()[0]
                c.execute(f"INSERT INTO {SCHEMA}.visibility_event VALUES (?,?,NULL,?)", [event, "unadjusted", utcnow()])
                c.execute(
                    f"UPDATE {SCHEMA}.dataset_pointer SET snapshot_id=NULL,event_id=? WHERE adjustment_basis=?",
                    [event, "unadjusted"],
                )
            remaining = c.execute(f"SELECT count(*) FROM {SCHEMA}.consumable").fetchone()[0]
            if remaining:
                raise AdmissionError("Emergency withdrawal failed")
        write_json(self.root / "withdrawal.json", {"status": "passed", "visible_rows": remaining, "event_id": event})

    def run(self):
        """Execute every gate and retain a receipt even when a stage fails."""
        result = {
            "format": "baostock18-driver-v1",
            "config": self.config,
            "resolved_admission_root": str(Path(self.config["admission_root"]).resolve(strict=True)),
        }
        try:
            candidate = (dict(status="deferred_to_visibility_verify", target_catalog_connections=0)
                         if self.scope == CATALOG_ONLY else
                         precheck_input_references(self.config, destination=self.root / "capture-directories.jsonl"))
            self.capture_inventory = candidate
            result["capture_reference_verification"] = candidate
            write_json(self.root / "candidate-reference-precheck.json", candidate)
            if candidate["status"] not in ("passed", "deferred_to_visibility_verify"):
                raise AdmissionError("Candidate capture directory verification failed")
            common = self.prepare("before-import")
            self.cli("production-preflight", "preflight", common=common)
            imported = self.cli("production-import", "import", "--manifest", self.config["manifest"], common=common)
            expected = {
                "rows": self.config["expected_rows"],
                "clean": self.config["expected_clean_rows"],
                "quarantine": self.config["expected_quarantine"],
                "conditional": 0,
                "batches": self.config["expected_batches"],
                "new_batches": self.config["expected_batches"],
            }
            if any(imported[k] != v for k, v in expected.items()):
                raise AdmissionError("Import reconciliation differs from approved counts")
            sample = self.inspect("unpublished", published=False)
            self.reader("reader-unpublished", sample, published=False)
            if self.visibility == "deferred":
                # End the service window here. Reference freeze/backup/verify of
                # the imported catalog belongs to the separate verification run.
                self.checkpoint("staged")
                observed = self.cli(
                    "production-fingerprint", "staged-fingerprint", "--production-target", self.target,
                    "--window-started-at", self.config["window_started_at"],
                )
                write_json(self.root / "deferred-config.json", self.config)
                from openbb_duckdb.baostock18.visibility import DEFERRED

                result.update(
                    status=DEFERRED, visibility=self.visibility, imported=imported,
                    catalog_fingerprint=observed["fingerprint"],
                    backup_scope=self.scope,
                    reference_verification="deferred_to_visibility_verify",
                    restore_check=load_json(self.root / "before-import-backup/receipt.json")["restore_check"],
                    visibility_counts=load_json(self.root / "unpublished.json")["visibility_counts"],
                    next_commands={
                        "verify": [sys.executable, "-m", "openbb_duckdb.baostock18", "visibility-verify",
                                   "--config", str(self.root / "deferred-config.json"),
                                   "--deferred-receipt", str(self.root / "receipt.json"),
                                   "--output", str(self.root / "verification")],
                        "approve": f"Run argv from {self.root / 'verification' / 'approve-command.json'} after PASS",
                    },
                )
                return result
            common = self.prepare("after-import", staged=True)
            self.cli("production-preflight", "preflight-after-import", common=common)
            verified = self.cli("production-verify", "verify", common=common)
            if (
                verified["clean_unadjusted"] != self.config["expected_clean_rows"]
                or verified["quarantine"] != self.config["expected_quarantine"]
                or verified["batches"] != self.config["expected_batches"]
            ):
                raise AdmissionError("Verify differs from approved reconciliation")
            self.before_visibility = fingerprint(self.target)
            visible = self.cli(
                "production-visible",
                "visible",
                "--snapshot-id",
                self.config["approved_manifest_hash"],
                "--basis",
                "unadjusted",
                "--expected-clean-rows",
                self.config["expected_clean_rows"],
                common=common,
            )
            self.published = True
            sample = self.inspect("published", published=True)
            self.reader("reader", sample, published=True)
            event = datetime.fromisoformat(visible["known_at"])
            self.reader("reader-before", sample, published=False, as_of=(event - timedelta(microseconds=1)).isoformat())
            self.reader("reader-at", sample, published=True, as_of=event.isoformat())
            self.cli(
                "production-fingerprint",
                "replay-fingerprint",
                "--production-target",
                self.target,
                "--window-started-at",
                self.config["window_started_at"],
            )
            replay_common = common[:]
            fp_index = replay_common.index("--expected-catalog-fingerprint") + 1
            replay_common[fp_index] = str(self.root / "replay-fingerprint.json")
            replay_common = replay_common[:-2]  # Replay is READ_ONLY and grants no writer authority.
            replay = self.cli("production-replay", "replay", "--manifest", self.config["manifest"], common=replay_common)
            if replay["new_batches"] != 0 or any(replay[k] != imported[k] for k in expected if k != "new_batches"):
                raise AdmissionError("Replay was not idempotent")
            result.update(status="passed", imported=imported, verified=verified, replay=replay, visible=visible)
        except BaseException as exc:
            result.update(status="aborted", error=repr(exc))
            try:
                self.withdraw()
            except BaseException as withdrawal_error:
                result["withdrawal_error"] = repr(withdrawal_error)
            raise
        finally:
            result.update(
                seconds=time.monotonic() - self.started,
                commands=self.commands,
                peak_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
                children_peak_rss_kib=resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss,
                io_blocks={kind: {"read": usage.ru_inblock, "write": usage.ru_oublock}
                           for kind, usage in (("self", resource.getrusage(resource.RUSAGE_SELF)),
                                               ("children", resource.getrusage(resource.RUSAGE_CHILDREN)))},
            )
            write_json(self.root / "receipt.json", result)
        return result


def main():
    """Use an explicit persisted packet; no deployment defaults or implicit target."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
    parser.add_argument(
        "--check-input-references",
        action="store_true",
        help="Read only candidate/file metadata; never connect to target catalog",
    )
    parser.add_argument("--output", help="New precheck receipt, required with --check-input-references")
    args = parser.parse_args()
    config = load_json(args.config)
    if args.check_input_references:
        if not args.output:
            parser.error("--output required with --check-input-references")
        result = precheck_input_references(config, destination=Path(args.output).with_suffix(".directories.jsonl"))
        write_json(args.output, result)
        print(json.dumps(result), flush=True)
        if result["status"] != "passed":
            parser.exit(2, "Candidate references refused before opening a production window\n")
        return
    result = Driver(config).run()
    print(json.dumps({k: result[k] for k in ("status", "seconds", "next_commands") if k in result}), flush=True)


if __name__ == "__main__":
    main()

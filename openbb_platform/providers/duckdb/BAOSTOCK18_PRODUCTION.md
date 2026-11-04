# BaoStock18 production admission (Stage A)

This change implements the production entrypoint. Stage A executes it only on a
new, small local copy. It does not write a production catalog or stop/start any
production service. Dated test, wheel and local execution receipts belong in the
application output directory, linked in the delivery report.

## Required evidence and fail-closed behavior

All production commands use `python -m openbb_duckdb.baostock18 production-*`.
Every preflight/import/verify/visible call requires:

- `--production-target`: an existing, canonical absolute path whose filename is
  exactly `dq_quant_invest_data.duckdb`. Relative paths, symlinks (including parent
  aliases), hard links, directories, missing targets and access-window policies
  are refused. A copy may use that filename in its own local directory.
- `--window-started-at`: actual ISO8601 window start, including timezone.
- `--expected-catalog-fingerprint`: a caller-supplied JSON file produced on-site
  after that start by `production-fingerprint`. All five fields must match:
  `size`, `mtime_ns`, `device`, `inode`, and `header_sha256` (SHA256 of the first
  4096 bytes). Observation time, target and window must also match.
- `--approved-manifest-hash`: SHA256 of **canonical manifest JSON**, using
  `manifest.encoded`, which is also the snapshot ID. This differs from the hash
  of an indented manifest file. `--approved-batch-size` is explicitly required,
  an integer from 50000 through 100000. Import rejects any different actual
  manifest/batch. Visibility rejects any different snapshot ID.
- `--baseline`: the approved foreign physical table inventory and exact counts
  for all eight `a_share` tables. Its format is
  `{"format":"baostock18-production-baseline-v1","tables":[["schema","table"],...],
  "a_share_counts":{"attempt":N,"bar_revision":N,"bar_snapshot":N,"calendar_day":N,
  "factor_revision":N,"instrument_version":N,"job":N,"reference_snapshot":N}}`.
  Obtain it through the existing reviewed baseline process; do not auto-accept
  current counts as their own baseline.
- `--backup-receipt`: format `baostock18-production-backup-v1`, produced by
  `production-backup`. It binds this exact current fingerprint, approved
  manifest/batch, window, baseline SHA256, checkpoint certificate and backup
  manifest SHA256. Preflight rehashes the manifest, **every backup payload**, and
  the whole source catalog. Exactly one payload must be this catalog.
- `--output`: a new receipt file. Existing receipts are never overwritten.

Preflight opens only READ_ONLY DuckDB connections, with external access disabled.
It requires the existing `<catalog-stem>.market.lock`, opened O_RDONLY and locked
exclusively with LOCK_NB. It never creates/deletes a lock, waits for a lease,
replays/removes WAL or executes CHECKPOINT. A WAL of any size fails. Native
DuckDB locking also refuses a writer that ignores the market mutex. The mutex
remains held continuously through preflight, migration, batch import and writer
close. Existing admission migration version/checksum and all publication bases
are inspected before opening a writer. Staged typed bars permit the existing
unadjusted/dynamic-capture bases; admissions/events/pointers permit only
`unadjusted`. A fixed forward basis is never admitted.

CHECKPOINT is a separate, authorized window preparation step, **before** the
read-only fingerprint and backup. Its certificate must have format
`baostock18-checkpoint-v1`, target, window_started_at, checkpoint_completed=true,
completed_at and catalog_fingerprint. The timestamp must lie between window start
and fingerprint observation. A certificate is operator evidence, not a cryptographic
attestation of operator authority. The command sequence below records it after a
real CHECKPOINT and closed connection; absence of WAL is independently checked.

`production-backup` is source-read-only. It copies the closed catalog and each
explicit `--dependency` into a new absolute backup directory outside the catalog
directory, verifies copied bytes, and records the per-file manifest. Include the
frozen referenced manifests, Parquet, raw/reference and configuration files from
the reviewed dependency inventory. Its receipt certifies exactly those listed
files; catalog-only backup is not evidence that external references can be
restored. Restore checks belong in a new directory, never over the source.

For large inventories use `--dependency-manifest` instead of repeated
`--dependency` flags. The file contains sorted unique canonical absolute paths,
one JSON string per line, excluding the catalog (which is inserted automatically).
The transport persists batches of at most **512 paths**, runs `rsync --from0 --files-from` for each batch, flushes the backup filesystem
with `syncfs`, and hashes each source and backup independently. No dependency list is expanded through argv or argparse. The
streaming `manifest.jsonl` uses `baostock18-backup-files-v2`; preflight verifies
both that format and the original v1 receipts. Existing small explicit-list
callers retain their v1 format.

Manifest mode requires measured `--expected-files`/`--expected-bytes`, an
`--estimated-seconds` value derived from a real NAS exercise, and a positive
`--max-backup-seconds` cap. Counts include the catalog. The exact totals must
reconcile, and the estimate must fit the cap before copying. The transport checks
the deadline between hash blocks and while rsync runs. On expiry it requests
SIGINT only on its own rsync process, waits for exit, and retains partial evidence
without a success receipt. NAS I/O and fsync calls may finish after a
deadline before Python regains control; the cap never justifies killing a service.
A backup error aborts the window and restores the previously running services.
First validate a complete real NAS subtree through backup, per-file SHA256 and
restore reconciliation while services are still running.

### Explicit catalog and reference verification scope

`--backup-scope` defaults to `full`. To explicitly choose the smaller recovery
set, pass `--backup-scope catalog_with_reference_verification`,
`--reference-manifest <inventory.jsonl>`, `--reference-contract <contract.json>`
and `--max-backup-seconds 14400` (a smaller positive cap is permitted). The named
`MAX_REDUCED_SCOPE_BACKUP_SECONDS` limit allows four hours: the measured reference
inventory is about 596 GB across 2.54 million files, and hashing at about
4.4 GB/min takes roughly two hours, with margin for verification and restore.
Zero, negative, nonnumeric and larger caps are refused. This limit applies only
to the explicit reduced scope; full-scope behavior is unchanged. Passing
reference options without the explicit scope is an error. This scope copies the
catalog and small explicitly listed metadata; referenced payloads stay in place.
It does not supply independent recovery of bronze/silver archives.

The frozen inventory is sorted unique JSONL: `{"path":"/absolute/file",
"size":123,"sha256":"<64 hex characters>"}`. The contract has format
`baostock18-catalog-reference-contract-v1`, the exact `catalog_fingerprint`,
`columns` containing all physical `[schema,table,column]` triples sorted by those
three fields, and `rules` with `schema`, `table`, `path_column`, `hash_column`
and canonical absolute `root`. Every projection is rerun READ_ONLY, every row
must be in the inventory, and every inventory entry must be seen. Catalog hashes
must match the frozen inventory. A null hash column requires separately frozen
byte hashes; a logical content ID must not be treated as a byte SHA256.
`provenance_columns` may explain only logical `business_key`, `source_key`,
`provider_key`, or relocated `original_path` fields. Other path columns,
unsupported external views, unresolved embedded JSON file references, unknown schema,
missing references and hash mismatches all abort. This contract never silently
certifies unsupported reference formats.

Nested JSON file references are traversed recursively and require an explicit
canonical `json_root`. `freeze_reference_inventory` can build the inventory from
these same projections, comparing available catalog byte hashes and measuring
byte hashes for unpinned references. Backup then independently rehashes all of
them; logical IDs are never substituted for file hashes.

Each reference is opened with O_NOFOLLOW and hashed in bounded blocks, with
size/mtime/device/inode stability checks. Catalog projections stream without
DISTINCT/aggregation; SQLite bounds the deduplication memory. An owned DuckDB
interrupt timer also enforces the deadline while execute is awaiting results.
`reference-verification.jsonl` and its summary retain count/bytes/missing/mismatch
and projection coverage. Metadata copies are limited to 64 MiB per file and
256 MiB total. A failed directory is retained without a success receipt.

The catalog is independently copied to a new `restore_check` directory, opened
READ_ONLY with external access disabled, and checked against the eight approved
a_share counts, zero admission physical tables and a `LIMIT 3` read. Source,
backup and restored hashes must agree. A shared monotonic deadline covers the
entire reduced operation, including enumeration and restore. Receipt `scope`,
`uncopied_reference_bytes`, reference coverage, before/after whole catalog SHA256,
absent WAL, fingerprint and restore duration are mandatory for this scope.
The original requirement for fresh backup evidence after catalog changes remains;
a reduced pre-admission backup cannot authorize an already modified catalog.
After import, explicitly pass `--restore-admission-state staged_unpublished` for
the required fresh backup. This additional restore gate reconciles the approved
ready snapshot in both source and restored catalog, requires exactly one snapshot
and zero rows in basis_admission, visibility_event and dataset_pointer, and checks
the unchanged a_share baseline. The default `baseline` restore continues to
require zero admission physical tables. A staged restore never authorizes a
published catalog or relaxes the ordinary production verification gates.

The default offline/rehearsal rejection remains. The production filename cannot
use an implicit offline writer even when empty. `rehearsal_receipt` still binds
only `rehearsal.duckdb` and cannot be combined with production authorization.

The production writer uses the identical migration and importer: composite PKs,
lineage, versioned quality, 50000–100000-row transactions with atomic batch
receipts, interruption rollback/resume and idempotent replay. Import leaves the
snapshot invisible. Before publication, full read-only reconciliation checks
snapshot/manifest, memberships, typed counts, lineage coverage, quarantine quality,
batch order/counts/size and clean count. Only the final admission/event/pointer
mutation runs in the short visibility transaction. Reader basis rejection,
fixed-2026-10-15 rejection, visibility-time as-of and quarantine exclusion are
shared with offline mode. No legacy `a_share` consumer view is repointed.

Every catalog modification invalidates the old fingerprint/backup, including a
partially committed import. Resume using the same candidate manifest and approved
batch size, but obtain a new CHECKPOINT certificate, fingerprint and backup first.
Do the same between import and visibility. This deliberately requires a backup
of the current staged state; retain the original pre-import recovery set as well.

## Future production window CLI sequence (not executed in Stage A)

Run the Docker commands on the NAS host. The binary/image/container names below
come from the earlier deployment inventory; read back actual state at window
start. Use a separate maintenance container and install the approved wheel into
its window directory. The production service venvs are not installation targets.

Before these commands, finish active native jobs/in-flight requests and prevent
new requests. `docker stop` is not the application drain mechanism. The three
native job containers have startup/once behavior: their recovery commands also
resume that work. The window's existing authorization must cover those jobs.

```bash
set -euo pipefail
DOCKER=/share/CACHEDEV1_DATA/.qpkg/container-station/bin/docker
PHYSICAL=/share/CACHEDEV1_DATA/homes/zhudingqiu/dq-platform
FS="$PHYSICAL/fs/home/zdqclawbot"
DB=/home/zdqclawbot/work/app/dq-quant-invest-data/data/catalog/dq_quant_invest_data.duckdb
BB_PY=/home/zdqclawbot/work/app/openbb/.venv-market-data/bin/python
# Set these absolute artifact paths/values from the approved window packet.
: "${WINDOW_ID:?new unique window id}"
: "${APPROVED_WHEEL:?absolute wheel path under the mounted home}"
: "${APPROVED_WHEEL_SHA256:?reviewed wheel checksum}"
: "${MANIFEST:?approved manifest absolute path under the mounted home}"
: "${APPROVED_MANIFEST_HASH:?approved canonical manifest hash}"
: "${APPROVED_BATCH_SIZE:?approved batch size}"
: "${BASELINE:?approved baseline absolute path under the mounted home}"
: "${DEPENDENCY_LIST:?absolute sorted JSONL file of frozen dependency paths}"
WINDOW=/maintenance-window
HOST_WINDOW="$PHYSICAL/recovery/baostock18_$WINDOW_ID"
MAINT="baostock18-production-$WINDOW_ID"
mkdir -m 700 "$HOST_WINDOW"
"$DOCKER" inspect dq-quant-invest-data-nas-api-1 \
  dq-quant-invest-data-nas-data-api-1 dq-quant-invest-data-nas-daily-stocks-1 \
  dq-quant-invest-data-nas-stock-backfill-1 dq-quant-invest-data-nas-financedatabase-names-1 \
  > "$HOST_WINDOW/services-before.json"
# After drain has completed, stop exactly the running members recorded above.
"$DOCKER" stop --time -1 dq-quant-invest-data-nas-daily-stocks-1
"$DOCKER" stop --time -1 dq-quant-invest-data-nas-stock-backfill-1
"$DOCKER" stop --time -1 dq-quant-invest-data-nas-financedatabase-names-1
"$DOCKER" stop --time -1 dq-quant-invest-data-nas-api-1
"$DOCKER" stop --time -1 dq-quant-invest-data-nas-data-api-1
# Read back status and re-audit native catalog/lock FDs on the host before proceeding.
"$DOCKER" inspect dq-quant-invest-data-nas-api-1 \
  dq-quant-invest-data-nas-data-api-1 dq-quant-invest-data-nas-daily-stocks-1 \
  dq-quant-invest-data-nas-stock-backfill-1 dq-quant-invest-data-nas-financedatabase-names-1 \
  > "$HOST_WINDOW/services-drained.json"
"$DOCKER" run -d --pull never --name "$MAINT" --network none \
  --read-only --user 1000:1000 --cpus 2 --memory 6g --memory-swap 6g \
  --pids-limit 128 --tmpfs /tmp:rw,size=128m \
  --mount "type=bind,src=$FS,dst=/home/zdqclawbot,readonly" \
  --mount "type=bind,src=$FS/work/app/dq-quant-invest-data/data/catalog,dst=$(dirname "$DB")" \
  --mount "type=bind,src=$HOST_WINDOW,dst=$WINDOW" \
  --entrypoint "$BB_PY" \
  sha256:ed8e52eeb364184907bd7ae8c8797892dd54b043b4055a8d87da81ad8ec41b64 \
  -B -c 'import signal; signal.pause()'
# Checksum and install without touching deployed environments or using the network.
"$DOCKER" exec "$MAINT" "$BB_PY" -c \
  'import hashlib,sys; assert hashlib.file_digest(open(sys.argv[1],"rb"),"sha256").hexdigest()==sys.argv[2]' \
  "$APPROVED_WHEEL" "$APPROVED_WHEEL_SHA256"
"$DOCKER" exec "$MAINT" "$BB_PY" -m pip install --no-deps --no-index \
  --target "$WINDOW/provider" "$APPROVED_WHEEL"
# Preserve the actual window timestamp; never reuse the Stage A observation.
WINDOW_STARTED_AT=$("$DOCKER" exec "$MAINT" "$BB_PY" -c \
  'from datetime import datetime,timezone; print(datetime.now(timezone.utc).isoformat())')
runbb() {
  "$DOCKER" exec -e PYTHONPATH="$WINDOW/provider" "$MAINT" "$BB_PY" -B \
    -m openbb_duckdb.baostock18 "$@"
}
```

Run the following shell block in the same host shell. The preparation function
executes CHECKPOINT as a separate window operation. It also holds the existing
publisher mutex before the market mutex, matching the deployed backup lock order.
Preflight/import subsequently need only the market mutex. No lock is created.

```bash
prepare_current() {
  local label=$1
  "$DOCKER" exec -i -e PYTHONPATH="$WINDOW/provider" "$MAINT" "$BB_PY" -B - \
    "$DB" "$WINDOW_STARTED_AT" "$WINDOW/$label-checkpoint.json" <<'PY'
import fcntl, os, sys
from pathlib import Path
import duckdb
from openbb_duckdb.baostock18.production import production_path, market_lock, fingerprint, write_json
from openbb_duckdb.baostock18.store import utcnow
path = production_path(sys.argv[1])
publisher = path.with_suffix('.publisher.lock')
fd = os.open(publisher, os.O_RDONLY | os.O_NOFOLLOW)
with os.fdopen(fd, 'rb') as lock:
    fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    with market_lock(path):
        with duckdb.connect(str(path), config={'enable_external_access': False}) as c:
            c.execute('CHECKPOINT')
        write_json(sys.argv[3], {'format':'baostock18-checkpoint-v1', 'target':str(path),
            'window_started_at':sys.argv[2], 'checkpoint_completed':True,
            'completed_at':utcnow().isoformat(), 'catalog_fingerprint':fingerprint(path)})
PY
  runbb production-fingerprint --production-target "$DB" \
    --window-started-at "$WINDOW_STARTED_AT" --output "$WINDOW/$label-fingerprint.json"
  COMMON=(--production-target "$DB" \
    --expected-catalog-fingerprint "$WINDOW/$label-fingerprint.json" \
    --window-started-at "$WINDOW_STARTED_AT" --baseline "$BASELINE" \
    --approved-manifest-hash "$APPROVED_MANIFEST_HASH" \
    --approved-batch-size "$APPROVED_BATCH_SIZE")
  # DEPENDENCY_LIST is a sorted UTF-8 JSONL file: one canonical absolute
  # path encoded as a JSON string per line. Do not expand its contents in argv.
  # Freeze the inventory after drain; counts include the catalog exactly once.
  runbb production-backup "${COMMON[@]}" \
    --dependency-manifest "$DEPENDENCY_LIST" \
    --expected-files "$EXPECTED_FILES" --expected-bytes "$EXPECTED_BYTES" \
    --estimated-seconds "$ESTIMATED_BACKUP_SECONDS" \
    --max-backup-seconds "$MAX_BACKUP_SECONDS" \
    --destination "$WINDOW/$label-backup" \
    --checkpoint-receipt "$WINDOW/$label-checkpoint.json" \
    --output "$WINDOW/$label-backup-command.json"
  COMMON+=(--backup-receipt "$WINDOW/$label-backup/receipt.json")
}

prepare_current before-import
runbb production-preflight "${COMMON[@]}" --output "$WINDOW/preflight.json"
runbb production-import "${COMMON[@]}" --manifest "$MANIFEST" --output "$WINDOW/import.json"

# Mandatory refresh after import (also after a partially committed interrupted import).
prepare_current after-import
runbb production-preflight "${COMMON[@]}" --output "$WINDOW/preflight-after-import.json"
runbb production-verify "${COMMON[@]}" --output "$WINDOW/verify.json"
CLEAN=$("$DOCKER" exec "$MAINT" "$BB_PY" -c \
  'import json,sys; print(json.load(open(sys.argv[1]))["clean_unadjusted"])' "$WINDOW/verify.json")
# Compare verify.json against the approved candidate reconciliation before this switch.
runbb production-visible "${COMMON[@]}" --snapshot-id "$APPROVED_MANIFEST_HASH" \
  --basis unadjusted --expected-clean-rows "$CLEAN" --output "$WINDOW/visible.json"
runbb read --database "$DB" --symbols SH.600000 \
  --start-date 2026-10-13 --end-date 2026-10-15 --basis unadjusted --limit 100 \
  > "$HOST_WINDOW/new-reader-smoke.json"
# as-of uses the actual visible.json known_at, not the securities date.
ASOF=$("$DOCKER" exec "$MAINT" "$BB_PY" -c \
  'import json,sys; print(json.load(open(sys.argv[1]))["known_at"])' "$WINDOW/visible.json")
runbb read --database "$DB" --symbols SH.600000 \
  --start-date 2026-10-13 --end-date 2026-10-15 --basis unadjusted --limit 100 \
  --as-of "$ASOF" > "$HOST_WINDOW/asof-reader-smoke.json"
```

Retain the full pre-import and staged backups and manifests. Verify the old
`a_share` queries and approved clean/quarantine/lineage totals before restoring
services. The CLI is an additive explicit dataset entrypoint; it does not replace
the legacy `a_share` route. Restore only services recorded running before the
window, in this order; do not use `compose up` or start previously exited services:

```bash
restore_if_previously_running() {
  local container=$1
  if "$DOCKER" exec "$MAINT" "$BB_PY" -c '
import json,sys
rows=json.load(open(sys.argv[1]))
sys.exit(0 if any(x["Name"].lstrip("/")==sys.argv[2] and x["State"]["Running"] for x in rows) else 1)
' "$WINDOW/services-before.json" "$container"; then
    "$DOCKER" start "$container"
  fi
}
restore_if_previously_running dq-quant-invest-data-nas-data-api-1
"$DOCKER" inspect dq-quant-invest-data-nas-data-api-1 > "$HOST_WINDOW/data-api-restored.json"
# Check its read-only endpoint/health and bounded legacy query before continuing.
restore_if_previously_running dq-quant-invest-data-nas-api-1
"$DOCKER" inspect dq-quant-invest-data-nas-api-1 > "$HOST_WINDOW/api-restored.json"
restore_if_previously_running dq-quant-invest-data-nas-daily-stocks-1
restore_if_previously_running dq-quant-invest-data-nas-stock-backfill-1
restore_if_previously_running dq-quant-invest-data-nas-financedatabase-names-1
"$DOCKER" inspect dq-quant-invest-data-nas-api-1 \
  dq-quant-invest-data-nas-data-api-1 dq-quant-invest-data-nas-daily-stocks-1 \
  dq-quant-invest-data-nas-stock-backfill-1 dq-quant-invest-data-nas-financedatabase-names-1 \
  > "$HOST_WINDOW/services-restored.json"
"$DOCKER" kill --signal INT "$MAINT"
```

If import fails, leave the pointer unchanged and preserve committed batches.
Resume only after refreshing current evidence. If the pointer transaction fails,
its admission/event/pointer changes all roll back. A later withdrawal uses the
same gated `production-visible` command with `--snapshot-id` omitted and fresh
current evidence. Physical recovery must restore to a separate directory and
reconcile changes by other datasets before any catalog replacement.

No production drain/backup/restore duration is claimed by Stage A. The local
copy timings and counts in its receipts describe only that local execution.

## Reproduce the local wheel demonstration

```bash
# PYTHONPATH points at the package installed from the newly built wheel.
PYTHONPATH=/absolute/wheel-install /absolute/test-environment/bin/python \
  -m openbb_platform.providers.duckdb.tests.production_demo \
  --output /new/absolute/local-run
```

It saves all CLI argv, stdout/stderr, elapsed times, fingerprint/full-hash abort
proof, per-operation checkpoint/backup manifests and final reconciliation in
`LOCAL_DEMO_RECEIPT.json`. The synthetic input has 3 clean rows and 15838 rejected
rows; these are measured/asserted by the runner, rather than production totals.

## Reusable complete-chain driver

`python -m openbb_duckdb.baostock18.driver --config /absolute/run.json` executes
one implementation for an isolated production-shaped copy and the production
catalog. There is no alternate production import script. The JSON packet must
supply canonical absolute `target`, new `output`, `manifest`, approved `baseline`,
`reference_root`, `admission_root`, `json_root`, `window_started_at`,
`approved_manifest_hash`, `approved_batch_size`, `max_backup_seconds` (at most
14400), `max_freeze_seconds`, `max_verify_seconds`, `expected_rows`,
`expected_clean_rows`, `expected_quarantine`, `expected_batches`, `reader_limit`,
and optional small `metadata` files. Fingerprint/checkpoint/contract paths are
namespaced under the explicitly supplied output directory and created fresh.
The freeze and backup bounds are separate and both recorded in the packet.

The driver performs CHECKPOINT under publisher then market locks; fingerprint;
reference freeze; the actual `production-backup` CLI with the explicit reduced
scope and cap; restore-check receipt checks; preflight; migration/import; exact
unpublished counts and a bounded reader; a fresh staged backup/restore;
preflight/verify; visibility; bounded current and visibility-time as-of readers;
and an identical-input replay. Every command retains argv, stdout, stderr, return
code and measured seconds. Batch receipts preserve each batch's rows and measured
elapsed seconds. A failure retains evidence and stops; a post-switch failure
withdraws this run's current pointer in an explicit compensating transaction.
No input, catalog lock or WAL is removed.

Reference contracts normalize DuckDB tuple rows to JSON list triples **before**
in-memory reference enumeration. The freeze reads back the persisted contract,
so its schema representation matches that supplied to the backup CLI. Production
results are loaded from the independent CLI receipt, since DuckDB progress output
can share stdout with printed JSON.

`production-replay` is deliberately read-only. It requires fresh target/window
fingerprint, approved manifest/batch and baseline, verifies every frozen input,
requires an already-ready exact snapshot, and fully reconciles receipts, quality,
lineage and typed counts. It returns the existing import receipt with
`new_batches=0`. It grants no write authority and needs no fresh published-state
backup. Ordinary `production-import`, verify and visibility backup gates remain
unchanged; an incomplete or changed candidate cannot use replay to resume.

From the repository root, `scripts/baostock18/rehearsal.py --ddl <saved-ddl.json>
--output <new-absolute-copy-directory> --max-backup-seconds 14400
--max-freeze-seconds 14400 --max-verify-seconds 600` constructs all saved physical
tables (at least 79), views and indexes, populates all eight legacy a_share tables,
and executes the installed driver with two real batches: 50,001 clean and 15,838
quarantined rows. These are synthetic fixture counts, never production results.
Use `PYTHONPATH` to select the exact wheel installation and repository test factory.

`scripts/baostock18/window.py --config <deployment-packet.json>` wraps that same
wheel driver with configured SSH/Docker paths, service identities, mount paths,
health URLs and waiting bounds. It requires a green local chain receipt, waits
for native jobs to finish naturally using the read-only host audit, stops only
previously running targets using `stop --time -1`, and restores data-api, api,
then the three native services in the configured order. Originally exited
containers are never started. A remote supervisor records actual driver exit;
service recovery waits for that receipt even after a transport failure. Preserve
the original container inventory and every run directory. The wrapper neither
installs into deployed service environments nor rewrites the approved baseline.

Before CHECKPOINT the driver resolves candidate `capture_inventory.relative` as
a **directory** against `admission_root`. The production container mapping is
`/home/zdqclawbot/work/app/openbb/output`; the receipt records the actual resolved
root. `--check-input-references --output <new-receipt.json>` runs the same check
without connecting to the target catalog. It requires canonical directories,
recursively enumerates every regular member and measures its byte SHA256, size
and stable identity. Missing/unreadable directories or members, symlinks,
nonregular entries, escaped roots and changes during hashing abort. The receipt
records capture/directory/file counts, bytes, missing/changed/error counts,
elapsed seconds and measured throughput; periodic hashing progress is retained.

The adjacent `*.directories.jsonl` freezes each directory's complete file list,
individual sizes and SHA256 values. The driver also freezes an inventory inside
its execution directory and binds its hash in the staged reference contract.
Only admission `row_lineage.source_path` has directory semantics: the resolver
compares its complete membership to that inventory and projects each regular
file into the existing independent backup hash verification. Directory/content
IDs are never treated as file hashes. Other reference and admission gates retain
their existing semantics. The service wrapper completes this check before any
target stop. The local production-shaped fixture uses the same relative capture
directory shape with authority, eligibility, page body and fragment files.

The wrapper also supports `--precheck-only` with an **already running** maintenance
container and a new directory within its existing recovery mount. It installs the
same rehearsed wheel only there, runs the driver's read-only input-reference
mode, and records before/after identities for every preexisting container. This
mode never starts/stops a service or connects to the target catalog; a failed
reference gate is an aborted packet with no service recovery needed.

# BaoStock18 offline admission

`openbb_duckdb.baostock18` stages a separate narrow price-basis dataset. It is
an offline Python API and CLI, with no acquisition calls or production mode.
Install `openbb-duckdb[baostock18]` to supply PyArrow for the importer. The reader
needs DuckDB only (in addition to the provider's normal OpenBB dependencies).
No provider registration, router, service or `a_share` table/view is changed.

## Contract and migration

`migrations/001_narrow_basis.sql` is applied transactionally with a recorded
SHA256. A later run refuses a changed migration/version. A writer first inspects
an existing file read-only and refuses catalogs containing other datasets. There
is no default database path. Use a dedicated local scratch file; this command is
not the approved production migration/publisher entrypoint.

The Python functions `import_candidate` and `set_visible` accept an explicit
`rehearsal_receipt` for a production-shape **copy**. The CLI has no such option.
The JSON receipt must be adjacent to an existing `rehearsal.duckdb`, use format
`baostock18-rehearsal-clone-v1`, declare purpose
`migration rehearsal only; NOT a production backup` and `production_admitted:
false`, and bind its absolute `target_path`, `[st_dev, st_ino]` `target_identity`,
distinct `source_path`/`source_physical_path`/`source_identity`, and exact
`baseline_tables` pairs of schema/table names. Missing files, hard links, changed
identities or table inventories, and access-window policies are rejected before
a writable connection opens. The baseline tables are left intact; only the
checksummed admission migration and its own rows are written. Generate this
receipt only after verifying a new consistent clone. It is an explicit rehearsal
opt-in, not a production permission or a backup certificate. Default calls still
reject every catalog containing other datasets.

For a large materialized candidate, sorting `row_lineage` by `value_id` allows
each batch's parameterized value-id bounds to prune unrelated row groups while
preserving all lineage matches.

All tables live in `baostock18_admission`. A snapshot identity hashes a canonical
manifest binding the source contract, transform, selection, the candidate file
and **every local Parquet dependency**. Extra evidence files can bind raw hash
ledgers and selection receipts. The manifest verifies those bytes, not the
underlying raw archive entries named in a ledger. Raw reachability and payload
re-audit remain separate admission checks.

`literal_version` retains the exact eighteen strings (including empty strings),
requested/actual flags, original quality flags and disposition, keyed by
`(snapshot_id,value_id)`. This composite key also expresses snapshot membership;
typed `bar_version` has a unique `(snapshot_id,symbol,date,adjustment_basis)` and
foreign keys to literals. `row_lineage` retains every matching capture/page/row,
source/body/receipt hashes, source path, source status, complete/partial state,
terminal/historical status and complete capture JSON. `quality` records versioned
rules. There are no invented job, reference, factor or historical availability
records. `source_available_at` stays NULL; local import/visibility times do not
make reconstructed captures PIT safe.

Prices and ratios use DECIMAL(38,18), volume uses integer shares, amount is CNY,
and the session timezone is Asia/Shanghai. Percent strings are multiplied by an
exact decimal 0.01. Excess precision is quarantined instead of rounded. NULL and
zero stay distinct. Empty core prices, flag mismatch, invalid identity, numeric
or OHLC data, conflicts and inadmissible sources are quarantined. Source partial,
warning or unknown/conflicting calendar states stay conditional. Neither cohort
enters the clean consumption view. No missing dates/prices are filled.

Both requested=actual=3 (`unadjusted`) and requested=actual=2
(`forward_dynamic_snapshot`) can be staged as narrow typed records. Only clean
unadjusted data can be explicitly admitted for consumption. The reader refuses
missing base, forward/backward/ambiguous or unapproved basis, and any fixed
2026-10-15 forward adjustment request. Dynamic captures are never combined to
claim a fixed forward base.

## Import and read

```bash
python -m openbb_duckdb.baostock18 manifest \
  --candidate /absolute/candidate.duckdb --output /scratch/manifest.json \
  --start-date 1990-12-19 --end-date 2026-10-15
python -m openbb_duckdb.baostock18 import \
  --manifest /scratch/manifest.json --target /scratch/offline.duckdb \
  --batch-size 50000
```

The importer streams sorted literals in 50,000--100,000 row batches (a final
batch may be smaller). Each transaction includes literals, all matching lineage,
quality, typed bars and its receipt. Receipts save last value ID, digest,
classification counts, lineage count, actual commit time and elapsed seconds.
Interruption rolls back the entire current batch. Retrying the identical frozen
manifest resumes after the last committed ID. Completed replay is a no-op.
Any changed source needs a new manifest/snapshot. A snapshot becomes `ready`
only after input, stored literals and receipt counts reconcile; importing never
changes the visible pointer.

After reviewing the local receipt, use its actual clean unadjusted count:

```bash
python -m openbb_duckdb.baostock18 visible --target /scratch/offline.duckdb \
  --snapshot-id <manifest-sha256> --basis unadjusted --expected-clean-rows <count>
python -m openbb_duckdb.baostock18 read --database /scratch/offline.duckdb \
  --symbols SH.600000 --start-date 2026-10-13 --end-date 2026-10-15 \
  --basis unadjusted --limit 100 --as-of 2026-11-04T04:00:00+00:00
```

Pointer changes are short transactions with append-only visibility events.
Omitting `--snapshot-id` in `visible` records a withdrawal; prior as-of queries
still resolve the former event. Each version replaces its complete selected
scope, so removed dates/symbols do not silently fall back to older snapshots.
`consumable` joins the current admitted pointer, ready snapshot and clean literals.
`read_bars` uses the same filters, resolving the event at `as_of` when supplied.
Before first visibility or after withdrawal it returns an empty list.

The reader opens `read_only=True`, disables external access, bounds date/symbols
and limits rows to 100,000 (overflow raises, without silent truncation). It takes
an existing market lock through an O_RDONLY descriptor and never creates one.
It refuses an access-window policy it cannot enforce. It does not read legacy
wide bars or automatically approve any basis. This module is not registered as
a new OpenBB router command; invoke the explicit API/CLI above.

## Validation

```bash
python -m pytest openbb_platform/providers/duckdb/tests
python -m ruff check openbb_platform/providers/duckdb
```

Tests use synthetic local responses and exercise real DuckDB transactions,
interrupt/resume, replay, 15,838 rejected versions, partial/warning lineage,
exact decimal ratios, dependency hashes, as-of correction/date removal,
withdrawal, basis rejection, reader bounds, checksummed migrations and refusal
of a foreign catalog. Real candidate rehearsal receipts and old/new
reconciliation belong in the application run directory, not this source tree.

Production still requires independent raw/reference acceptance, conflict review,
deployment integration and an explicitly authorized maintenance/backup window.
Passing these offline checks does not mean central admission occurred.

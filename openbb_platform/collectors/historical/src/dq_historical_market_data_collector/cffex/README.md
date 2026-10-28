# CFFEX daily basket collector

This engine is separate from the fixed-symbol historical collector. Acquisition
captures the source response as raw evidence before parsing and normalization.
Calendar acquisition uses the installed OpenBB AKShare provider/QueryExecutor.
Every requested trading day is one full-market request, apart from up to two
bounded retries. No scheduler or remote publishing is included.

## Configuration

```yaml
version: 1
root: ./state/cffex
calendar_file: ./cffex-calendar.json
start_date: 2010-04-16
# Set explicitly when the daily updater must catch up older history:
# update_start_date: 2026-09-01
not_before: "18:30"
source_id: cffex
source:
  provider: akshare
  model: AkshareFuturesHistDailyCffex
  credentials_env: {}
  timeout_seconds: 60
  min_interval_seconds: 2
  retries: 2
  retry_delay_seconds: 2
  max_rows: 10000
  max_response_bytes: 8000000
```

Before committing rows, set `units` with all four nonempty fields: `price_unit`,
`volume_unit`, `open_interest_unit`, and `turnover_unit`. Determine their values
from the source contract; the example intentionally does not guess them and
therefore refuses committed rows with `units_undetermined`. Units participate in
the capture fingerprint, so changing them creates a new generation.

Paths are relative to the YAML file. Unknown settings, duplicate YAML keys,
credentials, and malformed date values are rejected. The other accepted daily
model is `AkshareCffexDaily`. A separately typed, internal descriptor allows
`AkshareToolTradeDateHistSina` only for calendar acquisition.

The calendar JSON explicitly asserts complete coverage, not merely the dates
for which this collector has received rows:

```json
{
  "timezone": "Asia/Shanghai",
  "coverage_start": "2026-09-17",
  "coverage_end": "2026-09-20",
  "trading_dates": ["2026-09-17", "2026-09-18"],
  "source": "operator-supplied complete reference calendar"
}
```

Requests outside coverage fail before any provider call. Only dates omitted
*within explicit coverage* are closed days. Empty responses on trading days
are errors, never evidence of a holiday. A refreshed calendar also contains
`captured_at` in UTC.

## Commands

The installed commands are `dq-cffex-daily-collector` and `dq-cffex-acquire`.
The latter accepts `--plan <json> --root <dir>` and captures raw evidence without
loading the collector configuration or journal. HTTP response bodies and status codes
are stored together in the integrity-checked capture receipt. Only 2xx responses
can succeed; old raw captures without a status cannot be replayed as successes.
Failed responses remain as evidence and a retry uses a new generation.
CFFEX writes `raw-acquisition-v2` in its own versioned namespace; v1 remains
unchanged for other providers. A matching legacy v1 capture blocks acquisition
without a source call, because its HTTP status cannot be established. Generation
identity material remains stable so the legacy capture can be found.
Offline consumption also verifies the capture's durable terminal state and bound
raw HTTP status; incomplete, statusless and non-2xx raw captures are refused.
Existing provider-transformed captures do not require an HTTP status.
Use `--help` for its arguments.
The daily module is also runnable directly:

```bash
python -m dq_historical_market_data_collector.cffex.cli calendar-refresh --config cffex.yaml
python -m dq_historical_market_data_collector.cffex.cli backfill --config cffex.yaml \
  --start-date 2010-04-16 --end-date 2026-09-18 --max-days 10
python -m dq_historical_market_data_collector.cffex.cli update --config cffex.yaml
python -m dq_historical_market_data_collector.cffex.cli update --config cffex.yaml --as-of 2026-09-18
python -m dq_historical_market_data_collector.cffex.cli status --config cffex.yaml --limit 100
python -m dq_historical_market_data_collector.cffex.cli export --config cffex.yaml \
  --output quotes.csv --start-date 2026-09-01 --end-date 2026-09-18
```

Both backfill and export bounds are **inclusive**. Backfill defaults to the
configured launch date and the last publication-eligible calendar day. At
18:30 Shanghai time the current date becomes eligible; before then eligibility
ends on the previous calendar day. A date-only `--as-of` can restrict eligibility,
never override the current cutoff or authorize future data.

Normal backfills skip completed days. Repeat the same bounded command to
resume. `--max-days` limits requested dates, not successful dates or retries;
deferred work returns exit code 3. `--refresh` explicitly re-fetches completed
days and reconciles corrections. Combining `--refresh` and `--max-days` always
forces the first bounded subset again: use explicit successive date windows
when performing a bounded correction sweep.

Update re-fetches the last seven **calendar** days (end minus six through end),
plus unfinished trading days since its durable update origin. The first update
origin is `update_start_date`, or the beginning of the correction window when
not configured. It is persisted before requests. Later invocations scan that
origin to detect failed/missing days after downtime, while skipping completed
days outside the rolling window. `update_cursor` advances only through
contiguously complete dates; explicit closed days can advance it. Updates are
not a substitute for a full backfill unless an earlier `update_start_date` is
configured. Every individual request has a hard, killable worker timeout;
there is no total-run time budget for update.

Starting a date attempt invalidates `update_cursor` atomically with marking that
date running. Failed/interrupted refreshes therefore cannot leave an old cursor
asserting completeness. Fatal errors terminalize this run's unfinished work,
preserve prior quotes, and retain the original exception even if cleanup fails.
Recovery also invalidates the cursor when it finds abandoned running attempts
or dates. An interrupted update leaves it absent/null; the next finished update
recomputes contiguous coverage (a partial update stops at its first failed date).
Read-only `status` and opening a clean journal do not invalidate a valid cursor.

Calendar refresh needs no pre-existing file, uses an empty OpenBB query, sorts
unique ISO `trade_date` values, rejects weekends and coverage regressions, and
atomically replaces only a validated JSON calendar. Source attribution is
`akshare.tool_trade_date_hist_sina (Sina/SSE reference calendar; not an exchange tick/event calendar)`.
Failures preserve the previous file and return nonzero. Refresh attempts and
returned envelopes are journaled. An external scheduler may run calendar
refresh before update; it must stop on a nonzero refresh result.

Refresh also rejects **any previously recorded trading date removed within the
overlap**, including an interior date with unchanged coverage endpoints, with
`calendar_sessions_removed`. Added sessions are allowed. Rejected responses are
archived and retried within the configured bound; the previous calendar file
remains byte-for-byte intact. Deliberate closure corrections require an explicit,
manually reviewed edit of the operator calendar, with the evidence retained;
automatic refresh never silently revokes known sessions.

Every command emits a JSON summary on stdout. Exit codes: 0 complete, 1 data
or provider failure, 2 invalid configuration/fatal runtime or CLI usage,
3 partial/deferred, 4 interrupted, 5 another writer holds the lock.

## Persistence and data contract

- `root/cffex.sqlite3` uses WAL and `synchronous=FULL`. `runs`, `attempts`,
  `day_status`, and `meta` preserve progress, diagnostic codes, retries, and the
  update cursor. Opening under the writer lock recovers abandoned running work
  as interrupted and makes the affected date retryable.
- `raw/YYYY-MM-DD/<attempt-id>.json` contains the complete OpenBB response rows,
  metadata, warnings, query, descriptor, and UTC observation time; calendar
  envelopes are under `raw/calendar/`. Files are published from fsynced temporary
  files with exclusive hard links and mode 0444, without replacing old files.
  This is application-level immutability, not a privileged-user-proof WORM store.
- A complete validated day is committed in one SQLite transaction: revision
  snapshot, current `futures_daily` upserts keyed by `(trading_date,
  contract_symbol)`, attempt success, and date
  checkpoint. A failed refresh never replaces previous good quotes. Historical
  snapshots remain in `revisions.rows_json`, linked to their raw attempt.
  An unchanged normalized refresh adds an acquisition attempt/raw envelope but
  not a duplicate normalized revision; current rows retain the observation time
  of the revision that produced them.
- A same-date refresh must contain **all** previously stored contract symbols.
  Losing any symbol rejects the entire response with `contract_set_shrink`, before
  mutation, both in the runtime and defensively inside the storage transaction.
  The raw response remains archived, previous quotes/revision remain untouched,
  and the date is failed/retryable rather than falsely complete. Added contracts
  and corrected values are accepted. Different trading dates may legitimately
  have different baskets; no cross-date universe constraint or partial-basket
  merge is used. There is no silent contract-deletion override.
- Exact futures identifiers are `(IF|IH|IC|IM|TS|TF|T|TL)YYMM` with valid month.
  Options, unknown products, variety conflicts, duplicate contracts, mismatched
  row dates, invalid OHLC, nonfinite/negative values, and fractional volume/OI
  reject the **whole date**. No expiration, timestamp, price, or missing contract
  is synthesized.
- OHLC may be missing or source-zero only for zero-volume contracts, with
  explicit quality flags. Zero sentinels remain zero. Known settlement and
  previous settlement must be finite and positive. Missing settlement/OI/
  turnover are nullable and flagged. Volume is required; volume and known OI
  must be nonnegative signed-64-bit integers, including exact numeric strings.
- Turnover and prices use SQLite REAL; source values and precision remain in
  raw files. Source units are retained when supplied and otherwise explicitly
  `provider_defined`, with **no inferred conversion**. Metadata includes the
  OpenBB provider/model/package lineage. Observation time is UTC; historical
  event precision is a date, and publication time remains unknown/null.
- CSV export reads a consistent snapshot and atomically publishes a complete
  file. It may use an ordinary export directory inside the root, but may not
  replace the SQLite journal/sidecars, writer lock, or raw archive. The CLI also
  protects the operational YAML and calendar paths.

## Limits

The calendar is a supplied coverage assertion; this collector cannot prove
that a nonempty provider basket includes every listed contract, nor independently
prove that the Sina/SSE reference calendar exactly matches every historic CFFEX
closure. On a new date, universe completeness remains **unknown**; the same-date
no-shrink guard only protects previously observed contracts, not unobserved ones.
The strict product allowlist requires a code change for a new CFFEX product.
Linux/POSIX `flock`, hard links, and directory fsync on a local filesystem are
required. Keep the entire state directory together when backing up and use
SQLite's backup API for a live journal. Orphan raw envelopes from a crash are
retained for audit; there is no pruning, migration beyond schema v1, remote
delivery, or background scheduler. Provider installation and live exchange
availability are integration concerns, not simulated by offline tests.

### Deployment boundary

Use a local, independent evidence directory on the new computer. Do not share it
with the old computer or copy old evidence into it. The directory is created as
needed; no initialization command or writer-drain step is required. Historical
evidence migration, if needed later, requires separate reconciliation.
Without a reconciled historical inventory, the new computer cannot know which
dates the old computer captured; deduplication and recovery use the evidence
available in the local directory. Overlapping acquisition plans with the same
source authority, token and attempt reuse locally verified successful date
captures, regardless of the plan's date bounds. Explicit refreshes or changed
capture authority intentionally use different identities.

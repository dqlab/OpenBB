# Offline BaoStock literal-18 candidates

`python -m openbb_collector_core.baostock_postprocess` materializes **copied,
anchored, existing** BaoStock captures. It never calls a source, acknowledges a
delivery, repairs an acquisition, writes a central database, or publishes data.
Use a separate environment with this checkout's collector core, BaoStock provider,
DuckDB provider, `duckdb` and `pyarrow`. Do not install into a production environment.

The existing `baostock_page_capture.read_capture` validates authority, fragments,
page receipts, terminal state and the external capture anchor. Existing delivery
helpers validate paths and file hashes. This private page-capture format is not a
unified delivery session; `read_received_session` must not be made to accept it by
fabricating a delivery receipt. Existing normalized historical quality code coerces
values and rejects conflicts, so it cannot preserve this literal-source contract.
The offline candidate retains those distinctions and exposes provider-compatible
views through the existing DuckDB historical reader.

## Contract

The original eighteen columns remain strings, including empty strings:

```text
date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,peTTM,psTTM,pcfNcfTTM,pbMRQ,isST
```

The requested observation key is `(code, date, requested_adjustflag)`. Exact
versions group **all eighteen original strings plus the requested adjustment**.
Requests for different adjustments remain distinct contracts even if a source
incorrectly returns identical values. Numeric casts, rounding, empty-to-zero
conversion, alias remapping and requested-to-returned flag replacement never
decide equality. `value_id` is SHA-256 of DuckDB's JSON string array containing
the requested flag followed by the eighteen original values. Hash collisions are
checked against grouping on the actual strings. Every differing version of a
requested observation survives; multiple versions are excluded from consumption
views. No latest-value winner is silently selected.

A manifest is JSONL, with one unique `relative` capture directory per record:

```json
{"relative":"campaign/private/baostock-page-capture-v1/<identity>","params":{"code":"sh.600000","fields":"<exact eighteen fields>","start_date":"1999-11-10","end_date":"2026-10-15","frequency":"d","adjustflag":"3"},"anchor":"<saved capture hash>","terminal_anchor":"<saved terminal hash>","confirmed_partial_data":false}
```

Supply the original `task` when anchored to a campaign handoff. Otherwise its
native authority is bound by the external capture anchor. Optional
`expected_rows`, `expected_rows_sha256`, `expected_dates_sha256`, and
`expected_files` bind independent predecessor evidence. Do not invent anchors.
Mark `confirmed_partial_data=true` **only** for independently accepted retained
partial data. This neither changes `complete` nor creates `eligible.json`.
Other acknowledged partial prefixes survive in raw observations with their
original quality status. Unacknowledged pending bytes remain in the raw copy and
capture inventory, outside parsed rows. Malformed/unverifiable captures retain
their raw directory and a sealed failure record; their failure count must be
reported separately.

`row_lineage` retains every source observation, including exact duplicates:
`value_id`, capture identity and anchor, source body and receipt hashes, capture
page (zero based), SDK page (original source string), page row index (zero based),
and capture row index (zero based). `capture_inventory` supplies the NAS-relative
path, requested parameters, original terminal state and proof references. Capture
start time is not a historical publication/availability time. This reconstructed
candidate is not guaranteed point-in-time safe.

`adjustflag=2` denotes the source's dynamic forward-adjusted snapshot. A query
cutoff does not establish its normalization basis. No fixed 2026-10-15 forward
basis is asserted or calculated. Source codes are never renamed. Projection
currency/session metadata is explicit, while numeric values remain in the source
scale; no volume/amount rescaling is performed.

## Run and resume

Use a new local directory, retaining immutable NAS copies and their transfer
receipts outside the dataset output. First validate representative real captures.
For a manifest named `manifest.jsonl`:

```bash
python -m openbb_collector_core.baostock_postprocess parse \
  --manifest manifest.jsonl --raw-root /isolated/raw --output /isolated/candidate
python -m openbb_collector_core.baostock_postprocess materialize \
  --output /isolated/candidate --evidence /isolated/evidence --buckets 32
```

The evidence directory contains reviewed predecessor CSVs named
`prior_calendar_unknown_review958.csv`, `prior_calendar_conflict_source_rows.csv`,
`prior_missing_combination_review564.csv`,
`prior_plan_scope_and_coverage11124.csv`, and `prior_segments18.csv`. These
campaign-specific references are explicit inputs, not inferred independent
calendar verification. Keep their original seals and provenance with the run.

Parsing defaults to 64 captures per checkpoint. `--start-index` and `--stop-index`
can process aligned disjoint ranges after their download receipts are committed.
The manifest and batch size remain fixed across retries. Checkpointed Parquet is
hashed and read back with Arrow equality across every column. Materialization uses
32 stable code buckets, two DuckDB threads and a 3 GB memory limit. Each bucket
commits its version table, all source references, counts and hashes. Resume skips
only matching checkpoints; source/output contract conflicts raise errors.
Uncommitted `.part` files belong to the interrupted batch, not accepted output.
Do not change the implementation or quality contract during a resumed run.

Review `parse_summary.json`, `failures/`, all bucket receipts, and independent
reconciliation before declaring processing complete. A complete conversion is
separate from full source-price/calendar/adjustment acceptance.

## Query

`candidate.duckdb` is an isolated local catalog over Parquet. Its relations are:

| Relation | Meaning |
| --- | --- |
| `raw_observations` | Every parsed original observation plus source coordinates |
| `literal18_variants` | Exact deduplicated literal versions and quality flags |
| `row_lineage` | All source references, including duplicate origins |
| `capture_inventory` | Original capture status and proof references |
| `conflicts`, `quarantine` | Differing versions and explicit consumption exclusions |
| `early_missing_review`, `calendar_review` | Missing candidates and calendar uncertainty, including absent rows |
| `confirmed_partial_segments` | Independent eighteen-segment data acceptance |
| `equity_unadjusted` | Matching flag-3 source rows through the existing OpenBB model |
| `equity_forward_dynamic` | Matching flag-2 dynamic source snapshots; fixed basis is NULL |

Consumption views exclude conflicts, wrong returned adjustments, invalid
identity/date/numeric/OHLC/flag values, empty core prices, and unconfirmed-only
partial data. Calendar uncertainty remains visible as flags. No comprehensive
calendar or financial-ratio correctness is implied by the remaining rows.

Read through the unchanged existing provider:

```python
from openbb_duckdb.models.equity_historical import DuckDBEquityHistoricalFetcher

query = DuckDBEquityHistoricalFetcher.transform_query({
    "database_path": "/isolated/candidate/candidate.duckdb",
    "table": "equity_unadjusted", "adjustment": "unadjusted",
    "symbol": "sh.600000", "start_date": "2026-10-13",
    "end_date": "2026-10-15", "limit": 100,
})
records = DuckDBEquityHistoricalFetcher.transform_data(
    query, DuckDBEquityHistoricalFetcher.extract_data(query, credentials={})
)
```

For flag 2, select `equity_forward_dynamic` with `adjustment="forward"`. Every
record includes its `value_id`; trace it with a parameterized bounded local query:

```sql
SELECT v.code, v.date, v.close, l.*, c.relative, c.terminal_status
FROM literal18_variants v
JOIN row_lineage l USING (value_id)
JOIN capture_inventory c ON l.capture_id = c.capture_id
WHERE v.value_id = ?
LIMIT 100;
```

## Offline validation

```bash
BAOSTOCK18_REAL_SAMPLE=/absolute/run/root python -m pytest \
  openbb_platform/collectors/core/tests/test_baostock_postprocess.py
python -m pytest openbb_platform/providers/duckdb/tests
```

The acceptance tests require actual externally supplied sample artifacts; they
skip when none is configured. They do not substitute synthetic source rows.
Record the dataset's own independent per-capture counts, date/row hashes, fields,
reference counts, quality findings, physical row readbacks, provider queries,
performance, disk usage, dependency versions and final artifact checksums.

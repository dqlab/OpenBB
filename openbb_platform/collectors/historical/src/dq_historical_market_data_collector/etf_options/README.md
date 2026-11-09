# ETF options: daily post-close chains

`dq-etf-option-chain-collector` collects the current SSE/SZSE ETF option universe
through the OpenBB AKShare provider, then records bounded Sina bid/ask batches.
The CLI uses collector-core's killable ProcessClient, not direct SDK/HTTP calls.

## Meaning of closing-chain data

This is a **post-close capture of source-displayed quotes**, not a guarantee that
all contracts have simultaneous 15:00 bid/ask. Preserve each contract's source
quote time separately from acquisition time. Same-day earlier/midnight source
quotes are diagnostic and not timing-eligible for pricing; missing/wrong-day,
future-time and invalid-identity responses are acquisition failures. No source
prices, dates, multipliers or official coverage are fabricated.

Source policies are explicit:
- SSE `source: sina`: enumerate configured ETF categories, source-returned
  maturities and both directions. Completeness is relative to the discovered
  Sina set, not independently verified against an official SSE master. Effective
  strikes/expirations come from quotes; unobserved multipliers stay null, so these
  rows cannot qualify for multiplier-dependent pricing.
- SZSE `source: official`: use the current official option-master file, retaining
  its trading date, contract units and adjustment information. Quotes are still
  acquired from Sina. Provenance of master and quote is separate.

A zero/null quote side or crossed market remains a flagged diagnostic row, not
an invented executable market. `response_complete` and `pricing_eligible_count`
are independent. Verify quality flags rather than assuming complete retrieval
means a trading-quality chain.

## Install and run

```bash
pip install 'dq-historical-market-data-collector[etf-options]'
dq-etf-option-chain-collector calendar-refresh --config etf_options_daily.yaml
dq-etf-option-chain-collector collect --config etf_options_daily.yaml
dq-etf-option-chain-collector status --config etf_options_daily.yaml --limit 5
dq-etf-option-chain-collector export --config etf_options_daily.yaml \
  --date YYYY-MM-DD --output chain.csv
```

The sample config is `configs/etf_options_daily.yaml`. Paths are resolved relative
to that file. Dates are exact ISO dates; collection is current Shanghai-date only
and gated after 15:10, with a validated reference trading calendar. Do not attempt
to reconstruct historical bid/ask by applying today's universe/quotes to old dates.

Use the calendar-first adapter for the existing NAS runner:

```bash
python -m dq_historical_market_data_collector.etf_options.scheduled \
  --config etf_options_daily.yaml
```

Failures, lock contention and cancellation remain nonzero. The adapter is not a
persistent scheduler itself: the existing runner owns timing, retry and shutdown.

## Persistence and recovery

- Native-local-filesystem SQLite WAL plus a process-level writer flock.
- Immutable, fsynced provider-response envelopes for every attempt, including
  transport errors and rejected quality rows. These are provider envelopes, not
  privileged-user-proof WORM storage.
- Capture manifests, effective contract metadata, normalized current snapshots,
  source-content revisions and resumable batch checkpoints. Observation-only
  timestamps do not create false source revisions.
- Whole requested-universe reconciliation before completion, no silent omissions.
  Shrinkage against a previously complete same-day snapshot fails closed;
  `--approve-universe-change` is a separate audited manual approval.
- Source waits and retries are bounded; cancellation and success finalization are
  transaction-aware. Good committed snapshots survive failed/interrupted refreshes.

## Production target and performance

Use the existing QNAP Compose/runner deployment, an isolated `.venv-etf-options`,
and a separate `collectors/state/etf_options` journal. Do not modify CFFEX,
equity/SPX environments or old job manifests. Only add `etf-options-daily`.

Collecting up to 50 contracts per HTTP request avoids an HTTP round trip for every
contract. Pacing and hard deadlines are retained; source batching does not imply
synchronized event timestamps. Limit the new container's CPU/memory and use
single-thread BLAS. Daily runner slots are 15:20 and 18:30 Asia/Shanghai, with
startup capture/retry. Compare old container identities/start times after rollout.

## Shared data plane

Configure `delivery` using the existing collector-core transport. The calendar-first
adapter collects and then delivers sealed captures; acquisition and delivery status
remain separate. A verified delivery receipt does **not** mean central publication
has completed: the existing platform publisher performs that step.

```bash
# Re-deliver previously completed captures without network calls or date gates.
dq-etf-option-chain-collector deliver --config etf_options_daily.yaml --date YYYY-MM-DD
dq-etf-option-chain-collector delivery-status --config etf_options_daily.yaml
```

The shared path is immutable raw/normalized artifacts → schema-v1 live sessions
(`observation_schema: live_v1`) → existing delivery/publisher → shared catalog.
SQLite remains the internal acquisition and outbox recovery journal, not the
consumer-facing market-data store. Original provider envelopes are retained as
audit artifacts; constructed raw snapshots are explicitly labelled and contain
one complete source-declared underlying membership, not individual transport batches.

Register `sse_etf_option_chains` / `szse_etf_option_chains` in the existing publisher
and read configs with collector `openbb-etf-options`, collections `sse_etf_chain` /
`szse_etf_chain`, provider `akshare`, kind `chain`. Preserve all existing routes.
Use the existing `dq_quant_data` provider and specify the ETF dataset explicitly:

```python
obb.derivatives.options.chains(
    symbol="510050", dataset="sse_etf_option_chains", provider="dq_quant_data",
    layer="silver", allow_incomplete=True,
)
```

Silver preserves diagnostic rows and source quality flags. Gold additionally needs
explicit collector pricing permission and the central quality/capture gates;
unknown contract units are never inferred. Source-displayed event time, persisted
capture completion time and actual central receipt/publication time remain distinct.
Replayed deliveries are idempotent. Publication adds bounded file/hash work, not
another source request or a competing DuckDB writer.

## Provider interface and develop integration

The collector package is `dq-historical-market-data-collector` 0.1.7. Its
`etf-options` extra requires `openbb-akshare>=1.2,<2`; install the checkout's
collector-core, historical collector, core and AKShare packages together for
development. Existing CFFEX daily, Futu and CTP collectors keep their own code
and entry points.

The AKShare 1.2.0 provider adds four commands to its existing router:

```python
obb.akshare.etf_option_contracts(
    exchange="SSE", source="sina", underlyings="510050", provider="akshare"
)
obb.akshare.etf_option_quotes(symbols="10000001,10000002", provider="akshare")
obb.akshare.cffex_option_expiries(product="IO", provider="akshare")
obb.akshare.cffex_option_quotes(expiry_symbol="io2610", provider="akshare")
```

Identifiers above illustrate query syntax and do not promise current listings.
The CFFEX option models preserve native lowercase maturity/contract identifiers;
their source supplies neither an event clock nor a verified contract multiplier,
so both fields remain null. Discovery describes Sina's displayed expiries.

ETF source parsing and transport live in `openbb_akshare.models.etf_options` so
this promotion stays within the selected model files. `make_contract_request`
and `make_quote_request` are separate injection points for the shared OpenBB
HTTP helper. The collector reuses develop's collector-core v1 capture/delivery
contracts and does not depend on the old `cffex_options` collector directory.
Official masters and quote batches retain literal response bodies. Sina's
multi-request master retains a provider-transformed envelope with explicit
per-request evidence losses; it does not claim a literal aggregate response.

From the repository root, after installing the packages in an isolated environment:

```bash
openbb-build
PYTHONPATH="$PWD/openbb_platform/collectors/historical${PYTHONPATH:+:$PYTHONPATH}" \
  python -m pytest --import-mode=importlib \
  openbb_platform/collectors/historical/tests/test_etf_options*.py \
  openbb_platform/providers/akshare/tests
python -m ruff check --no-fix \
  openbb_platform/collectors/historical/src/dq_historical_market_data_collector/etf_options \
  openbb_platform/collectors/historical/tests/test_etf_options*.py \
  openbb_platform/providers/akshare
```

The source branch contains four collector test files; this promotion adds
provider-dispatch and packaging test files. Tests use synthetic/saved source
responses. The optional publisher test needs `dq_quant_invest_data` on the Python
path. The historical freeze-receipt audit needs the original external bundle in
`ETFO_RED_EVIDENCE`; it skips when that artifact is absent. Live exchange/Sina
availability and deployed NAS/publisher configuration require separate checks.

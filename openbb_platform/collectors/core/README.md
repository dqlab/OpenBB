# OpenBB collector core

Shared response contracts, engine discovery, configuration-driven provider
execution and a bounded process client. See the [architecture guide](../README.md).

`CollectorExtension(name, load_config, factory)` is the engine registration object.
Each engine publishes it through `openbb_collector_extension`. Discover it using:

```python
from openbb_collector_core.registry import get_collector
engine = get_collector("historical")
config = engine.load_config("historical.yaml")
```

`ProviderFailure` carries a safe diagnostic code. `Response` carries rows, warning
count and provenance. `ProviderDispatcher` discovers only the selected installed
`openbb_provider_extension` and calls OpenBB's `QueryExecutor`; it unwraps
`AnnotatedResult` without losing provider metadata. `ProcessClient` enforces
source pacing, row/byte limits, timeouts, cancellation and process cleanup.
Changing a source configuration recycles that source's worker while retaining
pacing. No provider is imported until dispatch or provider-aware validation.

Install `openbb-core` (or this package's `openbb` extra) and your selected providers
for execution. The base package uses only the Python standard library.

## Session delivery

The shared `delivery` module provides the persisted outbox, local and optional SFTP
transports, verified cleanup with configurable local retention, and
`read_received_session` for master consumers.
Install the `ssh` extra for SFTP. See [session delivery](../SESSION_DELIVERY.md).

### Retained outbox performance and diagnostics

With `local_retention: forever`, delivery uses a partial index over pending jobs
and does not scan already-delivered manifests for cleanup. A status index also
keeps routine outbox counts independent of manifest size. Existing journals gain
these indexes when the delivery store is opened; no jobs or artifacts are removed.

Unexpected provider exceptions retain the public `provider_error` outcome and
emit a `provider_request_failed` diagnostic to the service log. Diagnostics contain
only exception type and source-code locations, not exception messages, request
parameters or traceback local values.


### Optional provider batch hook (0.1.3)

A fetcher may declare `collection_batch_limit` and implement
`collect_batch(params: list[dict], credentials: dict) -> list[AnnotatedResult]`.
The hook must validate each query and preserve request order and cardinality;
partial failures should return an empty item with safe diagnostic metadata.
Credentials are filtered through the same OpenBB executor policy as single
requests. Collectors opt in with `batch_size`; no outer concurrency is added to
the shared worker pipe. Single-request behavior remains compatible.

The worker accepts one list envelope, enforces `max_rows` on every item and
`max_response_bytes` on the entire encoded reply, and retains its existing
killable timeout, stop handling and pacing between calls. A provider must also
pace its own wire requests inside the batch. Unsupported batch hooks fail
explicitly; the dispatcher never silently substitutes sequential calls.

## Independent daily raw acquisition (0.1.6)

`dq-daily-raw` captures one configured source request for one explicit date via
`raw-acquisition-v1` and the registered OpenBB provider/model. BaoStock ETF
uses the separate `baostock-page-capture-v1` durable SDK observation protocol. Install this
package with its `openbb` extra and the selected provider. For BaoStock's
all-market ETF daily SDK operation use the registered source-evidence model:

```json
{
  "root": "/local/private/raw",
  "source": {"provider": "baostock", "model": "BaostockDailyHistoryKETFSource"},
  "params": {"literal": {}},
  "date_path": ["literal", "date"],
  "schedule": {
    "timezone": "Asia/Shanghai", "time": "16:30",
    "calendar": "/local/private/trading-calendar.json", "max_attempts": 3
  }
}
```

The calendar is an explicit JSON object with `coverage_start`, `coverage_end`,
and `trading_dates` (ISO dates). Supply and maintain the authoritative calendar;
no calendar, instrument list, central database or production scheduler is installed
by this package. `limits` optionally configures `timeout_seconds`,
`min_interval_seconds`, `max_response_bytes` (at most 50 MB), and `max_rows`. `credentials_env` optionally maps provider credential names to
environment variable names; values are never stored in task identities.
Use a private POSIX local filesystem with atomic rename/link, flock and fsync;
NAS is not qualified by the raw protocol.

```sh
dq-daily-raw acquire --config daily.json --date 2024-01-02
dq-daily-raw report --config daily.json --date 2024-01-02 --attempt 1
dq-daily-raw schedule --config daily.json
```

Invoke `schedule` externally at the desired frequency. It only processes the
current local trading date at or after the configured time; missed days require
explicit acquisition. Each invocation makes at most one attempt. A lock serializes
scheduler invocations, and durable evidence is the retry ledger: completed success
is retained, failed/cancelled attempts remain available and a later invocation
uses the next generation up to `max_attempts`. Interrupted attempts consume a
generation and remain preserved; they are not reissued in place. Manual retries use `--attempt N`; replaying the same task never
re-fetches a terminal failure. Changing source, parameters or date creates a
new task identity. Schedule limits do not constrain manually selected attempts.

For ETF, each SDK observation is durably journaled before any next request; the
SDK operation is a single response and never issues a pagination request. At or
above 20,000 rows its completeness is unknown and the attempt remains partial.
The page capture has its own bounded resource limits; generic `limits` do not
control those limits; ETF rejects `limits` and `credentials_env` rather than ignoring them. Source status and transport status are separate; only a
complete source capture can satisfy the schedule. Existing raw-v1 attempts
remain readable and count toward the retry budget.

Captured bytes are SDK representations or provider-transformed process responses, not wire bytes.
The source-evidence envelope retains SDK fields, rows and protocol metadata;
source availability and correction history are not established. `report` reads
the archived evidence separately and reports response/row counts and, for source
evidence envelopes, SDK status, reason and page counts; it does not
assert market completeness, and its failure cannot prevent evidence capture.
Provider response limits and source errors can prevent a complete capture.

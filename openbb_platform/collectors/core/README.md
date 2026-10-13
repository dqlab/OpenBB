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

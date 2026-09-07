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

# OpenBB collection extension

`openbb-collection` contains the thin OpenBB routers for the independently installed
[historical](../../collectors/historical/README.md) and
[live](../../collectors/live/README.md) engines. Scheduling, provider acquisition,
quality checks and persistence belong to those sibling collector/provider packages.

Follow the [collector installation guide](../../collectors/README.md), installing
this package and both engines in the same Python 3.11+ environment, then run
`openbb-build`. The historical engine no longer registers the router itself.

| Namespace | Commands |
| --- | --- |
| `obb.collection` | `validate_config`, `plan`, `once`, `status`, `query` |
| `obb.collection.live` | `validate_config`, `plan`, `once`, `status`, `export` |

Validation is provider-aware and performs no network acquisition. Plans and
status calls are read-only. `once` accepts `max_seconds` in `(0, 600]`, observes
cancellation, and finishes journal cleanup. Use the preserved engine CLIs for
continuous scheduling. Live `export` replays bounded pending output batches.
Configuration file paths refer to the OpenBB host; relative storage roots are
resolved against those files.

```python
from openbb import obb
obb.collection.validate_config(config_path="historical.yaml")
obb.collection.live.validate_config(config_path="live.yaml")
```

Run router tests from this directory with `python -m pytest tests`; run the live
router fixture tests with the live engine suite. Installation, configuration v2
migration and all affected validation commands are documented in the collector
guide. Offline tests do not verify network credentials or market-data entitlement.

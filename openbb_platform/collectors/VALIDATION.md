# Collector architecture validation — 2026-09-07

The sibling collector packages and configuration-driven provider dispatch were
validated offline in an isolated environment. That offline validation made no
gateway or paid-data request, and no artifacts were published.

## Results

| Suite | Result |
| --- | --- |
| Collector core, discovery and development installer | 10 passed |
| Historical engine | 70 passed |
| Live engine, including live router fixtures | 95 passed |
| Collection router | 16 passed |
| Complete IBKR provider suite | 46 passed |
| Total | **237 passed** |

All ten generated `obb.collection` / `obb.collection.live` commands passed
synthetic collection, journal readback, export and checkpoint-reuse probes.
`openbb-build`, affected Ruff checks and `git diff --check` passed. The four bundled deployment/resource examples passed provider query preflight.
The derivative template parsed and correctly failed preflight until its missing
qualified contract IDs are supplied. All five affected packages built as wheels and sdists; wheel
contents, engine/router entry points and packaged configuration resources were
inspected. The relocated live CLI also imported successfully in the sibling data
repository's Python 3.11 environment without importing OpenBB or a provider.

Tests cover arbitrary registered providers through the actual QueryExecutor,
strict query mappings, explicit fixed intervals, credentials, bounded worker
reuse/reconfiguration, vendor request bounds, identity checks, revisions,
reconciliation, retries, scheduling and existing storage behavior.

See [the architecture guide](README.md#validation) for reproducible package test
commands. Network credentials, subscriptions, actual live/delayed delivery and
provider coverage were not exercised by this refactor validation.

## Environment

Python 3.12.3; openbb-core 1.6.13, pydantic 2.13.5, pytest 9.1.1, ruff 0.15.22.

## Local artifacts

These checksums identify the local build outputs; versions were not published.

```text
aabb91329995db813b1834dbf2f88dffc6614417ed3e79dff838c452a84f6391  collectors/core/dist/openbb_collector_core-0.1.0-py3-none-any.whl
56564c7e2407d4b0959b4fe7934c201ff09f5281db041ff7dc3398a7ce65105c  collectors/core/dist/openbb_collector_core-0.1.0.tar.gz
7016c5e60caac82a2b4792b294a3c662ae9ce07c2b9a8dee684aad3ff0145bd5  collectors/historical/dist/dq_historical_market_data_collector-0.1.0-py3-none-any.whl
14171d48f0089b5e89de41df224221036bc9eeb476a31864a8e36f7beaea86ac  collectors/historical/dist/dq_historical_market_data_collector-0.1.0.tar.gz
b2e9974dbb5a856486339c97ff32978b5e64365df4fc99a616b52e362d32394f  collectors/live/dist/dq_live_market_data_collector-0.1.0-py3-none-any.whl
6ae3a0f47d1311091e572590296375c240c4584827cfec13e6c0573427f0af3e  collectors/live/dist/dq_live_market_data_collector-0.1.0.tar.gz
20e208932956b746f4e2e849bd0f70e698143ad7f0eafdc8a79b5ac061cde335  extensions/collection/dist/openbb_collection-0.1.0-py3-none-any.whl
26b4b7ab0d48eb67a9102a0b5d576e055e770128e968322aa2d9c7ed7dea7630  extensions/collection/dist/openbb_collection-0.1.0.tar.gz
b67644998ebcdff60d79765747d6780849fa677d0f9ed605e34d03e955e2c3a5  providers/ibkr/dist/openbb_ibkr-0.2.0-py3-none-any.whl
1b645d409515ad64f32118a84904a422624080feab27bb8125b2f24afac12b79  providers/ibkr/dist/openbb_ibkr-0.2.0.tar.gz
```

## Subsequent Gateway and entitlement checks — 2026-09-07

The requested read-only network checks subsequently connected to the existing
paper Gateway on `127.0.0.1:4002` (API server 176). AAPL, SPY and SPX daily
historical TRADES requests returned September 3–4 bars without gateway errors.
All three live quote requests failed entitlement checks with code 354 and no
prices. Explicit delayed requests returned actual feed-type callback 3.

Nine provider-dispatch requests and two additional historical/live isolated
worker requests exercised the new runtime paths. Workers and diagnostic
connections closed. The current live quality policy rejected all six captured
quote responses: missing prices on denied live requests, and delayed-mode
rejection plus unavailable equity bid/ask values on delayed requests. Suspicious
delayed equity volumes and missing provider-row actual-feed callback provenance
remain follow-up findings. These results do not establish production live
readiness. September 7 was a U.S. cash-equity market holiday; derivatives and
other venues were not checked.

The full report, exact probes, configurations, sanitized response evidence and
SHA256 manifest are retained in the sibling application workspace at
`/home/zdqclawbot/work/app/openbb/output/gateway-entitlements-20260907T133013Z/`.
No account subscription, production schedule or provider source was changed.

## Delayed collection enabled — 2026-09-07

The subsequent requested change enables delayed acquisition in the live
configuration examples and retains actual Gateway feed type independently of
requested mode. The bounded IBKR quote provider preserves original decoded
values in `raw_quote`, maps unavailable bid/ask markers to null, and keeps
legitimate negative futures prices. No generic collector validation was relaxed.

Affected checks passed: **50 IBKR provider tests + 97 live collector tests =
147 tests**, affected Ruff, provider-aware configuration preflight and
`git diff --check`. A real bounded collector CLI pass archived and exported one
AAPL, SPY and SPX quote each, all labeled delayed from Gateway callback 3, with
zero rejected rows or fallback. Export replay wrote zero records and left raw
and Parquet hashes unchanged.

The new run is retained at
`/home/zdqclawbot/work/app/openbb/output/delayed-quotes-20260907T134517Z/`.
Its report, exact configuration, raw responses, journal, Parquet exports and
source/artifact hashes document the result. Earlier entitlement evidence remains
unchanged. Delayed equity volume scaling remains unverified and values were
preserved as supplied. The local build checksums above describe the earlier
refactor artifacts, before this delayed-quote source/configuration update.

## Verified session delivery — 2026-09-07

Optional local-directory and SSH/SFTP delivery is implemented in collector core
and invoked by both engines after session closure. The version 2 config accepts
an optional `delivery` block without changing observation fingerprints. Raw
envelopes are indexed before normalization; immutable session snapshots and
SHA-256 manifests preserve evidence while mutable acquisition state remains local.
See [deployment and protocol documentation](SESSION_DELIVERY.md).

Validation ran in the isolated Python 3.12 collection environment. Paramiko 4.0.0
and its dependencies were installed only in a separate test dependency directory;
no user's runtime, SSH configuration, Gateway, or master server was changed.

| Complete affected suite | Result |
| --- | --- |
| Collector core, including local transfer faults and loopback SSH/SFTP | 32 passed |
| Historical collector | 75 passed |
| Live collector, including its router tests | 101 passed |
| Collection extension | 16 passed |
| Total | **224 passed** |

The loopback SFTP tests exercised public-key authentication, pinned-host acceptance,
unknown-host rejection, actual file transfer/readback, verified source removal, and
remote symlink rejection. Local tests covered checksum conflicts, partial transfer
retry, receipt-before-cleanup recovery, delivery-target changes, protected journals,
normalization failure evidence, export recovery, and resumed live sessions after
prior artifacts were moved. The master reader requires the commit marker and
verifies every object. New CLI commands were checked in the installed environment.

Affected Ruff checks and `git diff --check` passed. Existing deprecation warnings
from OpenBB dependencies remain. Provider registrations and router signatures did
not change, so an SDK rebuild was not required for this increment. No new Gateway
requests were made. These checks ran on Linux/WSL; native Windows operation and
transfer to the actual master remain untested. No release was published.

Run each package separately to avoid the engines' test-fixture module collision:

```bash
python -m pip install -e 'openbb_platform/collectors/core[dev,ssh]'
python -m pytest openbb_platform/collectors/core/tests
python -m pytest openbb_platform/collectors/historical/tests
python -m pytest openbb_platform/collectors/live/tests
python -m pytest openbb_platform/extensions/collection/tests
python -m ruff check openbb_platform/collectors/core \
  openbb_platform/collectors/historical openbb_platform/collectors/live
```

Fresh wheel and source builds for all three updated packages were inspected for
delivery modules, SSH extras, dependencies and packaged example configuration.
They are stored under each package's `dist/session-delivery-20260907/`, preserving
earlier build outputs. Versions remain 0.1.0; these are unpublished local builds.

| Package artifact | SHA-256 |
| --- | --- |
| `core/openbb_collector_core-0.1.0-py3-none-any.whl` | `8112a70278b04f3f40e1dc1156cdef69ca2e469a4127917917d304c6e28b0f14` |
| `core/openbb_collector_core-0.1.0.tar.gz` | `6c3ef00e26806c2da38d83853b61bdb37c14ed65c56272193773fc4a00c26013` |
| `historical/dq_historical_market_data_collector-0.1.0-py3-none-any.whl` | `107ca92a5135f5f7d99cbd8cb217517bd525478c7dba8b61dd459eba7fb1a34e` |
| `historical/dq_historical_market_data_collector-0.1.0.tar.gz` | `32d5a09bc351f575aa47a313fbd17215395d01a3cb303b3a62168d972078928a` |
| `live/dq_live_market_data_collector-0.1.0-py3-none-any.whl` | `a74145235d8a9acf302457633463f7cdd8b27a6c2d893a2310b87f968fc9a60b` |
| `live/dq_live_market_data_collector-0.1.0.tar.gz` | `082a0be879b3d48d0630312c09df4038327d43149226deaeb587bc9bcc287f70` |

Local development evidence: `.pytest_cache/collector-delivery-validation/` contains
suite/build logs and source hashes. Production activation awaits the actual master
SSH endpoint, verified host key/account, and destination data root. The example
delivery blocks remain commented out, and existing collected data was not moved.

## Configurable local retention — 2026-09-07

`delivery.local_retention` now supports immediate cleanup after verification,
fixed durations (`1d`, `1w`, `12h`), calendar months (`1mo`), and `forever`.
Transfers still occur at session close. The first successful local acknowledgment
time is stored in the outbox; retries and restarts do not reset it. Expired copies
are removed only after a fresh verification of the central objects. Read-only
delivery status exposes the verification time, cleanup deadline, and local state.
The old boolean configuration remains compatible when the new field is omitted.

Validation: **249 tests passed** across the complete affected suites (collector
core 56, historical 75, live 102, collection extension 16). New cases cover exact
expiry, UTC fixed periods, month-end and leap-year dates, delayed successful
transfer, checksum conflicts at cleanup, persistent timestamps across restart and
retry, legacy outbox migration/read-only inspection, and later collector cleanup
without acquiring new quotes. Existing SFTP integration tests also passed. Ruff
checks for all collector packages and `git diff --check` passed.

Complete logs and source hashes are under
`.pytest_cache/collector-retention-validation/`; use the affected-package commands
above to reproduce the suites. Existing dependency deprecation warnings remain.
Native Windows and the actual master remain untested; production configuration and
previously collected data were not changed. No publication or SDK rebuild occurred.

All three packages were built as fresh 0.1.0 wheels and source distributions under
`dist/session-retention-20260907/`, preserving earlier builds. Delivery modules, SSH
extras, packaged retention examples and installed CLI commands were verified.

| Package artifact | SHA-256 |
| --- | --- |
| `core/openbb_collector_core-0.1.0-py3-none-any.whl` | `df099618e3952c219417e5ff84a614a3db03ae31cd93bad9287775136f8f5b7c` |
| `core/openbb_collector_core-0.1.0.tar.gz` | `76aefcd598518a5b78877da58653e6e0989f50474023c739554f268d3e559b54` |
| `historical/dq_historical_market_data_collector-0.1.0-py3-none-any.whl` | `11d512dbb21fa29729f1e969162d2ff80a0a4bbb49ca5f8368eb83f135a2e045` |
| `historical/dq_historical_market_data_collector-0.1.0.tar.gz` | `7cd21bb627820bca9a51e4d07767a0850b001617b2371ede6def165a8632241d` |
| `live/dq_live_market_data_collector-0.1.0-py3-none-any.whl` | `8eedc5b3a5be7d5b1e8569e4943b52b7e8029282c090ed087f491a64d95a92f2` |
| `live/dq_live_market_data_collector-0.1.0.tar.gz` | `3e4f36e07979d913ad0a77fe3f43ea047a15890d79fe8d0162dba8062f084d06` |

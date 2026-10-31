# Futu provider (FUTUNN Open API)

The provider supports FUTUNN HTTP snapshot and an experimental, bounded WebSocket collector. Preserve market prefixes and leading zeros, e.g. `HK.00700`. No account entitlement or live session has been verified. HTTP and WebSocket behavior follows the [FUTUNN auth](https://open.futunn.com/zh-cn/api/quote/push/auth), [subscription](https://open.futunn.com/zh-cn/api/quote/push/subscribe), and [push format](https://open.futunn.com/zh-cn/api/quote/push/data-format) documentation.

Install the existing collector core and live collector packages in an isolated environment before installing this provider with its `collector` extra; these local packages may not be available from an index. The collector can also be run from source with both provider and live collector `src` on `PYTHONPATH`:

```bash
python -m openbb_futu.collector --config /secure/config.json --token-file /secure/token.json --output /data/futu
```

The user supplies a private, separately stored token JSON file, owned by the current user, mode `0600`, in a directory owned by that user and not group/world writable. It contains `client_id`, `access_token`, `refresh_token`, and numeric Unix `expires_at` in seconds. Obtain initial credentials separately; this command does not perform interactive authorization. Never put credentials in the config, command line, archive, or logs. An example config without credentials:

```json
{"channels":{"quote":["US.AAPL"],"order_book":["US.AAPL"],"ticker":["US.AAPL"],"kline":[{"symbol":"US.AAPL","period":"1m","adjust":"none"}]},"max_reconnects":5,"channel_refresh":300}
```

The connection owns the local lock through REST token exchange and reconnects; each exchange also consumes the local SQLite request budget. `FUTU_COORDINATION_DIR` defaults to `~/.local/state/openbb-futu`. All cooperating processes must use the same trusted local directory. The default 70 requests/60 seconds is an engineering limit, not an official allowance; other hosts, directories, and applications are unaccounted for. A separate process cannot acquire the lock during a session. Refresh uses an explicit borrowed owner, never releases or reacquires it. The `FUTU_TEST_HTTP_BASE` override accepts only loopback HTTP and is intended for local integration tests.

A token rotation creates and fsyncs a sibling `.refresh-pending` marker before the HTTP request, atomically replaces and fsyncs the private token file after success, then removes the marker. A crash or error during the exchange leaves the marker and blocks further use: the server may have rotated the refresh token. Resolve the token state with the provider using a secure independent process before manually removing the marker. This design deliberately favors avoiding reuse of an uncertain token over unattended recovery.

One WebSocket reader routes acknowledgements and business frames concurrently. Standard WebSocket ping/pong uses the library heartbeat. The collector authenticates, subscribes, refreshes the channel before ten minutes, refreshes OAuth before expiry, and reconnects with bounded exponential delay and a bounded number of attempts. Each successful reconnect and the default five-minute interval reissue the configured subscription; nonzero subscription responses fail explicitly. `SIGTERM`/`SIGINT` stops reception and drains the bounded writer. Queue saturation, suspicious frames, subscription rejection and exhausted reconnects fail explicitly. Event JSONL records sanitized failures, disconnect gap start/end or unresolved gaps, and the manifest path. A gap end means resubscribed, not recovered data. The archive retains raw messages, including repeated frames and entire ticker lists. Typed unknown business-shaped messages are stored with unknown status; ambiguous, control-shaped, duplicate-key, and known sensitive-key messages fail closed. Arbitrary secrets hidden in unrecognized fields cannot be reliably identified; only use feeds whose payloads and storage access are trusted.

The raw archive is a candidate, not a complete or point-in-time safe canonical dataset. Event timestamps and availability vary by message; no normalization, deduplication, entitlement, calendar reconciliation, replay, missing-data recovery, complete durable manifest or crash-tail quarantine is claimed. The writer dependency and restart/storage failure behavior still need production validation. The legacy `WebSocketSource` is manual and should not be used concurrently for receive and acknowledgements. Snapshot omissions fail explicitly; unverified application error codes remain `provider_error`. No live trading, deployment, or live-account validation is implied.

The sample [collector config](configs/collector.example.json) is a bounded illustration, not a universe. `openbb_futu.discovery.discover` accepts explicit option/future seed dictionaries and a separately supplied access token. It uses the official `GET /api/v1.0/quote/{symbol}/option-chain` and `GET /api/v1.0/quote/{symbol}/reference-future` endpoints. These require a known underlying; the schema specifies no pagination parameters for either. An unexpected non-null pagination object is treated as unknown, never as completion. `stock-basicinfo` requires supplied `code_list` and is not a market directory. All four universe totals remain unknown. JSON HTTP bodies including failures are stored in a private directory only after bounded size and sensitive-content checks; rejected bodies are not persisted. This does not establish that arbitrary response text contains no other secret. The evidence CLI described below captures explicit seeds; a configured discovery report can be referenced by collector planning.

At startup while holding the local owner lock, interrupted `.jsonl.open` segments are copied into a valid prefix and tail under `futu-recovery`, without changing the original, and an unresolved gap is recorded. Repeated starts may produce repeated recovery evidence. This is not historical backfill, and cannot determine the data lost before the crash. Sealed segments are checked at close and hashed using bounded reads; manifest errors still require operator attention. The event log and manifest cannot be guaranteed when the filesystem itself is full.

### Explicit seed plan and conservative coverage

`configs/collector.example.json` is valid JSON for `openbb-futu-collector --config
configs/collector.example.json --token-file /private/token.json --output /private/output`.
Replace the illustrative option/future identifiers and all other seeds with identifiers
confirmed by the operator. Each dimension declares an asset, symbol, channel and optional
integer priority; `subscription_budget` limits symbol/channel dimensions. Selection is
deterministic. These configured seeds are evidence of operator intent, not an official
universe, account entitlement, or verified quota. Legacy `channels` configs remain accepted.

For dimension configs, the collector saves the plan and reconstructs a coverage JSON
from its event log and checksum-verified sealed archive manifest. Subscription acknowledgements
remain unknown per dimension. Ticker observations require the documented top-level symbol
and individual `ticker_list` entries with integer `time_ms` and `sequence`; each reference
contains its archive local sequence and list index. Missing or malformed entries are not
counted. On a later start, prior plans are reconstructed again; without a valid final
manifest durable observations remain unknown. Gap events retain their recorded times.
A clean shutdown or a reconstructed report does not prove complete market coverage.


### Evidence CLI (partial integration)

After installing the collector extra, `openbb-futu-evidence discover --seeds /private/seeds.json --token-file /private/token.json --output /private/evidence` accepts a JSON array such as `[{"asset":"option","symbol":"HK.00700"}]`. It captures bounded JSON responses only after checking known sensitive keys, Bearer strings and the supplied credentials. Invalid JSON, duplicate keys, oversized bodies and detected secrets are rejected without storing the body. Unknown secrets without recognizable markers cannot be reliably identified. The result contains raw file hashes and paths in a private discovery directory; it is seed-scoped and does not establish entitlement or a market universe. Collector planning can reference the report as described below.

`openbb-futu-evidence coverage --output /private/output --run-id RUN_ID` reconstructs the existing conservative report to stdout without modifying the archive or publishing a report. A missing manifest yields interrupted/unknown evidence, not proof of successful collection. Coverage references can grow with the number of observations even though segment checksum reads are bounded.

For seed-scoped evidence, set `discovery_report` in the collector JSON to the
`report_file` emitted by `openbb-futu-evidence discover`. The compiler checks
response paths, hashes, response status and observed symbols before associating
matching configured dimensions with request indexes. This does not expand the
configured subscription list, prove a complete universe, or establish permission.
Keep the evidence directory private. The report contains paths and market symbols;
unknown secrets cannot be reliably detected in arbitrary source payloads.

`openbb-futu-evidence coverage --output DIR --run-id ID` reads existing evidence
without writing. Per dimension, synced counts and sample locators come from
checksum-verified sealed segments up to the manifest's durable sequence. Received
and written counts are conservative lower bounds derived from those same rows;
frames accepted or written after the last verified sync cannot be reconstructed as
per-dimension counts. Samples are capped at 100 per dimension and timeline output
at 1000 events; total counts and the complete on-disk event log remain available.
An interrupted run without a valid manifest cannot establish synced observations.

### Offline replay and sealed evidence verification

Install the `collector` extra and provide UTF-8 JSONL with one complete raw business
frame per line. This is synthetic/offline input; no WebSocket or account is opened:

```sh
openbb-futu-evidence replay --input frames.jsonl --output ./evidence
openbb-futu-evidence verify --manifest ./evidence/futu-manifests/SESSION.json
```

Replay passes each whole frame through the collector's `Archive.accept`, queue and
writer. It preserves repeated frames, ticker lists and unknown fields. It stops on
rejection, malformed or incomplete input, and returns nonzero with an uncertain
loss flag. Verify reads the manifest and segments without writing; it checks SHA-256,
byte counts, run/session identity, contiguous local sequence beginning at 1,
segment metadata, durable prefix and failure status. It does not authenticate a
manifest against an external signature, prove upstream completeness, or reconstruct
missing data. Manifest segment paths are recorded paths and must still be accessible.

New ingress limits: 1 MiB per UTF-8 frame (also WebSocket `max_size`), 2 MiB +
8192 bytes per serialized archive line, 64 MiB of serialized queued records in
addition to the configured queue count, and 10,000 segments per run. Configuration
is limited to 1 MiB and 10,000 dimensions/subscriptions; token file is limited to
64 KiB, each used token/client ID to 8192 UTF-8 bytes, and retained known secrets to
256 per run. On exhaustion collection fails; queued or unseen frames are uncertain,
not silently truncated. These are serialized-byte budgets, not a bound on Python
object overhead, total process RSS or throughput. Files, manifests, events, recovery
copies and successive runs can grow without a global disk quota; provision and monitor
storage independently. A full disk may also prevent recording the failure.

Limits apply to new ingress, not historical evidence. Verification reads segments
incrementally and does not rewrite them; older manifests without `uncertain_loss`
remain supported. A historical individual JSON line and manifest are parsed in memory
and historical recovery/coverage scans may grow with file count, event count and run
IDs. For unusually large old evidence use isolated, resource-provisioned read-only
inspection and select a session/manifest manually; do not delete or truncate evidence.
No guarantee is made for unrecognized secrets hidden in ordinary unknown fields.

REST requests refuse all HTTP redirects, including same-origin redirects, and report
`redirect`; 429 remains `rate_limit`. Coverage applies the same sealed-manifest
verification as `verify` before counting rows. Invalid evidence yields a failed
coverage status, zero synchronized rows, and a nonzero CLI exit. Verification
independently compares the listed segments with the session under the archive root
(the parent of `futu-manifests`), rejects paths outside that session and symlinks,
and fails if the segment or date-directory budget is exceeded. An unsigned manifest
and its hashes establish internal consistency only; they do not authenticate the
source or protect against coordinated replacement of both manifest and data.

### Bounded REST sampled snapshots

`python -m openbb_futu.sampled_snapshot --config configs/sampled_snapshot.example.json --token-file /secure/token.json --output /private/futu` runs an independent bounded polling session. Replace every illustrative symbol, especially the option and future placeholders. The configuration explicitly retains equity, ETF, option and future labels; it does not discover a universe. Set `max_polls`, `interval_seconds`, `timeout_seconds`, and optionally `queue_capacity`. The token file uses the same private format and local coordination directory as the WebSocket collector. The sampler does not refresh tokens.

The shared archive writes `transport=rest`, `data_kind=sampled_snapshot`, request ID, UTC start, status, requested symbols and the HTTP content-decoded application body in base64, together with the declared content encoding. These are not wire bytes. It does not archive response headers. Bounded responses are accepted only if they are unambiguous JSON and pass credential checks; oversize, malformed or sensitive responses fail closed, so their bodies cannot be archived. Recognizable credentials are checked, but unknown secret formats cannot be guaranteed detectable. Safe HTTP error bodies are archived and return a nonzero exit status. Coverage reports under `futu-sampled-coverage` mark each requested symbol returned_code, missing, or unknown using only records at or below the writer's synced durable sequence. A partial response is scoped to that request, not evidence of a complete universe. The owner reserves the shared local budget before spawning a single HTTP worker. The parent monitors a monotonic request deadline, stop signals and writer failure, then terminates and, if needed, kills the worker with bounded waits. Interrupted attempts record the request ID, unknown send status and unknown response durability; no retry is implied. The deadline covers worker startup and HTTP processing, not lock acquisition, budget storage, archive shutdown or report publication. Reports are atomically replaced and synced, but abrupt parent death or storage failure can prevent an attempt from being recorded. The child disables HTTP debug and only uses the official endpoint or the validated local test endpoint; redirects are disabled. The sampler limits the encoded HTTP entity and content-decoded application payload independently to 1 MiB each. It reads the entity incrementally and bounds gzip output during decompression; only identity and a single complete gzip member are accepted. Unknown or layered encodings, truncated or invalid gzip, concatenated members and trailing bytes fail closed. Entity counts exclude HTTP transfer framing and are not wire-byte counts. Failed attempts retain the HTTP status and encoded/decoded bytes observed when available, with body omitted and loss and durability unknown. A parent deadline or signal can interrupt the child before these observations are reported. JSON inspection, IPC and archive serialization make additional bounded copies and consume CPU and memory beyond the byte limits; the limits are not process resource quotas. This is sampled snapshot data, with no minute bars, ticks, history, or real account verification.

For a request that fails without a complete HTTP response, the coverage report records the same request ID and UTC start, a sanitized `timeout` or `transport` category, null status and body, and `durability=unknown`. This attempt is not a durable response and cannot establish observed or missing symbols. Connection and DNS errors share `transport`; an interrupted response body may also be classified this way. A failed run exits nonzero and reports its category on stderr without exception text. `missing` means a successfully parsed, synced REST response omitted that requested symbol; it does not describe a WebSocket disconnection or streaming gap. Across polls, a prior durable returned code remains `returned_code` even if a later response omits it. This is code evidence only: `quote_evidence` separately reports response item presence, while quote field presence, validity, actual quote availability, and permission remain unknown. Old reports using `observed` must not be interpreted as quote evidence; rebuild from their verified REST manifest using the current rules. No business fields are removed or fabricated. Coverage report persistence itself does not assert hardware-level durability.

Local validation observation (2026-10-31): the full FUTU pytest run passed 83 tests in 65.79 s wall time, with 28.74 s user CPU, 6.67 s system CPU, and 83,976 KiB maximum RSS reported by `/usr/bin/time -v`. This is a test-suite measurement including spawned processes, not a per-request benchmark or a limit on aggregate concurrent memory. The additional oversized-redirect case passed in the targeted suite. No live FUTUNN service or account was used.

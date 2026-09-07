# Session delivery to the data-platform master

Each collector can run on its own machine. `storage.root` is its working data
location; `delivery.destination_root` is the master data platform's durable data
root. Historical and live collectors use the same delivery protocol.

```text
provider -> collector storage.root -> durable delivery queue
                                       |
                              verified local copy or SFTP
                                       |
                 master / remote_collectors / collector_id
                          objects + session manifests
                                       |
                       data-platform ingestion / publication
```

Delivery is optional for existing deployments. With `delivery` omitted, all data
stays local. With delivery configured, `local_retention` controls how long local
data copies remain after the central copy is first checksum-verified. Transfers
still occur when sessions close; retention delays local cleanup, not delivery.

## Configure a collector

Add this top-level block to the collector's existing version 2 YAML. The hostname
and paths below are deployment placeholders, not an activated destination.

```yaml
delivery:
  collector_id: quotes-edge-01
  transport: sftp
  destination_root: /var/lib/dq-quant-invest-data
  ssh:
    host: master.example.internal
    port: 22
    user: collector-ingest
    known_hosts: /etc/dq-collector/known_hosts
    private_key: /etc/dq-collector/master_ed25519
  local_retention: 1w
  retry_seconds: 60
  timeout_seconds: 20
  max_attempt_seconds: 300
  max_files: 100000
  max_bytes: 100000000000
```

Use a stable, unique `collector_id` for each collector storage root. A collector
running on the master, or using an already mounted central filesystem, can use:

```yaml
delivery:
  collector_id: historical-master-01
  transport: local
  destination_root: /var/lib/dq-quant-invest-data
  local_retention: 1w
```

The local destination and collector storage directories must not contain one
another. The destination filesystem must support hard links for atomic publication. Keep acquisition journals on local disk. Local destination paths and SSH
key/known-hosts paths resolve relative to the YAML file; absolute paths are also
supported, including native Windows paths for collector-side files. SFTP
`destination_root` must be an absolute server-side POSIX path. Symlink components
are rejected in remote directories and artifact paths.

Install the chosen engine, collector core, and providers on every collection host.
For SFTP, install the engine's `ssh` extra, for example from this checkout:

```bash
python -m pip install -e 'openbb_platform/collectors/core[ssh]' \
  -e 'openbb_platform/collectors/live[openbb,parquet,ssh]'
```

Use the historical engine's corresponding extras for a historical collector.
Local delivery needs no SSH dependency. SFTP needs an SSH server with its SFTP
subsystem enabled and a dedicated account permitted to write the collector's inbox;
no remote Python, rsync, or OpenBB installation is required for the transfer.
Provision a verified host key in `known_hosts`: unknown or changed host keys fail
closed. Authentication uses the specified private key, or the SSH agent if
`private_key` is omitted. Inline passwords and automatic host-key acceptance are
not supported. Restrict the master account's permissions to the intended inbox.

## Local retention

Choose one policy in the top-level `delivery` block:

```yaml
delivery:
  # Keep the existing collector_id, transport, destination_root, and SSH settings.
  local_retention: 1w
```

| `local_retention` | Local cleanup becomes eligible |
| --- | --- |
| `after_verification` | Immediately after successful central checksum verification and a durable local receipt |
| `1d` | One day after the first verified delivery |
| `1w` | One week after the first verified delivery |
| `1mo` | One calendar month after the first verified delivery |
| `forever` | Never automatically remove local data copies |

Positive integer durations support `h` (hours), `d` (24-hour days), `w` (7-day
weeks), and `mo` (calendar months), for example `12h`, `7d`, `2w`, or `3mo`.
Calendar calculations use UTC and clamp to the last day of a shorter month:
January 31 plus `1mo` expires on February 28, or February 29 in a leap year.
A duration cannot exceed 100 years. Zero, negative, fractional and ambiguous
units such as `1m` are rejected; use `after_verification` for immediate cleanup.

The first successful verification time is saved in the delivery outbox. Restarts,
retries and later verification do not reset it. At expiry the collector re-verifies
the central copy before deleting eligible local files. An unavailable or corrupted
central copy leaves the local files intact and queues a retry. A transfer failure
does not start the retention clock.

Expiry means eligible for cleanup on the next collector delivery pass or manual
`deliver` invocation. If the collector is stopped, the files remain until it runs
again; no separate background service is created. `deliver` can bypass retry
backoff, but cannot bypass the retention deadline. A delivered session waiting
for retention expiry remains successfully delivered, rather than transfer-pending.
`delivery-status` shows `first_verified_at`, `cleanup_after`, and `local_state`
(`awaiting_verification`, `retained`, `cleanup_due`, or `removed`).

Replace the older `remove_local_after_verification` setting with `local_retention`
when adopting this policy. Backward compatibility is retained: without
`local_retention`, the old `true` value means `after_verification` and `false`
means `forever`. Omitting both retains local copies indefinitely. Combining
`local_retention` with the old `true` flag is rejected to avoid conflicting intent.

Policy changes apply to retained data using its original verification time. They
can extend or shorten the remaining period; they do not restore files already
removed. Existing outboxes without a stored first-verification timestamp migrate
additively. Their retained sessions start the period at their first successful
re-verification by the updated collector.

## Session completion and retry

A historical `once` run seals its session after finishing collection and exports,
including partial or interrupted results. A live session seals when its configured
window closes or the collector shuts down. Live bounded runs produce an
interrupted-session snapshot if the market window is still open. Resuming that
window produces a new immutable report revision under the same session ID.

1. Export pending observations. Sessions with unfinished exports remain local.
2. Seal a manifest of raw responses, observations, attempt/rejection evidence,
   the session report, and eligible native exports.
3. Persist the manifest and a delivery job in `storage.root/delivery/state.sqlite3`.
4. Upload missing objects to temporary names, verify SHA-256 by reading them back,
   and publish immutable names without overwriting existing objects.
5. Publish the manifest and `.received.json` marker after every object verifies.
   Save a durable local receipt before starting local cleanup.
6. When the local retention policy permits cleanup, remove only manifest-listed
   data files whose local checksum still matches. Keep local acquisition state and
   delivery receipts.

Failure leaves the job pending and retains unacknowledged local data. The running
collector retries after `retry_seconds`; a new process also drains its persisted
queue. Objects completed before a connection failure are reused after verification.
An incomplete object is retried from the beginning. Existing objects with different
bytes are treated as integrity conflicts and never overwritten.

Transfers run synchronously at lifecycle/flush boundaries. The default pass stages
and attempts at most 10 session revisions, with a shared 300-second transfer budget.
A slow master can delay subsequent polling; choose a budget appropriate to the
collection schedule and the largest file. Connection/channel operations also have
a timeout. Process termination can interrupt delivery; the outbox survives it.
The supplied systemd examples allow 420 seconds for graceful shutdown. Increase
that grace if increasing transfer timeouts or budgets.

These commands inspect or retry delivery without loading providers or opening a
Gateway connection:

```bash
dq-live-market-data-collector delivery-status --config /etc/dq-collector/collector.yaml
dq-live-market-data-collector deliver --config /etc/dq-collector/collector.yaml --limit 20

dq-historical-market-data-collector delivery-status --config /etc/dq-collector/collector.yaml
dq-historical-market-data-collector deliver --config /etc/dq-collector/collector.yaml --limit 20
```

`delivery-status` is read-only and can run while acquisition is active. `deliver`
takes the engine's writer lock, recovers exports and interrupted sessions, and
forces a retry without waiting for backoff. Run it while that collector is stopped.
A pending transfer, pending output session, or reached batch limit returns exit 1;
configuration/runtime errors return exit 2. Repeat a bounded `deliver` invocation
until the queue is drained. Acquisition success and delivery status are separate:
a master outage does not turn a successful collection report into failed acquisition.

## What moves and what remains local

| Artifact | Master representation | Local cleanup after verification and retention expiry |
| --- | --- | --- |
| Raw provider result envelopes | Original immutable bytes | Removed after expiry |
| Normalized observations | Session JSONL snapshot with original record IDs/times | Snapshot removed after expiry; journal rows retained |
| Session-owned JSONL, CSV, Parquet exports | Original immutable bytes | Removed after expiry |
| Rejected evidence | Historical quarantine files; live attempt snapshot plus raw responses | Quarantine data files removed after expiry; attempt snapshot retained |
| Session reports and attempts | Immutable snapshot/report bytes | Retained |
| SQLite journal, checkpoints, mutable DuckDB outputs | Not transferred as database files | Retained for resume, deduplication, and local queries |
| Delivery queue, manifests and receipts | Separate local operational state | Retained |

For SQLite/DuckDB output modes, the normalized session JSONL snapshot is the portable
transfer representation. A historical export batch shared by multiple sessions is
kept local; its records are still delivered in each owning session's snapshot.
Repeated live snapshots are cumulative: the platform must deduplicate by
`record_id`, and must not sum all snapshot row counts as new observations.

New raw envelopes are indexed independently of successful normalization so a later
normalization failure still leaves deliverable evidence. Older journals remain
readable: existing attempt references identify their raw files, and older historical
quarantine files are matched with a bounded scan. Unreferenced files left by a crash
between filesystem publication and journal indexing are retained for recovery.

Raw envelopes contain the provider Fetcher result and metadata. They are not a
capture of original network packets. Delivery preserves those bytes and all stored
availability, event-time, feed-type, and revision semantics.

Changing delivery settings does not change observation identities. Changing the
destination creates a separate delivery target. If files were already moved to a
previous master, restore them or migrate that master archive before redirecting
those old sessions; a receipt for one destination cannot authorize deletion for
another destination.

## Master inbox contract

```text
<destination_root>/remote_collectors/<collector_id>/
  objects/sha256/<first-two-hash-characters>/<file-sha256>
  sessions/<historical-or-live>/<session_id>/
    <manifest-sha256>.manifest.json
    <manifest-sha256>.received.json
```

The manifest maps each logical collector-relative path to its byte hash, byte size,
role and cleanup eligibility. It records engine, session, report revision,
configuration fingerprint and collection status. Its file name is SHA-256 of the
manifest bytes. Report and snapshot revisions remain immutable. No source credentials
or SSH authentication material are placed in manifests.

A `.received.json` marker records completed transport verification. It is written
by the collector through the destination filesystem/SFTP service; it is not a data
platform quality approval, database transaction, replication acknowledgment, or
object-lock guarantee. Filesystem/SFTP verification does not itself provide storage
WORM retention. Use the master's storage permissions, snapshots, backups and archival
policy for those requirements. Leave canonical publication to the data platform.

A master consumer can install `openbb-collector-core` and verify a committed session:

```python
from pathlib import Path
from openbb_collector_core.delivery import read_received_session

root = Path('/var/lib/dq-quant-invest-data')
manifest_path = 'remote_collectors/quotes-edge-01/sessions/live/SESSION/HASH.manifest.json'
session = read_received_session(root, manifest_path)
for artifact in session['files']:
    actual_file = root / artifact['object_path']
    logical_name = artifact['path']
    # Pass the verified artifact to the platform's ingestion/reconciliation contract.
```

The reader checks the commit marker, manifest identity, every object's SHA-256,
file counts and byte totals before returning resolved object paths. Consumers
should process only committed manifests, retain lineage to their manifest hashes,
and apply the data platform's normal ingestion and publication gates. The platform's
existing rsync pull workflow remains separate; it does not automatically ingest
this session inbox format.

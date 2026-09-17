# Deferred quote retries and journal lookup performance

Version 0.1.6 adds idempotent indexes for export batches, session export status,
and collection availability. Existing observations and poll identities are
preserved. Index creation occurs under the journal's writer lock at startup;
large journals should be backed up and upgraded during a controlled stop.
Collector core 0.1.2 uses indexed latest-observation lookups, and the local
heartbeat timestamp is written after a flush completes.

A quote collection can optionally declare:

```yaml
quote_retry:
  max_attempts: 3
  delay_seconds: 60
  opening_delay_seconds: 1200
```

Missing prices, missing quote fields, and bounded timeouts can be retried. The
next retry is no earlier than `delay_seconds` after the failure or
`opening_delay_seconds` after the trading-session open, whichever is later.
Permanent contract/entitlement errors and invalid contract identities are not
deferred. Retries stay within the configured session and never enable overnight
collection. At most ten pending polls are processed before each regular sweep.

The queue is durable. A failed initial request does not occupy the collector
while waiting. Its archived evidence and all later attempt details are retained
until the final poll is committed. The final poll reconciles all input rows,
rejections, and accepted observations atomically; its identity and slot remain
the initial poll's identity and slot. No already-published poll is rewritten.
Queues survive process restarts. Expired or superseded queued polls are finalized
without another provider request. Failed initial requests can remain pending,
rather than appear as finalized failures, while retries are still scheduled.

The deferred-round limit is separate from a source's immediate retry count.
Deployments using deferred retries should normally set source `retries: 0`.
The production equity profile uses three rounds and a ten-second maximum
field-readiness wait. IBKR still reports the actual live/delayed feed type.

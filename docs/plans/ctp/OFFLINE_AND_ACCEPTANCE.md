# CTP tick offline use

Install `dq-live-market-data-collector[ctp]` with `openbb-ctp` and the compatible
collector core available to the installer. The native extra selects
`openctp-ctp==6.7.11.0`. Python 3.11–3.13 is supported by the CTP provider.
The native source is experimental; securities and securities options profiles
are unsupported. No real account, entitlement, live session, reconnect behavior,
or power-loss durability is established by the offline checks.

A synthetic replay requires no account:

```sh
dq-ctp-tick replay ticks.jsonl --root ./tick-output --source-id replay
dq-ctp-tick verify ./tick-output
```

The repository fixture `openbb_platform/collectors/live/tests/fixtures/ticks_synthetic.jsonl`
is suitable for a local replay. Verify reports readable prefixes and corruption;
it does not repair data or prove upstream completeness or fsync durability. Use
`dq-ctp-tick native --help` for native configuration and supervision options.
The supervisor's hard budget is the sum of `--startup-timeout` (default 10
seconds), `--duration`, and `--shutdown-timeout`, measured after child launch.
The startup allowance is shared with acquisition and shutdown: delays consume
the remaining total budget. A stop signal shortens the deadline to at most the
shutdown timeout. Exceeding the budget kills the child and reports failure.
Do not run a native connection without a separately authorized account and endpoint.

# Trading-session filters for sparse historical bars

Version 0.1.4 supports `calendar.require_all_bars: false`. It preserves weekday,
holiday, early-close, and intraday session filtering while leaving complete-bar
coverage unverified. This suits sparse TRADES bars and an incomplete current day.

```yaml
calendar:
  start: '09:30'
  end: '16:00'
  weekdays: [0, 1, 2, 3, 4]
  holidays: ['2026-11-26']
  overrides:
    '2026-11-27': {start: '09:30', end: '13:00'}
  require_all_bars: false
```

Confirmed closed dates are checkpointed as `calendar_closed` without provider
requests. Returned bars outside a configured session are excluded. Accepted
sparse data has coverage status `unknown` and basis `session_filter_only`; it
is not represented as complete minute coverage. The default remains strict
coverage, and existing calendar fingerprints retain their old default meaning.

The application supplies exchange calendars for regular-hours equities and
cash indexes. The SOFR extended-hours historical profile retains its separate
date-window behavior; an equity calendar must not be applied to its overnight
sessions. This version also adds an instrument/availability index for bounded
historical heartbeat lookups.

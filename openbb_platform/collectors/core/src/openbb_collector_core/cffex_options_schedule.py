"""Bounded current-snapshot scheduler for CFFEX options."""
from __future__ import annotations

import argparse
import fcntl
import json
import os
import subprocess
import sys
from datetime import date, datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

TZ = ZoneInfo('Asia/Shanghai')


def run(config, now=None):
    now = (now or datetime.now(TZ)).astimezone(TZ)
    start = date.fromisoformat(config['start_date'])
    calendar = json.loads(Path(config['calendar']).read_text())
    end = min(date.fromisoformat(calendar['coverage_end']), date(2026, 12, 31))
    if start < date.fromisoformat(calendar['coverage_start']):
        raise ValueError('start before calendar coverage')
    slots = [time.fromisoformat(s) for s in config['slots']]
    if len(slots) != 2 or len(set(slots)) != 2 or slots != sorted(slots):
        raise ValueError('two distinct ordered slots required')
    window = int(config['window_seconds'])
    attempts = int(config['max_attempts'])
    if not 60 <= window <= 1800 or not 1 <= attempts <= 5:
        raise ValueError('invalid window or request budget')
    root = Path(config['root'])
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    if root.stat().st_mode & 0o777 != 0o700:
        raise ValueError('root must have mode 0700')
    with (root / 'schedule.lock').open('a+') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print('ALERT options scheduler locked', file=sys.stderr)
            return 1
        ledger_path = root / 'schedule.json'
        ledger = json.loads(ledger_path.read_text()) if ledger_path.exists() else {}
        days = sorted(date.fromisoformat(x) for x in calendar['trading_dates'] if start.isoformat() <= x <= min(now.date(), end).isoformat())
        current = None
        for day in days:
            for slot in slots:
                due = datetime.combine(day, slot, TZ)
                if due > now:
                    continue
                key = f'{day.isoformat()}-{slot.strftime("%H%M%S")}'
                if ledger.get(key, {}).get('status') == 'complete':
                    continue
                if now >= due + timedelta(seconds=window):
                    if ledger.get(key, {}).get('status') != 'missed':
                        ledger[key] = {'status': 'missed', 'due': due.isoformat(), 'recorded_at': now.isoformat(), 'previous': ledger.get(key)}
                    continue
                current = (key, due)
        def save():
            tmp = ledger_path.with_suffix('.tmp')
            with tmp.open('w') as f:
                json.dump(ledger, f, sort_keys=True, indent=2)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, ledger_path)
            fd = os.open(root, os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
        save()
        missed = [k for k, v in ledger.items() if v['status'] == 'missed']
        if missed:
            print('ALERT missed options slots: ' + ', '.join(sorted(missed)), file=sys.stderr)
        if now.date() > end:
            print('ALERT options calendar expired ' + end.isoformat(), file=sys.stderr)
            return 1
        if current is None:
            return 0
        key, due = current
        snapshot = 'cffex-options-' + key
        remaining = int((due + timedelta(seconds=window) - now).total_seconds())
        cmd = [sys.executable, '-m', 'openbb_collector_core.cffex_options_raw', '--root', str(root), '--snapshot-id', snapshot, '--products', 'IO,HO,MO', '--max-attempts', str(attempts)]
        ledger[key] = {'status': 'running', 'due': due.isoformat(), 'snapshot_id': snapshot, 'started_at': now.isoformat()}
        save()
        try:
            result = subprocess.run(cmd, timeout=remaining, check=False)
            status = 'complete' if result.returncode == 0 else 'failed'
        except subprocess.TimeoutExpired:
            status = 'failed'
        ledger[key].update(status=status, finished_at=datetime.now(TZ).isoformat())
        save()
        if status != 'complete':
            print('ALERT options slot failed: ' + key, file=sys.stderr)
            return 1
        return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', required=True)
    args = parser.parse_args()
    try:
        return run(json.loads(Path(args.config).read_text()))
    except Exception as exc:
        print(f'ALERT options scheduler: {type(exc).__name__}: {exc}', file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())

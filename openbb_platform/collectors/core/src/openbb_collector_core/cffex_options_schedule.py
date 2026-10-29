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
    simulated = now is not None
    now = (now or datetime.now(TZ)).astimezone(TZ)
    start = date.fromisoformat(config['start_date'])
    calendar = json.loads(Path(config['calendar']).read_text())
    end = min(date.fromisoformat(calendar['coverage_end']), date(2026, 12, 31))
    if start < date.fromisoformat(calendar['coverage_start']):
        raise ValueError('start before calendar coverage')
    interval = config['interval_minutes']
    if type(interval) is not int or interval != 5:
        raise ValueError('interval must be five minutes')
    sessions = [(time.fromisoformat(a), time.fromisoformat(b)) for a, b in config['sessions']]
    allowed = [(time(9, 30), time(11, 30)), (time(13), time(15))]
    if (not sessions or sessions != sorted(sessions)
            or any(a >= b or not any(x <= a < b <= y for x, y in allowed) for a, b in sessions)
            or any(sessions[i - 1][1] > sessions[i][0] for i in range(1, len(sessions)))):
        raise ValueError('sessions must be ordered, disjoint and within CFFEX trading windows')
    slots = []
    for begin, finish in sessions:
        cursor = datetime.combine(date.min, begin)
        stop = datetime.combine(date.min, finish)
        while cursor < stop:
            slots.append((cursor.time(), finish))
            cursor += timedelta(minutes=interval)
    window = int(config['window_seconds'])
    attempts = int(config['max_attempts'])
    if not 1 <= window <= 300 or not 1 <= attempts <= 5:
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
            for slot, session_end in slots:
                due = datetime.combine(day, slot, TZ)
                if due > now:
                    continue
                key = f'{day.isoformat()}-{slot.strftime("%H%M%S")}'
                if ledger.get(key, {}).get('status') == 'complete':
                    continue
                deadline = min(due + timedelta(seconds=window), datetime.combine(day, session_end, TZ))
                if now >= deadline:
                    if ledger.get(key, {}).get('status') != 'missed':
                        ledger[key] = {'status': 'missed', 'due': due.isoformat(), 'recorded_at': now.isoformat(), 'previous': ledger.get(key)}
                    continue
                current = (key, due, deadline)
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
        key, due, deadline = current
        snapshot = 'cffex-options-' + key
        launch_time = now if simulated else datetime.now(TZ)
        remaining = int((deadline - launch_time).total_seconds())
        if remaining < 1:
            return 0
        cmd = [sys.executable, '-m', 'openbb_collector_core.cffex_options_raw', '--root', str(root), '--snapshot-id', snapshot, '--products', 'IO,HO,MO', '--max-attempts', str(attempts)]
        ledger[key] = {'status': 'running', 'due': due.isoformat(), 'snapshot_id': snapshot, 'started_at': now.isoformat()}
        save()
        try:
            launch_time = now if simulated else datetime.now(TZ)
            remaining = int((deadline - launch_time).total_seconds())
            if remaining < 1:
                raise subprocess.TimeoutExpired(cmd, 0)
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

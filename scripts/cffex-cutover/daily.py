#!/usr/bin/env python3
"""Run today's CFFEX acquisition and report, never acquire overdue dates."""
import argparse
import datetime as dt
import fcntl
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
from zoneinfo import ZoneInfo

START = dt.date(2026, 10, 30)
ZONE = ZoneInfo('Asia/Shanghai')
SOURCE = {'provider': 'akshare', 'credentials_env': {}, 'timeout_seconds': 60.0,
          'min_interval_seconds': 2.0, 'max_rows': 10000, 'max_response_bytes': 8000000,
          'retries': 2, 'retry_delay_seconds': 2.0, 'model': 'AkshareFuturesHistDailyCffex'}
UNITS = dict.fromkeys(('price_unit', 'volume_unit', 'open_interest_unit', 'turnover_unit'), 'provider_defined')


def run(config, now, invoke=subprocess.run):
    root = Path(config['root']).expanduser()
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(root, 0o700)
    with (root / 'lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        cal = json.loads(Path(config['calendar_file']).read_text())
        today = now.astimezone(ZONE).date()
        cutoff = dt.time.fromisoformat(config.get('not_before', '18:30'))
        end = dt.date.fromisoformat(config['coverage_end'])
        start = dt.date.fromisoformat(cal['coverage_start'])
        cal_end = dt.date.fromisoformat(cal['coverage_end'])
        if cal['timezone'] != 'Asia/Shanghai' or start > START or end > cal_end or today > end:
            raise ValueError('calendar coverage invalid or expired')
        dates = [dt.date.fromisoformat(s) for s in cal['trading_dates']]
        if dates != sorted(set(dates)) or any(d < start or d > cal_end for d in dates):
            raise ValueError('invalid calendar dates')
        budget = config.get('max_invocations', 3)
        if type(budget) is not int or not 1 <= budget <= 10:
            raise ValueError('invalid invocation limit')
        db = sqlite3.connect(root / 'scheduler.sqlite3')
        try:
            db.execute('PRAGMA synchronous=FULL')
            db.execute('CREATE TABLE IF NOT EXISTS days (day TEXT PRIMARY KEY, attempts INTEGER NOT NULL DEFAULT 0, status TEXT NOT NULL)')
            # Any elapsed date absent from our ledger is an explicit missed obligation.
            for d in dates:
                if START <= d < today:
                    db.execute("INSERT OR IGNORE INTO days(day,status) VALUES (?, 'missed')", (d.isoformat(),))
            db.commit()
            eligible = today >= START and today in dates and now.astimezone(ZONE).time().replace(tzinfo=None) >= cutoff
            if eligible:
                day = today.isoformat()
                db.execute("INSERT OR IGNORE INTO days(day,status) VALUES (?, 'pending')", (day,))
                row = db.execute('SELECT attempts,status FROM days WHERE day=?', (day,)).fetchone()
                if row[1] != 'complete' and row[0] < budget:
                    plan = {'version': 'cffex-acquire-plan-v2', 'plan_id': 'daily-' + day,
                            'source_id': 'cffex', 'units': UNITS, 'token': 'daily-cutover-v1',
                            'attempt': 0, 'max_attempts': 1, 'source': SOURCE,
                            'requests': {'dates': [day]}}
                    plans = root / 'plans'
                    plans.mkdir(mode=0o700, exist_ok=True)
                    plan_path = plans / (day + '.json')
                    body = json.dumps(plan, sort_keys=True, separators=(',', ':')) + '\n'
                    if plan_path.exists() and plan_path.read_text() != body:
                        raise ValueError('existing plan differs')
                    if not plan_path.exists():
                        with plan_path.open('x') as f:
                            os.chmod(plan_path, 0o600)
                            f.write(body)
                            f.flush()
                            os.fsync(f.fileno())
                    db.execute("UPDATE days SET attempts=attempts+1,status='running' WHERE day=?", (day,))
                    db.commit()  # Reserve before child: crashes consume budget.
                    child = invoke([config['acquire'], '--plan', str(plan_path), '--root', str(root / 'evidence')],
                                   capture_output=True, text=True, timeout=180, check=False)
                    print(child.stdout, end='')
                    if child.stderr:
                        print(child.stderr, file=sys.stderr, end='')
                    try:
                        report = json.loads(child.stdout)
                        success = child.returncode == 0 and report.get('status') == 'complete' and report.get('command') == 'acquire' and report.get('plan_id') == 'daily-' + day and report.get('captured') == 1
                    except (ValueError, AttributeError):
                        success = False
                    db.execute('UPDATE days SET status=? WHERE day=?', ('complete' if success else 'failed', day))
                    db.commit()
            problems = db.execute("SELECT day,attempts,status FROM days WHERE status != 'complete' ORDER BY day").fetchall()
            print(json.dumps({'scheduler': 'cffex-daily', 'today': today.isoformat(), 'eligible': eligible,
                              'outstanding': problems}, ensure_ascii=False), flush=True)
            return 1 if problems else 0
        finally:
            db.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', required=True)
    args = parser.parse_args()
    os.umask(0o077)
    try:
        return run(json.loads(Path(args.config).read_text()), dt.datetime.now(ZONE))
    except Exception as exc:
        print(json.dumps({'scheduler': 'cffex-daily', 'status': 'error', 'error': str(exc)}), file=sys.stderr)
        return 1

if __name__ == '__main__':
    sys.exit(main())

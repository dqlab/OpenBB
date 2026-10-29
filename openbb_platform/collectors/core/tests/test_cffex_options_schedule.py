import json
from datetime import datetime
from pathlib import Path
from unittest.mock import patch
from zoneinfo import ZoneInfo

from openbb_collector_core import cffex_options_schedule as s


def setup(tmp_path):
    cal = tmp_path / 'cal.json'
    cal.write_text(json.dumps({'coverage_start': '2026-01-01', 'coverage_end': '2026-12-31', 'trading_dates': ['2026-10-29', '2026-10-30', '2026-12-31']}))
    return {'root': str(tmp_path / 'state'), 'calendar': str(cal), 'start_date': '2026-10-30', 'sessions': [['09:30', '11:30'], ['13:00', '15:00']], 'interval_minutes': 5, 'window_seconds': 120, 'max_attempts': 2}


def at(value):
    return datetime.fromisoformat(value).replace(tzinfo=ZoneInfo('Asia/Shanghai'))


def test_slots_restart_missed_and_boundary(tmp_path, capsys):
    cfg = setup(tmp_path)
    with patch.object(s.subprocess, 'run') as run:
        run.return_value.returncode = 0
        assert s.run(cfg, at('2026-10-29T18:30')) == 0
        assert run.call_count == 0
        assert s.run(cfg, at('2026-10-30T09:30')) == 0
        assert s.run(cfg, at('2026-10-30T09:31')) == 0
        assert run.call_count == 1
        assert s.run(cfg, at('2026-10-30T09:35')) == 0
        assert run.call_count == 2
        ids = [x.args[0][x.args[0].index('--snapshot-id') + 1] for x in run.call_args_list]
        assert ids == ['cffex-options-2026-10-30-093000', 'cffex-options-2026-10-30-093500']
        assert s.run(cfg, at('2027-01-01T12:00')) == 1
        assert run.call_count == 2
    ledger = json.loads((Path(cfg['root']) / 'schedule.json').read_text())
    assert ledger['2026-12-31-093000']['status'] == 'missed'
    assert 'ALERT' in capsys.readouterr().err


def test_failure_budget_and_expiry(tmp_path):
    cfg = setup(tmp_path)
    with patch.object(s.subprocess, 'run') as run:
        run.return_value.returncode = 1
        assert s.run(cfg, at('2026-10-30T09:30')) == 1
        assert s.run(cfg, at('2026-10-30T09:33')) == 0
        assert run.call_count == 1
        assert '--max-attempts' in run.call_args.args[0]
    ledger = json.loads((Path(cfg['root']) / 'schedule.json').read_text())
    assert ledger['2026-10-30-093000']['status'] == 'missed'


def test_real_collector_cli_across_slots(tmp_path):
    import ast
    import os
    import subprocess
    import sys

    # Reuse the raw CLI fixture: all urlopen calls return synthetic IO/HO/MO data.
    source = Path(__file__).with_name('test_cffex_options_raw.py').read_text()
    tree = ast.parse(source)
    fn = next(x for x in tree.body if isinstance(x, ast.FunctionDef) and x.name == 'test_cli_offline')
    write = next(x for x in ast.walk(fn) if isinstance(x, ast.Call) and isinstance(x.func, ast.Attribute) and x.func.attr == 'write_text')
    (tmp_path / 'sitecustomize.py').write_text(ast.literal_eval(write.args[0]))
    (tmp_path / 'interrupted').touch()
    cfg = setup(tmp_path)
    cfgfile = tmp_path / 'config.json'
    cfgfile.write_text(json.dumps(cfg))
    package = Path(s.__file__).parents[1]
    env = {**os.environ, 'PYTHONPATH': str(tmp_path) + os.pathsep + str(package), 'CALL_LOG': str(tmp_path / 'calls'), 'FAIL_MARKER': str(tmp_path / 'failed'), 'INTERRUPT_MARKER': str(tmp_path / 'interrupted')}
    script = 'import json,sys; from datetime import datetime; from openbb_collector_core.cffex_options_schedule import run,TZ; sys.exit(run(json.load(open(sys.argv[1])),datetime.fromisoformat(sys.argv[2]).replace(tzinfo=TZ)))'
    def invoke(when):
        return subprocess.run([sys.executable, '-c', script, str(cfgfile), when], env=env, capture_output=True, text=True, timeout=60)
    for when in ['2026-10-30T09:30', '2026-10-30T09:31', '2026-10-30T09:35', '2026-10-30T09:36']:
        result = invoke(when)
        assert result.returncode == 0, result.stderr
    assert len((tmp_path / 'calls').read_text().splitlines()) == 13
    assert len(list(Path(cfg['root']).glob('snapshot-*.json'))) == 2


def test_market_boundaries_and_invalid_sessions(tmp_path):
    cfg = setup(tmp_path)
    with patch.object(s.subprocess, 'run') as run:
        run.return_value.returncode = 0
        for when in ['2026-10-30T09:29:59', '2026-10-30T11:30', '2026-10-30T12:59:59', '2026-10-30T15:00']:
            assert s.run(cfg, at(when)) == 0
        assert run.call_count == 0
        for when in ['2026-10-30T13:00', '2026-10-30T14:55']:
            assert s.run(cfg, at(when)) == 0
        assert run.call_count == 2
    cfg['sessions'] = [['09:25', '11:30']]
    import pytest
    with pytest.raises(ValueError):
        s.run(cfg, at('2026-10-30T09:25'))


def test_actual_schedule_cli_with_fake_clock_and_transport(tmp_path):
    import ast
    import os
    import subprocess
    import sys
    source = Path(__file__).with_name('test_cffex_options_raw.py').read_text()
    fn = next(x for x in ast.parse(source).body if isinstance(x, ast.FunctionDef) and x.name == 'test_cli_offline')
    write = next(x for x in ast.walk(fn) if isinstance(x, ast.Call) and isinstance(x.func, ast.Attribute) and x.func.attr == 'write_text')
    fixture = ast.literal_eval(write.args[0])
    fixture += """
import datetime as dt
class FakeDateTime(dt.datetime):
    @classmethod
    def now(cls, tz=None):
        return cls.fromisoformat(os.environ['FAKE_NOW']).replace(tzinfo=tz)
dt.datetime = FakeDateTime
"""
    (tmp_path / 'sitecustomize.py').write_text(fixture)
    (tmp_path / 'interrupted').touch()
    cfg = setup(tmp_path)
    config = tmp_path / 'config.json'
    config.write_text(json.dumps(cfg))
    env = {**os.environ, 'PYTHONPATH': str(tmp_path) + os.pathsep + str(Path(s.__file__).parents[1]), 'CALL_LOG': str(tmp_path / 'calls'), 'FAIL_MARKER': str(tmp_path / 'failed'), 'INTERRUPT_MARKER': str(tmp_path / 'interrupted')}
    for when, count in [('2026-10-30T09:29:59', 0), ('2026-10-30T09:30', 7), ('2026-10-30T09:31', 7), ('2026-10-30T09:35', 13), ('2026-10-30T11:30', 13), ('2026-10-30T15:00', 13)]:
        result = subprocess.run([sys.executable, '-m', 'openbb_collector_core.cffex_options_schedule', '--config', str(config)], env={**env, 'FAKE_NOW': when}, capture_output=True, text=True, timeout=60)
        assert result.returncode == 0, result.stderr
        assert len((tmp_path / 'calls').read_text().splitlines()) == count if count else not (tmp_path / 'calls').exists()

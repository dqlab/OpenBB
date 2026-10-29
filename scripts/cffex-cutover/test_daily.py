import datetime as dt
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

spec = importlib.util.spec_from_file_location('daily', Path(__file__).with_name('daily.py'))
daily = importlib.util.module_from_spec(spec)
spec.loader.exec_module(daily)

class SchedulerTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.base = Path(self.tmp.name)
        self.calendar = self.base / 'calendar.json'
        self.calendar.write_text(json.dumps({'timezone':'Asia/Shanghai', 'coverage_start':'2026-10-30', 'coverage_end':'2026-11-03', 'trading_dates':['2026-10-30', '2026-11-02', '2026-11-03']}))
        self.config = {'root':str(self.base/'state'), 'calendar_file':str(self.calendar), 'coverage_end':'2026-11-03', 'acquire':'fake', 'max_invocations':3}
        self.calls = []
        self.ok = True
    def invoke(self, args, **kwargs):
        self.calls.append(json.loads(Path(args[2]).read_text()))
        class R: stderr = ''; returncode = 0
        r = R()
        r.stdout = json.dumps({'command':'acquire', 'status':'complete' if self.ok else 'failed', 'captured':1 if self.ok else 0, 'plan_id':self.calls[-1]['plan_id']})
        return r
    def go(self, day, hour=19):
        return daily.run(self.config, dt.datetime.fromisoformat(day).replace(hour=hour, tzinfo=daily.ZONE), self.invoke)
    def test_today_once_cutoff_and_closed_day(self):
        self.assertEqual(self.go('2026-10-30', 18), 0)
        self.assertEqual(self.go('2026-10-30'), 0)
        self.assertEqual(self.go('2026-10-30'), 0)
        self.assertEqual(self.go('2026-10-31'), 0)
        self.assertEqual([p['requests']['dates'] for p in self.calls], [['2026-10-30']])
    def test_retries_survive_restart_and_stop(self):
        self.ok = False
        for _ in range(5): self.assertEqual(self.go('2026-10-30'), 1)
        self.assertEqual(len(self.calls), 3)
        self.assertTrue(all(p == self.calls[0] for p in self.calls))
    def test_offline_debt_visible_without_old_calls(self):
        self.assertEqual(self.go('2026-11-02'), 1)
        self.assertEqual(self.calls[0]['requests']['dates'], ['2026-11-02'])
        self.assertEqual(self.go('2026-11-03'), 1)
        self.assertEqual(self.calls[-1]['requests']['dates'], ['2026-11-03'])
    def test_expiry_fails_before_call(self):
        with self.assertRaises(ValueError): self.go('2026-11-04')
        self.assertEqual(self.calls, [])
    def test_crash_consumes_attempt_and_restart(self):
        def crash(*args, **kwargs): raise RuntimeError('crash')
        with self.assertRaises(RuntimeError): daily.run(self.config, dt.datetime(2026,10,30,19,tzinfo=daily.ZONE), crash)
        self.ok = False
        for _ in range(3): self.go('2026-10-30')
        self.assertEqual(len(self.calls), 2)

if __name__ == '__main__': unittest.main()

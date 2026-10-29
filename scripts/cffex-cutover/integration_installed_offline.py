"""Synthetic end-to-end check of installed acquire, with network disabled."""
import datetime as dt
import importlib.util
import json
import os
from pathlib import Path
import sys
import tempfile

with tempfile.TemporaryDirectory(prefix='cffex-cutover-offline-') as td:
    base = Path(td)
    (base / 'sitecustomize.py').write_text('''import os, socket\nfrom dq_historical_market_data_collector.cffex import source\ndef no_network(*args, **kwargs): raise RuntimeError("network disabled")\nsocket.create_connection = no_network\nclass Response:\n status_code = int(os.environ["FAKE_STATUS"])\n content = b"synthetic-only"\n headers = {"Content-Type": "text/csv"}\ndef send(url, timeout): return Response()\nsource.transport = lambda: send\n''')
    calendar = base / 'calendar.json'
    calendar.write_text(json.dumps({'timezone':'Asia/Shanghai', 'coverage_start':'2026-10-30', 'coverage_end':'2026-10-30', 'trading_dates':['2026-10-30']}))
    config = {'root':str(base/'state'), 'calendar_file':str(calendar), 'coverage_end':'2026-10-30', 'acquire':str(Path(sys.executable).parent/'dq-cffex-acquire')}
    os.environ['PYTHONPATH'] = str(base)
    spec = importlib.util.spec_from_file_location('cutover', Path(__file__).with_name('daily.py'))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    now = dt.datetime(2026,10,30,19,tzinfo=module.ZONE)
    os.environ['FAKE_STATUS'] = '503'
    assert module.run(config, now) == 1
    os.environ['FAKE_STATUS'] = '200'
    assert module.run(config, now) == 0
    assert module.run(config, now) == 0
    print('installed synthetic 503, retry 200 and completed restart: PASS')

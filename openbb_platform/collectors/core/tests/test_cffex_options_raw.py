import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from openbb_collector_core import cffex_options_raw as mod


def html(product):
    return ('<ul id="option_suffix"><li data-value="%s2611">%s2611</li></ul>' % (product.lower(), product.lower())).encode()


def quote(product):
    return json.dumps({'result': {'status': {'code': 0}, 'data': {'info': {'relate': {'IO': 'sh000300', 'HO': 'sh000016', 'MO': 'sh000852'}[product], 'name': {'IO': '沪深300指数', 'HO': '上证50指数', 'MO': '中证1000指数'}[product], 'expire_date': '2026-11-20'}, 'up': [[1,2,3,4,5,6,7,3000,product.lower()+'2611C3000']], 'down': [[1,2,3,4,5,6,7,product.lower()+'2611P3000']]}}}, ensure_ascii=False).encode()


def test_replay_and_failure(tmp_path):
    root = tmp_path / 'evidence'; root.mkdir(mode=0o700)
    calls = []
    def fetch(kind, token):
        calls.append((kind, token))
        if kind == 'quote' and token == 'ho2611' and calls.count((kind, token)) == 1:
            return 503, b'failure'
        return 200, html(token) if kind == 'discovery' else quote(token[:2].upper())
    assert not mod.collect(root, 'snapshot', ['IO','HO','MO'], 1, fetch)
    assert mod.collect(root, 'snapshot', ['IO','HO','MO'], 1, fetch, epoch=1)
    before = list(calls)
    assert mod.collect(root, 'snapshot', ['IO','HO','MO'], 1, fetch, epoch=1)
    assert calls == before
    assert calls.count(('quote','io2611')) == 1
    assert calls.count(('quote','ho2611')) == 2
    assert calls.count(('quote','mo2611')) == 1
    with pytest.raises(ValueError):
        mod.collect(root, 'snapshot', ['IO'], 2, fetch)


def test_private_root(tmp_path):
    root = tmp_path / 'public'; root.mkdir(mode=0o755); root.chmod(0o755)
    with pytest.raises(ValueError):
        mod.collect(root, 's', ['IO'], 1, lambda *_: (200, b''))


def test_cli_offline(tmp_path):
    root = tmp_path / 'root'; root.mkdir(mode=0o700)
    patch = tmp_path / 'sitecustomize.py'
    patch.write_text('''import urllib.request, json, os\nclass R:\n def __init__(self, body, status=200): self.body=body; self.status=status\n def __enter__(self): return self\n def __exit__(self,*a): pass\n def read(self,n): return self.body\ndef open_(request, timeout):\n url=request.full_url\n with open(os.environ["CALL_LOG"],"a") as f: f.write(url+"\\n")\n if "getOptionData" in url:\n  from urllib.parse import urlparse, parse_qs\n  t=parse_qs(urlparse(url).query)["pinzhong"][0]; p=t[:2].upper()\n  interrupt=os.environ["INTERRUPT_MARKER"]\n  if t == "io2611" and not os.path.exists(interrupt):\n   open(interrupt,"w").close(); os._exit(77)\n  marker=os.environ["FAIL_MARKER"]\n  if t == "ho2611" and not os.path.exists(marker):\n   open(marker,"w").close(); return R(b"unavailable",503)\n  info={"IO":["sh000300","沪深300指数"],"HO":["sh000016","上证50指数"],"MO":["sh000852","中证1000指数"]}[p]\n  return R(json.dumps({"result":{"status":{"code":0},"data":{"info":{"relate":info[0],"name":info[1],"expire_date":"2026-11-20"},"up":[[1,2,3,4,5,6,7,3000,t+"C3000"]],"down":[[1,2,3,4,5,6,7,t+"P3000"]]}}},ensure_ascii=False).encode())\n p="ho" if "/ho/cffex" in url else "mo" if "/mo/cffex" in url else "io"\n return R(("<ul id=\\"option_suffix\\"><li data-value=\\""+p+"2611\\">"+p+"2611</li></ul>").encode())\nurllib.request.urlopen=open_\n''')
    env = {**os.environ, 'PYTHONPATH': str(tmp_path) + os.pathsep + str(Path(mod.__file__).parents[1]), 'CALL_LOG': str(tmp_path/'calls'), 'FAIL_MARKER': str(tmp_path/'failed'), 'INTERRUPT_MARKER': str(tmp_path/'interrupted')}
    cmd = [sys.executable, '-m', 'openbb_collector_core.cffex_options_raw', '--root', str(root), '--snapshot-id', 'cli', '--products', 'IO,HO,MO']
    interrupted = subprocess.run(cmd + ['--max-attempts', '1'], env=env, capture_output=True, text=True)
    assert interrupted.returncode == 77
    first = subprocess.run(cmd + ['--max-attempts', '1'], env=env, capture_output=True, text=True)
    assert first.returncode != 0
    for _ in range(2):
        result = subprocess.run(cmd + ['--max-attempts', '1', '--epoch', '1'], env=env, capture_output=True, text=True)
        assert result.returncode == 0, result.stderr
    assert len((tmp_path/'calls').read_text().splitlines()) == 8


def test_interrupted_replays_completed_evidence(tmp_path):
    root = tmp_path / 'root'; root.mkdir(mode=0o700)
    calls = []
    def interrupted(kind, token):
        calls.append((kind, token))
        if kind == 'quote':
            raise RuntimeError('interrupt')
        return 200, html(token)
    with pytest.raises(RuntimeError):
        mod.collect(root, 'interrupted', ['IO'], 2, interrupted)
    def resume(kind, token):
        calls.append((kind, token))
        return 200, quote('IO')
    assert mod.collect(root, 'interrupted', ['IO'], 2, resume)
    assert calls.count(('discovery', 'IO')) == 1


def test_http_failure_bytes_are_bound(tmp_path):
    from openbb_collector_core import raw_acquisition as raw
    root = tmp_path / 'root'; root.mkdir(mode=0o700)
    assert not mod.collect(root, 'bad', ['IO'], 1, lambda *_: (503, b'raw error'))
    receipts = list(root.glob('raw-acquisition-v2/*/responses/000000/receipt.json'))
    assert len(receipts) == 1
    receipt = json.loads(receipts[0].read_text())
    assert receipt['http_status'] == 503
    assert (receipts[0].parent / 'body.bin').read_bytes() == b'raw error'
    assert receipt['sha256'] == raw._sha(b'raw error')


def test_saved_native_discovery_fragments():
    fixtures = json.loads((Path(__file__).parent / 'cffex_native_discovery.json').read_text())
    for product, fragment in fixtures:
        found = mod.expiries(fragment.encode('ascii'), product)
        assert len(found) == 6
        assert set(found) == {product.lower() + month for month in ('2610', '2611', '2612', '2703', '2706', '2709')}

@pytest.mark.parametrize('mode', ['large', 'http503', 'networkfail', 'empty', '204'])
def test_review_attack_cli_budget_and_evidence(tmp_path, mode):
    root = tmp_path / 'root'; root.mkdir(mode=0o700)
    hook = (Path(__file__).parent / 'cffex_review_http_hook.py').read_text()
    hook = hook.replace("'/tmp/cffex-review-test/calls'", 'os.environ["CALL_LOG"]')
    (tmp_path / 'sitecustomize.py').write_text(hook)
    env = {**os.environ, 'PYTHONPATH': str(tmp_path) + os.pathsep + str(Path(mod.__file__).parents[1]), 'CALL_LOG': str(tmp_path / 'calls'), 'MODE': mode}
    cmd = [sys.executable, '-m', 'openbb_collector_core.cffex_options_raw', '--root', str(root), '--snapshot-id', 'review', '--products', 'IO', '--max-attempts', '1']
    first = subprocess.run(cmd, env=env, capture_output=True, text=True)
    assert first.returncode == 1
    replay = subprocess.run(cmd, env=env, capture_output=True, text=True)
    assert replay.returncode == 1
    assert len((tmp_path / 'calls').read_text().splitlines()) == 1
    receipts = list(root.glob('raw-acquisition-v2/*/responses/000000/receipt.json'))
    assert len(receipts) == 1
    receipt = json.loads(receipts[0].read_text())
    if mode == 'large':
        assert receipt['http_status'] == 503 and receipt['size'] == mod.LIMIT + 1
        assert len((receipts[0].parent / 'body.bin').read_bytes()) == mod.LIMIT + 1
        assert 'truncated' in first.stderr and 'truncated' in replay.stderr
        marker = json.loads(next(root.glob('epochs-*/*.truncated')).read_text())
        assert marker['http_status'] == 503 and marker['complete_response'] is False
    if mode == 'http503':
        assert receipt['http_status'] == 503
        assert (receipts[0].parent / 'body.bin').read_bytes() == b'failure'
    if mode == 'networkfail':
        assert receipt['status'] == 'failed'
    env['MODE'] = 'ok'
    recovered = subprocess.run(cmd + ['--epoch', '1'], env=env, capture_output=True, text=True)
    assert recovered.returncode == 0, recovered.stderr
    count = len((tmp_path / 'calls').read_text().splitlines())
    assert subprocess.run(cmd + ['--epoch', '1'], env=env, capture_output=True).returncode == 0
    assert len((tmp_path / 'calls').read_text().splitlines()) == count

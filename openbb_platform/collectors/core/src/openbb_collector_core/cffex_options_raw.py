"""Standalone Sina CFFEX option raw snapshots; no downstream dependencies."""
from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import re
import stat
import sys
from datetime import date
from html.parser import HTMLParser
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from . import raw_acquisition as raw
from .models import ProviderFailure

BASE = 'https://stock.finance.sina.com.cn/futures/view/optionsCffexDP.php'
DISCOVERY = {'IO': BASE, 'HO': BASE + '/ho/cffex', 'MO': BASE + '/mo/cffex'}
QUOTE = 'https://stock.finance.sina.com.cn/futures/api/openapi.php/OptionService.getOptionData'
INFO = {'IO': ('sh000300', '沪深300指数'), 'HO': ('sh000016', '上证50指数'), 'MO': ('sh000852', '中证1000指数')}
LIMIT = 1_000_000


class Expiries(HTMLParser):
    def __init__(self):
        super().__init__()
        self.depth = 0
        self.containers = 0
        self.token = None
        self.text = ''
        self.items = []

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if self.depth:
            self.depth += 1
            if tag == 'li':
                if self.token is not None:
                    raise ValueError('nested expiry')
                self.token, self.text = attrs.get('data-value', ''), ''
        elif attrs.get('id') == 'option_suffix':
            self.depth = 1
            self.containers += 1

    def handle_data(self, data):
        if self.token is not None:
            self.text += data

    def handle_endtag(self, tag):
        if self.depth:
            if tag == 'li' and self.token is not None:
                self.items.append((self.token, self.text.strip()))
                self.token = None
            self.depth -= 1


def expiries(body, product):
    parser = Expiries()
    parser.feed(body.decode('gb18030'))
    tokens = [token for token, text in parser.items if token == text]
    if (parser.containers != 1 or len(tokens) != len(parser.items) or not 1 <= len(tokens) <= 24
            or len(set(tokens)) != len(tokens) or any(not re.fullmatch(product.lower() + r'[0-9]{2}(0[1-9]|1[0-2])', t) for t in tokens)):
        raise ValueError('invalid discovery')
    return tokens


def valid_quote(body, token):
    def unique(pairs):
        obj = {}
        for key, value in pairs:
            if key in obj:
                raise ValueError('duplicate key')
            obj[key] = value
        return obj
    data = json.loads(body.decode('utf-8'), object_pairs_hook=unique, parse_constant=lambda _: (_ for _ in ()).throw(ValueError('invalid constant')))['result']
    if type(data['status']['code']) is not int or data['status']['code'] != 0:
        raise ValueError('source status')
    data = data['data']
    info = data['info']
    if (info['relate'], info['name']) != INFO[token[:2].upper()] or not re.fullmatch('20' + token[2:] + r'-[0-9]{2}', info['expire_date'].replace('-', '', 1)):
        raise ValueError('source identity')
    date.fromisoformat(info['expire_date'])
    if 'exchange' in info and info['exchange'] != 'cffex':
        raise ValueError('source exchange')
    strikes = []
    for side, letter, width in [('up', 'C', 9), ('down', 'P', 8)]:
        rows = data[side]
        if not isinstance(rows, list) or not 1 <= len(rows) <= 1000:
            raise ValueError('missing quotes')
        ids = set()
        for row in rows:
            if not isinstance(row, list) or len(row) != width or not isinstance(row[-1], str):
                raise ValueError('quote shape')
            m = re.fullmatch(re.escape(token + letter) + r'([1-9][0-9]*)', row[-1])
            if not m or m[1] in ids or (side == 'up' and str(row[7]) != m[1]):
                raise ValueError('quote identity')
            ids.add(m[1])
        strikes.append(ids)
    if strikes[0] != strikes[1]:
        raise ValueError('quote membership')


def fetch(kind, token):
    url = DISCOVERY[token] if kind == 'discovery' else QUOTE + '?' + urlencode({'type': 'futures', 'product': token[:2], 'exchange': 'cffex', 'pinzhong': token})
    request = Request(url, headers={'Referer': BASE, 'User-Agent': 'Mozilla/5.0', 'Accept-Encoding': 'identity'})
    try:
        try:
            response = urlopen(request, timeout=30)
        except HTTPError as exc:
            response = exc
        with response:
            body = response.read(LIMIT + 1)
            return response.status, body
    except (URLError, TimeoutError, OSError) as exc:
        raise ProviderFailure('source_unavailable') from exc


def collect(root, snapshot_id, products, max_attempts, sender=fetch, *, epoch=0):
    root = Path(os.path.abspath(root))
    if any(p.is_symlink() for p in (root, *root.parents)) or not root.is_dir():
        raise ValueError('evidence root must be an existing private directory')
    st = root.stat()
    if st.st_uid != os.geteuid() or stat.S_IMODE(st.st_mode) != 0o700:
        raise ValueError('evidence root must be owned by user and mode 0700')
    if not isinstance(snapshot_id, str) or not 1 <= len(snapshot_id) <= 128 or not products or len(set(products)) != len(products) or any(p not in DISCOVERY for p in products) or type(max_attempts) is not int or not 1 <= max_attempts <= 5 or type(epoch) is not int or not 0 <= epoch <= 1000:
        raise ValueError('invalid arguments')
    # One owner serializes the immutable authority and all captures under the root.
    with open(root / '.cffex-options.lock', 'a+b') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        identity = hashlib.sha256(snapshot_id.encode()).hexdigest()
        authority = root / ('snapshot-' + identity + '.json')
        raw._install(authority, raw._encode({'snapshot_id': snapshot_id, 'products': products}))

        # Each explicit epoch is an immutable operator authorization with its own
        # budget. Previous epochs and their successes remain available for replay.
        epochs = root / ('epochs-' + identity)
        raw._mkdir(epochs)
        if epoch and not (epochs / (str(epoch - 1) + '.json')).is_file():
            raise ValueError('previous epoch missing')
        raw._install(epochs / (str(epoch) + '.json'), raw._encode({'epoch': epoch, 'max_attempts': max_attempts}))

        def acquire(kind, token):
            task_id = hashlib.sha256(raw._encode([snapshot_id, kind, token])).hexdigest()
            for e in range(epoch + 1):
                config = raw._decode(raw._read(epochs / (str(e) + '.json'), raw.MAX_META_BYTES))
                for attempt in range(1, config['max_attempts'] + 1):
                    generation = str(attempt) if e == 0 else f'{e}:{attempt}'
                    task = {'version': raw.HTTP_VERSION, 'task_id': task_id, 'generation': generation, 'source': {'provider': 'sina', 'model': 'cffex-options-' + kind}, 'requests': [{'request_id': token, 'params': {'kind': kind, 'token': token}}]}
                    locator = root / raw.capture_locator(task_id, generation, version=raw.HTTP_VERSION)
                    reservation = epochs / (task_id + '-' + generation.replace(':', '-') + '.request')
                    # An uncertain prior call consumes its slot even if the process
                    # died before a receipt could be installed.
                    if reservation.exists():
                        if not (locator / 'durable.json').is_file():
                            continue
                    elif e < epoch:
                        continue
                    else:
                        raw._install(reservation, raw._encode({'task_id': task_id, 'generation': generation}))
                    def adapter(_):
                        status, body = sender(kind, token)
                        return raw.RawResponse(body, http_status=status)
                    state = raw.capture(root, task, adapter, response_max_bytes=LIMIT + 1)
                    if not state['complete']:
                        continue
                    item = state['responses'][0]
                    receipt, body = item['receipt'], item['body']
                    if body is not None and len(body) > LIMIT:
                        raw._install(reservation.with_suffix('.truncated'), raw._encode({
                            'task_id': task_id, 'generation': generation,
                            'http_status': receipt.get('http_status'), 'retained_bytes': len(body),
                            'complete_response': False, 'reason': 'response_limit_exceeded',
                        }))
                        print(f'response truncated: {kind} {token}, HTTP {receipt.get("http_status")}, received at least {len(body)} bytes', file=sys.stderr)
                        continue
                    if receipt['status'] == 'captured' and receipt.get('http_status') == 200:
                        try:
                            value = expiries(body, token) if kind == 'discovery' else valid_quote(body, token)
                            return value if kind == 'discovery' else True
                        except (ValueError, KeyError, TypeError, UnicodeError, json.JSONDecodeError):
                            pass
            return None

        ok = True
        for product in products:
            months = acquire('discovery', product)
            if months is None:
                ok = False
                continue
            for month in months:
                if not acquire('quote', month):
                    ok = False
        return ok


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', required=True)
    parser.add_argument('--snapshot-id', required=True)
    parser.add_argument('--products', default='IO,HO,MO')
    parser.add_argument('--max-attempts', type=int, default=2)
    parser.add_argument('--epoch', type=int, default=0, help='explicitly authorize this numbered retry epoch')
    args = parser.parse_args()
    try:
        success = collect(args.root, args.snapshot_id, args.products.split(','), args.max_attempts, epoch=args.epoch)
    except Exception as exc:
        print('acquisition error: ' + type(exc).__name__ + ': ' + str(exc), file=sys.stderr)
        return 1
    print('complete' if success else 'incomplete')
    return 0 if success else 1


if __name__ == '__main__':
    sys.exit(main())

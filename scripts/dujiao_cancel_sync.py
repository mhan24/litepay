#!/usr/bin/env python3
"""Synchronize committed Dujiao cancellations to one BEpusdt gateway.

The commerce database is read-only. Only the separate retry journal is written.
Run under the commerce DB owner; credentials remain in its existing channel config.
"""
import argparse
import hashlib
import json
import logging
import sqlite3
import time
import urllib.parse
import urllib.request
from pathlib import Path


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def candidates(source, gateway):
    rows = source.execute("""
        SELECT p.id, p.provider_ref, p.gateway_order_no, p.pay_url, c.config_json
        FROM payments p JOIN orders o ON o.id = p.order_id
        JOIN payment_channels c ON c.id = p.channel_id
        WHERE o.status = 'canceled' AND o.paid_at IS NULL
          AND o.canceled_at IS NOT NULL AND o.deleted_at IS NULL
          AND p.provider_type = 'bepusdt' AND p.status IN ('pending', 'expired', 'failed')
          AND p.paid_at IS NULL AND p.deleted_at IS NULL
          AND c.provider_type = 'bepusdt'
        ORDER BY p.id DESC
    """).fetchall()
    for payment_id, trade_id, order_id, pay_url, raw in rows:
        cfg = json.loads(raw or '{}')
        if str(cfg.get('gateway_url', '')).rstrip('/') != gateway:
            continue
        # A channel can be repointed. Never send its historical orders to the new host.
        if urllib.parse.urlsplit(pay_url or '').netloc != urllib.parse.urlsplit(gateway).netloc:
            continue
        if trade_id and order_id and cfg.get('auth_token'):
            yield payment_id, trade_id, order_id, cfg['auth_token']


def cancel(gateway, trade_id, order_id, token):
    body = {'trade_id': trade_id, 'order_id': order_id}
    signing = '&'.join(k + '=' + body[k] for k in sorted(body)) + token
    body['signature'] = hashlib.md5(signing.encode()).hexdigest()
    req = urllib.request.Request(gateway + '/api/v1/order/cancel-transaction',
                                 data=json.dumps(body).encode(),
                                 headers={'Content-Type': 'application/json',
                                          'User-Agent': 'Mozilla/5.0 LitePay-CancelSync/1.0'})
    with urllib.request.build_opener(NoRedirect).open(req, timeout=10) as response:
        result = json.loads(response.read(65536))
    if result.get('status_code') == 200:
        data = result.get('data') or {}
        if data.get('trade_id') == trade_id and data.get('order_id') == order_id:
            return 'closed'
        raise ValueError('gateway returned mismatched cancellation identifiers')
    if result.get('message') == '已支付订单不能关闭':
        return 'paid_conflict'
    if result.get('message') == 'record not found':
        return 'missing_gateway'
    # Do not put gateway responses, credentials, or signatures into logs/state.
    raise ValueError('gateway rejected cancellation')


def journal(path):
    db = sqlite3.connect(path)
    db.execute('''CREATE TABLE IF NOT EXISTS cancellations (
        gateway TEXT NOT NULL, payment_id INTEGER NOT NULL, trade_id TEXT NOT NULL,
        order_id TEXT NOT NULL, outcome TEXT NOT NULL, attempts INTEGER NOT NULL,
        next_at REAL NOT NULL, updated_at REAL NOT NULL,
        PRIMARY KEY(gateway, payment_id, trade_id, order_id))''')
    return db


def sync_once(database, state, gateway, sender=cancel, now=None):
    now = time.time() if now is None else now
    # Finish the snapshot read before making external calls: never hold a DB lock over HTTP.
    source = sqlite3.connect(Path(database).resolve().as_uri() + '?mode=ro', uri=True, timeout=5)
    try:
        pending = list(candidates(source, gateway))
    finally:
        source.close()
    processed = 0
    deadline = time.monotonic() + 5
    for payment_id, trade_id, order_id, token in pending:
        key = (gateway, payment_id, trade_id, order_id)
        previous = state.execute('SELECT outcome,attempts,next_at FROM cancellations WHERE gateway=? AND payment_id=? AND trade_id=? AND order_id=?', key).fetchone()
        if previous and (previous[0] != 'retry' or previous[2] > now):
            continue
        attempts = previous[1] + 1 if previous else 1
        try:
            outcome = sender(gateway, trade_id, order_id, token)
            if outcome not in ('closed', 'paid_conflict', 'missing_gateway'):
                raise ValueError('invalid cancellation outcome')
            if outcome == 'missing_gateway' and attempts < 3:
                outcome = 'retry'
        except Exception as error:
            outcome = 'retry'
            logging.warning('payment_id=%s cancellation retry error_type=%s', payment_id, type(error).__name__)
        next_at = now + min(300, 5 * 2 ** min(attempts - 1, 6))
        state.execute('INSERT OR REPLACE INTO cancellations VALUES (?,?,?,?,?,?,?,?)',
                      key + (outcome, attempts, next_at, now))
        state.commit()
        if outcome != 'retry':
            logging.log(logging.ERROR if outcome == 'paid_conflict' else logging.INFO,
                        'payment_id=%s outcome=%s', payment_id, outcome)
        processed += 1
        if processed >= 50 or time.monotonic() >= deadline:
            break
    return processed


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--database', required=True)
    parser.add_argument('--state', required=True)
    parser.add_argument('--gateway', required=True)
    parser.add_argument('--once', action='store_true')
    args = parser.parse_args()
    gateway = args.gateway.rstrip('/')
    url = urllib.parse.urlsplit(gateway)
    if url.scheme != 'https' or not url.hostname or url.username or url.password or url.port not in (None, 443) or url.path or url.query or url.fragment:
        parser.error('--gateway must be an HTTPS origin')
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    state = journal(args.state)
    while True:
        try:
            sync_once(args.database, state, gateway)
        except Exception as error:
            logging.error('sync failed error_type=%s', type(error).__name__)
            if args.once:
                raise SystemExit(1)
        if args.once:
            break
        time.sleep(5)


if __name__ == '__main__':
    main()

#!/usr/bin/env python3
"""Synchronize committed Dujiao cancellations to one BEpusdt gateway.

The commerce database is read-only. Only the separate retry journal is written.
Run under the commerce DB owner; credentials remain in its existing channel config.
"""
import argparse
import hashlib
import json
import logging
import os
import socket
import sqlite3
import time
import urllib.parse
import urllib.request
from pathlib import Path


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def source_connect(database):
    if str(database).startswith(('postgresql://', 'postgres://')):
        try:
            import psycopg
        except ImportError:
            raise RuntimeError('PostgreSQL requires psycopg 3; install psycopg[binary]') from None
        return psycopg.connect(database, connect_timeout=5, options='-c default_transaction_read_only=on')
    return sqlite3.connect(Path(database).resolve().as_uri() + '?mode=ro', uri=True, timeout=5)


# Source reads are bounded; the journal commits discoveries and cursors together.
RECENT_ORDER_BATCH = 200
DISCOVERY_ORDER_BATCH = 50
ORDER_PAYMENT_BATCH = 50
AUDIT_PAYMENT_BATCH = 500
AUDIT_INTERVAL = 30
SEND_BATCH = 50


def source_query(source, sql, params=()):
    # psycopg uses %s, sqlite uses ?. SQL text never includes user-provided values.
    if not isinstance(source, sqlite3.Connection):
        sql = sql.replace('?', '%s')
    return source.execute(sql, params)


def payment_rows(source, predicate, params, limit=None):
    sql = """
        SELECT p.id, p.provider_ref, p.gateway_order_no, p.pay_url, c.config_json
        FROM payments p JOIN orders o ON o.id = p.order_id
        JOIN payment_channels c ON c.id = p.channel_id
        WHERE o.status = 'canceled' AND o.paid_at IS NULL
          AND o.canceled_at IS NOT NULL AND o.deleted_at IS NULL
          AND p.provider_type = 'bepusdt' AND p.status IN ('pending', 'expired', 'failed')
          AND p.paid_at IS NULL AND p.deleted_at IS NULL
          AND c.provider_type = 'bepusdt' AND """ + predicate + ' ORDER BY p.id'
    if limit is not None:
        sql += ' LIMIT ?'
        params = tuple(params) + (limit,)
    return source_query(source, sql, params).fetchall()


def eligible(rows, gateway):
    for payment_id, trade_id, order_id, pay_url, raw in rows:
        cfg = raw if isinstance(raw, dict) else json.loads(raw or '{}')
        if str(cfg.get('gateway_url', '')).rstrip('/') != gateway:
            continue
        if urllib.parse.urlsplit(pay_url or '').netloc != urllib.parse.urlsplit(gateway).netloc:
            continue
        if trade_id and order_id and cfg.get('auth_token'):
            yield payment_id, trade_id, order_id, cfg['auth_token']


def enqueue(state, rows, gateway, now):
    for payment_id, trade_id, order_id, _ in eligible(rows, gateway):
        # Keys contain identities only. API credentials are fetched again before sending.
        state.execute("INSERT INTO cancellations VALUES(?,?,?,?,'queued',0,?,?) ON CONFLICT(gateway,payment_id,trade_id,order_id) DO UPDATE SET outcome='queued',next_at=excluded.next_at,updated_at=excluded.updated_at WHERE cancellations.outcome='merchant_changed'",
                      (gateway, payment_id, trade_id, order_id, now, now))


def discover(source, state, gateway, now):
    progress = state.execute('SELECT canceled_at,order_id,audit_payment_id,max_payment_id,next_audit FROM discovery WHERE gateway=?', (gateway,)).fetchone()
    stamp, order_id, audit_id, maximum, next_audit = progress
    current_max = source_query(source, 'SELECT COALESCE(MAX(id),0) FROM payments').fetchone()[0]
    if current_max < maximum:
        # A restored or truncated source can move behind persisted IDs. Retained terminal
        # journal entries deduplicate rediscovery; every queued send is revalidated.
        stamp, order_id, audit_id, next_audit = '', 0, 0, 0
    if stamp:
        # Separate equality and greater-than seeks: SQLite can otherwise scan every
        # row sharing a cancellation timestamp even with a composite index.
        orders = source_query(source, 'SELECT canceled_at,id FROM orders WHERE canceled_at=? AND id>? ORDER BY id LIMIT ?',
                              (stamp, order_id, RECENT_ORDER_BATCH)).fetchall()
        if len(orders) < RECENT_ORDER_BATCH:
            orders += source_query(source, 'SELECT canceled_at,id FROM orders WHERE canceled_at>? ORDER BY canceled_at,id LIMIT ?',
                                   (stamp, RECENT_ORDER_BATCH-len(orders))).fetchall()
    else:
        orders = source_query(source, 'SELECT canceled_at,id FROM orders WHERE canceled_at IS NOT NULL ORDER BY canceled_at,id LIMIT ?',
                              (RECENT_ORDER_BATCH,)).fetchall()

    with state:
        for at, oid in orders:
            state.execute('INSERT OR IGNORE INTO discovery_orders VALUES(?,?,0)', (gateway, oid))
        if orders:
            stamp, order_id = str(orders[-1][0]), orders[-1][1]
        state.execute('UPDATE discovery SET canceled_at=?,order_id=?,max_payment_id=? WHERE gateway=?',
                      (stamp, order_id, current_max, gateway))
        if current_max < maximum:
            state.execute('UPDATE discovery SET audit_payment_id=0,next_audit=0 WHERE gateway=?', (gateway,))
    # Recently canceled orders expand into payment identities in bounded, resumable pages.
    tasks = state.execute('SELECT order_id,after_payment_id FROM discovery_orders WHERE gateway=? ORDER BY order_id LIMIT ?',
                          (gateway, DISCOVERY_ORDER_BATCH)).fetchall()
    for oid, after in tasks:
        ids = [r[0] for r in source_query(source, 'SELECT id FROM payments WHERE order_id=? AND id>? ORDER BY id LIMIT ?',
                                        (oid, after, ORDER_PAYMENT_BATCH)).fetchall()]
        rows = payment_rows(source, 'p.id IN ('+','.join('?' for _ in ids)+')', tuple(ids)) if ids else []
        with state:
            enqueue(state, rows, gateway, now)
            if len(ids) < ORDER_PAYMENT_BATCH:
                state.execute('DELETE FROM discovery_orders WHERE gateway=? AND order_id=?', (gateway, oid))
            else:
                state.execute('UPDATE discovery_orders SET after_payment_id=? WHERE gateway=? AND order_id=?', (ids[-1], gateway, oid))
    # A slower cyclic sweep catches backdated cancellations, late payment metadata and
    # restored snapshots even when neither cancellation timestamps nor IDs advance.
    if now >= next_audit:
        ids = [r[0] for r in source_query(source, 'SELECT id FROM payments WHERE id>? ORDER BY id LIMIT ?',
                                        (audit_id, AUDIT_PAYMENT_BATCH)).fetchall()]
        rows = payment_rows(source, 'p.id IN ('+','.join('?' for _ in ids)+')', tuple(ids)) if ids else []
        with state:
            enqueue(state, rows, gateway, now)
            audit_id = ids[-1] if len(ids) == AUDIT_PAYMENT_BATCH else 0
            state.execute('UPDATE discovery SET audit_payment_id=?,next_audit=? WHERE gateway=?',
                          (audit_id, now+AUDIT_INTERVAL, gateway))




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
    db.executescript("""
        CREATE INDEX IF NOT EXISTS cancellations_due ON cancellations(gateway,outcome,next_at,updated_at);
        CREATE TABLE IF NOT EXISTS discovery(gateway TEXT PRIMARY KEY,canceled_at TEXT NOT NULL DEFAULT '',
            order_id INTEGER NOT NULL DEFAULT 0,audit_payment_id INTEGER NOT NULL DEFAULT 0,
            max_payment_id INTEGER NOT NULL DEFAULT 0,next_audit REAL NOT NULL DEFAULT 0);
        CREATE TABLE IF NOT EXISTS discovery_orders(gateway TEXT NOT NULL,order_id INTEGER NOT NULL,
            after_payment_id INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(gateway,order_id));
    """)
    return db


def sync_once(database, state, gateway, sender=cancel, now=None):
    now = time.time() if now is None else now
    with state:
        state.execute('INSERT OR IGNORE INTO discovery(gateway) VALUES(?)', (gateway,))
    source = source_connect(database)
    try:
        discover(source, state, gateway, now)
        due = state.execute("SELECT payment_id,trade_id,order_id,attempts FROM cancellations WHERE gateway=? AND outcome IN ('queued','retry') AND next_at<=? ORDER BY next_at,updated_at,payment_id LIMIT ?",
                            (gateway, now, SEND_BATCH)).fetchall()
        ids = [r[0] for r in due]
        rows = payment_rows(source, 'p.id IN ('+','.join('?' for _ in ids)+')', tuple(ids)) if ids else []
        active = {(gateway, pid, tid, oid): token for pid, tid, oid, token in eligible(rows, gateway)}
    finally:
        source.close()
    processed = 0
    deadline = time.monotonic() + 5
    for payment_id, trade_id, order_id, previous_attempts in due:
        key = (gateway, payment_id, trade_id, order_id)
        if key not in active:
            with state:
                state.execute("UPDATE cancellations SET outcome='merchant_changed',updated_at=? WHERE gateway=? AND payment_id=? AND trade_id=? AND order_id=?", (now,) + key)
            continue
        attempts = previous_attempts + 1
        try:
            outcome = sender(gateway, trade_id, order_id, active[key])
            if outcome not in ('closed', 'paid_conflict', 'missing_gateway'):
                raise ValueError('invalid cancellation outcome')
            if outcome == 'missing_gateway' and attempts < 3:
                outcome = 'retry'
        except Exception as error:
            outcome = 'retry'
            logging.warning('payment_id=%s cancellation retry error_type=%s', payment_id, type(error).__name__)
        next_at = now + min(300, 5 * 2 ** min(attempts - 1, 6))
        with state:
            state.execute('UPDATE cancellations SET outcome=?,attempts=?,next_at=?,updated_at=? WHERE gateway=? AND payment_id=? AND trade_id=? AND order_id=?',
                          (outcome, attempts, next_at, now) + key)
        if outcome != 'retry':
            logging.log(logging.ERROR if outcome == 'paid_conflict' else logging.INFO,
                        'payment_id=%s outcome=%s', payment_id, outcome)
        processed += 1
        if time.monotonic() >= deadline:
            break
    return processed


def report_health(database, state, gateway, healthy):
    source = source_connect(database)
    try:
        configs = source.execute("SELECT config_json FROM payment_channels WHERE provider_type='bepusdt'").fetchall()
        tokens = set()
        for (raw,) in configs:
            cfg = raw if isinstance(raw, dict) else json.loads(raw or '{}')
            if str(cfg.get('gateway_url', '')).rstrip('/') == gateway and cfg.get('auth_token'):
                tokens.add(cfg['auth_token'])
        if len(tokens) != 1:
            raise ValueError('one consistent gateway token is required for health reports')
    finally:
        source.close()
    counts = dict(state.execute("SELECT outcome,count(*) FROM cancellations WHERE gateway=? AND outcome IN ('queued','retry','paid_conflict') GROUP BY outcome", (gateway,)))
    conflicts = counts.get('paid_conflict', 0)
    failures = counts.get('retry', 0)
    pending = counts.get('queued', 0) + failures
    pending += state.execute('SELECT count(*) FROM discovery_orders WHERE gateway=?', (gateway,)).fetchone()[0]
    body = {'source': 'postgresql' if str(database).startswith(('postgresql://', 'postgres://')) else 'sqlite',
            'timestamp': str(int(time.time())), 'healthy': 'true' if healthy else 'false',
            'pending': str(pending), 'failures': str(failures), 'conflicts': str(conflicts)}
    token = next(iter(tokens))
    body['signature'] = hashlib.md5(('&'.join(k+'='+body[k] for k in sorted(body)) + token).encode()).hexdigest()
    req = urllib.request.Request(gateway+'/api/v1/order/cancel-sync-status', data=json.dumps(body).encode(),
                                 headers={'Content-Type': 'application/json', 'User-Agent': 'Mozilla/5.0 LitePay-CancelSync/1.0'})
    with urllib.request.build_opener(NoRedirect).open(req, timeout=3) as response:
        if json.loads(response.read(4096)).get('status_code') != 200:
            raise ValueError('health report rejected')


def systemd_notify(message):
    address = os.environ.get('NOTIFY_SOCKET')
    if not address:
        return
    if address.startswith('@'):
        address = '\0' + address[1:]
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as connection:
            connection.sendto(message.encode(), address)
    except OSError:
        pass


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sources = parser.add_mutually_exclusive_group(required=True)
    sources.add_argument('--database', help='SQLite database path')
    sources.add_argument('--database-url-env', help='Environment variable containing a PostgreSQL DSN')
    parser.add_argument('--state', required=True)
    parser.add_argument('--gateway', required=True)
    parser.add_argument('--once', action='store_true')
    parser.add_argument('--report-health', action='store_true')
    parser.add_argument('--rescan-history', action='store_true', help='Restart bounded historical discovery without clearing deduplication or retries')
    args = parser.parse_args()
    database = args.database or os.environ.get(args.database_url_env, '')
    if not database:
        parser.error('database source is missing')
    if args.database and str(database).startswith(('postgresql://', 'postgres://')):
        parser.error('use --database-url-env for PostgreSQL credentials')
    gateway = args.gateway.rstrip('/')
    url = urllib.parse.urlsplit(gateway)
    if url.scheme != 'https' or not url.hostname or url.username or url.password or url.port not in (None, 443) or url.path or url.query or url.fragment:
        parser.error('--gateway must be an HTTPS origin')
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    state = journal(args.state)
    state.execute('CREATE TABLE IF NOT EXISTS health(id INTEGER PRIMARY KEY,last_attempt REAL,last_success REAL)')
    if args.rescan_history:
        with state:
            state.execute('INSERT OR IGNORE INTO discovery(gateway) VALUES(?)', (gateway,))
            state.execute("UPDATE discovery SET canceled_at='',order_id=0,audit_payment_id=0,next_audit=0 WHERE gateway=?", (gateway,))
    next_report = 0
    systemd_notify('READY=1')
    while True:
        healthy = True
        try:
            sync_once(database, state, gateway)
        except Exception as error:
            healthy = False
            logging.error('sync failed error_type=%s', type(error).__name__)
        now = time.time()
        state.execute('INSERT INTO health VALUES(1,?,?) ON CONFLICT(id) DO UPDATE SET last_attempt=excluded.last_attempt,last_success=CASE WHEN excluded.last_success>0 THEN excluded.last_success ELSE last_success END', (now, now if healthy else 0))
        state.commit()
        if args.report_health and now >= next_report:
            try:
                report_health(database, state, gateway, healthy)
                next_report = now + 15
            except Exception as error:
                logging.error('health report failed error_type=%s', type(error).__name__)
        if healthy:
            systemd_notify('WATCHDOG=1')
        if args.once:
            raise SystemExit(0 if healthy else 1)
        time.sleep(5)


if __name__ == '__main__':
    main()

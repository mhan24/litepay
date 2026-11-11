#!/usr/bin/env python3
"""Reconcile Dujiao cancellations and closed-order successful payments with one gateway.

The commerce database is read-only. Only the separate retry journal is written.
Run under the commerce DB owner; credentials remain in its existing channel config.
"""
import argparse
import datetime
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



# These codes can be emitted before the closed-order check (superseded/underpaid).
# The order itself must still be canceled and unpaid; normal fulfilled payments never qualify.
ANOMALY_CODES = ('closed_order_payment_succeeded', 'superseded_payment_succeeded',
                 'duplicate_payment_succeeded', 'underpaid_payment_succeeded')
ANOMALY_BATCH = 200


def anomaly_rows(source, predicate, params):
    return source_query(source, """
        SELECT p.id,p.provider_ref,p.gateway_order_no,p.pay_url,c.config_json,p.exception_code,p.paid_at
        FROM payments p JOIN orders o ON o.id=p.order_id JOIN payment_channels c ON c.id=p.channel_id
        WHERE o.status='canceled' AND o.paid_at IS NULL AND o.canceled_at IS NOT NULL AND o.deleted_at IS NULL
          AND p.provider_type='bepusdt' AND p.status='success' AND p.paid_at IS NOT NULL AND p.deleted_at IS NULL
          AND p.exception_code IN (?,?,?,?) AND c.provider_type='bepusdt' AND """+predicate+' ORDER BY p.id',
        ANOMALY_CODES+tuple(params)).fetchall()


def eligible_anomalies(rows, gateway):
    for row in rows:
        for identity in eligible([row[:5]], gateway):
            yield identity + (row[5], str(row[6]))


def enqueue_anomalies(state, rows, gateway, now):
    for pid,tid,oid,_,code,paid_at in eligible_anomalies(rows,gateway):
        state.execute("""INSERT INTO payment_anomalies(gateway,payment_id,trade_id,order_id,exception_code,paid_at,outcome,next_at,updated_at)
            VALUES(?,?,?,?,?,?,'queued',?,?) ON CONFLICT(gateway,payment_id,trade_id,order_id) DO UPDATE SET
            exception_code=excluded.exception_code,paid_at=excluded.paid_at,outcome='queued',next_at=excluded.next_at,updated_at=excluded.updated_at
            WHERE payment_anomalies.outcome='merchant_changed' OR payment_anomalies.exception_code<>excluded.exception_code OR payment_anomalies.paid_at<>excluded.paid_at
               OR (payment_anomalies.outcome IN ('reviewed_conflict','paid_conflict','source_review') AND payment_anomalies.checked_generation<>COALESCE((SELECT generation FROM gateway_epochs WHERE gateway=excluded.gateway),''))""",
            (gateway,pid,tid,oid,code,paid_at,now,now))


def discover_anomalies(source,state,gateway,now):
    state.execute('INSERT OR IGNORE INTO anomaly_discovery(gateway) VALUES(?)',(gateway,))
    stamp,pid,maximum=state.execute('SELECT updated_at,payment_id,max_payment_id FROM anomaly_discovery WHERE gateway=?',(gateway,)).fetchone()
    current_max=source_query(source,'SELECT COALESCE(MAX(id),0) FROM payments').fetchone()[0]
    if current_max<maximum:
        stamp,pid='',0
    # UpdatedAt has a source index. Seek equality separately to bound large timestamp groups.
    rows=source_query(source,'SELECT updated_at,id FROM payments WHERE updated_at=? AND id>? ORDER BY id LIMIT ?',
                      (stamp,pid,ANOMALY_BATCH)).fetchall() if stamp else []
    if len(rows)<ANOMALY_BATCH:
        rows+=source_query(source,'SELECT updated_at,id FROM payments WHERE updated_at>? ORDER BY updated_at,id LIMIT ?',
                           (stamp,ANOMALY_BATCH-len(rows))).fetchall() if stamp else source_query(source,
                           'SELECT updated_at,id FROM payments WHERE updated_at IS NOT NULL ORDER BY updated_at,id LIMIT ?', (ANOMALY_BATCH,)).fetchall()
    next_stamp,next_id=(str(rows[-1][0]),rows[-1][1]) if rows else (stamp,pid)
    unread=source_query(source,'SELECT updated_at FROM payments WHERE updated_at=? AND id>? ORDER BY id LIMIT 1',
                        (next_stamp,next_id)).fetchone() if next_stamp else None
    if unread is None:
        unread=source_query(source,'SELECT updated_at FROM payments WHERE updated_at>? ORDER BY updated_at,id LIMIT 1',(next_stamp,)).fetchone() if next_stamp else source_query(source,'SELECT updated_at FROM payments WHERE updated_at IS NOT NULL ORDER BY updated_at,id LIMIT 1').fetchone()
    oldest=0
    if unread:
        try:
            at=datetime.datetime.fromisoformat(str(unread[0]).replace('Z','+00:00'))
            oldest=at.replace(tzinfo=at.tzinfo or datetime.timezone.utc).timestamp()
        except (ValueError,OverflowError):
            pass
    ids=[r[1] for r in rows]
    candidates=anomaly_rows(source,'p.id IN ('+','.join('?' for _ in ids)+')',ids) if ids else []
    with state:
        enqueue_anomalies(state,candidates,gateway,now)
        state.execute('UPDATE anomaly_discovery SET updated_at=?,payment_id=?,max_payment_id=?,pending=?,oldest_unread_at=? WHERE gateway=?',
                      (next_stamp,next_id,current_max,int(unread is not None),oldest,gateway))


def inspect_anomaly(gateway,trade_id,order_id,token):
    # CancelReference establishes a merchant review/hold when money is already PAID,
    # or closes the old instruction while the scanner keeps collecting real evidence.
    result=cancel(gateway,trade_id,order_id,token)
    if result=='paid_conflict':
        return 'paid_conflict'
    status=signed_post(gateway,'/api/v1/order/cancellation-review',
                       {'timestamp':str(int(time.time())),'trade_id':trade_id,'order_id':order_id},token).get('data') or {}
    if status.get('trade_id')!=trade_id or status.get('order_id')!=order_id or status.get('cancellation_review_status') not in ('none','pending','recheck_required','refund_recorded'):
        raise ValueError('gateway review response is incomplete')
    return 'paid_conflict' if status['cancellation_review_status']!='none' else 'awaiting_funds'


def due_anomalies(source,state,gateway,now):
    due=state.execute("SELECT payment_id,trade_id,order_id,attempts FROM payment_anomalies WHERE gateway=? AND outcome IN ('queued','retry','awaiting_funds') AND next_at<=? ORDER BY next_at,updated_at,payment_id LIMIT ?",(gateway,now,SEND_BATCH)).fetchall()
    ids=[r[0] for r in due]
    rows=anomaly_rows(source,'p.id IN ('+','.join('?' for _ in ids)+')',ids) if ids else []
    active={(gateway,pid,tid,oid):token for pid,tid,oid,token,_,_ in eligible_anomalies(rows,gateway)}
    return due,active


def send_anomalies(state,gateway,now,due,active,sender):
    processed=0
    deadline=time.monotonic()+5
    for pid,tid,oid,previous in due:
        key=(gateway,pid,tid,oid)
        if key not in active:
            # A previously observed successful payment is not silently cleared because
            # a source record changed or disappeared. Keep a durable human-visible conflict.
            with state:
                state.execute("UPDATE payment_anomalies SET outcome='source_review',updated_at=?,checked_generation=COALESCE((SELECT generation FROM gateway_epochs WHERE gateway=?),'') WHERE gateway=? AND payment_id=? AND trade_id=? AND order_id=?",(now,gateway)+key)
            continue
        attempts=previous+1
        try:
            outcome=sender(gateway,tid,oid,active[key])
            if outcome not in ('paid_conflict','awaiting_funds'):
                raise ValueError('invalid anomaly outcome')
        except Exception as error:
            outcome='retry'
            logging.warning('payment_id=%s anomaly retry error_type=%s',pid,type(error).__name__)
        with state:
            state.execute("UPDATE payment_anomalies SET outcome=?,attempts=?,next_at=?,updated_at=?,checked_generation=COALESCE((SELECT generation FROM gateway_epochs WHERE gateway=?),'') WHERE gateway=? AND payment_id=? AND trade_id=? AND order_id=?",
                          (outcome,attempts,now+min(300,5*2**min(attempts-1,6)),now,gateway)+key)
        logging.warning('payment_id=%s anomaly outcome=%s',pid,outcome)
        processed+=1
        if time.monotonic()>=deadline:
            break
    return processed

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
        state.execute("INSERT INTO cancellations VALUES(?,?,?,?,'queued',0,?,?) ON CONFLICT(gateway,payment_id,trade_id,order_id) DO UPDATE SET outcome='queued',next_at=excluded.next_at,updated_at=excluded.updated_at WHERE cancellations.outcome='merchant_changed' OR (cancellations.outcome IN ('closed','reviewed_missing','reviewed_conflict','missing_gateway','paid_conflict') AND EXISTS(SELECT 1 FROM gateway_epochs e WHERE e.gateway=cancellations.gateway AND NOT EXISTS(SELECT 1 FROM epoch_receipts r WHERE r.gateway=cancellations.gateway AND r.payment_id=cancellations.payment_id AND r.trade_id=cancellations.trade_id AND r.order_id=cancellations.order_id AND r.generation=e.generation)))",
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

    next_stamp, next_id = (str(orders[-1][0]), orders[-1][1]) if orders else (stamp, order_id)
    # One indexed lookahead detects unread source work even when this page contains
    # no eligible payments. Do not COUNT or rescan the complete cancellation history.
    unread = source_query(source, 'SELECT canceled_at FROM orders WHERE canceled_at=? AND id>? ORDER BY id LIMIT 1',
                          (next_stamp, next_id)).fetchone() if next_stamp else None
    if unread is None:
        unread = source_query(source, 'SELECT canceled_at FROM orders WHERE canceled_at>? ORDER BY canceled_at,id LIMIT 1',
                              (next_stamp,)).fetchone() if next_stamp else source_query(source, 'SELECT canceled_at FROM orders WHERE canceled_at IS NOT NULL ORDER BY canceled_at,id LIMIT 1').fetchone()
    oldest_unread = 0
    if unread:
        try:
            at = datetime.datetime.fromisoformat(str(unread[0]).replace('Z', '+00:00'))
            oldest_unread = at.replace(tzinfo=at.tzinfo or datetime.timezone.utc).timestamp()
        except (ValueError, OverflowError):
            pass

    with state:
        for at, oid in orders:
            state.execute('INSERT OR IGNORE INTO discovery_orders VALUES(?,?,0)', (gateway, oid))
        if orders:
            stamp, order_id = str(orders[-1][0]), orders[-1][1]
        state.execute('UPDATE discovery SET canceled_at=?,order_id=?,max_payment_id=? WHERE gateway=?',
                      (stamp, order_id, current_max, gateway))
        state.execute('INSERT INTO discovery_health VALUES(?,?,?,?) ON CONFLICT(gateway) DO UPDATE SET pending=excluded.pending,oldest_unread_at=excluded.oldest_unread_at,checked_at=excluded.checked_at',
                      (gateway, int(unread is not None), oldest_unread, now))
        if current_max < maximum:
            state.execute('UPDATE discovery SET audit_payment_id=0,next_audit=0 WHERE gateway=?', (gateway,))
    # Recently canceled orders expand into payment identities in bounded, resumable pages.
    tasks = state.execute('SELECT order_id,after_payment_id FROM discovery_orders WHERE gateway=? ORDER BY order_id LIMIT ?',
                          (gateway, DISCOVERY_ORDER_BATCH)).fetchall()
    for oid, after in tasks:
        ids = [r[0] for r in source_query(source, 'SELECT id FROM payments WHERE order_id=? AND id>? ORDER BY id LIMIT ?',
                                        (oid, after, ORDER_PAYMENT_BATCH)).fetchall()]
        rows = payment_rows(source, 'p.id IN ('+','.join('?' for _ in ids)+')', tuple(ids)) if ids else []
        anomalies = anomaly_rows(source, 'p.id IN ('+','.join('?' for _ in ids)+')', tuple(ids)) if ids else []
        with state:
            enqueue(state, rows, gateway, now)
            enqueue_anomalies(state, anomalies, gateway, now)
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
        anomalies = anomaly_rows(source, 'p.id IN ('+','.join('?' for _ in ids)+')', tuple(ids)) if ids else []
        with state:
            enqueue(state, rows, gateway, now)
            enqueue_anomalies(state, anomalies, gateway, now)
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
        CREATE TABLE IF NOT EXISTS discovery_health(gateway TEXT PRIMARY KEY,pending INTEGER NOT NULL,
            oldest_unread_at REAL NOT NULL,checked_at REAL NOT NULL);
    """)
    db.executescript("""
        CREATE TABLE IF NOT EXISTS gateway_epochs(gateway TEXT PRIMARY KEY,generation TEXT NOT NULL,audit_id INTEGER NOT NULL DEFAULT 0,scanned INTEGER NOT NULL DEFAULT 0);
        CREATE TABLE IF NOT EXISTS epoch_receipts(gateway TEXT,payment_id INTEGER,trade_id TEXT,order_id TEXT,generation TEXT,outcome TEXT,updated_at REAL,
            PRIMARY KEY(gateway,payment_id,trade_id,order_id,generation));
        CREATE TABLE IF NOT EXISTS conflict_reviews(id INTEGER PRIMARY KEY,gateway TEXT,payment_id INTEGER,trade_id TEXT,order_id TEXT,generation TEXT,operator TEXT,reason TEXT,created_at REAL);
        CREATE TABLE IF NOT EXISTS missing_reviews(id INTEGER PRIMARY KEY,gateway TEXT,payment_id INTEGER,trade_id TEXT,order_id TEXT,generation TEXT,operator TEXT,reason TEXT,created_at REAL);
    """)
    db.executescript("""
        CREATE TABLE IF NOT EXISTS payment_anomalies(gateway TEXT NOT NULL,payment_id INTEGER NOT NULL,trade_id TEXT NOT NULL,order_id TEXT NOT NULL,
            exception_code TEXT NOT NULL,paid_at TEXT NOT NULL,outcome TEXT NOT NULL,attempts INTEGER NOT NULL DEFAULT 0,next_at REAL NOT NULL,updated_at REAL NOT NULL,
            review_generation TEXT NOT NULL DEFAULT '',checked_generation TEXT NOT NULL DEFAULT '',PRIMARY KEY(gateway,payment_id,trade_id,order_id));
        CREATE INDEX IF NOT EXISTS payment_anomalies_due ON payment_anomalies(gateway,outcome,next_at,updated_at);
        CREATE TABLE IF NOT EXISTS anomaly_discovery(gateway TEXT PRIMARY KEY,updated_at TEXT NOT NULL DEFAULT '',payment_id INTEGER NOT NULL DEFAULT 0,
            max_payment_id INTEGER NOT NULL DEFAULT 0,pending INTEGER NOT NULL DEFAULT 1,oldest_unread_at REAL NOT NULL DEFAULT 0);
    """)
    return db


def queue_status(state, gateway):
    counts = dict(state.execute("SELECT outcome,count(*) FROM cancellations WHERE gateway=? AND outcome IN ('queued','retry','missing_gateway','paid_conflict') GROUP BY outcome", (gateway,)))
    missing = counts.get('missing_gateway', 0)
    conflicts = state.execute("SELECT count(*) FROM (SELECT payment_id,trade_id,order_id FROM cancellations WHERE gateway=? AND outcome='paid_conflict' UNION SELECT payment_id,trade_id,order_id FROM payment_anomalies WHERE gateway=? AND outcome IN ('paid_conflict','source_review'))",(gateway,gateway)).fetchone()[0]
    failures = counts.get('retry', 0) + missing
    pending = counts.get('queued', 0) + counts.get('retry', 0)
    anomalies = dict(state.execute('SELECT outcome,count(*) FROM payment_anomalies WHERE gateway=? GROUP BY outcome',(gateway,)))
    failures += anomalies.get('retry',0)
    pending += sum(anomalies.get(k,0) for k in ('queued','retry','awaiting_funds'))
    pending += state.execute('SELECT count(*) FROM discovery_orders WHERE gateway=?', (gateway,)).fetchone()[0]
    epoch = state.execute('SELECT generation,scanned FROM gateway_epochs WHERE gateway=?', (gateway,)).fetchone()
    discovery = state.execute('SELECT pending,oldest_unread_at FROM discovery_health WHERE gateway=?', (gateway,)).fetchone()
    discovery_pending = discovery[0] if discovery else 1
    lag = max(0, int(time.time()-discovery[1])) if discovery and discovery[0] and discovery[1] else 0
    abnormal_discovery=state.execute('SELECT pending,oldest_unread_at FROM anomaly_discovery WHERE gateway=?',(gateway,)).fetchone()
    discovery_pending=max(discovery_pending,abnormal_discovery[0] if abnormal_discovery else 1)
    if abnormal_discovery and abnormal_discovery[0] and abnormal_discovery[1]:
        lag=max(lag,max(0,int(time.time()-abnormal_discovery[1])))
    return {'pending': pending, 'failures': failures, 'missing': missing, 'conflicts': conflicts,
            'discovery_pending': discovery_pending, 'discovery_lag_seconds': lag,
            'generation': epoch[0] if epoch else '', 'replay_complete': bool(epoch and epoch[1] and pending == 0 and failures == 0 and conflicts==0 and discovery_pending==0)}


def replay_generation(source, state, gateway, now):
    epoch = state.execute('SELECT generation,audit_id,scanned FROM gateway_epochs WHERE gateway=?', (gateway,)).fetchone()
    if not epoch or epoch[2]:
        return
    ids = [r[0] for r in source_query(source, 'SELECT id FROM payments WHERE id>? ORDER BY id LIMIT ?', (epoch[1], AUDIT_PAYMENT_BATCH)).fetchall()]
    rows = payment_rows(source, 'p.id IN ('+','.join('?' for _ in ids)+')', tuple(ids)) if ids else []
    anomalies=anomaly_rows(source,'p.id IN ('+','.join('?' for _ in ids)+')',tuple(ids)) if ids else []
    with state:
        enqueue(state, rows, gateway, now)
        enqueue_anomalies(state,anomalies,gateway,now)
        state.execute('UPDATE gateway_epochs SET audit_id=?,scanned=? WHERE gateway=? AND generation=?',
                      (ids[-1] if ids else epoch[1], int(len(ids)<AUDIT_PAYMENT_BATCH), gateway, epoch[0]))


def sync_once(database, state, gateway, sender=cancel, now=None, anomaly_sender=inspect_anomaly):
    now = time.time() if now is None else now
    with state:
        state.execute('INSERT OR IGNORE INTO discovery(gateway) VALUES(?)', (gateway,))
    source = source_connect(database)
    try:
        replay_generation(source, state, gateway, now)
        discover(source, state, gateway, now)
        discover_anomalies(source,state,gateway,now)
        anomaly_due,anomaly_active=due_anomalies(source,state,gateway,now)
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
        if outcome in ('closed', 'missing_gateway', 'paid_conflict'):
            with state:
                state.execute("INSERT OR REPLACE INTO epoch_receipts SELECT ?,?,?,?,generation,?,? FROM gateway_epochs WHERE gateway=?", key+(outcome,now,gateway))
        if outcome != 'retry':
            logging.log(logging.ERROR if outcome == 'paid_conflict' else logging.INFO,
                        'payment_id=%s outcome=%s', payment_id, outcome)
        processed += 1
        if time.monotonic() >= deadline:
            break
    return processed+send_anomalies(state,gateway,now,anomaly_due,anomaly_active,anomaly_sender)


def gateway_token(database, gateway):
    source = source_connect(database)
    try:
        configs = source.execute("SELECT config_json FROM payment_channels WHERE provider_type='bepusdt'").fetchall()
        tokens = set()
        for (raw,) in configs:
            cfg = raw if isinstance(raw, dict) else json.loads(raw or '{}')
            if str(cfg.get('gateway_url', '')).rstrip('/') == gateway and cfg.get('auth_token'):
                tokens.add(cfg['auth_token'])
        if len(tokens) != 1:
            raise ValueError('one consistent gateway token is required for signed reports')
        return next(iter(tokens))
    finally:
        source.close()


def signed_post(gateway, path, body, token):
    body = dict(body)
    body['signature'] = hashlib.md5(('&'.join(k+'='+body[k] for k in sorted(body)) + token).encode()).hexdigest()
    req = urllib.request.Request(gateway+path, data=json.dumps(body).encode(),
                                 headers={'Content-Type': 'application/json', 'User-Agent': 'Mozilla/5.0 LitePay-CancelSync/1.0'})
    with urllib.request.build_opener(NoRedirect).open(req, timeout=3) as response:
        result = json.loads(response.read(4096))
        if result.get('status_code') != 200:
            raise ValueError('signed request rejected')
        return result


def check_generation(database, state, gateway):
    result = signed_post(gateway, '/api/v1/order/cancel-sync-generation', {'timestamp': str(int(time.time()))}, gateway_token(database, gateway))
    data = result.get('data') or {}
    generation = data.get('generation', '')
    if len(generation) != 32 or any(c not in '0123456789abcdef' for c in generation):
        raise ValueError('invalid recovery generation')
    with state:
        state.execute("INSERT INTO gateway_epochs(gateway,generation) VALUES(?,?) ON CONFLICT(gateway) DO UPDATE SET generation=excluded.generation,audit_id=0,scanned=0 WHERE gateway_epochs.generation<>excluded.generation", (gateway, generation))


def report_health(database, state, gateway, healthy):
    status = queue_status(state, gateway)
    body = {'source': 'postgresql' if str(database).startswith(('postgresql://', 'postgres://')) else 'sqlite',
            'timestamp': str(int(time.time())), 'healthy': 'true' if healthy and status['failures']==0 and status['conflicts']==0 and status['pending']==0 and status['replay_complete'] else 'false',
            'pending': str(status['pending']), 'failures': str(status['failures']), 'conflicts': str(status['conflicts']),
            'discovery_pending': str(status['discovery_pending']), 'discovery_lag_seconds': str(status['discovery_lag_seconds']),
            'missing': str(status['missing']), 'generation': status['generation'], 'replay_complete': 'true' if status['replay_complete'] else 'false'}
    return signed_post(gateway, '/api/v1/order/cancel-sync-status', body, gateway_token(database, gateway))


def review_missing(database, state, gateway, trade_id, reason):
    import getpass
    if not reason.strip() or len(reason)>300:
        raise ValueError('a review reason (1-300 characters) is required')
    rows = state.execute("SELECT payment_id,trade_id,order_id FROM cancellations WHERE gateway=? AND trade_id=? AND outcome='missing_gateway'", (gateway,trade_id)).fetchall()
    if len(rows)!=1:
        raise ValueError('exactly one unresolved missing task must match')
    pid,tid,oid=rows[0]
    source=source_connect(database)
    try:
        valid=list(eligible(payment_rows(source,'p.id=?',(pid,)),gateway))
    finally:
        source.close()
    if len(valid)!=1 or valid[0][:3]!=(pid,tid,oid):
        raise ValueError('merchant cancellation no longer matches; recheck merchant state')
    result=cancel(gateway,tid,oid,valid[0][3])
    if result not in ('missing_gateway','closed'):
        raise ValueError('gateway result changed; recheck payment before review')
    outcome='reviewed_missing' if result=='missing_gateway' else 'closed'
    epoch=state.execute('SELECT generation FROM gateway_epochs WHERE gateway=?',(gateway,)).fetchone()
    generation=epoch[0] if epoch else ''
    with state:
        state.execute('INSERT INTO missing_reviews(gateway,payment_id,trade_id,order_id,generation,operator,reason,created_at) VALUES(?,?,?,?,?,?,?,?)',(gateway,pid,tid,oid,generation,getpass.getuser(),reason.strip(),time.time()))
        state.execute("UPDATE cancellations SET outcome=?,updated_at=? WHERE gateway=? AND payment_id=? AND trade_id=? AND order_id=?",(outcome,time.time(),gateway,pid,tid,oid))
        if generation:
            state.execute("INSERT OR REPLACE INTO epoch_receipts VALUES(?,?,?,?,?,?,?)",(gateway,pid,tid,oid,generation,outcome,time.time()))


def review_conflict(database, state, gateway, trade_id, reason):
    import getpass
    if not reason.strip() or len(reason)>300:
        raise ValueError('a review reason (1-300 characters) is required')
    check_generation(database,state,gateway)
    rows=state.execute("SELECT payment_id,trade_id,order_id FROM cancellations WHERE gateway=? AND trade_id=? AND outcome='paid_conflict' UNION SELECT payment_id,trade_id,order_id FROM payment_anomalies WHERE gateway=? AND trade_id=? AND outcome<>'reviewed_conflict'",(gateway,trade_id,gateway,trade_id)).fetchall()
    if len(rows)!=1:
        raise ValueError('exactly one unresolved paid conflict must match')
    pid,tid,oid=rows[0]
    source=source_connect(database)
    try:
        valid=list(eligible(payment_rows(source,'p.id=?',(pid,)),gateway))+[row[:4] for row in eligible_anomalies(anomaly_rows(source,'p.id=?',(pid,)),gateway)]
    finally:
        source.close()
    if len(valid)!=1 or valid[0][:3]!=(pid,tid,oid):
        raise ValueError('merchant cancellation identity changed')
    anomaly=state.execute('SELECT 1 FROM payment_anomalies WHERE gateway=? AND payment_id=? AND trade_id=? AND order_id=?',(gateway,pid,tid,oid)).fetchone()
    outcome=cancel(gateway,tid,oid,valid[0][3])
    if outcome!='paid_conflict' and not (anomaly and outcome=='closed'):
        raise ValueError('gateway payment state changed; recheck funds')
    result=signed_post(gateway,'/api/v1/order/cancellation-review',{'timestamp':str(int(time.time())),'trade_id':tid,'order_id':oid},valid[0][3])
    data=result.get('data') or {}
    epoch=state.execute('SELECT generation FROM gateway_epochs WHERE gateway=?',(gateway,)).fetchone()
    if not epoch or data.get('generation')!=epoch[0] or data.get('trade_id')!=tid or data.get('order_id')!=oid or data.get('refund_recorded') is not True:
        raise ValueError('gateway has no current audited refund record')
    generation=epoch[0]
    with state:
        if state.execute('SELECT generation FROM gateway_epochs WHERE gateway=?',(gateway,)).fetchone()[0]!=generation:
            raise ValueError('gateway generation changed during review')
        state.execute('INSERT INTO conflict_reviews(gateway,payment_id,trade_id,order_id,generation,operator,reason,created_at) VALUES(?,?,?,?,?,?,?,?)',(gateway,pid,tid,oid,generation,getpass.getuser(),reason.strip(),time.time()))
        state.execute("UPDATE cancellations SET outcome='reviewed_conflict',updated_at=? WHERE gateway=? AND payment_id=? AND trade_id=? AND order_id=? AND outcome='paid_conflict'",(time.time(),gateway,pid,tid,oid))
        state.execute("UPDATE payment_anomalies SET outcome='reviewed_conflict',review_generation=?,checked_generation=?,updated_at=? WHERE gateway=? AND payment_id=? AND trade_id=? AND order_id=?",(generation,generation,time.time(),gateway,pid,tid,oid))
        state.execute("INSERT OR REPLACE INTO epoch_receipts VALUES(?,?,?,?,?,?,?)",(gateway,pid,tid,oid,generation,'reviewed_conflict',time.time()))


def run_cycle(database, state, gateway, report=False, now=None):
    healthy=True;reported=True
    try:
        if report:
            check_generation(database,state,gateway)
        sync_once(database,state,gateway,now=now)
    except Exception as error:
        healthy=False
        logging.error('sync failed error_type=%s',type(error).__name__)
    status=queue_status(state,gateway)
    now=time.time() if now is None else now

    if report:
        try:
            report_health(database,state,gateway,healthy)
        except Exception as error:
            reported=False
            logging.error('health report failed error_type=%s',type(error).__name__)
    success=healthy and reported and status['failures']==0 and status['conflicts']==0 and status['pending']==0 and status['discovery_pending']==0 and (not report or status['replay_complete'])
    state.execute('CREATE TABLE IF NOT EXISTS health(id INTEGER PRIMARY KEY,last_attempt REAL,last_success REAL)')
    with state:
        state.execute('INSERT INTO health VALUES(1,?,?) ON CONFLICT(id) DO UPDATE SET last_attempt=excluded.last_attempt,last_success=CASE WHEN excluded.last_success>0 THEN excluded.last_success ELSE last_success END',(now,now if success else 0))
    if healthy:
        systemd_notify('WATCHDOG=1')
    return success


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
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    sources = parser.add_mutually_exclusive_group(required=True)
    sources.add_argument('--database', help='SQLite database path')
    sources.add_argument('--database-url-env', help='Environment variable containing a PostgreSQL DSN')
    parser.add_argument('--state', required=True)
    parser.add_argument('--gateway', required=True)
    parser.add_argument('--once', action='store_true')
    reviews=parser.add_mutually_exclusive_group()
    reviews.add_argument('--review-conflict', help='Reconcile one paid conflict after an audited external refund is recorded at the gateway')
    reviews.add_argument('--review-missing', help='Gateway trade ID requiring audited operator review')
    parser.add_argument('--reason', default='', help='Required factual review reason')
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
            state.execute('DELETE FROM discovery_health WHERE gateway=?', (gateway,))
            state.execute('DELETE FROM anomaly_discovery WHERE gateway=?',(gateway,))
    if args.review_conflict:
        try:
            review_conflict(database,state,gateway,args.review_conflict,args.reason)
        except Exception as error:
            parser.exit(1,'Paid-conflict review failed: '+type(error).__name__+'; verify merchant cancellation and the gateway refund record.\n')
        return
    if args.review_missing:
        try:
            review_missing(database,state,gateway,args.review_missing,args.reason)
        except Exception as error:
            parser.exit(1,'Missing-order review failed: '+type(error).__name__+'; verify merchant cancellation and gateway response.\n')
        return
    systemd_notify('READY=1')
    while True:
        success=run_cycle(database,state,gateway,args.report_health)
        if args.once:
            raise SystemExit(0 if success else 1)
        time.sleep(5)



if __name__ == '__main__':
    main()

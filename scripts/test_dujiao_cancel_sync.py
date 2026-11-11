import hashlib
import http.server
import threading
import importlib.util
import json
import sqlite3
import tempfile
import unittest
from unittest.mock import Mock, patch
import sys
from pathlib import Path

spec = importlib.util.spec_from_file_location('sync', Path(__file__).with_name('dujiao_cancel_sync.py'))
sync = importlib.util.module_from_spec(spec)
spec.loader.exec_module(sync)


class SyncTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / 'commerce.db'
        self.gateway = 'https://pay.example.com'
        self.db = sqlite3.connect(self.path)
        self.addCleanup(self.db.close)
        self.db.executescript('''CREATE TABLE orders(id INTEGER PRIMARY KEY,status,paid_at,canceled_at,deleted_at);
            CREATE TABLE payments(id INTEGER PRIMARY KEY,order_id,channel_id,provider_type,status,paid_at,deleted_at,provider_ref,gateway_order_no,pay_url);
            CREATE TABLE payment_channels(id INTEGER PRIMARY KEY,provider_type,config_json);
            CREATE INDEX orders_cancel_cursor ON orders(canceled_at);
            CREATE INDEX payments_order_cursor ON payments(order_id);''')
        self.db.execute('INSERT INTO payment_channels VALUES (1,?,?)', ('bepusdt', json.dumps({'gateway_url': self.gateway, 'auth_token': 'private'})))
        self.state = sync.journal(':memory:')
        self.addCleanup(self.state.close)

    def add(self, number, order_status='canceled', payment_status='expired', pay_url=None):
        self.db.execute('INSERT INTO orders VALUES (?,?,NULL,?,NULL)', (number, order_status, '2026-01-01'))
        self.db.execute('INSERT INTO payments VALUES (?,?,1,?,?,NULL,NULL,?,?,?)', (number, number, 'bepusdt', payment_status, 'trade'+str(number), 'order'+str(number), pay_url or self.gateway+'/pay/test'))
        self.db.commit()

    def test_only_committed_unpaid_matching_gateway_and_durable_dedup(self):
        self.add(1)
        self.add(2, order_status='pending_payment')
        self.add(3, payment_status='success')
        self.add(4, pay_url='https://old.example.com/pay/test')
        calls = []
        def sender(*args):
            calls.append(args)
            return 'closed'
        self.assertEqual(sync.sync_once(self.path, self.state, self.gateway, sender), 1)
        self.assertEqual(calls, [(self.gateway, 'trade1', 'order1', 'private')])
        self.assertEqual(sync.sync_once(self.path, self.state, self.gateway, sender), 0)
        self.assertNotIn('private', str(self.state.execute('SELECT * FROM cancellations').fetchall()))

    def test_failure_retry_and_paid_conflict(self):
        self.add(1)
        def failure(*args):
            raise TimeoutError()
        self.assertEqual(sync.sync_once(self.path, self.state, self.gateway, failure, now=10), 1)
        self.assertEqual(sync.sync_once(self.path, self.state, self.gateway, failure, now=14), 0)
        self.assertEqual(sync.sync_once(self.path, self.state, self.gateway, lambda *a: 'paid_conflict', now=15), 1)
        self.assertEqual(sync.sync_once(self.path, self.state, self.gateway, failure, now=1000), 0)
        self.assertEqual(self.state.execute('SELECT outcome,attempts FROM cancellations').fetchone(), ('paid_conflict', 2))

    def test_signed_http_contract_and_acknowledgment(self):
        captured = []
        class Handler(http.server.BaseHTTPRequestHandler):
            def do_POST(handler):
                body = json.loads(handler.rfile.read(int(handler.headers['Content-Length'])))
                captured.append((handler.path, body))
                result = {'status_code': 200, 'data': {'trade_id': body['trade_id'], 'order_id': body['order_id']}}
                handler.send_response(200)
                handler.end_headers()
                handler.wfile.write(json.dumps(result).encode())
            def log_message(*args):
                pass
        server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            self.assertEqual(sync.cancel('http://127.0.0.1:' + str(server.server_port), 'trade', 'order', 'private'), 'closed')
            self.assertEqual(captured[0][0], '/api/v1/order/cancel-transaction')
            self.assertEqual(captured[0][1]['signature'], hashlib.md5(b'order_id=order&trade_id=tradeprivate').hexdigest())
        finally:
            server.shutdown()
            server.server_close()
            thread.join()

    def test_rechecks_merchant_before_retry(self):
        self.add(1)
        def failure(*args):
            raise TimeoutError()
        sync.sync_once(self.path, self.state, self.gateway, failure, now=10)
        self.db.execute("UPDATE orders SET status='completed'")
        self.db.commit()
        self.assertEqual(sync.sync_once(self.path, self.state, self.gateway, failure, now=100), 0)

    def test_missing_gateway_is_recorded_after_three_attempts(self):
        self.add(1)
        sender = lambda *args: 'missing_gateway'
        for now in (10, 15, 25):
            self.assertEqual(sync.sync_once(self.path, self.state, self.gateway, sender, now=now), 1)
        self.assertEqual(self.state.execute('SELECT outcome,attempts FROM cancellations').fetchone(), ('missing_gateway', 3))
        self.assertEqual(sync.sync_once(self.path, self.state, self.gateway, sender, now=1000), 0)

    def test_postgresql_connections_are_read_only_and_bounded(self):
        driver = Mock()
        with patch.dict(sys.modules, {'psycopg': driver}):
            self.assertIs(sync.source_connect('postgresql://fixture'), driver.connect.return_value)
        driver.connect.assert_called_once_with('postgresql://fixture', connect_timeout=5,
                                               options='-c default_transaction_read_only=on')

    def test_signed_health_report_without_cancellation_candidates(self):
        self.db.commit()
        captured = []
        class Response:
            def __enter__(self): return self
            def __exit__(self, *args): pass
            def read(self, limit): return b'{"status_code":200}'
        class Opener:
            def open(self, request, timeout):
                captured.append((request, timeout))
                return Response()
        with patch.object(sync.urllib.request, 'build_opener', return_value=Opener()):
            sync.report_health(self.path, self.state, self.gateway, True)
        request, timeout = captured[0]
        body = json.loads(request.data)
        signature = body.pop('signature')
        expected = hashlib.md5(('&'.join(k+'='+body[k] for k in sorted(body))+'private').encode()).hexdigest()
        self.assertEqual(signature, expected)
        self.assertEqual(body['source'], 'sqlite')
        self.assertEqual(body['failures'], '0')
        self.assertEqual(timeout, 3)

    def test_persistent_discovery_and_retries_survive_stop_and_journal_restore(self):
        self.add(1)
        journal_path = Path(self.tmp.name) / 'journal.db'
        state = sync.journal(journal_path)
        try:
            sync.sync_once(self.path, state, self.gateway, lambda *a: (_ for _ in ()).throw(TimeoutError()), now=10)
            saved = sqlite3.connect(Path(self.tmp.name) / 'saved.db')
            state.backup(saved)
            saved.close()
            state.close()
            # Both the cancellation and its retry survive a restored journal snapshot.
            self.add(2)
            self.db.execute("UPDATE orders SET canceled_at='2026-01-02' WHERE id=2")
            self.db.commit()
            state = sync.journal(Path(self.tmp.name) / 'saved.db')
            calls = []
            sync.sync_once(self.path, state, self.gateway, lambda *a: calls.append(a[1]) or 'closed', now=15)
            self.assertEqual(set(calls), {'trade1', 'trade2'})
            self.assertEqual(state.execute("SELECT count(*) FROM cancellations WHERE outcome='closed'").fetchone()[0], 2)
            state.close()
            state = sync.journal(Path(self.tmp.name) / 'saved.db')
            self.assertEqual(sync.sync_once(self.path, state, self.gateway, lambda *a: self.fail('duplicate'), now=20), 0)
        finally:
            state.close()

    def test_backdated_cancellation_and_late_payment_metadata_are_discovered(self):
        # Advance both the recent cursor and the historical sweep past a pending order.
        self.add(1, order_status='pending_payment')
        self.add(2)
        self.db.execute("UPDATE orders SET canceled_at=NULL WHERE id=1")
        self.db.execute("UPDATE orders SET canceled_at='2026-02-01' WHERE id=2")
        self.db.commit()
        sync.sync_once(self.path, self.state, self.gateway, lambda *a: 'closed', now=10)
        # Backfill an old cancellation with an older ID and timestamp, without touching
        # payment IDs. It must be found by the durable cyclic sweep.
        self.db.execute("UPDATE orders SET status='canceled',canceled_at='2025-01-01' WHERE id=1")
        self.db.execute("UPDATE payments SET provider_ref='' WHERE id=1")
        self.db.commit()
        self.assertEqual(sync.sync_once(self.path, self.state, self.gateway, lambda *a: 'closed', now=40), 0)
        self.db.execute("UPDATE payments SET provider_ref='late-trade' WHERE id=1")
        self.db.commit()
        calls = []
        sync.sync_once(self.path, self.state, self.gateway, lambda *a: calls.append(a[1]) or 'closed', now=70)
        self.assertEqual(calls, ['late-trade'])

    def test_source_restore_rewinds_discovery_without_losing_retry_identity_checks(self):
        self.add(100)
        sync.sync_once(self.path, self.state, self.gateway, lambda *a: 'closed', now=10)
        self.db.execute('DELETE FROM payments')
        self.db.execute('DELETE FROM orders')
        self.db.commit()
        self.add(1)
        self.db.execute("UPDATE orders SET canceled_at='2020-01-01'")
        self.db.commit()
        calls = []
        sync.sync_once(self.path, self.state, self.gateway, lambda *a: calls.append(a[1]) or 'closed', now=15)
        self.assertEqual(calls, ['trade1'])
        self.assertEqual(self.state.execute('SELECT max_payment_id FROM discovery').fetchone()[0], 1)

    def test_discovery_cursor_and_queue_commit_atomically(self):
        self.add(1)
        with patch.object(sync, 'enqueue', side_effect=RuntimeError('disk failed')):
            with self.assertRaises(RuntimeError):
                sync.sync_once(self.path, self.state, self.gateway, lambda *a: self.fail('should not send'))
        self.assertEqual(self.state.execute('SELECT after_payment_id FROM discovery_orders').fetchone()[0], 0)
        self.assertEqual(sync.sync_once(self.path, self.state, self.gateway, lambda *a: 'closed'), 1)

    def test_large_order_expansion_is_resumable_and_bounded(self):
        self.add(1)
        self.db.executemany("INSERT INTO payments VALUES(?,1,1,'bepusdt','expired',NULL,NULL,?,?,?)",
                            [(i, 't'+str(i), 'o'+str(i), self.gateway+'/pay/a') for i in range(2, 122)])
        self.db.commit()
        # Disable the independent audit just for this test: exercise order pagination.
        self.state.execute("INSERT INTO discovery(gateway,next_audit) VALUES(?,100000)", (self.gateway,))
        self.state.commit()
        calls = []
        for n in range(3):
            sync.sync_once(self.path, self.state, self.gateway, lambda *a: calls.append(a[1]) or 'closed', now=n+1)
            self.assertLessEqual(len(calls), (n+1)*sync.ORDER_PAYMENT_BATCH)
        self.assertEqual(len(calls), 121)
        self.assertEqual(len(set(calls)), 121)
        self.assertEqual(self.state.execute('SELECT count(*) FROM discovery_orders').fetchone()[0], 0)

    def test_hundred_thousand_closed_records_do_not_expand_steady_state_work(self):
        import time
        start = time.perf_counter()
        n = 100000
        self.db.executemany("INSERT INTO orders VALUES(?,'canceled',NULL,'2026-01-01',NULL)", ((i,) for i in range(1, n+1)))
        self.db.executemany("INSERT INTO payments VALUES(?,?,1,'bepusdt','expired',NULL,NULL,?,?,?)",
                            ((i, i, 't'+str(i), 'o'+str(i), self.gateway+'/pay/a') for i in range(1, n+1)))
        self.db.commit()
        self.state.executemany("INSERT INTO cancellations VALUES(?,?,?,?,'closed',1,0,0)",
                               ((self.gateway, i, 't'+str(i), 'o'+str(i)) for i in range(1, n+1)))
        self.state.execute("INSERT INTO discovery VALUES(?,'2026-01-01',?,0,?,0)", (self.gateway, n, n))
        self.state.commit()
        metrics = {'instructions': 0, 'source_rows': 0}
        original = sync.source_connect
        def connect(path):
            c = original(path)
            def count():
                metrics['instructions'] += 100
                if metrics['instructions'] > 100000:
                    self.fail('source work grew with synchronized history')
                return 0
            c.set_progress_handler(count, 100)
            c.set_trace_callback(lambda sql: None)
            return c
        original_query = sync.source_query
        class Cursor:
            def __init__(self, cursor): self.cursor = cursor
            def fetchall(self):
                rows = self.cursor.fetchall();metrics['source_rows'] += len(rows);return rows
            def fetchone(self):
                row = self.cursor.fetchone();metrics['source_rows'] += int(row is not None);return row
        with patch.object(sync, 'source_connect', side_effect=connect), patch.object(sync, 'source_query', side_effect=lambda *args: Cursor(original_query(*args))):
            self.assertEqual(sync.sync_once(self.path, self.state, self.gateway, lambda *a: self.fail('duplicate'), now=10), 0)
        self.assertLessEqual(metrics['source_rows'], 1001)
        self.assertLess(metrics['instructions'], 100000)
        print('100k synchronized cancellations: source rows=%s SQLite VM instructions=%s elapsed=%.3fs (includes fixture setup)' % (metrics['source_rows'], metrics['instructions'], time.perf_counter()-start))

    def test_stop_during_large_equal_timestamp_backlog_never_skips_or_resends(self):
        n = 1500
        self.db.executemany("INSERT INTO orders VALUES(?,'canceled',NULL,'2026-01-01',NULL)", ((i,) for i in range(1, n+1)))
        self.db.executemany("INSERT INTO payments VALUES(?,?,1,'bepusdt','expired',NULL,NULL,?,?,?)",
                            ((i, i, 't'+str(i), 'o'+str(i), self.gateway+'/pay/a') for i in range(1, n+1)))
        self.db.commit()
        path = Path(self.tmp.name) / 'resume.db'
        state = sync.journal(path)
        calls = []
        try:
            for step in range(35):
                sync.sync_once(self.path, state, self.gateway, lambda *a: calls.append(a[1]) or 'closed', now=10+step*5)
                # Restart after every discovery/send batch; there is no in-memory cursor.
                state.close()
                state = sync.journal(path)
            self.assertEqual(len(calls), n)
            self.assertEqual(len(set(calls)), n)
            self.assertEqual(state.execute('SELECT count(*) FROM discovery_orders').fetchone()[0], 0)
        finally:
            state.close()

    def test_reeligible_order_after_source_restore_requeues_unattempted_cancellation(self):
        self.add(1)
        sync.sync_once(self.path, self.state, self.gateway, lambda *a: (_ for _ in ()).throw(TimeoutError()), now=10)
        self.db.execute("UPDATE orders SET status='pending_payment'")
        self.db.commit()
        sync.sync_once(self.path, self.state, self.gateway, lambda *a: self.fail('inactive order sent'), now=15)
        self.assertEqual(self.state.execute('SELECT outcome FROM cancellations').fetchone()[0], 'merchant_changed')
        # A restored snapshot can make the same source identity canceled again.
        self.db.execute("UPDATE orders SET status='canceled'")
        self.db.commit()
        self.assertEqual(sync.sync_once(self.path, self.state, self.gateway, lambda *a: 'closed', now=40), 1)
        self.assertEqual(self.state.execute('SELECT outcome FROM cancellations').fetchone()[0], 'closed')

    def test_three_missing_results_remain_an_unhealthy_persistent_exception(self):
        self.add(1)
        for at in (10,15,25):
            sync.sync_once(self.path,self.state,self.gateway,lambda *a:'missing_gateway',now=at)
        status=sync.queue_status(self.state,self.gateway)
        self.assertEqual(status['missing'],1)
        self.assertEqual(status['failures'],1)
        with patch.object(sync,'signed_post',return_value={'status_code':200}) as post:
            sync.report_health(self.path,self.state,self.gateway,True)
        body=post.call_args[0][2]
        self.assertEqual(body['healthy'],'false')
        self.assertEqual(body['missing'],'1')
        self.assertEqual(body['failures'],'1')

    def test_paid_conflict_remains_unhealthy_across_cycles_and_restart(self):
        self.add(1)
        path = Path(self.tmp.name) / 'conflict-state.db'
        state = sync.journal(path)
        state.execute("INSERT INTO gateway_epochs(gateway,generation) VALUES(?,?)", (self.gateway,'a'*32))
        state.commit()
        sync.sync_once(self.path,state,self.gateway,lambda *a:'paid_conflict',now=10)
        state.close()
        state = sync.journal(path)
        self.addCleanup(state.close)
        for at in (20,1000):
            sync.sync_once(self.path,state,self.gateway,lambda *a:self.fail('terminal conflict resent'),now=at)
            status = sync.queue_status(state,self.gateway)
            self.assertEqual(status['failures'],0)
            self.assertEqual(status['conflicts'],1)
            self.assertFalse(status['replay_complete'])
            with patch.object(sync,'signed_post',return_value={'status_code':200}) as post:
                sync.report_health(self.path,state,self.gateway,True)
            body = post.call_args[0][2]
            self.assertEqual(body['healthy'],'false')
            self.assertEqual(body['conflicts'],'1')

    def test_fresh_source_without_completed_reconciliation_reports_unhealthy(self):
        self.db.commit()
        self.state.execute("INSERT INTO gateway_epochs(gateway,generation) VALUES(?,?)", (self.gateway,'a'*32))
        self.state.commit()
        with patch.object(sync,'signed_post',return_value={'status_code':200}) as post:
            sync.report_health(self.path,self.state,self.gateway,True)
        self.assertEqual(post.call_args[0][2]['healthy'],'false')
        self.state.execute("UPDATE gateway_epochs SET scanned=1")
        self.state.execute("INSERT INTO discovery_health VALUES(?,0,0,0)",(self.gateway,))
        self.state.commit()
        with patch.object(sync,'signed_post',return_value={'status_code':200}) as post:
            sync.report_health(self.path,self.state,self.gateway,True)
        self.assertEqual(post.call_args[0][2]['healthy'],'true')

    def test_unread_cancellations_block_health_with_an_empty_local_queue(self):
        self.add(3)
        self.db.executemany("INSERT INTO orders VALUES(?,'canceled',NULL,'2026-01-01',NULL)",[(1,),(2,)])
        self.db.commit()
        path = Path(self.tmp.name) / 'lookahead.db'
        state = sync.journal(path)
        now = __import__('time').time()
        state.execute("INSERT INTO discovery(gateway,max_payment_id,next_audit) VALUES(?,?,?)",(self.gateway,3,now+1000))
        state.execute("INSERT INTO gateway_epochs(gateway,generation,scanned) VALUES(?,?,1)",(self.gateway,'a'*32))
        state.commit()
        with patch.object(sync,'RECENT_ORDER_BATCH',2):
            self.assertEqual(sync.sync_once(self.path,state,self.gateway,lambda *a:self.fail('first page has no gateway payments'),now=now),0)
        status=sync.queue_status(state,self.gateway)
        self.assertEqual(status['pending'],0)
        self.assertEqual(status['failures'],0)
        self.assertEqual(status['discovery_pending'],1)
        self.assertGreater(status['discovery_lag_seconds'],0)
        self.assertFalse(status['replay_complete'])
        state.close()
        state=sync.journal(path)
        self.addCleanup(state.close)
        self.assertEqual(sync.queue_status(state,self.gateway)['discovery_pending'],1)
        with patch.object(sync,'signed_post',return_value={'status_code':200}) as post:
            sync.report_health(self.path,state,self.gateway,True)
        self.assertEqual(post.call_args[0][2]['healthy'],'false')
        self.assertEqual(post.call_args[0][2]['pending'],'0')
        self.assertEqual(post.call_args[0][2]['discovery_pending'],'1')
        self.assertEqual(sync.sync_once(self.path,state,self.gateway,lambda *a:'closed',now=now+5),1)
        status=sync.queue_status(state,self.gateway)
        self.assertEqual(status['discovery_pending'],0)
        self.assertEqual(status['discovery_lag_seconds'],0)
        self.assertTrue(status['replay_complete'])

    def test_gateway_generation_replays_closed_receipts_and_survives_restart(self):
        self.add(1)
        sync.sync_once(self.path,self.state,self.gateway,lambda *a:'closed',now=10)
        self.state.execute("INSERT INTO gateway_epochs(gateway,generation) VALUES(?,?)",(self.gateway,'a'*32));self.state.commit()
        calls=[]
        sync.sync_once(self.path,self.state,self.gateway,lambda *a:calls.append(a[1]) or 'closed',now=15)
        self.assertEqual(calls,['trade1'])
        self.assertTrue(sync.queue_status(self.state,self.gateway)['replay_complete'])
        with patch.object(sync,'signed_post',return_value={'status_code':200,'data':{'generation':'b'*32}}):
            sync.check_generation(self.path,self.state,self.gateway)
        self.assertFalse(sync.queue_status(self.state,self.gateway)['replay_complete'])
        sync.sync_once(self.path,self.state,self.gateway,lambda *a:calls.append(a[1]) or 'closed',now=20)
        self.assertEqual(calls,['trade1','trade1'])
        self.assertTrue(sync.queue_status(self.state,self.gateway)['replay_complete'])
        sync.sync_once(self.path,self.state,self.gateway,lambda *a:self.fail('duplicate current generation'),now=25)
        self.assertEqual(self.state.execute('SELECT count(*) FROM epoch_receipts').fetchone()[0],2)

    def test_once_cycle_returns_failure_for_all_timeouts_and_reporting_failure(self):
        self.add(1)
        original=sync.sync_once
        def timeout_cycle(database,state,gateway,now=None):
            return original(database,state,gateway,lambda *a:(_ for _ in ()).throw(TimeoutError()),now=now)
        with patch.object(sync,'sync_once',side_effect=timeout_cycle):
            self.assertFalse(sync.run_cycle(self.path,self.state,self.gateway,False,now=10))
        self.assertEqual(sync.queue_status(self.state,self.gateway)['failures'],1)
        with patch.object(sync,'check_generation'),patch.object(sync,'report_health',side_effect=TimeoutError()):
            self.assertFalse(sync.run_cycle(self.path,self.state,self.gateway,True,now=11))

    def test_once_cli_uses_cycle_exit_status(self):
        args=['sync','--database',str(self.path),'--state',str(Path(self.tmp.name)/'cli.db'),'--gateway',self.gateway,'--once','--report-health']
        with patch.object(sys,'argv',args),patch.object(sync,'run_cycle',return_value=False):
            with self.assertRaises(SystemExit) as status:
                sync.main()
        self.assertEqual(status.exception.code,1)
        with patch.object(sys,'argv',args),patch.object(sync,'run_cycle',return_value=True):
            with self.assertRaises(SystemExit) as status:
                sync.main()
        self.assertEqual(status.exception.code,0)

    def test_once_cli_actual_timeout_writes_retry_and_exits_nonzero(self):
        self.add(1)
        path=Path(self.tmp.name)/'timeout-cli.db'
        args=['sync','--database',str(self.path),'--state',str(path),'--gateway',self.gateway,'--once']
        opener=Mock();opener.open.side_effect=TimeoutError()
        with patch.object(sys,'argv',args),patch.object(sync.urllib.request,'build_opener',return_value=opener):
            with self.assertRaises(SystemExit) as status:
                sync.main()
        self.assertEqual(status.exception.code,1)
        with sqlite3.connect(path) as state:
            self.assertEqual(state.execute('SELECT outcome FROM cancellations').fetchone()[0],'retry')

    def test_once_cli_actual_health_report_timeout_exits_nonzero(self):
        self.add(1)
        path=Path(self.tmp.name)/'report-cli.db'
        args=['sync','--database',str(self.path),'--state',str(path),'--gateway',self.gateway,'--once','--report-health']
        class Response:
            def __init__(self,data):self.data=data
            def __enter__(self):return self
            def __exit__(self,*args):pass
            def read(self,limit):return json.dumps(self.data).encode()
        class Opener:
            def open(self,request,timeout):
                if request.full_url.endswith('/cancel-sync-generation'):
                    return Response({'status_code':200,'data':{'generation':'a'*32}})
                if request.full_url.endswith('/cancel-transaction'):
                    body=json.loads(request.data)
                    return Response({'status_code':200,'data':{'trade_id':body['trade_id'],'order_id':body['order_id']}})
                raise TimeoutError()
        with patch.object(sys,'argv',args),patch.object(sync.urllib.request,'build_opener',return_value=Opener()):
            with self.assertRaises(SystemExit) as status:
                sync.main()
        self.assertEqual(status.exception.code,1)
        with sqlite3.connect(path) as state:
            self.assertEqual(state.execute('SELECT outcome FROM cancellations').fetchone()[0],'closed')

    def test_manual_missing_review_is_audited_and_invalidated_by_new_generation(self):
        self.add(1)
        self.state.execute('INSERT INTO gateway_epochs(gateway,generation) VALUES(?,?)',(self.gateway,'a'*32));self.state.commit()
        for at in (10,15,25):sync.sync_once(self.path,self.state,self.gateway,lambda *a:'missing_gateway',now=at)
        with self.assertRaises(ValueError):sync.review_missing(self.path,self.state,self.gateway,'trade1','')
        with patch.object(sync,'cancel',return_value='missing_gateway'):
            sync.review_missing(self.path,self.state,self.gateway,'trade1','Verified canceled merchant order and absent gateway reference')
        self.assertEqual(self.state.execute('SELECT count(*) FROM missing_reviews').fetchone()[0],1)
        self.assertEqual(self.state.execute('SELECT outcome FROM cancellations').fetchone()[0],'reviewed_missing')
        self.assertEqual(sync.queue_status(self.state,self.gateway)['missing'],0)
        with patch.object(sync,'signed_post',return_value={'status_code':200,'data':{'generation':'b'*32}}):
            sync.check_generation(self.path,self.state,self.gateway)
        calls=[]
        sync.sync_once(self.path,self.state,self.gateway,lambda *a:calls.append(a[1]) or 'closed',now=30)
        self.assertEqual(calls,['trade1'])


if __name__ == '__main__':
    unittest.main()

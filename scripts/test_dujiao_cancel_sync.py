import hashlib
import http.server
import threading
import importlib.util
import json
import sqlite3
import tempfile
import unittest
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
        self.db.executescript('''CREATE TABLE orders(id,status,paid_at,canceled_at,deleted_at);
            CREATE TABLE payments(id,order_id,channel_id,provider_type,status,paid_at,deleted_at,provider_ref,gateway_order_no,pay_url);
            CREATE TABLE payment_channels(id,provider_type,config_json);''')
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


if __name__ == '__main__':
    unittest.main()

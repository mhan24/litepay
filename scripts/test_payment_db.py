import fcntl
import importlib.util
from pathlib import Path
import sqlite3
import tempfile
import unittest

spec = importlib.util.spec_from_file_location('dbtool', Path(__file__).with_name('payment_db.py'))
tool = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tool)


class PaymentDBTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.path = self.root / 'live.db'
        self.db = sqlite3.connect(self.path)
        self.addCleanup(self.db.close)
        self.db.executescript('''PRAGMA user_version=9; PRAGMA journal_mode=WAL;
            CREATE TABLE orders(id INTEGER PRIMARY KEY,status,provider,currency,pay_units);
            CREATE TABLE transactions(provider,event_id,order_id,state);
            CREATE TABLE callbacks(order_id,attempt_base,attempts,recoveries);
            CREATE TABLE slots(order_id,provider,currency,units);
            CREATE TABLE scan_cursors(provider,scanned_until);
            CREATE TABLE settings(payload);
            CREATE TABLE credentials(name,value);
            CREATE TABLE notify_logs(id);
            CREATE TABLE confirmation_tasks(provider,event_id,state,deadline);
            CREATE TABLE ton_scan_checkpoints(receiver,active,"from","to",pages,lt,hash);
            CREATE TABLE callback_recoveries(id);
            INSERT INTO orders VALUES(1,'PAID','okx','USDT',100);
            INSERT INTO transactions VALUES('okx','deposit',1,'confirmed');
            INSERT INTO callbacks VALUES(1,5,6,1);
            INSERT INTO credentials VALUES('secret','do-not-print');
            INSERT INTO settings VALUES('{}');
            INSERT INTO scan_cursors VALUES('okx','2026-11-10');
            INSERT INTO ton_scan_checkpoints VALUES('wallet',1,'2026-11-09','2026-11-10',40,'200','hash');
        ''')
        self.db.commit()

    def test_wal_consistent_snapshot_and_restore_preserve_recovery_state(self):
        backup, restored = self.root / 'backup.db', self.root / 'restored.db'
        result = tool.snapshot(self.path, backup)
        self.assertNotIn('do-not-print', str(result))
        self.assertEqual(backup.stat().st_mode & 0o777, 0o600)
        tool.restore(backup, restored)
        self.assertEqual(tool.fingerprint(backup), tool.fingerprint(restored))
        self.assertEqual(tool.drill(self.path)['drill'], 'passed')

    def test_restore_refuses_active_lock_and_existing_database(self):
        backup, restored = self.root / 'backup.db', self.root / 'restored.db'
        tool.snapshot(self.path, backup)
        with open(str(restored) + '.lock', 'w') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with self.assertRaises(BlockingIOError):
                tool.restore(backup, restored)
        tool.restore(backup, restored)
        with self.assertRaises(ValueError):
            tool.restore(backup, restored)
        with self.assertRaises(FileExistsError):
            tool.snapshot(self.path, backup)

    def test_checksum_and_payment_invariants(self):
        backup = self.root / 'backup.db'
        tool.snapshot(self.path, backup)
        with open(backup, 'ab') as file:
            file.write(b'changed')
        with self.assertRaises(ValueError):
            tool.restore(backup, self.root / 'restored.db')
        self.db.execute('DELETE FROM transactions')
        self.db.commit()
        with self.assertRaises(ValueError):
            tool.validate(self.path)

class PaymentAttemptDBTest(PaymentDBTest):
    def setUp(self):
        super().setUp()
        self.db.executescript('''PRAGMA user_version=10;
            ALTER TABLE orders ADD COLUMN signing_key_id INTEGER DEFAULT 1;
            ALTER TABLE callbacks ADD COLUMN signing_key_id INTEGER DEFAULT 1;
            ALTER TABLE transactions ADD COLUMN attempt_id INTEGER DEFAULT NULL;
            ALTER TABLE transactions ADD COLUMN receiver TEXT;
            ALTER TABLE transactions ADD COLUMN currency TEXT;
            ALTER TABLE transactions ADD COLUMN units INTEGER;
            ALTER TABLE transactions ADD COLUMN occurred_at TEXT;
            CREATE TABLE payment_attempts(id,order_id,provider,receiver,currency,units,selected_at,expires_at,settle_until);
            CREATE TABLE signing_keys(id,token);
            CREATE TABLE payment_recoveries(order_id,operator,reason);
            CREATE TABLE historical_scans(order_id,attempt_id,cursor,"from","to",operator,reason);
            INSERT INTO signing_keys VALUES(1,'private-key-version');
            INSERT INTO payment_attempts VALUES(1,1,'okx','receiver','USDT',100,'2026-11-10 00:00','2026-11-10 01:00','2026-11-12 01:00');
        ''')
        self.db.commit()

    def test_versioned_keys_and_historical_attempts_preserved(self):
        self.assertEqual(tool.drill(self.path)['schema'], 10)
        self.db.execute('UPDATE orders SET signing_key_id=99')
        self.db.commit()
        with self.assertRaises(ValueError):
            tool.validate(self.path)


class CapacityDBTest(PaymentAttemptDBTest):
    def setUp(self):
        super().setUp()
        self.db.executescript('''PRAGMA user_version=11;
            ALTER TABLE orders ADD COLUMN selection_changes INTEGER DEFAULT 1;
            ALTER TABLE orders ADD COLUMN next_expiry_check_at TEXT;
            ALTER TABLE callbacks ADD COLUMN state TEXT DEFAULT 'merchant_review';
            ALTER TABLE callbacks ADD COLUMN delivered_at TEXT;
        ''')
        self.db.commit()

    def test_versioned_keys_and_historical_attempts_preserved(self):
        self.assertEqual(tool.drill(self.path)['schema'], 11)
        self.db.execute('UPDATE orders SET signing_key_id=99')
        self.db.commit()
        with self.assertRaises(ValueError):
            tool.validate(self.path)

    def test_review_hold_and_expiry_schedule_invariants(self):
        self.assertEqual(tool.validate(self.path)['schema'], 11)
        self.db.execute("UPDATE callbacks SET delivered_at='2026-11-10'")
        self.db.commit()
        with self.assertRaises(ValueError):
            tool.validate(self.path)
        self.db.execute('UPDATE callbacks SET delivered_at=NULL')
        self.db.execute("INSERT INTO orders(id,status) VALUES(2,'READY')")
        self.db.commit()
        with self.assertRaises(ValueError):
            tool.validate(self.path)


if __name__ == '__main__':
    unittest.main()

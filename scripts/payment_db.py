#!/usr/bin/env python3
"""Consistent SQLite backup, integrity checks, restoration to a NEW path, and an offline drill.

Backups contain account credentials. All artifacts are mode 0600; store them on protected,
separately backed-up storage. This tool never sends callbacks or contacts a payment provider.
"""
import argparse
import datetime
import fcntl
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import tempfile

BASE_TABLES = ('orders', 'transactions', 'callbacks', 'scan_cursors', 'settings', 'credentials', 'slots', 'notify_logs')
NEW_TABLES = ('confirmation_tasks', 'ton_scan_checkpoints', 'callback_recoveries')


def connect(path):
    return sqlite3.connect(Path(path).resolve().as_uri() + '?mode=ro', uri=True, timeout=20)


def validate(path):
    db = connect(path)
    try:
        if db.execute('PRAGMA integrity_check').fetchall() != [('ok',)]:
            raise ValueError('SQLite integrity check failed')
        if db.execute('PRAGMA foreign_key_check').fetchall():
            raise ValueError('foreign key check failed')
        version = db.execute('PRAGMA user_version').fetchone()[0]
        if version not in (8, 9):
            raise ValueError('unsupported payment schema')
        tables = BASE_TABLES + (NEW_TABLES if version == 9 else ())
        counts = {name: db.execute('SELECT count(*) FROM ' + name).fetchone()[0] for name in tables}
        problems = [
            "SELECT count(*) FROM orders o WHERE o.status='PAID' AND ((SELECT count(*) FROM transactions t WHERE t.order_id=o.id AND t.state='confirmed')<>1 OR (SELECT count(*) FROM callbacks c WHERE c.order_id=o.id)<>1)",
            "SELECT count(*) FROM transactions t LEFT JOIN orders o ON o.id=t.order_id WHERE t.order_id IS NOT NULL AND (o.id IS NULL OR o.status<>'PAID' OR t.state<>'confirmed')",
            "SELECT count(*) FROM callbacks c LEFT JOIN orders o ON o.id=c.order_id WHERE o.id IS NULL OR o.status<>'PAID'",
            "SELECT count(*) FROM slots s LEFT JOIN orders o ON o.id=s.order_id WHERE s.order_id IS NOT NULL AND (o.id IS NULL OR o.status<>'READY' OR s.provider<>o.provider OR s.currency<>o.currency OR s.units<>o.pay_units)",
        ]
        if version == 9:
            problems += [
                "SELECT count(*) FROM confirmation_tasks q LEFT JOIN transactions t ON t.provider=q.provider AND t.event_id=q.event_id WHERE t.event_id IS NULL OR q.state NOT IN ('pending','resolved','manual_review') OR q.deadline IS NULL",
                "SELECT count(*) FROM ton_scan_checkpoints WHERE active=1 AND (receiver='' OR \"from\">\"to\" OR (pages>0 AND (lt='' OR hash='')))",
                "SELECT count(*) FROM callbacks WHERE attempt_base>attempts OR recoveries<0",
            ]
        if any(db.execute(sql).fetchone()[0] for sql in problems):
            raise ValueError('payment evidence, reservation, or recovery invariants failed')
        for (payload,) in db.execute('SELECT payload FROM settings'):
            if not isinstance(json.loads(payload), dict):
                raise ValueError('invalid stored configuration')
        return {'schema': version, 'integrity': 'ok', 'counts': counts}
    finally:
        db.close()


def fsync_file(path):
    with open(path, 'rb') as f:
        os.fsync(f.fileno())


def fsync_dir(path):
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def snapshot(source, output):
    source, output = Path(source).resolve(), Path(output).absolute()
    if source == output:
        raise ValueError('backup destination must differ from live database')
    output.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
    # O_EXCL protects existing snapshots and rejects symlinks.
    fd = os.open(output, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    os.close(fd)
    try:
        src, dest = connect(source), sqlite3.connect(output)
        try:
            src.backup(dest, pages=256, sleep=0.01)
            dest.execute("PRAGMA journal_mode=DELETE")
        finally:
            dest.close()
            src.close()
        result = validate(output)
        fsync_file(output)
        result['sha256'] = hashlib.sha256(output.read_bytes()).hexdigest()
        result['created_at'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        with open(str(output) + '.json', 'x', opener=lambda name, flags: os.open(name, flags, 0o600)) as file:
            json.dump(result, file, indent=2)
            file.flush()
            os.fsync(file.fileno())
        fsync_dir(output.parent)
        return result
    except Exception:
        output.unlink(missing_ok=True)
        raise


def restore(source, target):
    source, target = Path(source).resolve(), Path(target).absolute()
    sidecar = Path(str(source) + '.json')
    if not sidecar.is_file():
        raise ValueError('backup manifest is required')
    expected = json.loads(sidecar.read_text())['sha256']
    if hashlib.sha256(source.read_bytes()).hexdigest() != expected:
        raise ValueError('backup checksum mismatch')
    validate(source)
    target.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
    lock_fd = os.open(str(target) + '.lock', os.O_CREAT | os.O_RDWR, 0o600)
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if any(os.path.lexists(str(target) + suffix) for suffix in ('', '-wal', '-shm', '-journal')):
            raise ValueError('restore requires a new empty target path; existing data is never overwritten')
        with tempfile.TemporaryDirectory(prefix='.litepay-restore-', dir=target.parent) as directory:
            staged = Path(directory) / 'restored.db'
            result = snapshot(source, staged)
            if result['sha256'] != expected:
                # SQLite may legitimately change page organization. Verify logical data instead.
                if fingerprint(source) != fingerprint(staged):
                    raise ValueError('restored database differs from backup')
            os.replace(staged, target)
            fsync_dir(target.parent)
        return validate(target)
    finally:
        os.close(lock_fd)


def fingerprint(path):
    db = connect(path)
    try:
        digest = hashlib.sha256()
        for line in db.iterdump():
            digest.update(line.encode())
        return digest.hexdigest()
    finally:
        db.close()


def drill(source):
    with tempfile.TemporaryDirectory(prefix='litepay-recovery-drill-') as directory:
        backup, restored = Path(directory) / 'backup.db', Path(directory) / 'restored.db'
        snapshot(source, backup)
        result = restore(backup, restored)
        if fingerprint(backup) != fingerprint(restored):
            raise ValueError('orders, evidence, cursors, credentials, or task state changed during recovery')
        result['drill'] = 'passed'
        return result


def main():
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=('check', 'backup', 'restore', 'drill'))
    parser.add_argument('--database', required=True, help='live DB, or backup DB for restore')
    parser.add_argument('--output', help='new backup or restoration path')
    args = parser.parse_args()
    if args.command in ('backup', 'restore') and not args.output:
        parser.error('--output is required')
    try:
        if args.command == 'check':
            result = validate(args.database)
        elif args.command == 'backup':
            result = snapshot(args.database, args.output)
        elif args.command == 'restore':
            result = restore(args.database, args.output)
        else:
            result = drill(args.database)
        print(json.dumps(result))
    except Exception as error:
        # Never dump configuration, credential values, or raw DB rows into diagnostics.
        parser.exit(1, 'Payment DB operation failed: ' + type(error).__name__ + '\n')


if __name__ == '__main__':
    main()

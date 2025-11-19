import sqlite3
import threading
import time
import uuid
import json
import hashlib
from queue import Queue, Empty
import unittest
from contextlib import contextmanager
from typing import Optional, Any, Dict, List

class SQLiteRepo:
    def __init__(self):
        self.conn = sqlite3.connect(':memory:', check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._init_schema()
        self.lock = threading.RLock()

    def _init_schema(self):
        c = self.conn.cursor()
        c.execute('''
            CREATE TABLE users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                email TEXT UNIQUE NOT NULL,
                age INTEGER NOT NULL,
                password_hash TEXT NOT NULL
            )
        ''')
        c.execute('''
            CREATE TABLE orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                item TEXT NOT NULL,
                amount REAL NOT NULL,
                created_at REAL NOT NULL,
                FOREIGN KEY(user_id) REFERENCES users(id)
            )
        ''')
        c.execute('''
            CREATE TABLE meta (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        ''')
        self.conn.commit()

    @contextmanager
    def transaction(self):
        with self.lock:
            cur = self.conn.cursor()
            try:
                yield cur
                self.conn.commit()
            except Exception:
                self.conn.rollback()
                raise

    def add_user(self, name: str, email: str, age: int, password_hash: str) -> Dict[str, Any]:
        with self.transaction() as cur:
            cur.execute('INSERT INTO users (name, email, age, password_hash) VALUES (?, ?, ?, ?)',
                        (name, email, age, password_hash))
            user_id = cur.lastrowid
            return self.get_user_by_id(user_id)

    def get_user_by_email(self, email: str) -> Optional[Dict[str, Any]]:
        with self.lock:
            cur = self.conn.cursor()
            cur.execute('SELECT * FROM users WHERE email = ?', (email,))
            row = cur.fetchone()
            return dict(row) if row else None

    def get_user_by_id(self, user_id: int) -> Optional[Dict[str, Any]]:
        with self.lock:
            cur = self.conn.cursor()
            cur.execute('SELECT * FROM users WHERE id = ?', (user_id,))
            row = cur.fetchone()
            return dict(row) if row else None

    def create_order(self, user_id: int, item: str, amount: float) -> Dict[str, Any]:
        with self.transaction() as cur:
            cur.execute('INSERT INTO orders (user_id, item, amount, created_at) VALUES (?, ?, ?, ?)',
                        (user_id, item, amount, time.time()))
            order_id = cur.lastrowid
            cur.execute('SELECT * FROM orders WHERE id = ?', (order_id,))
            row = cur.fetchone()
            return dict(row)

    def get_orders_for_user(self, user_id: int) -> List[Dict[str, Any]]:
        with self.lock:
            cur = self.conn.cursor()
            cur.execute('SELECT * FROM orders WHERE user_id = ? ORDER BY created_at DESC', (user_id,))
            rows = cur.fetchall()
            return [dict(r) for r in rows]

    def set_meta(self, key: str, value: str):
        with self.transaction() as cur:
            cur.execute('REPLACE INTO meta (key, value) VALUES (?, ?)', (key, value))

    def get_meta(self, key: str) -> Optional[str]:
        with self.lock:
            cur = self.conn.cursor()
            cur.execute('SELECT value FROM meta WHERE key = ?', (key,))
            row = cur.fetchone()
            return row[0] if row else None

class ExternalPaymentGateway:
    def __init__(self):
        self.should_fail = False
        self.charges = []

    def charge(self, user_email: str, amount: float) -> Dict[str, Any]:
        time.sleep(0.001)
        if self.should_fail:
            raise RuntimeError('Payment gateway error')
        charge_id = uuid.uuid4().hex
        record = {'charge_id': charge_id, 'email': user_email, 'amount': amount, 'ts': time.time()}
        self.charges.append(record)
        return record

class SimpleCache:
    def __init__(self, maxsize=128):
        self.store = {}
        self.maxsize = maxsize
        self.lock = threading.RLock()

    def set(self, key, value):
        with self.lock:
            if len(self.store) >= self.maxsize:
                k = next(iter(self.store))
                del self.store[k]
            self.store[key] = (value, time.time())

    def get(self, key, default=None):
        with self.lock:
            v = self.store.get(key)
            return v[0] if v else default

class AuthService:
    def __init__(self):
        self.tokens = {}
        self.lock = threading.RLock()

    def issue_token(self, user_id: int) -> str:
        token = uuid.uuid4().hex
        with self.lock:
            self.tokens[token] = {'user_id': user_id, 'issued_at': time.time()}
        return token

    def validate(self, token: str) -> Optional[int]:
        with self.lock:
            info = self.tokens.get(token)
            return info['user_id'] if info else None

class MessageQueueWorker(threading.Thread):
    def __init__(self, queue: Queue, repo: SQLiteRepo):
        super().__init__(daemon=True)
        self.queue = queue
        self._stop = threading.Event()
        self.repo = repo

    def run(self):
        while not self._stop.is_set():
            try:
                task = self.queue.get(timeout=0.01)
            except Empty:
                continue
            try:
                self._process(task)
            finally:
                self.queue.task_done()

    def _process(self, task):
        t = task.get('type')
        if t == 'welcome_email':
            key = 'welcome_sent_count'
            cur = self.repo.get_meta(key)
            cur_val = int(cur) if cur else 0
            cur_val += 1
            self.repo.set_meta(key, str(cur_val))

    def stop(self):
        self._stop.set()

class UserService:
    def __init__(self, repo: SQLiteRepo, queue: Queue):
        self.repo = repo
        self.queue = queue

    def _hash_password(self, password: str) -> str:
        return hashlib.sha256(password.encode()).hexdigest()

    def register(self, name: str, email: str, age: int, password: str) -> Dict[str, Any]:
        if age < 13:
            raise ValueError('user_too_young')
        if '@' not in email:
            raise ValueError('invalid_email')
        existing = self.repo.get_user_by_email(email)
        if existing:
            raise ValueError('email_taken')
        phash = self._hash_password(password)
        user = self.repo.add_user(name, email, age, phash)
        self.queue.put({'type': 'welcome_email', 'email': email})
        return user

    def authenticate(self, email: str, password: str) -> Optional[int]:
        user = self.repo.get_user_by_email(email)
        if not user:
            return None
        if user['password_hash'] != self._hash_password(password):
            return None
        return user['id']

class OrderService:
    def __init__(self, repo: SQLiteRepo, gateway: ExternalPaymentGateway, cache: SimpleCache):
        self.repo = repo
        self.gateway = gateway
        self.cache = cache

    def create_order(self, user_id: int, item: str, amount: float) -> Dict[str, Any]:
        if amount <= 0:
            raise ValueError('invalid_amount')
        user = self.repo.get_user_by_id(user_id)
        if not user:
            raise KeyError('user_not_found')
        charge = self.gateway.charge(user['email'], amount)
        order = self.repo.create_order(user_id, item, amount)
        self.cache.set(f'recent_order:{user_id}', order)
        return {'order': order, 'charge': charge}

class ApiServer:
    def __init__(self, user_service: UserService, order_service: OrderService, auth_service: AuthService):
        self.user_service = user_service
        self.order_service = order_service
        self.auth_service = auth_service

    def post_register(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        try:
            user = self.user_service.register(payload['name'], payload['email'], int(payload['age']), payload['password'])
            return {'status': 201, 'data': user}
        except ValueError as e:
            return {'status': 400, 'error': str(e)}

    def post_login(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        uid = self.user_service.authenticate(payload['email'], payload['password'])
        if not uid:
            return {'status': 401, 'error': 'invalid_credentials'}
        token = self.auth_service.issue_token(uid)
        return {'status': 200, 'data': {'token': token}}

    def post_order(self, token: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        user_id = self.auth_service.validate(token)
        if not user_id:
            return {'status': 401, 'error': 'unauthorized'}
        try:
            res = self.order_service.create_order(user_id, payload['item'], float(payload['amount']))
            return {'status': 201, 'data': res}
        except (ValueError, KeyError, RuntimeError) as e:
            status = 400 if isinstance(e, (ValueError, KeyError)) else 502
            return {'status': status, 'error': str(e)}

    def get_orders(self, token: str) -> Dict[str, Any]:
        user_id = self.auth_service.validate(token)
        if not user_id:
            return {'status': 401, 'error': 'unauthorized'}
        orders = self.order_service.repo.get_orders_for_user(user_id)
        return {'status': 200, 'data': orders}



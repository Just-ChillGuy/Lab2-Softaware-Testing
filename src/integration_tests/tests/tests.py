import sys
import os

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(ROOT)

import unittest
import time
import threading
from queue import Queue

from db.db import (
    SQLiteRepo,
    ExternalPaymentGateway,
    SimpleCache,
    AuthService,
    UserService,
    OrderService,
    ApiServer,
    MessageQueueWorker
)

class IntegrationTests(unittest.TestCase):
    def setUp(self):
        self.repo = SQLiteRepo()
        self.queue = Queue()
        self.gateway = ExternalPaymentGateway()
        self.cache = SimpleCache(maxsize=10)
        self.user_service = UserService(self.repo, self.queue)
        self.order_service = OrderService(self.repo, self.gateway, self.cache)
        self.auth = AuthService()
        self.api = ApiServer(self.user_service, self.order_service, self.auth)
        self.worker = MessageQueueWorker(self.queue, self.repo)
        self.worker.start()

    def tearDown(self):
        self.worker.stop()
        self.worker.join(timeout=1)

    def test_register_and_background_welcome(self):
        """register_and_background_welcome — register user + background welcome email processing"""
        payload = {'name': 'Test User', 'email': 'test@example.com', 'age': 20, 'password': 'secret'}
        r = self.api.post_register(payload)
        self.assertEqual(r['status'], 201)
        user = r['data']
        self.assertIsNotNone(user.get('id'))
        self.queue.join()
        count = int(self.repo.get_meta('welcome_sent_count') or '0')
        self.assertEqual(count, 1)
        self.assertIsNotNone(self.repo.get_user_by_email('test@example.com'))

    def test_order_success_flow(self):
        """order_success_flow — full flow: register, login, charge, persist, cache"""
        self.api.post_register({'name': 'Buyer', 'email': 'buyer@example.com', 'age': 30, 'password': 'pwd'})
        login = self.api.post_login({'email': 'buyer@example.com', 'password': 'pwd'})
        self.assertEqual(login['status'], 200)
        token = login['data']['token']
        uid = self.auth.validate(token)
        resp = self.api.post_order(token, {'item': 'Widget', 'amount': 9.99})
        self.assertEqual(resp['status'], 201)
        self.assertEqual(len(self.gateway.charges), 1)
        orders = self.repo.get_orders_for_user(uid)
        self.assertEqual(len(orders), 1)
        cached = self.cache.get(f'recent_order:{uid}')
        self.assertIsNotNone(cached)

    def test_order_external_failure_rolls_back(self):
        """order_external_failure_rolls_back — gateway failure should not create DB order"""
        self.api.post_register({'name': 'Buyer2', 'email': 'buyer2@example.com', 'age': 25, 'password': 'pwd2'})
        login = self.api.post_login({'email': 'buyer2@example.com', 'password': 'pwd2'})
        self.assertEqual(login['status'], 200)
        token = login['data']['token']
        uid = self.auth.validate(token)
        self.gateway.should_fail = True
        resp = self.api.post_order(token, {'item': 'Expensive', 'amount': 1000.0})
        self.assertEqual(resp['status'], 502)
        orders = self.repo.get_orders_for_user(uid)
        self.assertEqual(len(orders), 0)

    def test_auth_required_for_orders(self):
        """auth_required_for_orders — requests without valid token are rejected"""
        bad_token = 'invalidtoken'
        resp = self.api.post_order(bad_token, {'item': 'X', 'amount': 1.0})
        self.assertEqual(resp['status'], 401)

    def test_concurrent_orders_and_cache_consistency(self):
        """concurrent_orders_and_cache_consistency — concurrency: multiple orders, cache correctness"""
        self.api.post_register({'name': 'Concurrent', 'email': 'conc@example.com', 'age': 40, 'password': 'pwd'})
        login = self.api.post_login({'email': 'conc@example.com', 'password': 'pwd'})
        self.assertEqual(login['status'], 200)
        token = login['data']['token']
        uid = self.auth.validate(token)
        def make_order(i):
            return self.api.post_order(token, {'item': f'Item-{i}', 'amount': 1.0 + i})
        threads = []
        results = []
        for i in range(5):
            t = threading.Thread(target=lambda idx=i: results.append(make_order(idx)))
            threads.append(t)
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(len(self.gateway.charges), 5)
        orders = self.repo.get_orders_for_user(uid)
        self.assertEqual(len(orders), 5)
        cached = self.cache.get(f'recent_order:{uid}')
        self.assertIsNotNone(cached)
        self.assertAlmostEqual(cached['created_at'], orders[0]['created_at'], places=2)

def print_report():
    report = {
        'project': 'Single-file Python demo (API → service → repo → external gateway + background queue + cache)',
        'integration_points': [
            'API handlers → Business services (UserService, OrderService)',
            'OrderService → ExternalPaymentGateway (external dependency)',
            'Business services → SQLite repo (persistence, transactions)',
            'UserService → MessageQueue (background jobs)',
            'OrderService → Cache (recent-order caching)'
        ],
        'tests': [
            'test_register_and_background_welcome: register user + background welcome email processing',
            'test_order_success_flow: full flow including external charge and persistence',
            'test_order_external_failure_rolls_back: simulate payment gateway failure and ensure no DB write remains',
            'test_auth_required_for_orders: ensure endpoints enforce auth',
            'test_concurrent_orders_and_cache_consistency: concurrency across orders, cache correctness'
        ]
    }
    print('\n=== REPORT ===')
    for k, v in report.items():
        print(f"\n{k}:\n")
        if isinstance(v, list):
            for it in v:
                print(' -', it)
        else:
            print(' ', v)
    print('\n=== END REPORT ===\n')

class SeparatedTestResult(unittest.TextTestResult):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.executed_tests = []
        self._start_times = {}

    def startTest(self, test):
        super().startTest(test)
        desc = test.shortDescription() or f"{test.__class__.__name__}.{test._testMethodName}"
        self._start_times[desc] = time.time()
        self.executed_tests.append({'name': desc, 'status': 'RUNNING', 'duration': None})
        print("\n" + "=" * 70)
        print(f"RUNNING: {desc}")

    def _set_result(self, test, status):
        desc = test.shortDescription() or f"{test.__class__.__name__}.{test._testMethodName}"
        start = self._start_times.get(desc, None)
        duration = (time.time() - start) if start else None
        for entry in reversed(self.executed_tests):
            if entry['name'] == desc and entry['status'] == 'RUNNING':
                entry['status'] = status
                entry['duration'] = duration
                break

    def addSuccess(self, test):
        super().addSuccess(test)
        self._set_result(test, 'OK')
        print(f" -> RESULT: OK")

    def addFailure(self, test, err):
        super().addFailure(test, err)
        self._set_result(test, 'FAIL')
        print(f" -> RESULT: FAIL")
        print(self._exc_info_to_string(err, test))

    def addError(self, test, err):
        super().addError(test, err)
        self._set_result(test, 'ERROR')
        print(f" -> RESULT: ERROR")
        print(self._exc_info_to_string(err, test))

    def addSkip(self, test, reason):
        super().addSkip(test, reason)
        self._set_result(test, f'SKIPPED({reason})')
        print(f" -> RESULT: SKIPPED ({reason})")

def run_integration_tests_and_report():
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromTestCase(IntegrationTests)
    runner = unittest.TextTestRunner(verbosity=2, resultclass=SeparatedTestResult)
    result = runner.run(suite)
    print("\n" + "=" * 70)
    print("ALL EXECUTED TESTS (name — status — duration(s)):")
    for i, t in enumerate(result.executed_tests, 1):
        dur = f"{t['duration']:.4f}s" if t['duration'] is not None else "N/A"
        print(f"{i}. {t['name']} — {t['status']} — {dur}")
    print("=" * 70 + "\n")
    print_report()
    return result

if __name__ == '__main__':
    run_integration_tests_and_report()

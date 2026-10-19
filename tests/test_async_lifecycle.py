"""Test real transaction code without starting ROS or an executor."""

import importlib.util
from pathlib import Path
from threading import Barrier, Thread
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import patch


SOURCE = Path(__file__).resolve().parents[1] / 'ros_actor/lib_main.py'


def load_runtime():
    """Only stub imports of ROS/pytwb; actor implementations are not mocked."""
    modules = {name: ModuleType(name) for name in (
        'yaml', 'rclpy', 'rclpy.action', 'rclpy.node', 'rclpy.callback_groups',
        'pytwb', 'pytwb.lib_main', 'actor_interfaces', 'actor_interfaces.action')}
    modules['rclpy.action'].ActionServer = object
    modules['rclpy.action'].ActionClient = object
    modules['rclpy.node'].Node = object
    modules['rclpy.callback_groups'].ReentrantCallbackGroup = object
    modules['actor_interfaces.action'].ComplexMotion = object
    modules['pytwb'].lib_main = modules['pytwb.lib_main']
    spec = importlib.util.spec_from_file_location('runtime_under_test', SOURCE)
    runtime = importlib.util.module_from_spec(spec)
    modules[spec.name] = runtime
    with patch.dict('sys.modules', modules):
        spec.loader.exec_module(runtime)
    return runtime


class AsyncLifecycleTest(unittest.TestCase):
    def setUp(self):
        self.runtime = load_runtime()
        self.runtime.max_task = 2
        self.tasks = []
        self.runtime.actor_executor = SimpleNamespace(create_task=self.schedule)
        self.received = []

    def schedule(self, callback, *args):
        self.tasks.append((callback, args))
        return object()

    def finish_task(self):
        callback, args = self.tasks.pop(0)
        callback(*args)

    def start_async_actor(self, callback=None):
        self.completion = None

        def impl(done):
            self.completion = done

        actor = self.runtime.AsyncActor('async', impl)
        transaction = actor.run(self.runtime.TransactionType.ASYNC, (), {},
                                callback=callback or self.received.append)
        return transaction

    def start_sync_actor(self, impl=None, callback=None, timed=None):
        actor = self.runtime.SyncActor('sync', impl or (lambda: 'done'))
        return actor.run(self.runtime.TransactionType.ASYNC, (), {},
                         callback=callback or self.received.append, timed=timed)

    def assert_closed(self, transaction):
        self.assertFalse(transaction.is_active)
        self.assertEqual(self.runtime.active_tran, [])

    def test_async_abort_suppresses_late_completion(self):
        transaction = self.start_async_actor()
        transaction.abort()
        self.completion('late')
        self.assertEqual(self.received, [])
        self.assert_closed(transaction)

    def test_async_close_suppresses_late_completion(self):
        transaction = self.start_async_actor()
        transaction.close()
        self.completion('late')
        self.assertEqual(self.received, [])
        self.assert_closed(transaction)

    def test_normal_none_result_is_not_a_cancellation(self):
        for kind in ('async', 'sync'):
            with self.subTest(kind=kind):
                self.received.clear()
                if kind == 'async':
                    transaction = self.start_async_actor()
                    self.completion(None)
                else:
                    transaction = self.start_sync_actor(impl=lambda: None)
                    self.finish_task()
                self.assertEqual(self.received, [None])
                self.assert_closed(transaction)
                self.assertEqual(self.runtime.num_task, 0)

    def test_async_completion_is_delivered_once(self):
        transaction = self.start_async_actor()
        self.completion('done')
        self.completion('duplicate')
        self.assertEqual(self.received, ['done'])
        self.assert_closed(transaction)

    def test_async_callback_exception_still_closes_transaction(self):
        def fail(result):
            raise RuntimeError('consumer failed')

        transaction = self.start_async_actor(callback=fail)
        with self.assertRaisesRegex(RuntimeError, 'consumer failed'):
            self.completion('done')
        self.assert_closed(transaction)

    def test_sync_abort_is_callable_and_reports_failure_once(self):
        transaction = self.start_sync_actor()
        transaction.abort()
        transaction.abort()
        self.assertEqual(self.received, [False])
        self.assert_closed(transaction)
        # An aborted job still occupies its executor slot until it exits.
        self.assertEqual(self.runtime.num_task, 1)
        self.finish_task()
        self.assertEqual(self.runtime.num_task, 0)
        self.assertEqual(self.received, [False])

    def test_aborted_queued_sync_job_does_not_run_implementation(self):
        calls = []
        transaction = self.start_sync_actor(impl=lambda: calls.append('run'))
        transaction.abort()
        self.finish_task()
        self.assertEqual(calls, [])
        self.assertEqual(self.runtime.num_task, 0)

    def test_abort_running_sync_job_does_not_deliver_late_success(self):
        def impl():
            transaction.abort()
            return 'late success'

        transaction = self.start_sync_actor(impl=impl)
        self.finish_task()
        self.assertEqual(self.received, [False])
        self.assert_closed(transaction)
        self.assertEqual(self.runtime.num_task, 0)

    def test_sync_close_does_not_leak_executor_slot(self):
        transaction = self.start_sync_actor()
        transaction.close()
        self.finish_task()
        self.assertEqual(self.received, [])
        self.assertEqual(self.runtime.num_task, 0)

    def test_sync_completion_then_abort_does_not_notify_twice(self):
        transaction = self.start_sync_actor()
        self.finish_task()
        transaction.abort()
        self.assertEqual(self.received, ['done'])
        self.assert_closed(transaction)
        self.assertEqual(self.runtime.num_task, 0)

    def test_sync_implementation_exception_releases_slot(self):
        def fail():
            raise RuntimeError('implementation failed')

        transaction = self.start_sync_actor(impl=fail)
        with self.assertRaisesRegex(RuntimeError, 'implementation failed'):
            self.finish_task()
        self.assert_closed(transaction)
        self.assertEqual(self.runtime.num_task, 0)

    def test_sync_callback_exception_releases_slot(self):
        def fail(result):
            raise RuntimeError('consumer failed')

        transaction = self.start_sync_actor(callback=fail)
        with self.assertRaisesRegex(RuntimeError, 'consumer failed'):
            self.finish_task()
        self.assert_closed(transaction)
        self.assertEqual(self.runtime.num_task, 0)

    def test_callbacks_can_abort_other_transactions_without_deadlock(self):
        for kind in ('async', 'sync', 'abort'):
            with self.subTest(kind=kind):
                runtime = load_runtime()
                runtime.max_task = 2
                tasks, completions, transactions, received, errors = [], [], [], [], []
                runtime.actor_executor = SimpleNamespace(
                    create_task=lambda f, *a: tasks.append((f, a)))
                barrier = Barrier(2)

                def consumer(index):
                    def callback(result):
                        barrier.wait(timeout=2)
                        transactions[1 - index].abort()
                        received.append((index, result))
                    return callback

                for index in range(2):
                    actor = (runtime.AsyncActor(str(index), completions.append)
                             if kind == 'async' else
                             runtime.SyncActor(str(index), lambda: 'done'))
                    transactions.append(actor.run(runtime.TransactionType.ASYNC, (), {},
                                                  callback=consumer(index)))
                if kind == 'async':
                    actions = [lambda f=f: f('done') for f in completions]
                elif kind == 'sync':
                    actions = [lambda f=f, a=a: f(*a) for f, a in tasks]
                else:
                    actions = [t.abort for t in transactions]

                def invoke(action):
                    try:
                        action()
                    except Exception as error:
                        errors.append(error)

                threads = [Thread(target=invoke, args=(a,), daemon=True) for a in actions]
                for thread in threads:
                    thread.start()
                for thread in threads:
                    thread.join(timeout=.5)
                self.assertFalse(any(t.is_alive() for t in threads), 'callback deadlock')
                self.assertEqual(errors, [])
                expected = False if kind == 'abort' else 'done'
                self.assertEqual(sorted(received), [(0, expected), (1, expected)])
                if kind == 'abort':
                    for function, args in tasks:
                        function(*args)
                self.assertEqual(runtime.active_tran, [])
                self.assertEqual(runtime.num_task, 0)

    def test_watchdog_expires_all_due_transactions_in_one_pass(self):
        transactions = [self.start_async_actor() for _ in range(3)]
        for transaction in transactions:
            transaction.is_timed = True
            transaction.time = .5
        now = max(t.start for t in transactions) + 1
        with patch.object(self.runtime.time, 'time', return_value=now):
            self.runtime.actor_watchdog()
        self.assertTrue(all(not t.is_active for t in transactions))
        self.assertEqual(self.runtime.active_tran, [])

    def test_watchdog_can_abort_timed_async_actor(self):
        transaction = self.start_async_actor()
        transaction.is_timed = True
        transaction.time = .5
        with patch.object(self.runtime.time, 'time', return_value=transaction.start + 1):
            self.runtime.actor_watchdog()
        self.completion('late')
        self.assertEqual(self.received, [])
        self.assert_closed(transaction)

    def test_watchdog_can_abort_timed_sync_job(self):
        transaction = self.start_sync_actor(timed=.5)
        with patch.object(self.runtime.time, 'time', return_value=transaction.start + 1):
            self.runtime.actor_watchdog()
        self.finish_task()
        self.assertEqual(self.received, [False])
        self.assert_closed(transaction)
        self.assertEqual(self.runtime.num_task, 0)


if __name__ == '__main__':
    unittest.main()

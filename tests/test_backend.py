import unittest
import sys
import os
import uuid
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from unittest.mock import patch

# Ensure src is in path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

from django.conf import settings
from django.tasks import Task, TaskResultStatus
from django.test import override_settings
from django.utils import timezone as dj_timezone

if not settings.configured:
    settings.configure(
        DATABASES={"default": {"ENGINE": "django.db.backends.sqlite3", "NAME": ":memory:"}},
        INSTALLED_APPS=["reproq_django"],
        TASKS={
            "default": {
                "BACKEND": "reproq_django.backend.ReproqBackend",
                "QUEUES": ["default", "test-queue", "q"],
            }
        },
    )
    import django
    django.setup()

from reproq_django.backend import ReproqBackend
from reproq_django.db import (
    format_result_id,
    parse_result_id,
    resolve_queue_db,
    should_prefix_result_ids,
)
from reproq_django.proxy import TaskResultProxy
from reproq_django.models import TaskRun, PeriodicTask, Worker

try:
    from django.tasks.exceptions import InvalidTask
except Exception:
    InvalidTask = ValueError

try:
    from django.tasks.exceptions import InvalidTaskError
except Exception:
    InvalidTaskError = InvalidTask

def my_func(*args, **kwargs):
    return args or kwargs

class TestReproqBackend(unittest.TestCase):
    def setUp(self):
        from django.core.management import call_command
        call_command('migrate', verbosity=0)
        TaskRun.objects.all().delete()
        self.backend = ReproqBackend(
            alias="default",
            params={"QUEUES": ["default", "test-queue", "q"]},
        )

    def test_enqueue_creates_model(self):
        my_func.__module__ = "test_module"

        task = Task(func=my_func, priority=10, queue_name="test-queue", backend="default", run_after=None)
        result = self.backend.enqueue(task, (1, 2), {"debug": True})
        
        self.assertIsInstance(result, TaskResultProxy)
        
        # Verify DB record
        run = TaskRun.objects.get(result_id=result.id)
        self.assertEqual(run.queue_name, "test-queue")
        self.assertEqual(run.priority, 10)
        self.assertEqual(run.spec_json["task_path"], "test_module.my_func")
        self.assertEqual(run.task_path, "test_module.my_func")
        self.assertEqual(run.spec_json["args"], [1, 2])
        self.assertEqual(run.spec_json["kwargs"], {"debug": True})

    def test_enqueue_rejects_unknown_queue(self):
        my_func.__module__ = "test_module"
        with self.assertRaises((InvalidTaskError, InvalidTask)):
            Task(func=my_func, priority=0, queue_name="unknown", backend="default", run_after=None)

    def test_dedupe_active(self):
        my_func.__module__ = "test_module"
        task = Task(func=my_func, priority=0, queue_name="q", backend="default", run_after=None)

        res1 = self.backend.enqueue(task, (1,), {})
        res2 = self.backend.enqueue(task, (1,), {})

        self.assertEqual(res1.id, res2.id)
        self.assertEqual(TaskRun.objects.count(), 1)

    def test_scheduling_names_remain_function_kwargs(self):
        from reproq_django.serialization import decode_args_kwargs
        my_func.__module__ = "test_module"
        task = Task(func=my_func, priority=0, queue_name="q", backend="default", run_after=None)
        payload = {"run_after": timedelta(minutes=5), "priority": 42, "lock_key": "argument",
                   "concurrency_key": "argument-key", "concurrency_limit": 7}
        result = self.backend.enqueue(task, (), payload)
        run = TaskRun.objects.get(pk=result.id)
        _, restored = decode_args_kwargs(run.spec_json["args"], run.spec_json["kwargs"])
        self.assertEqual(restored, payload)
        self.assertIsNone(run.run_after)
        self.assertEqual(run.priority, 0)
        self.assertIsNone(run.lock_key)
        self.assertIsNone(run.concurrency_key)
        self.assertEqual(run.concurrency_limit, 0)

    def test_native_using_options_and_same_named_arguments_coexist(self):
        my_func.__module__ = "test_module"
        deadline = dj_timezone.now() + timedelta(minutes=5)
        task = Task(func=my_func, priority=0, queue_name="q", backend="default", run_after=None)
        task = task.using(priority=42, run_after=deadline)
        result = self.backend.enqueue(task, (), {"priority": 7, "run_after": "task-argument"})
        run = TaskRun.objects.get(pk=result.id)
        self.assertEqual(run.priority, 42)
        self.assertEqual(run.run_after, deadline)
        self.assertEqual(run.spec_json["kwargs"], {"priority": 7, "run_after": "task-argument"})

    def test_owned_context_updates_taskrun_metadata(self):
        from reproq_django.context import ExecutionClaim, TaskContext
        my_func.__module__ = "test_module"
        task = Task(func=my_func, priority=0, queue_name="q", backend="default", run_after=None)
        result = self.backend.enqueue(task, (), {})
        token = uuid.uuid4()
        TaskRun.objects.filter(result_id=result.result_id).update(
            status="RUNNING", leased_by="metadata-worker", lease_token=token,
            attempts=1, worker_ids=['metadata-worker'],
            leased_until=dj_timezone.now() + timedelta(minutes=1),
        )
        context = TaskContext.from_claim(ExecutionClaim(
            result_id=result.result_id, budget_attempt=1, expected_task_path=task.module_path,
            db_alias='default', worker_id='metadata-worker', lease_token=str(token),
        ))
        context.metadata['step'] = 'running'
        context.save_metadata()
        run = TaskRun.objects.get(result_id=result.id)
        self.assertEqual(run.metadata_json, {"step": "running"})

    def test_spec_hash_handles_decimal_and_uuid(self):
        my_func.__module__ = "test_module"
        task = Task(func=my_func, priority=0, queue_name="q", backend="default", run_after=None)
        token = uuid.uuid4()

        result = self.backend.enqueue(
            task, (), {"amount": Decimal("1.23"), "token": token}
        )
        run = TaskRun.objects.get(result_id=result.id)

        self.assertEqual(len(run.spec_hash), 64)

    def test_default_max_attempts(self):
        my_func.__module__ = "test_module"
        task = Task(func=my_func, priority=0, queue_name="q", backend="default", run_after=None)

        result = self.backend.enqueue(task, (), {})
        run = TaskRun.objects.get(result_id=result.id)

        self.assertEqual(run.max_attempts, 3)
        self.assertEqual(run.spec_json["exec"]["max_attempts"], 3)

    def test_bulk_enqueue_dedupes(self):
        my_func.__module__ = "test_module"
        task = Task(func=my_func, priority=0, queue_name="q", backend="default", run_after=None)

        results = self.backend.bulk_enqueue(
            [
                (task, (1,), {"debug": True}),
                (task, (1,), {"debug": True}),
            ]
        )

        self.assertEqual(results[0].id, results[1].id)
        self.assertEqual(TaskRun.objects.count(), 1)

    def test_bulk_enqueue_applies_expires_in(self):
        my_func.__module__ = "test_module"
        task = Task(func=my_func, priority=0, queue_name="q", backend="default", run_after=None)

        if self.backend.options is None:
            self.backend.options = {}
        self.backend.options["EXPIRES_IN"] = timedelta(minutes=5)

        results = self.backend.bulk_enqueue([(task, (), {})])
        run = TaskRun.objects.get(result_id=results[0].id)

        self.assertIsNotNone(run.expires_at)
        self.assertGreater(run.expires_at, dj_timezone.now())

class TestReproqManagement(unittest.TestCase):
    @patch("reproq_django.management.commands.reproq.Command.get_dsn")
    @patch("reproq_django.capabilities._capture_worker_capabilities")
    def test_check_command(self, mock_version, mock_get_dsn):
        mock_version.return_value = b'{"worker_version":"owned","schema_id":"reproq-schema-v2","schema_version":2,"executor_protocol":2}'
        mock_get_dsn.return_value = "postgres://user:pass@localhost:5432/db"
        from django.core.management import call_command
        from io import StringIO
        out = StringIO()
        call_command("reproq", "check", stdout=out)
        self.assertIn("Configuration looks good", out.getvalue())

    @patch("os.execvpe")
    @patch("reproq_django.management.commands.reproq.Command._resolve_worker_bin")
    @patch("reproq_django.management.commands.reproq.Command.get_dsn")
    @patch("reproq_django.management.commands.reproq.require_worker_capabilities")
    def test_worker_command_replaces_foreground_process(self, mock_capabilities, mock_get_dsn, mock_resolve_bin, mock_exec):
        from django.core.management import call_command
        mock_get_dsn.return_value = "postgres://user:pass@localhost:5432/db"
        mock_resolve_bin.return_value = ("reproq", "/usr/local/bin/reproq", True)
        call_command("reproq", "worker", "--concurrency", "5")
        self.assertTrue(mock_exec.called)
        args = mock_exec.call_args[0][1]
        self.assertIn("worker", args)
        self.assertIn("--concurrency", args)
        self.assertIn("5", args)
        self.assertNotIn("--dsn", args)
        self.assertNotIn(mock_get_dsn.return_value, args)
        self.assertEqual(mock_exec.call_args[0][2]["REPROQ_DATABASE_URL"], mock_get_dsn.return_value)

class TestReproqModels(unittest.TestCase):
    def test_periodic_task_creation(self):
        pt = PeriodicTask.objects.create(
            name="daily_cleanup",
            cron_expr="0 0 * * *",
            task_path="myapp.tasks.cleanup",
            next_run_at=datetime.now(timezone.utc)
        )
        self.assertEqual(str(pt), "daily_cleanup (0 0 * * *)")

    def test_worker_status(self):
        w = Worker.objects.create(
            worker_id="w1",
            hostname="host1",
            concurrency=10,
            queues=["default"]
        )
        self.assertEqual(w.worker_id, "w1")

class TestTaskResultProxyStatusMapping(unittest.TestCase):
    def setUp(self):
        from django.core.management import call_command
        call_command('migrate', verbosity=0)
        TaskRun.objects.all().delete()
        self.backend = ReproqBackend(
            alias="default",
            params={"QUEUES": ["default"]},
        )

    def test_ready_maps_to_native_ready(self):
        run = TaskRun.objects.create(
            spec_json={},
            spec_hash="ready" + "0" * 59,
            status="READY",
        )
        proxy = TaskResultProxy(str(run.result_id), self.backend)
        self.assertEqual(proxy.status, TaskResultStatus.READY)
        self.assertEqual(proxy.raw_status, "READY")

    def test_cancelled_projects_to_native_failed_and_preserves_raw_status(self):
        run = TaskRun.objects.create(
            spec_json={},
            spec_hash="cancel" + "0" * 58,
            status="CANCELLED",
        )
        proxy = TaskResultProxy(str(run.result_id), self.backend)
        self.assertEqual(proxy.status, TaskResultStatus.FAILED)
        self.assertEqual(proxy.raw_status, "CANCELLED")

    def test_wait_returns_on_cancelled(self):
        run = TaskRun.objects.create(
            spec_json={},
            spec_hash="cancelwait" + "0" * 54,
            status="CANCELLED",
        )
        proxy = TaskResultProxy(str(run.result_id), self.backend)
        self.assertIs(proxy.wait(timeout=0, poll_interval=0), proxy)
        self.assertEqual(proxy.status, TaskResultStatus.FAILED)
        self.assertEqual(proxy.raw_status, "CANCELLED")

class TestMultiDatabaseRouting(unittest.TestCase):
    @override_settings(
        DATABASES={
            "default": {"ENGINE": "django.db.backends.sqlite3", "NAME": ":memory:"},
            "secondary": {"ENGINE": "django.db.backends.sqlite3", "NAME": ":memory:"},
        },
        REPROQ_QUEUE_DATABASES={"secondary-queue": "secondary"},
    )
    def test_queue_routes_to_secondary(self):
        alias = resolve_queue_db("secondary-queue")
        self.assertEqual(alias, "secondary")
        self.assertTrue(should_prefix_result_ids())
        formatted = format_result_id(123, "secondary")
        parsed_alias, raw_id = parse_result_id(formatted)
        self.assertEqual(parsed_alias, "secondary")
        self.assertEqual(raw_id, "123")

if __name__ == "__main__":
    unittest.main()

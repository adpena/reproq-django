import unittest
import sys
import os
import json
import subprocess
import uuid
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from unittest.mock import MagicMock, patch

# Ensure src is in path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

from django.conf import settings
from django.tasks import Task, TaskResultStatus
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
from reproq_django.proxy import TaskResultProxy
from reproq_django.models import TaskRun, PeriodicTask, Worker

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

    def test_dedupe_active(self):
        my_func.__module__ = "test_module"
        task = Task(func=my_func, priority=0, queue_name="q", backend="default", run_after=None)

        res1 = self.backend.enqueue(task, (1,), {})
        res2 = self.backend.enqueue(task, (1,), {})

        self.assertEqual(res1.id, res2.id)
        self.assertEqual(TaskRun.objects.count(), 1)

    def test_run_after_kwarg_is_reserved(self):
        my_func.__module__ = "test_module"
        task = Task(func=my_func, priority=0, queue_name="q", backend="default", run_after=None)
        delay = timedelta(minutes=5)

        result = self.backend.enqueue(task, (), {"run_after": delay, "debug": True})
        run = TaskRun.objects.get(result_id=result.id)

        self.assertIsInstance(run.spec_json.get("run_after"), str)
        self.assertEqual(run.spec_json["kwargs"], {"debug": True})
        self.assertIsNotNone(run.run_after)
        self.assertGreater(run.run_after, dj_timezone.now() + timedelta(minutes=4))

    def test_priority_override(self):
        my_func.__module__ = "test_module"
        task = Task(func=my_func, priority=0, queue_name="q", backend="default", run_after=None)

        result = self.backend.enqueue(task, (), {"priority": 42})
        run = TaskRun.objects.get(result_id=result.id)

        self.assertEqual(run.priority, 42)
        self.assertEqual(run.spec_json["priority"], 42)
        self.assertNotIn("priority", run.spec_json["kwargs"])

    def test_lock_key_changes_spec_hash(self):
        my_func.__module__ = "test_module"
        task = Task(func=my_func, priority=0, queue_name="q", backend="default", run_after=None)

        res1 = self.backend.enqueue(task, (), {"lock_key": "alpha"})
        res2 = self.backend.enqueue(task, (), {"lock_key": "beta"})

        self.assertNotEqual(res1.id, res2.id)
        self.assertEqual(TaskRun.objects.count(), 2)

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
    @patch("django.db.connection.introspection.table_names", return_value=["task_runs"])
    @patch("reproq_django.management.commands.reproq.Command.get_dsn")
    @patch("subprocess.check_output")
    def test_check_command(self, mock_version, mock_get_dsn, _mock_tables):
        mock_version.return_value = b"reproq version v0.1.0"
        mock_get_dsn.return_value = "postgres://user:pass@localhost:5432/db"
        from django.core.management import call_command
        from io import StringIO
        out = StringIO()
        call_command("reproq", "check", stdout=out)
        self.assertIn("Configuration looks good", out.getvalue())

    @patch("subprocess.run")
    @patch("reproq_django.management.commands.reproq.Command._resolve_worker_bin")
    @patch("reproq_django.management.commands.reproq.Command.get_dsn")
    def test_worker_command_spawns_process(self, mock_get_dsn, mock_resolve_bin, mock_run):
        from django.core.management import call_command
        mock_get_dsn.return_value = "postgres://user:pass@localhost:5432/db"
        mock_resolve_bin.return_value = ("reproq", "/usr/local/bin/reproq", True)
        # We don't actually want to run it, just see if it calls subprocess
        with patch("sys.exit"): # avoid exit on keyboard interrupt
            try:
                call_command("reproq", "worker", "--concurrency", "5")
            except Exception:
                pass
        
        self.assertTrue(mock_run.called)
        args = mock_run.call_args[0][0]
        self.assertIn("worker", args)
        self.assertIn("--concurrency", args)
        self.assertIn("5", args)

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

    def test_ready_maps_to_pending_when_available(self):
        run = TaskRun.objects.create(
            spec_json={},
            spec_hash="ready" + "0" * 59,
            status="READY",
        )
        proxy = TaskResultProxy(str(run.result_id), self.backend)
        if "PENDING" in TaskResultStatus.__members__:
            self.assertEqual(proxy.status, TaskResultStatus.PENDING)
        else:
            self.assertEqual(proxy.raw_status, "READY")

    def test_cancelled_maps_when_available(self):
        run = TaskRun.objects.create(
            spec_json={},
            spec_hash="cancel" + "0" * 58,
            status="CANCELLED",
        )
        proxy = TaskResultProxy(str(run.result_id), self.backend)
        if "CANCELLED" in TaskResultStatus.__members__:
            self.assertEqual(proxy.status, TaskResultStatus.CANCELLED)
        else:
            self.assertEqual(proxy.raw_status, "CANCELLED")

    def test_wait_returns_on_cancelled(self):
        run = TaskRun.objects.create(
            spec_json={},
            spec_hash="cancelwait" + "0" * 54,
            status="CANCELLED",
        )
        proxy = TaskResultProxy(str(run.result_id), self.backend)
        if "CANCELLED" in TaskResultStatus.__members__:
            self.assertIs(proxy.wait(timeout=0.1, poll_interval=0), proxy)
        else:
            self.assertEqual(proxy.raw_status, "CANCELLED")

if __name__ == "__main__":
    unittest.main()

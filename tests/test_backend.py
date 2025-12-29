import unittest
import sys
import os
import json
import subprocess
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, patch

# Ensure src is in path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

from django.conf import settings
from django.tasks import Task, TaskResultStatus

if not settings.configured:
    settings.configure(
        DATABASES={"default": {"ENGINE": "django.db.backends.sqlite3", "NAME": ":memory:"}},
        INSTALLED_APPS=["reproq_django"],
        TASKS={"default": {"BACKEND": "reproq_django.backend.ReproqBackend"}},
    )
    import django
    django.setup()

from reproq_django.backend import ReproqBackend
from reproq_django.proxy import TaskResultProxy
from reproq_django.models import TaskRun, PeriodicTask, Worker

class TestReproqBackend(unittest.TestCase):
    def setUp(self):
        from django.core.management import call_command
        call_command('migrate', verbosity=0)
        self.backend = ReproqBackend(alias="default", params={})

    def test_enqueue_creates_model(self):
        def my_func(x, y): return x + y
        my_func.__module__ = "test_module"

        task = Task(func=my_func, priority=10, queue_name="test-queue", backend="default")
        result = self.backend.enqueue(task, (1, 2), {"debug": True})
        
        self.assertIsInstance(result, TaskResultProxy)
        
        # Verify DB record
        run = TaskRun.objects.get(result_id=result.id)
        self.assertEqual(run.queue_name, "test-queue")
        self.assertEqual(run.priority, 10)
        self.assertEqual(run.spec_json["task_path"], "test_module.my_func")
        self.assertEqual(run.spec_json["args"], [1, 2])
        self.assertEqual(run.spec_json["kwargs"], {"debug": True})

    def test_dedupe_active(self):
        def my_func(x): return x
        my_func.__module__ = "test_module"
        task = Task(func=my_func, priority=0, queue_name="q", backend="default")

        res1 = self.backend.enqueue(task, (1,), {})
        res2 = self.backend.enqueue(task, (1,), {})

        self.assertEqual(res1.id, res2.id)
        self.assertEqual(TaskRun.objects.count(), 1)

class TestReproqManagement(unittest.TestCase):
    def test_check_command(self):
        from django.core.management import call_command
        from io import StringIO
        out = StringIO()
        call_command("reproq", "check", stdout=out)
        self.assertIn("Configuration looks good", out.getvalue())

    @patch("subprocess.run")
    def test_worker_command_spawns_process(self, mock_run):
        from django.core.management import call_command
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

if __name__ == "__main__":
    unittest.main()

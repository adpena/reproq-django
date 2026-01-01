import os
import sys
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

from django.conf import settings

if not settings.configured:
    settings.configure(
        DATABASES={"default": {"ENGINE": "django.db.backends.sqlite3", "NAME": ":memory:"}},
        INSTALLED_APPS=["reproq_django"],
    )
    import django
    django.setup()

from django.core.management import call_command
from reproq_django.models import PeriodicTask
from reproq_django import recurring as recurring_module


class TestRecurringTasks(unittest.TestCase):
    def setUp(self):
        call_command("migrate", verbosity=0)
        PeriodicTask.objects.all().delete()
        recurring_module._registry.clear()

    def test_sync_recurring_tasks_creates_periodic_task(self):
        @recurring_module.recurring(schedule="*/5 * * * *", key="test-recurring", args=(1,), kwargs={"x": 2})
        def dummy_task():
            return "ok"

        count = recurring_module.sync_recurring_tasks(using="default")
        self.assertEqual(count, 1)
        task = PeriodicTask.objects.get(name="test-recurring")
        self.assertEqual(task.queue_name, "default")
        self.assertEqual(task.payload_json, {"args": [1], "kwargs": {"x": 2}})
        self.assertTrue(task.enabled)


if __name__ == "__main__":
    unittest.main()

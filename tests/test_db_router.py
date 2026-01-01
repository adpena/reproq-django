from django.test import SimpleTestCase, override_settings

from reproq_django.db_router import ReproqRouter
from reproq_django.models import TaskRun


class TestReproqRouter(SimpleTestCase):
    def test_default_routes_to_default_alias(self):
        router = ReproqRouter()
        self.assertEqual(router.db_for_read(TaskRun), "default")
        self.assertEqual(router.db_for_write(TaskRun), "default")

    @override_settings(
        REPROQ_DEFAULT_DB_ALIAS="queues",
        REPROQ_QUEUE_DATABASES={"bulk": "queues"},
        DATABASES={
            "default": {"ENGINE": "django.db.backends.sqlite3", "NAME": ":memory:"},
            "queues": {"ENGINE": "django.db.backends.sqlite3", "NAME": ":memory:"},
        },
    )
    def test_routes_to_configured_alias(self):
        router = ReproqRouter()
        self.assertEqual(router.db_for_read(TaskRun), "queues")
        self.assertEqual(router.db_for_write(TaskRun), "queues")
        self.assertTrue(router.allow_migrate("queues", "reproq_django"))
        self.assertFalse(router.allow_migrate("default", "reproq_django"))

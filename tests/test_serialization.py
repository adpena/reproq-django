import os
import sys
from datetime import timedelta
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
from reproq_django.models import TaskRun
from reproq_django.serialization import decode_args_kwargs, encode_args_kwargs


class TestSerialization(unittest.TestCase):
    def setUp(self):
        call_command("migrate", verbosity=0)
        TaskRun.objects.all().delete()

    def test_encode_decode_timedelta(self):
        args, kwargs = encode_args_kwargs((timedelta(seconds=30),), {"delay": timedelta(minutes=1)})
        decoded_args, decoded_kwargs = decode_args_kwargs(args, kwargs, using="default")
        self.assertEqual(decoded_args[0], timedelta(seconds=30))
        self.assertEqual(decoded_kwargs["delay"], timedelta(minutes=1))

    def test_encode_decode_model(self):
        run = TaskRun.objects.create(spec_json={}, spec_hash="m" * 64)
        args, kwargs = encode_args_kwargs((run,), {})
        decoded_args, decoded_kwargs = decode_args_kwargs(args, kwargs, using="default")
        self.assertEqual(decoded_kwargs, {})
        self.assertEqual(decoded_args[0].pk, run.pk)


if __name__ == "__main__":
    unittest.main()

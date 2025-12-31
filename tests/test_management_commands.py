import os
import sys
import json
import tempfile
import tomllib
import unittest
from io import StringIO
from unittest.mock import patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

from django.conf import settings

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

from django.core.management import call_command
from django.core.management.base import CommandError
from reproq_django.models import TaskRun
from reproq_django.management.commands.reproq import Command


class TestManagementCommands(unittest.TestCase):
    def setUp(self):
        call_command("migrate", verbosity=0)
        TaskRun.objects.all().delete()

    def _create_taskrun(self, **kwargs):
        defaults = {
            "spec_json": {},
            "spec_hash": "t" * 64,
        }
        defaults.update(kwargs)
        return TaskRun.objects.create(**defaults)

    def test_cancel_command_marks_cancel(self):
        run = self._create_taskrun(spec_hash="c" * 64)
        out = StringIO()
        call_command("reproq", "cancel", "--id", str(run.result_id), stdout=out)
        run.refresh_from_db()
        self.assertTrue(run.cancel_requested)

    def test_logs_command_tails(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as handle:
            handle.write("line1\nline2\nline3\n")
            path = handle.name
        try:
            run = self._create_taskrun(spec_hash="l" * 64, logs_uri=path)
            out = StringIO()
            call_command(
                "reproq",
                "logs",
                "--id",
                str(run.result_id),
                "--tail",
                "2",
                stdout=out,
            )
            output = [line.strip() for line in out.getvalue().splitlines() if line.strip()]
            self.assertEqual(output, ["line2", "line3"])
        finally:
            os.unlink(path)

    def test_allowlist_write_updates_config(self):
        config_body = '\n'.join(
            [
                'dsn = "postgres://user:pass@localhost:5432/reproq"',
                "",
                "[worker]",
                'allowed_task_modules = ["old.module."]',
                "",
            ]
        )
        with tempfile.NamedTemporaryFile("w", delete=False, suffix=".toml", encoding="utf-8") as handle:
            handle.write(config_body)
            config_path = handle.name
        try:
            with patch(
                "reproq_django.management.commands.reproq.Command._compute_allowed_task_modules"
            ) as mock_allowlist:
                mock_allowlist.return_value = (["new.module."], ["new.module.task"], [])
                out = StringIO()
                call_command(
                    "reproq",
                    "allowlist",
                    "--write",
                    "--config",
                    config_path,
                    stdout=out,
                )
            with open(config_path, "rb") as handle:
                data = tomllib.load(handle)
            self.assertEqual(data["worker"]["allowed_task_modules"], ["new.module."])
        finally:
            os.unlink(config_path)

    def test_upgrade_invokes_install_and_migrate(self):
        with patch.object(Command, "run_install") as run_install, patch.object(
            Command, "run_migrate"
        ) as run_migrate:
            call_command("reproq", "upgrade", stdout=StringIO())
            self.assertTrue(run_install.called)
            self.assertTrue(run_migrate.called)

    def test_config_command_prints_worker_config(self):
        config_body = '\n'.join(
            [
                'dsn = "postgres://user:pass@localhost:5432/reproq"',
                "",
                "[worker]",
                'queues = ["alpha", "beta"]',
                "concurrency = 7",
                "",
            ]
        )
        with tempfile.NamedTemporaryFile("w", delete=False, suffix=".toml", encoding="utf-8") as handle:
            handle.write(config_body)
            config_path = handle.name
        try:
            out = StringIO()
            with patch.dict(os.environ, {"DATABASE_URL": ""}, clear=False):
                call_command(
                    "reproq",
                    "config",
                    "--config",
                    config_path,
                    "--mode",
                    "worker",
                    stdout=out,
                )
            payload = out.getvalue()
            start = payload.find("{")
            json_blob = payload[start:]
            config_data = json.loads(json_blob)
            self.assertEqual(config_data["worker"]["queues"], ["alpha", "beta"])
            self.assertEqual(config_data["worker"]["concurrency"], 7)
        finally:
            os.unlink(config_path)

    def test_pg_cron_requires_postgres(self):
        with self.assertRaises(CommandError):
            call_command("reproq", "pg-cron")


if __name__ == "__main__":
    unittest.main()

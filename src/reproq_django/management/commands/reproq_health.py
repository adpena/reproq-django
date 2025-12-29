from __future__ import annotations

import os
from datetime import timedelta

from django.core.management.base import BaseCommand, CommandError
from django.db import connection
from django.db.models import Count
from django.utils import timezone

from reproq_django.models import PeriodicTask, TaskRun, Worker


class Command(BaseCommand):
    help = "Check Reproq health (DB connectivity, worker heartbeat, queue stats)."

    def add_arguments(self, parser):
        parser.add_argument(
            "--stale-minutes",
            type=int,
            default=2,
            help="Minutes after which a worker is considered stale.",
        )
        parser.add_argument(
            "--recent-limit",
            type=int,
            default=5,
            help="Number of most recent tasks to display.",
        )

    def handle(self, *args, **options):
        stale_minutes = options["stale_minutes"]
        recent_limit = options["recent_limit"]
        now = timezone.now()

        self.stdout.write(self.style.MIGRATE_HEADING("Reproq health check"))

        try:
            connection.ensure_connection()
        except Exception as exc:
            raise CommandError(f"Database connection failed: {exc}") from exc

        if os.environ.get("DATABASE_URL"):
            self.stdout.write(self.style.SUCCESS("✅ DATABASE_URL set."))
        else:
            self.stdout.write(self.style.WARNING("⚠️ DATABASE_URL not set in env."))

        with connection.cursor() as cursor:
            table_names = set(connection.introspection.table_names(cursor))

        required_tables = {"task_runs", "reproq_workers", "periodic_tasks"}
        missing_tables = required_tables - table_names
        if missing_tables:
            missing_list = ", ".join(sorted(missing_tables))
            raise CommandError(f"Missing tables: {missing_list} (run migrations).")
        self.stdout.write(self.style.SUCCESS("✅ Reproq tables present."))

        task_counts = TaskRun.objects.values("status").annotate(count=Count("result_id"))
        if not task_counts:
            self.stdout.write(self.style.WARNING("⚠️ No task runs found."))
        else:
            self.stdout.write(self.style.MIGRATE_HEADING("\nTask counts"))
            for stat in task_counts:
                self.stdout.write(f"  {stat['status']}: {stat['count']}")

        self.stdout.write(self.style.MIGRATE_HEADING("\nRecent tasks"))
        recent_runs = TaskRun.objects.order_by("-enqueued_at")[:recent_limit]
        if not recent_runs:
            self.stdout.write("  (none)")
        else:
            for run in recent_runs:
                task_path = run.spec_json.get("task_path", "?")
                self.stdout.write(
                    f"  {run.result_id} {run.status} {task_path} at {run.enqueued_at}"
                )

        self.stdout.write(self.style.MIGRATE_HEADING("\nWorkers"))
        workers = Worker.objects.all()
        if not workers:
            self.stdout.write(self.style.WARNING("⚠️ No workers registered."))
            self.stdout.write(
                self.style.WARNING(
                    "   Hint: start `python manage.py reproq worker` in this service."
                )
            )
        else:
            stale_after = now - timedelta(minutes=stale_minutes)
            for worker in workers:
                status = "ALIVE" if worker.last_seen_at >= stale_after else "STALE"
                self.stdout.write(
                    f"  {worker.worker_id} ({worker.hostname}) "
                    f"{status} last_seen={worker.last_seen_at} queues={worker.queues}"
                )

        periodic_enabled = PeriodicTask.objects.filter(enabled=True).count()
        self.stdout.write(
            self.style.MIGRATE_HEADING(
                f"\nPeriodic tasks enabled: {periodic_enabled}"
            )
        )

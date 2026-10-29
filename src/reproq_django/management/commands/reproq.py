import os
import re
import shlex
import subprocess
import sys
import platform
import shutil
import urllib.request
from urllib.parse import quote
import tempfile
import hashlib
import json
import tomllib
import importlib.resources
from datetime import timedelta
from django.core.management.base import BaseCommand, CommandError
from django.conf import settings
from django.db import connections
from django.utils import timezone
from ...models import PeriodicTask, TaskRun, QueueControl
from ...db import default_db_alias, parse_result_id, queue_db_aliases
from ...recurring import periodic_definition_status, sync_recurring_tasks
from ...schema_contract import SchemaContractError, require_schema_contract
from ...schema_adoption import SchemaAdoptionError, adopt_legacy_schema, inspect_legacy_schema
from ...schema_inspection import SchemaInspectionError
from ...capabilities import require_worker_capabilities


def _add_worker_install_arguments(parser):
    parser.add_argument("--source", type=str, help="Path to reproq-worker source")
    parser.add_argument("--build", action="store_true", help="Force building from source")
    parser.add_argument("--tag", type=str, default="latest", help="GitHub release tag")
    parser.add_argument(
        "--sha256", type=str,
        help="Expected platform-specific binary SHA-256; download failures cannot fall back to a build.",
    )


class Command(BaseCommand):
    help = "Unified Reproq management command"

    def add_arguments(self, parser):
        subparsers = parser.add_subparsers(dest="subcommand", required=True)

        # Worker
        worker_parser = subparsers.add_parser("worker", help="Start the Go worker", allow_abbrev=False)
        # None means absent for both native call_command kwargs and CLI input.
        # Go owns ordinary runtime defaults after file/environment overrides.
        worker_parser.add_argument("--config", type=str, default=None, help="Path to reproq config file")
        worker_parser.add_argument("--concurrency", type=int, default=None)
        worker_parser.add_argument("--queues", type=str, default=None, help="Comma-separated queue names")
        worker_parser.add_argument("--database", type=str, default=None, help="Django database alias for worker DSN")
        worker_parser.add_argument("--allowed-task-modules", type=str, default=None, help="Comma-separated task module allow-list")
        worker_parser.add_argument("--logs-dir", type=str, default=None, help="Directory to persist stdout/stderr logs")
        worker_parser.add_argument("--payload-mode", type=str, default=None, help="Payload mode: stdin|file|inline")
        worker_parser.add_argument("--metrics-port", type=int, default=None, help="Port to serve Prometheus metrics")
        worker_parser.add_argument("--metrics-addr", type=str, default=None, help="Address to serve health/metrics")
        worker_parser.add_argument("--metrics-auth-token", type=str, default=None, help="Bearer token required for health/metrics")
        worker_parser.add_argument("--metrics-allow-cidrs", type=str, default=None, help="Comma-separated IP/CIDR allow-list")
        worker_parser.add_argument("--metrics-tls-cert", type=str, default=None, help="TLS certificate path for health/metrics")
        worker_parser.add_argument("--metrics-tls-key", type=str, default=None, help="TLS private key path for health/metrics")
        worker_parser.add_argument("--metrics-tls-client-ca", type=str, default=None, help="Client CA bundle to require mTLS for health/metrics")
        worker_parser.add_argument("--metrics-auth-limit", type=int, default=None, help="Unauthorized request limit per window")
        worker_parser.add_argument("--metrics-auth-window", type=str, default=None, help="Rate limit window (e.g. 1m)")
        worker_parser.add_argument("--metrics-auth-max-entries", type=int, default=None, help="Max tracked hosts for auth rate limiting")

        # Beat
        beat_parser = subparsers.add_parser("beat", help="Start the periodic task scheduler (beat)")
        beat_parser.add_argument("--config", type=str, default=None, help="Path to reproq config file")
        beat_parser.add_argument("--interval", type=str, default=None)
        beat_parser.add_argument("--once", action="store_true", default=None, help="Enqueue due periodic tasks once and exit")
        beat_parser.add_argument("--database", type=str, default=None, help="Django database alias for beat DSN")

        schedule_parser = subparsers.add_parser(
            "schedule",
            help="Enqueue due periodic tasks once and exit (cron-friendly)",
        )
        schedule_parser.add_argument("--config", type=str, default=None, help="Path to reproq config file")
        schedule_parser.add_argument("--interval", type=str, default=None)
        schedule_parser.add_argument("--database", type=str, default=None, help="Django database alias for beat DSN")

        # PG Cron
        pgcron_parser = subparsers.add_parser(
            "pg-cron",
            help="Configure Postgres-native periodic scheduling via pg_cron",
        )
        pgcron_parser.add_argument(
            "--install",
            action="store_true",
            help="Install/sync pg_cron jobs (default)",
        )
        pgcron_parser.add_argument(
            "--remove",
            action="store_true",
            help="Remove exact known owned pg_cron jobs (no function or extension DDL)",
        )
        pgcron_parser.add_argument(
            "--prefix",
            type=str,
            default="reproq_periodic",
            help="Job name prefix for pg_cron entries",
        )
        pgcron_parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Print actions without executing",
        )
        pgcron_parser.add_argument(
            "--if-supported",
            action="store_true",
            help="Skip if pg_cron is unavailable (non-Postgres or extension missing)",
        )
        pgcron_parser.add_argument("--database", type=str, default="", help="Django database alias for pg_cron")

        # Migrate
        migrate_parser = subparsers.add_parser("migrate-worker", help="Apply committed ReproQ Django migrations")
        migrate_parser.add_argument("--database", type=str, default="", help="Django queue database alias")
        adoption_parser = subparsers.add_parser("adopt-schema", help="Explicitly adopt a verified known legacy schema")
        adoption_parser.add_argument("--database", type=str, default="", help="Django queue database alias")
        adoption_parser.add_argument("--inspect", action="store_true", help="Read catalog/history without adopting")
        adoption_parser.add_argument("--expected-catalog-sha256", type=str, default="")
        adoption_parser.add_argument("--audit-output", type=str, default="", help="New JSON audit receipt file")

        # Check
        subparsers.add_parser("check", help="Verify configuration")

        # Install
        install_parser = subparsers.add_parser("install", help="Download or build the Go worker binary")
        _add_worker_install_arguments(install_parser)

        # Init
        init_parser = subparsers.add_parser("init", help="Bootstrap Reproq in the current project")
        init_parser.add_argument("--config", type=str, default="", help="Path to config file to write")
        init_parser.add_argument("--format", choices=["yaml", "toml"], default="toml", help="Config format")
        init_parser.add_argument("--force", action="store_true", help="Overwrite existing config file")
        init_parser.add_argument("--skip-install", action="store_true", help="Skip worker binary install")
        init_parser.add_argument("--skip-migrate", action="store_true", help="Skip Django migrations")
        init_parser.add_argument("--skip-worker-migrate", action="store_true", help="Skip the separate ReproQ migration step")
        init_parser.add_argument("--database", type=str, default="", help="Django queue database alias")
        _add_worker_install_arguments(init_parser)

        # Stats
        stats_parser = subparsers.add_parser("stats", help="Show task execution statistics")
        stats_parser.add_argument("--database", type=str, default="", help="Django database alias")
        stats_parser.add_argument("--all-databases", action="store_true", help="Aggregate across configured databases")
        status_parser = subparsers.add_parser("status", help="Show task execution statistics (alias)")
        status_parser.add_argument("--database", type=str, default="", help="Django database alias")
        status_parser.add_argument("--all-databases", action="store_true", help="Aggregate across configured databases")

        # Doctor
        doctor_parser = subparsers.add_parser("doctor", help="Validate configuration, schema, and worker binary")
        doctor_parser.add_argument("--config", type=str, default="", help="Path to reproq config file")
        doctor_parser.add_argument("--strict", action="store_true", help="Exit with error on warnings")
        doctor_parser.add_argument("--database", type=str, default="", help="Django queue database alias")

        # Config
        config_parser = subparsers.add_parser("config", help="Show effective worker/beat configuration")
        config_parser.add_argument("--config", type=str, default="", help="Path to reproq config file")
        config_parser.add_argument("--mode", choices=["worker", "beat", "all"], default="worker")
        config_parser.add_argument("--explain", action="store_true", help="Explain config precedence")
        config_parser.add_argument("--print", dest="print_config", action="store_true", help="Print effective config (default)")

        # Allowlist
        allowlist_parser = subparsers.add_parser(
            "allowlist",
            help="Compute ALLOWED_TASK_MODULES from installed task modules",
        )
        allowlist_parser.add_argument(
            "--format",
            choices=["env", "plain"],
            default="env",
            help="Output format (env prints ALLOWED_TASK_MODULES=...)",
        )
        allowlist_parser.add_argument(
            "--show-tasks",
            action="store_true",
            help="Print discovered task paths",
        )
        allowlist_parser.add_argument(
            "--write",
            action="store_true",
            help="Write allow-list to reproq config file",
        )
        allowlist_parser.add_argument(
            "--config",
            type=str,
            default="",
            help="Config file to update when using --write",
        )

        # Recurring sync
        sync_parser = subparsers.add_parser(
            "sync-recurring",
            help="Sync @recurring schedules into PeriodicTask rows",
        )
        sync_parser.add_argument("--database", type=str, default="", help="Django database alias")
        sync_parser.add_argument(
            "--reset-schedule-state",
            action="store_true",
            help="Explicitly reset existing enabled and next-run state to code defaults",
        )

        reconcile_parser = subparsers.add_parser(
            "reconcile-periodic", help="Canonicalize named legacy periodic definitions",
        )
        reconcile_parser.add_argument("--database", default="")
        reconcile_parser.add_argument("--name", action="append", required=True)
        reconcile_parser.add_argument("--dry-run", action="store_true")

        # Queue controls
        pause_parser = subparsers.add_parser("pause-queue", help="Pause a queue")
        pause_parser.add_argument("queue", type=str, help="Queue name to pause")
        pause_parser.add_argument("--reason", type=str, default="", help="Optional pause reason")
        pause_parser.add_argument("--database", type=str, default="", help="Django database alias")

        resume_parser = subparsers.add_parser("resume-queue", help="Resume a queue")
        resume_parser.add_argument("queue", type=str, help="Queue name to resume")
        resume_parser.add_argument("--database", type=str, default="", help="Django database alias")

        # Logs
        logs_parser = subparsers.add_parser("logs", help="Show task logs from logs_uri")
        logs_parser.add_argument("--id", type=str, required=True, help="Task result_id (optionally alias:ID)")
        logs_parser.add_argument("--tail", type=int, default=200, help="Tail N lines from the log file")
        logs_parser.add_argument("--max-bytes", type=int, default=1_000_000, help="Max bytes to read")
        logs_parser.add_argument("--show-path", action="store_true", help="Only print logs_uri path")
        logs_parser.add_argument("--database", type=str, default="", help="Django database alias")

        # Cancel
        cancel_parser = subparsers.add_parser("cancel", help="Request cancellation for a task run")
        cancel_parser.add_argument("--id", type=str, required=True, help="Task result_id (optionally alias:ID)")
        cancel_parser.add_argument("--database", type=str, default="", help="Django database alias")

        # Upgrade
        upgrade_parser = subparsers.add_parser("upgrade", help="Upgrade the Go worker binary")
        _add_worker_install_arguments(upgrade_parser)
        upgrade_parser.add_argument("--skip-worker-migrate", action="store_true", help="Skip committed ReproQ migrations")

        # systemd
        systemd_parser = subparsers.add_parser("systemd", help="Generate systemd service files")
        systemd_parser.add_argument("--user", type=str, help="User to run as")
        systemd_parser.add_argument("--group", type=str, help="Group to run as")
        systemd_parser.add_argument("--concurrency", type=int, default=10)
        systemd_parser.add_argument("--queues", type=str, default="", help="Comma-separated queue names")
        systemd_parser.add_argument("--allowed-task-modules", type=str, default="", help="Comma-separated task module allow-list")
        systemd_parser.add_argument("--logs-dir", type=str, default="", help="Directory to persist stdout/stderr logs")
        systemd_parser.add_argument("--payload-mode", type=str, default="", help="Payload mode: stdin|file|inline")
        systemd_parser.add_argument("--metrics-port", type=int, default=0, help="Port to serve Prometheus metrics")
        systemd_parser.add_argument("--metrics-addr", type=str, default="", help="Address to serve health/metrics")
        systemd_parser.add_argument("--metrics-auth-token", type=str, default="", help="Bearer token required for health/metrics")
        systemd_parser.add_argument("--metrics-allow-cidrs", type=str, default="", help="Comma-separated IP/CIDR allow-list")
        systemd_parser.add_argument("--metrics-tls-cert", type=str, default="", help="TLS certificate path for health/metrics")
        systemd_parser.add_argument("--metrics-tls-key", type=str, default="", help="TLS private key path for health/metrics")
        systemd_parser.add_argument("--metrics-tls-client-ca", type=str, default="", help="Client CA bundle to require mTLS for health/metrics")
        systemd_parser.add_argument("--metrics-auth-limit", type=int, default=None, help="Unauthorized request limit per window")
        systemd_parser.add_argument("--metrics-auth-window", type=str, default="", help="Rate limit window (e.g. 1m)")
        systemd_parser.add_argument("--metrics-auth-max-entries", type=int, default=None, help="Max tracked hosts for auth rate limiting")
        systemd_parser.add_argument("--env-file", type=str, default="", help="Optional EnvironmentFile path")
        systemd_parser.add_argument(
            "--schedule",
            action="store_true",
            help="Generate a systemd timer/service for cron-style scheduling",
        )
        systemd_parser.add_argument(
            "--schedule-on-calendar",
            type=str,
            default="*-*-* *:*:00",
            help="systemd OnCalendar expression for schedule timer",
        )

        # Reclaim
        reclaim_parser = subparsers.add_parser(
            "reclaim",
            help="Reclaim or fail tasks with expired leases",
        )
        reclaim_parser.add_argument(
            "--action",
            choices=["requeue", "fail"],
            default="requeue",
            help="What to do with expired leases",
        )
        reclaim_parser.add_argument(
            "--older-than",
            default="0s",
            help="Only target leases expired longer than this (e.g., 5m, 1h)",
        )
        reclaim_parser.add_argument(
            "--limit",
            type=int,
            default=0,
            help="Maximum number of tasks to process (0 = no limit)",
        )
        reclaim_parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Show what would be reclaimed without making changes",
        )
        reclaim_parser.add_argument(
            "--include-null-lease",
            action="store_true",
            help="Also target RUNNING tasks with no lease timestamp",
        )
        reclaim_parser.add_argument("--database", type=str, default="", help="Django database alias")
        reclaim_parser.add_argument("--all-databases", action="store_true", help="Reclaim across configured databases")

        prune_workers = subparsers.add_parser(
            "prune-workers",
            help="Delete workers not seen recently",
        )
        prune_workers.add_argument(
            "--older-than",
            default="10m",
            help="Delete workers older than this (e.g., 10m, 1h, 1d)",
        )
        prune_workers.add_argument(
            "--dry-run",
            action="store_true",
            help="Show how many workers would be deleted",
        )
        prune_workers.add_argument("--database", type=str, default="", help="Django database alias")
        prune_workers.add_argument("--all-databases", action="store_true", help="Prune across configured databases")

        prune_parser = subparsers.add_parser(
            "prune",
            help="Delete terminal task history by status and age",
        )
        prune_parser.add_argument(
            "--statuses",
            type=str,
            default="SUCCESSFUL,FAILED,CANCELLED",
            help="Comma-separated terminal statuses (SUCCESSFUL,FAILED,CANCELLED)",
        )
        prune_parser.add_argument(
            "--older-than",
            default="7d",
            help="Delete tasks older than this (e.g., 7d, 30d)",
        )
        prune_parser.add_argument(
            "--limit",
            type=int,
            default=0,
            help="Maximum number of tasks to delete (0 = no limit)",
        )
        prune_parser.add_argument(
            "--field",
            type=str,
            default="finished_at",
            choices=["finished_at", "updated_at", "enqueued_at", "started_at"],
            help="Timestamp field used for the cutoff",
        )
        prune_parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Show how many tasks would be deleted",
        )
        prune_parser.add_argument("--database", type=str, default="", help="Django database alias")
        prune_parser.add_argument("--all-databases", action="store_true", help="Prune across configured databases")

    def handle(self, *args, **options):
        subcommand = options["subcommand"]
        
        if subcommand == "check":
            self.run_check()
        elif subcommand == "init":
            self.run_init(options)
        elif subcommand == "doctor":
            self.run_doctor(options)
        elif subcommand == "config":
            self.run_config(options)
        elif subcommand == "upgrade":
            self.run_upgrade(options)
        elif subcommand == "stats":
            self.run_stats(options)
        elif subcommand == "status":
            self.run_stats(options)
        elif subcommand == "systemd":
            self.run_systemd(options)
        elif subcommand == "install":
            self.run_install(options)
        elif subcommand == "migrate-worker":
            self.run_migrate(options)
        elif subcommand == "adopt-schema":
            self.run_adopt_schema(options)
        elif subcommand == "allowlist":
            self.run_allowlist(options)
        elif subcommand == "sync-recurring":
            self.run_sync_recurring(options)
        elif subcommand == "reconcile-periodic":
            self.run_reconcile_periodic(options)
        elif subcommand == "pause-queue":
            self.run_pause_queue(options)
        elif subcommand == "resume-queue":
            self.run_resume_queue(options)
        elif subcommand == "logs":
            self.run_logs(options)
        elif subcommand == "cancel":
            self.run_cancel(options)
        elif subcommand == "reclaim":
            self.run_reclaim(options)
        elif subcommand == "prune-workers":
            self.run_prune_workers(options)
        elif subcommand == "prune":
            self.run_prune(options)
        elif subcommand == "pg-cron":
            self.run_pg_cron(options)
        elif subcommand == "schedule":
            options["once"] = True
            self.run_worker_or_beat("beat", options)
        elif subcommand in ["worker", "beat"]:
            self.run_worker_or_beat(subcommand, options)

    def run_check(self):
        self.stdout.write("Checking Reproq configuration...")
        failed = False
        db_alias = default_db_alias()
        worker_bin, resolved_bin, exists = self._resolve_worker_bin()
        self.stdout.write(f"Resolved worker binary: {resolved_bin or worker_bin}")
        try:
            version = require_worker_capabilities(worker_bin)["worker_version"]
            self.stdout.write(self.style.SUCCESS(f"✅ Worker binary: {version}"))
        except Exception as e:
            self.stderr.write(self.style.ERROR(f"❌ Worker binary invalid: {e}"))
            if resolved_bin:
                self.stderr.write(self.style.ERROR(f"   Resolved path: {resolved_bin}"))
            failed = True

        dsn = self.get_dsn(db_alias)
        if dsn:
            self.stdout.write(self.style.SUCCESS("✅ Database DSN detected (Django DATABASES)."))
            self.stdout.write(f"DSN: {self._mask_dsn(dsn)}")
        else:
            self.stderr.write(self.style.ERROR("❌ Database DSN not detected."))
            self.stderr.write(
                self.style.ERROR("   Set DATABASE_URL or configure DATABASES with USER/NAME.")
            )
            failed = True

        conn = connections[db_alias]
        if db_alias != "default":
            self.stdout.write(f"Database alias: {db_alias}")
        with conn.cursor() as cursor:
            tables = conn.introspection.table_names(cursor)
            if "task_runs" in tables:
                self.stdout.write(self.style.SUCCESS("✅ Database schema present."))
            else:
                self.stderr.write(self.style.ERROR("❌ Database schema missing (run migrate)."))
                failed = True

        try:
            require_schema_contract(conn)
        except SchemaContractError as exc:
            self.stderr.write(self.style.ERROR(str(exc)))
            failed = True

        if not failed:
            problems = self._periodic_registration_problems(conn.alias)
            for problem in problems:
                self.stderr.write(self.style.ERROR(problem))
            failed = bool(problems)

        if failed:
            raise CommandError("Reproq check failed.")

        self.stdout.write(self.style.SUCCESS("\n✨ Configuration looks good!"))

    def run_init(self, options):
        self.stdout.write(self.style.MIGRATE_HEADING("🚀 Bootstrapping Reproq..."))
        if "reproq_django" not in settings.INSTALLED_APPS:
            self.stderr.write(self.style.ERROR("❌ 'reproq_django' not found in INSTALLED_APPS."))
        else:
            self.stdout.write(self.style.SUCCESS("✅ 'reproq_django' is in INSTALLED_APPS."))
        config_path = self._resolve_init_config_path(options)
        if config_path:
            created = self._ensure_config_file(
                config_path,
                options.get("format", "toml"),
                options.get("force", False),
            )
            if created:
                self.stdout.write(self.style.SUCCESS(f"✅ Config written to {config_path}"))
            else:
                self.stdout.write(self.style.WARNING(f"Config already exists at {config_path}"))

        if not options.get("skip_install"):
            self.run_install(options)

        if not options.get("skip_worker_migrate"):
            self.run_migrate(options)

        if not options.get("skip_migrate"):
            from django.core.management import call_command
            # --database selects the queue database, not every business app.
            call_command("migrate", database="default", interactive=False)

        self.stdout.write(self.style.MIGRATE_HEADING("\n✨ Reproq is ready!"))

    def run_doctor(self, options):
        strict = options.get("strict", False)
        db_alias = self._resolve_db_alias(options)
        conn = connections[db_alias]
        warnings = []
        errors = []

        def warn(message):
            warnings.append(message)
            self.stdout.write(self.style.WARNING(f"⚠️ {message}"))

        def fail(message):
            errors.append(message)
            self.stderr.write(self.style.ERROR(f"❌ {message}"))

        self.stdout.write(self.style.MIGRATE_HEADING("🔎 Reproq Doctor"))

        if "reproq_django" not in settings.INSTALLED_APPS:
            fail("'reproq_django' not found in INSTALLED_APPS.")
        else:
            self.stdout.write(self.style.SUCCESS("✅ 'reproq_django' is in INSTALLED_APPS."))

        config_path = self._resolve_config_path(options.get("config"))
        file_config = None
        if config_path:
            if not os.path.exists(config_path):
                fail(f"Config file not found: {config_path}")
            else:
                try:
                    file_config, config_format = self._load_config_file(config_path)
                    self.stdout.write(
                        self.style.SUCCESS(
                            f"✅ Loaded config file ({config_format}): {config_path}"
                        )
                    )
                except CommandError as exc:
                    fail(str(exc))
        else:
            warn("No reproq config file found (using env/settings).")

        env_config, env_errors = self._build_env_config()
        for error in env_errors:
            fail(error)

        effective, sources = self._effective_config(file_config, env_config)
        settings_dsn = self.get_dsn(db_alias)
        if settings_dsn:
            self._apply_settings_dsn(effective, sources, settings_dsn, "settings")

        worker_dsn = self._resolve_effective_dsn(effective, "worker")
        if worker_dsn:
            self.stdout.write(self.style.SUCCESS("✅ Worker DSN configured."))
            self.stdout.write(f"Worker DSN: {self._mask_dsn(worker_dsn)}")
        else:
            fail("Worker DSN missing (use DATABASE_URL, settings, or config file).")

        beat_dsn = self._resolve_effective_dsn(effective, "beat")
        if beat_dsn:
            self.stdout.write(self.style.SUCCESS("✅ Beat DSN configured."))
        else:
            warn("Beat DSN missing (periodic tasks will not run).")

        expected_dsn = self.get_dsn(db_alias)
        for scope, dsn in [("worker", worker_dsn), ("beat", beat_dsn)]:
            if not dsn:
                continue
            try:
                matches = expected_dsn and self._dsn_target(dsn) == self._dsn_target(expected_dsn)
            except Exception:
                matches = False
            if not matches:
                fail(f"{scope.capitalize()} DSN does not match selected queue database alias {db_alias!r}.")

        worker_bin, resolved_bin, exists = self._resolve_worker_bin()
        if not exists:
            fail(
                "Worker binary not found. Run `python manage.py reproq install` or set "
                "REPROQ_WORKER_BIN."
            )
        else:
            try:
                version = require_worker_capabilities(worker_bin)["worker_version"]
                self.stdout.write(self.style.SUCCESS(f"✅ Worker binary: {version}"))
            except Exception as exc:
                fail(f"Worker binary failed version check: {exc}")
                if resolved_bin:
                    self.stderr.write(self.style.ERROR(f"Resolved path: {resolved_bin}"))

        try:
            conn.ensure_connection()
            self.stdout.write(self.style.SUCCESS("✅ Database connection ok."))
        except Exception as exc:
            fail(f"Database connection failed: {exc}")
        else:
            with conn.cursor() as cursor:
                tables = set(conn.introspection.table_names(cursor))
            required_tables = {
                "task_runs",
                "periodic_tasks",
                "reproq_workers",
                "rate_limits",
                "workflow_runs",
                "reproq_queue_controls",
            }
            missing = sorted(required_tables - tables)
            if missing:
                fail(
                    "Reproq schema missing tables: "
                    f"{', '.join(missing)} (apply committed Django migrations)."
                )
            else:
                self.stdout.write(self.style.SUCCESS("✅ Reproq schema present."))
                try:
                    require_schema_contract(conn)
                except SchemaContractError as exc:
                    fail(str(exc))
                else:
                    for problem in self._periodic_registration_problems(db_alias):
                        fail(problem)

        allowlist = effective.get("worker", {}).get("allowed_task_modules", [])
        if not allowlist:
            auto_allowed, _, allow_errors = self._compute_allowed_task_modules()
            if allow_errors:
                warn("Some task modules failed to import while computing allowlist.")
            if auto_allowed:
                warn(
                    "ALLOWED_TASK_MODULES not set; run "
                    "`python manage.py reproq allowlist --write`."
                )
            else:
                warn("No task modules discovered for allowlist.")
        else:
            self.stdout.write(self.style.SUCCESS("✅ ALLOWED_TASK_MODULES configured."))

        logs_dir = effective.get("worker", {}).get("logs_dir", "")
        if logs_dir and not os.path.isdir(logs_dir):
            warn(f"Logs directory does not exist yet: {logs_dir}")

        if errors or (strict and warnings):
            summary = f"Doctor found {len(errors)} error(s) and {len(warnings)} warning(s)."
            raise CommandError(summary)

        self.stdout.write(self.style.SUCCESS("✨ Doctor completed with no blocking issues."))

    def run_config(self, options):
        config_path = self._resolve_config_path(options.get("config"))
        if config_path and not os.path.exists(config_path):
            raise CommandError(f"Config file not found: {config_path}")

        file_config = None
        config_format = ""
        if config_path:
            file_config, config_format = self._load_config_file(config_path)

        env_config, env_errors = self._build_env_config()
        if env_errors:
            raise CommandError("; ".join(env_errors))

        effective, sources = self._effective_config(file_config, env_config)
        settings_dsn = self.get_dsn()
        if settings_dsn:
            self._apply_settings_dsn(effective, sources, settings_dsn, "settings")

        mode = options.get("mode", "worker")
        view = self._select_config_view(effective, mode)
        masked = self._mask_config(view)

        if config_path:
            self.stdout.write(f"Config file: {config_path} ({config_format})")
        else:
            self.stdout.write("Config file: (none detected)")

        self.stdout.write("Precedence: defaults < config file < env vars < CLI flags")
        self.stdout.write(json.dumps(masked, indent=2))

        if options.get("explain"):
            self.stdout.write("\nSources:")
            for path, value in self._flatten_config(view):
                source = sources.get(path, "default")
                masked_value = self._mask_value(path, value)
                self.stdout.write(f"{path}: {masked_value} ({source})")

    def run_upgrade(self, options):
        self.stdout.write(self.style.MIGRATE_HEADING("Upgrading Reproq Go Worker..."))
        self.run_install(options)
        if not options.get("skip_worker_migrate"):
            self.run_migrate()

    def run_install(self, options):
        self.stdout.write(self.style.MIGRATE_HEADING("Installing Reproq Go Worker..."))
        expected = options.get("sha256")
        if expected is not None:
            expected = self._validated_checksum(expected)

        override_path = getattr(settings, "REPROQ_WORKER_BIN", None) or os.environ.get(
            "REPROQ_WORKER_BIN"
        )

        system = platform.system().lower()
        arch = platform.machine().lower()
        if arch == "x86_64":
            arch = "amd64"
        if arch == "arm64" or arch == "aarch64":
            arch = "arm64"

        ext = ".exe" if system == "windows" else ""
        bin_name = f"reproq-{system}-{arch}{ext}"
        if override_path:
            target_path = os.path.abspath(os.path.expanduser(str(override_path)))
            bin_dir = os.path.dirname(target_path)
        else:
            bin_dir = os.path.join(os.getcwd(), ".reproq", "bin")
            target_path = os.path.join(bin_dir, f"reproq{ext}")
        os.makedirs(bin_dir, exist_ok=True)
        if override_path:
            self.stdout.write(f"Override path: {override_path}")
        self.stdout.write(f"Platform: {system}/{arch}")
        self.stdout.write(f"Target path: {target_path}")

        # Stage beside the destination so publication is one atomic replacement.
        with tempfile.NamedTemporaryFile(prefix=".reproq-install-", dir=bin_dir, delete=False) as tmp_file:
            tmp_path = tmp_file.name

        try:
            success = False
            checksum = expected
            if not options.get("build"):
                tag = options.get("tag", "latest")
                url = (
                    f"https://github.com/adpena/reproq-worker/releases/download/{quote(tag, safe='')}/{bin_name}"
                    if tag != "latest" else
                    f"https://github.com/adpena/reproq-worker/releases/latest/download/{bin_name}"
                )
                self.stdout.write(f"Downloading pre-built binary: {url}")
                try:
                    with urllib.request.urlopen(url, timeout=30) as response:
                        with open(tmp_path, "wb") as destination:
                            shutil.copyfileobj(response, destination)
                except Exception as error:
                    raise CommandError("Worker download failed; source fallback is disabled. Use --build --source explicitly.") from error
                else:
                    # Integrity errors must never enter the source-build fallback.
                    checksum = expected or self._download_checksum(url) or self._release_checksum(tag, bin_name)
                    success = True

            if not success:
                from ...worker_install import build_verified_source
                source_path = options.get("source")
                if not source_path:
                    raise CommandError("Source builds require an explicit --source worker checkout")
                self.stdout.write(f"Building canonical worker artifacts from: {source_path}")
                try:
                    built_digest = build_verified_source(source_path, tmp_path, system=system, architecture=arch)
                except Exception as error:
                    raise CommandError(f"Canonical worker source build failed: {error}") from error
                checksum = expected or built_digest

            actual = self._sha256_file(tmp_path)
            if checksum is not None:
                if actual != self._validated_checksum(checksum):
                    raise CommandError("Worker binary SHA-256 mismatch; nothing was executed or installed.")
                self.stdout.write(self.style.SUCCESS(f"SHA-256 verified: {actual}"))
            else:
                self.stdout.write(f"Source-built worker SHA-256: {actual}")
            if system != "windows":
                os.chmod(tmp_path, 0o755)
            try:
                require_worker_capabilities(tmp_path)
                os.replace(tmp_path, target_path)
            except Exception as error:
                raise CommandError(f"Worker verification or publication failed: {error}") from error
            self.stdout.write(self.style.SUCCESS(f"Successfully installed to {target_path}"))
        finally:
            try:
                os.unlink(tmp_path)
            except FileNotFoundError:
                pass

    def run_systemd(self, options):
        import getpass
        cwd = os.getcwd()
        python_bin = sys.executable
        manage_py = os.path.join(cwd, "manage.py")
        user = options["user"] or getpass.getuser()
        group = options["group"] or user
        project_name = os.path.basename(cwd)
        
        worker_args = [
            python_bin,
            manage_py,
            "reproq",
            "worker",
            "--concurrency",
            str(options["concurrency"]),
        ]
        if options.get("queues"):
            worker_args.extend(["--queues", options["queues"]])
        if options.get("allowed_task_modules"):
            worker_args.extend(["--allowed-task-modules", options["allowed_task_modules"]])
        if options.get("logs_dir"):
            worker_args.extend(["--logs-dir", options["logs_dir"]])
        if options.get("payload_mode"):
            worker_args.extend(["--payload-mode", options["payload_mode"]])
        if options.get("metrics_addr"):
            worker_args.extend(["--metrics-addr", options["metrics_addr"]])
        elif options.get("metrics_port"):
            worker_args.extend(["--metrics-port", str(options["metrics_port"])])
        if options.get("metrics_tls_cert"):
            worker_args.extend(["--metrics-tls-cert", options["metrics_tls_cert"]])
        if options.get("metrics_tls_key"):
            worker_args.extend(["--metrics-tls-key", options["metrics_tls_key"]])
        if options.get("metrics_tls_client_ca"):
            worker_args.extend(["--metrics-tls-client-ca", options["metrics_tls_client_ca"]])
        beat_args = [python_bin, manage_py, "reproq", "beat"]
        schedule_args = [python_bin, manage_py, "reproq", "schedule"]

        def wrap_scheduler_guard(cmd: str, mode: str, default_mode: str | None) -> str:
            if default_mode is None:
                env_expr = "${REPROQ_SCHEDULER_MODE:-}"
            else:
                env_expr = f"${{REPROQ_SCHEDULER_MODE:-{default_mode}}}"
            guarded = (
                f'if [ "{env_expr}" = "{mode}" ]; then {cmd}; '
                f'else echo "REPROQ_SCHEDULER_MODE=${{REPROQ_SCHEDULER_MODE}}; skipping reproq {mode}."; fi'
            )
            return f"/bin/bash -lc '{guarded}'"

        services = {
            "worker": {
                "cmd": " ".join(shlex.quote(str(arg)) for arg in worker_args),
                "type": "simple",
                "restart": True,
            },
            "beat": {
                "cmd": wrap_scheduler_guard(
                    " ".join(shlex.quote(str(arg)) for arg in beat_args),
                    "beat",
                    "beat",
                ),
                "type": "simple",
                "restart": True,
            },
        }

        env_lines = []
        if options.get("env_file"):
            env_lines.append(f"EnvironmentFile={options['env_file']}")
        if options.get("metrics_auth_token"):
            env_lines.append(self._render_env("METRICS_AUTH_TOKEN", options["metrics_auth_token"]))
        if options.get("metrics_allow_cidrs"):
            env_lines.append(self._render_env("METRICS_ALLOW_CIDRS", options["metrics_allow_cidrs"]))

        env_block = ""
        if env_lines:
            env_block = "\n".join(env_lines) + "\n"

        for name, service in services.items():
            cmd = service["cmd"]
            service_type = service["type"]
            restart_line = "Restart=always\nRestartSec=5\n" if service["restart"] else ""
            content = f"""[Unit]
Description=Reproq {name.capitalize()} - {project_name}
After=network.target postgresql.service

[Service]
Type={service_type}
User={user}
Group={group}
WorkingDirectory={cwd}
{env_block}ExecStart={cmd}
{restart_line}

[Install]
WantedBy=multi-user.target
"""
            service_name = f"reproq-{name}-{project_name}.service"
            with open(service_name, "w") as f:
                f.write(content)
            self.stdout.write(f"Generated {service_name}")

        if options.get("schedule"):
            schedule_cmd = wrap_scheduler_guard(
                " ".join(shlex.quote(str(arg)) for arg in schedule_args),
                "cron",
                None,
            )
            schedule_service = f"""[Unit]
Description=Reproq Schedule - {project_name}
After=network.target postgresql.service

[Service]
Type=oneshot
User={user}
Group={group}
WorkingDirectory={cwd}
{env_block}ExecStart={schedule_cmd}

[Install]
WantedBy=multi-user.target
"""
            schedule_service_name = f"reproq-schedule-{project_name}.service"
            with open(schedule_service_name, "w") as f:
                f.write(schedule_service)
            self.stdout.write(f"Generated {schedule_service_name}")

            schedule_on_calendar = options.get("schedule_on_calendar") or "*-*-* *:*:00"
            schedule_timer = f"""[Unit]
Description=Reproq Schedule Timer - {project_name}

[Timer]
OnCalendar={schedule_on_calendar}
Persistent=true

[Install]
WantedBy=timers.target
"""
            schedule_timer_name = f"reproq-schedule-{project_name}.timer"
            with open(schedule_timer_name, "w") as f:
                f.write(schedule_timer)
            self.stdout.write(f"Generated {schedule_timer_name}")

    def _parse_duration(self, value: str) -> timedelta:
        if not value:
            return timedelta(0)
        match = re.match(r"^\s*(\d+)\s*([smhd])\s*$", value)
        if not match:
            raise CommandError(
                "Invalid duration. Use formats like 30s, 5m, 2h, 1d."
            )
        count = int(match.group(1))
        unit = match.group(2)
        if unit == "s":
            return timedelta(seconds=count)
        if unit == "m":
            return timedelta(minutes=count)
        if unit == "h":
            return timedelta(hours=count)
        if unit == "d":
            return timedelta(days=count)
        raise CommandError("Invalid duration unit.")

    def _resolve_db_alias(self, options, result_id: str | None = None) -> str:
        alias = (options or {}).get("database") or ""
        if alias:
            return alias
        if result_id:
            parsed_alias, _ = parse_result_id(result_id)
            return parsed_alias
        return default_db_alias()

    def _resolve_db_aliases(self, options) -> list[str]:
        if (options or {}).get("all_databases"):
            return queue_db_aliases()
        alias = (options or {}).get("database") or ""
        return [alias or default_db_alias()]

    def run_reclaim(self, options):
        from ...schema_contract import require_schema_contract

        aliases = self._resolve_db_aliases(options)
        worker_bin, resolved_bin, exists = self._resolve_worker_bin()
        if not exists:
            raise CommandError("Install the compatible Go worker before reclaiming task leases")
        try:
            require_worker_capabilities(worker_bin)
        except ValueError as error:
            raise CommandError(str(error)) from error
        for alias in aliases:
            require_schema_contract(connections[alias])
            dsn = self.get_dsn(alias)
            if not dsn:
                raise CommandError(f"PostgreSQL DSN is required for queue alias {alias!r}")
            arguments = [worker_bin, "reclaim",
                         "--action", options["action"], "--older-than", f"{int(self._parse_duration(options['older_than']).total_seconds())}s",
                         "--limit", str(options["limit"] or 0)]
            if options["include_null_lease"]:
                arguments.append("--include-null-lease")
            if options["dry_run"]:
                arguments.append("--dry-run")
            self.stdout.write(f"Reclaiming expired leases on queue database {alias}")
            try:
                subprocess.run(arguments, check=True, env={**os.environ, "REPROQ_DATABASE_URL": dsn})
            except subprocess.CalledProcessError as error:
                # CalledProcessError's default text includes the private DSN.
                raise CommandError(f"Go reclaim failed on {alias} (exit {error.returncode})") from None
            except OSError as error:
                raise CommandError(f"Cannot start the compatible Go reclaimer: {error}") from None

    def run_prune_workers(self, options):
        from reproq_django.models import Worker
        cutoff = timezone.now() - self._parse_duration(options["older_than"])
        aliases = self._resolve_db_aliases(options)
        for alias in aliases:
            queryset = Worker.objects.using(alias).filter(last_seen_at__lt=cutoff)
            count = queryset.count()
            prefix = f"[{alias}] " if len(aliases) > 1 else ""
            if options["dry_run"]:
                self.stdout.write(self.style.WARNING(f"{prefix}Dry run: {count} worker(s) would be deleted."))
                continue
            queryset.delete()
            self.stdout.write(self.style.SUCCESS(f"{prefix}Deleted {count} stale worker(s)."))

    def run_prune(self, options):
        from ...operations import OperationError, prune_history

        statuses = [s.strip().upper() for s in options["statuses"].split(",") if s.strip()]
        if not statuses or not set(statuses).issubset({"SUCCESSFUL", "FAILED", "CANCELLED"}):
            raise CommandError("Prune accepts only terminal SUCCESSFUL, FAILED or CANCELLED history")
        limit = options["limit"]
        if type(limit) is not int or limit < 0:
            raise CommandError("Prune limit must be nonnegative (0 = no limit)")
        try:
            age = self._parse_duration(options["older_than"])
        except (ValueError, OverflowError) as error:
            raise CommandError('Invalid history retention age') from error
        field = options.get("field") or "finished_at"
        aliases = self._resolve_db_aliases(options)

        for alias in aliases:
            try:
                count = prune_history(using=alias, statuses=statuses, older_than=age,
                                      field=field, limit=limit, dry_run=options['dry_run'])
            except (OperationError, ValueError) as error:
                raise CommandError(str(error)) from error
            prefix = f"[{alias}] " if len(aliases) > 1 else ""
            if options["dry_run"]:
                self.stdout.write(self.style.WARNING(f"{prefix}Dry run: up to {count} terminal task(s) currently eligible."))
                continue
            self.stdout.write(self.style.SUCCESS(f"{prefix}Deleted {count} task(s)."))

    def run_sync_recurring(self, options):
        alias = options.get("database") or None
        count = sync_recurring_tasks(
            using=alias,
            reset_schedule_state=options.get("reset_schedule_state", False),
        )
        if count == 0:
            self.stdout.write(self.style.WARNING("No recurring tasks found."))
            return
        target = alias or "auto"
        self.stdout.write(self.style.SUCCESS(f"Synced {count} recurring task(s) ({target})."))

    def run_reconcile_periodic(self, options):
        from django.db import transaction
        from ...recurring import periodic_arguments, register_periodic
        from ...schema_contract import require_schema_contract
        from ...specs import read_periodic_spec

        alias = self._resolve_db_alias(options)
        require_schema_contract(connections[alias])
        names = sorted(set(options["name"]))
        with transaction.atomic(using=alias):
            rows = list(PeriodicTask.objects.using(alias).select_for_update().filter(name__in=names))
            if {row.name for row in rows} != set(names):
                raise CommandError("One or more named periodic definitions do not exist; nothing changed")
            for row in rows:
                if row.spec_version == 1:
                    try:
                        spec = read_periodic_spec(row)
                    except ValueError as error:
                        raise CommandError('Stored canonical intent is invalid; inspect it explicitly before reconciliation') from error
                    payload = spec.payload
                    if row.payload_json != {'args': payload['args'], 'kwargs': payload['kwargs']}:
                        raise CommandError('Canonical intent and stored projections disagree; no automatic repair was attempted')
                    self.stdout.write(f'{row.name}: already canonical; execution and operator intent preserved')
                    continue
                try:
                    args, kwargs = periodic_arguments(row.payload_json, spec_version=row.spec_version)
                except ValueError as error:
                    raise CommandError(str(error)) from error
                registered = register_periodic(
                    name=row.name, schedule=row.cron_expr, task=row.task_path,
                    args=args, kwargs=kwargs, using=alias, queue_name=row.queue_name,
                    priority=row.priority, max_attempts=row.max_attempts,
                    concurrency_key=row.concurrency_key, concurrency_limit=row.concurrency_limit,
                )
                self.stdout.write(f"{row.name}: canonical spec v{registered.spec_version} {registered.canonical_spec_hash}")
            if options.get("dry_run"):
                transaction.set_rollback(True, using=alias)
        self.stdout.write("Dry run rolled back" if options.get("dry_run") else "Named definitions reconciled; operator schedule state preserved")

    def run_pause_queue(self, options):
        alias = self._resolve_db_alias(options)
        queue = options["queue"].strip()
        if not queue:
            raise CommandError("Queue name is required.")
        now = timezone.now()
        QueueControl.objects.using(alias).update_or_create(
            queue_name=queue,
            defaults={
                "paused": True,
                "paused_at": now,
                "reason": options.get("reason") or "",
                "updated_at": now,
            },
        )
        self.stdout.write(self.style.SUCCESS(f"Paused queue {queue} ({alias})."))

    def run_resume_queue(self, options):
        alias = self._resolve_db_alias(options)
        queue = options["queue"].strip()
        if not queue:
            raise CommandError("Queue name is required.")
        now = timezone.now()
        QueueControl.objects.using(alias).update_or_create(
            queue_name=queue,
            defaults={
                "paused": False,
                "paused_at": None,
                "reason": "",
                "updated_at": now,
            },
        )
        self.stdout.write(self.style.SUCCESS(f"Resumed queue {queue} ({alias})."))

    def run_migrate(self, options=None):
        from django.core.management import call_command
        from django.db.migrations.recorder import MigrationRecorder
        from ...schema_adoption import BASELINE, COVERED_REPLACEMENTS

        db_alias = self._resolve_db_alias(options)
        conn = connections[db_alias]
        if conn.vendor == "postgresql":
            from ..._schema_v1_catalog import require_public_relations_v1

            try:
                require_public_relations_v1(conn, allow_missing=True)
            except SchemaAdoptionError as exc:
                raise CommandError(str(exc)) from exc
        tables = set(conn.introspection.table_names())
        applied = {name for app, name in MigrationRecorder(conn).applied_migrations()
                   if app == "reproq_django"}
        if "task_runs" in tables and not ({BASELINE, *COVERED_REPLACEMENTS} <= applied):
            raise CommandError(
                "Legacy ReproQ history requires explicit guarded adoption. Run "
                "reproq adopt-schema --inspect on the selected database; do not fake migrations."
            )
        call_command("migrate", "reproq_django", database=db_alias, interactive=False,
                     stdout=self.stdout, stderr=self.stderr)
        try:
            require_schema_contract(conn)
        except SchemaContractError as exc:
            raise CommandError(str(exc)) from exc
        self.stdout.write(self.style.SUCCESS(f"ReproQ Django schema contract is ready ({db_alias})."))

    def run_adopt_schema(self, options):
        conn = connections[self._resolve_db_alias(options)]
        try:
            if options.get("inspect"):
                result = inspect_legacy_schema(conn)
            else:
                if not options.get("audit_output"):
                    raise SchemaAdoptionError("Supply --audit-output for the guarded adoption receipt.")
                result = adopt_legacy_schema(
                    conn, expected_catalog_sha256=options.get("expected_catalog_sha256", ""),
                    audit_output=options["audit_output"],
                )
        except SchemaAdoptionError as exc:
            raise CommandError(str(exc)) from exc
        self.stdout.write(json.dumps(result, sort_keys=True))

    def run_worker_or_beat(self, cmd, options):
        db_alias = self._resolve_db_alias(options)
        dsn = self.get_dsn(db_alias)
        worker_bin, resolved_bin, exists = self._resolve_worker_bin()
        if not exists:
            hint = (
                "Run `python manage.py reproq install` or set REPROQ_WORKER_BIN "
                "to the installed binary path."
            )
            raise CommandError(
                f"Worker binary not found (resolved: {resolved_bin or worker_bin}). {hint}"
            )
        try:
            require_worker_capabilities(worker_bin)
        except ValueError as error:
            raise CommandError(str(error)) from error
        runtime_names = (
            "concurrency", "queues", "allowed_task_modules", "logs_dir", "payload_mode",
            "metrics_port", "metrics_addr", "metrics_auth_token", "metrics_allow_cidrs",
            "metrics_tls_cert", "metrics_tls_key", "metrics_tls_client_ca",
            "metrics_auth_limit", "metrics_auth_window", "metrics_auth_max_entries",
        ) if cmd == "worker" else ("interval",)
        if cmd == "worker" and "queue" in options:
            raise CommandError("The worker --queue option is retired; use --queues.")
        overrides = {name: options[name] for name in runtime_names if options.get(name) is not None}
        # Go also discovers a default file when flags are present. Resolve and
        # disclose that same file before validating its queues and DSN; runtime
        # flags override it rather than creating a second discovery policy.
        config_path = self._resolve_config_path(options.get("config"))
        use_config = bool(config_path)
        if use_config and not os.path.exists(config_path):
            raise CommandError(f"Config file not found: {config_path}")

        selected_alias = self._resolve_db_alias(options)
        expected_dsn = self.get_dsn(selected_alias)
        file_config = None
        if use_config:
            file_config, _ = self._load_config_file(config_path)
        env_config, errors = self._build_env_config()
        if errors:
            raise CommandError("; ".join(errors))
        effective, sources = self._effective_config(file_config, env_config)
        if dsn:
            self._apply_settings_dsn(effective, sources, dsn, "settings")
        effective_dsn = self._resolve_effective_dsn(effective, cmd)
        if cmd == 'worker':
            from ...db import resolve_queue_db

            if "queues" in overrides:
                queues = self._parse_comma_list(overrides["queues"]) or ['default']
            else:
                queues = effective.get('worker', {}).get('queues') or ['default']
            mismatches = [queue for queue in queues if resolve_queue_db(queue) != selected_alias]
            if mismatches:
                raise CommandError(
                    'Selected worker queues do not route to its Django database alias; '
                    'configure REPROQ_QUEUE_DATABASES before starting the worker.'
                )
        try:
            matches = bool(expected_dsn and effective_dsn) and self._dsn_target(effective_dsn) == self._dsn_target(expected_dsn)
        except Exception:
            matches = False
        if not matches:
            raise CommandError(
                f"{cmd.capitalize()} DSN does not match selected Django queue database "
                f"alias {selected_alias!r}; align the configuration before starting it."
            )

        if not dsn and not use_config:
            hint = "DATABASE_URL not set. Reproq worker requires a Postgres DSN."
            if db_alias:
                hint = f"DSN not set for database alias '{db_alias}'."
            raise CommandError(hint)

        self.stdout.write(f"Worker binary: {resolved_bin or worker_bin}")
        if use_config:
            self.stdout.write(f"Config: {config_path}")
        else:
            self.stdout.write(f"DSN: {self._mask_dsn(dsn)}")
            if db_alias:
                self.stdout.write(f"Database alias: {db_alias}")

        args = [worker_bin, cmd]
        if use_config:
            args.extend(["--config", config_path])
        child_environment = os.environ.copy()
        # DATABASE_URL belongs to Django's business database. Python task
        # processes inherit it unchanged, even when their queue is elsewhere.
        child_environment["REPROQ_DATABASE_URL"] = effective_dsn

        for name, value in overrides.items():
            # Do not use truthiness: empty strings clear inherited settings,
            # and zero can deliberately disable a metrics port.
            args.extend(["--" + name.replace("_", "-"), str(value)])
        if cmd == "worker" and not use_config and "allowed_task_modules" not in overrides:
            if os.environ.get("ALLOWED_TASK_MODULES"):
                self.stdout.write("ALLOWED_TASK_MODULES set in environment; using it.")
            else:
                auto_modules, task_paths, errors = self._compute_allowed_task_modules()
                if errors:
                    self._report_allowlist_errors(errors)
                if auto_modules:
                    auto_value = ",".join(auto_modules)
                    args.extend(["--allowed-task-modules", auto_value])
                    self.stdout.write(f"Auto-configured ALLOWED_TASK_MODULES={auto_value}")
                else:
                    self.stderr.write(
                        self.style.WARNING(
                            "No task modules discovered; default allow-list will be used."
                        )
                    )
        elif cmd == "beat" and options.get("once"):
            args.append("--once")
            
        self.stdout.write(f"Starting {cmd}...")
        connections.close_all()
        self.stdout.flush()
        self.stderr.flush()
        sys.stdout.flush()
        sys.stderr.flush()
        try:
            os.execvpe(worker_bin, args, child_environment)
        except OSError as error:
            raise CommandError(f"Cannot replace foreground process with {cmd}: {error}") from error

    def run_pg_cron(self, options):
        from ...pg_cron import PgCronError, is_available, reconcile

        alias = self._resolve_db_alias(options)
        conn = connections[alias]
        install, remove = options.get("install"), options.get("remove")
        if install and remove:
            raise CommandError("Choose either --install or --remove (not both).")
        if conn.vendor != "postgresql":
            if options.get("if_supported"):
                self.stdout.write("pg-cron unavailable (non-Postgres database); skipping.")
                return
            raise CommandError("pg-cron requires a PostgreSQL database.")
        with conn.cursor() as cursor:
            available = is_available(cursor)
        if not available and options.get("if_supported"):
            self.stdout.write("pg-cron extension is not installed; skipping.")
            return
        try:
            result = reconcile(conn, prefix=options.get("prefix") or "reproq_periodic",
                               remove=bool(remove), dry_run=bool(options.get("dry_run")))
        except (PgCronError, SchemaContractError, SchemaInspectionError, ValueError) as exc:
            raise CommandError(str(exc)) from exc
        self.stdout.write(json.dumps(result, sort_keys=True))

    def _periodic_registration_problems(self, alias):
        problems = [f"Periodic definition {row.name!r} requires explicit SDK reconciliation "
                "and scheduler provider assignment before it can run."
                for row in PeriodicTask.objects.using(alias).filter(enabled=True)
                if periodic_definition_status(row.spec_version, row.scheduler_provider) == "reconciliation_required"]
        legacy = TaskRun.objects.using(alias).filter(status__in=['READY', 'RUNNING', 'WAITING']).exclude(spec_version=1).count()
        if legacy:
            problems.append(f"{legacy} queued legacy/unknown-version task(s) require draining "
                            "with the old runtime or explicit audited operator action before schema2 cutover; "
                            "no historical payload or version will be rewritten automatically.")
        return problems

    def _pg_cron_available(self, cursor):
        from ...pg_cron import is_available

        return is_available(cursor)

    def run_allowlist(self, options):
        allowed, task_paths, errors = self._compute_allowed_task_modules()
        if errors:
            self._report_allowlist_errors(errors)
        if options.get("show_tasks"):
            if task_paths:
                self.stdout.write("Task paths:")
                for path in task_paths:
                    self.stdout.write(f"  - {path}")
            else:
                self.stdout.write("Task paths: (none discovered)")
        if not allowed:
            self.stdout.write(self.style.WARNING("No task modules discovered."))
            return
        allowed_value = ",".join(allowed)
        if options.get("write"):
            config_path = self._resolve_config_path(options.get("config"))
            if not config_path:
                raise CommandError(
                    "Config file not found. Pass --config or run `python manage.py reproq init`."
                )
            if not os.path.exists(config_path):
                raise CommandError(f"Config file not found: {config_path}")
            config_data, config_format = self._load_config_file(config_path)
            config_data.setdefault("worker", {})["allowed_task_modules"] = allowed
            self._write_config_file(config_path, config_data, config_format)
            self.stdout.write(
                self.style.SUCCESS(f"✅ Updated allowlist in {config_path}")
            )
        if options.get("format") == "plain":
            self.stdout.write(allowed_value)
        else:
            self.stdout.write(f"ALLOWED_TASK_MODULES={allowed_value}")

    def run_logs(self, options):
        result_id = options["id"]
        db_alias = self._resolve_db_alias(options, result_id=result_id)
        _, raw_id = parse_result_id(result_id)
        try:
            run = TaskRun.objects.using(db_alias).get(result_id=raw_id)
        except TaskRun.DoesNotExist as exc:
            raise CommandError(f"Task run {result_id} not found.") from exc

        logs_uri = (run.logs_uri or "").strip()
        if not logs_uri:
            self.stdout.write("No logs_uri recorded for this task run.")
            return
        if options.get("show_path"):
            self.stdout.write(logs_uri)
            return

        max_bytes = options.get("max_bytes", 1_000_000)
        if max_bytes <= 0:
            raise CommandError("--max-bytes must be positive.")

        data = self._read_logs_uri(logs_uri, max_bytes)
        lines = data.splitlines()
        tail = options.get("tail", 0)
        if tail and tail > 0:
            lines = lines[-tail:]
        if lines:
            self.stdout.write("\n".join(lines))
        else:
            self.stdout.write("(no log output)")

    def run_cancel(self, options):
        from ...operations import OperationError, task_operation

        alias = self._resolve_db_alias(options, result_id=options['id'])
        try:
            task_operation('cancel', options['id'], using=alias)
        except (OperationError, ValueError) as error:
            raise CommandError(str(error)) from error
        self.stdout.write('Cancellation request completed; inspect current task state.')

    def run_stats(self, options):
        from reproq_django.models import TaskRun, Worker
        from django.db.models import Count

        aliases = self._resolve_db_aliases(options)
        self.stdout.write(self.style.MIGRATE_HEADING("📊 Reproq Statistics"))

        totals = {}
        for alias in aliases:
            stats = TaskRun.objects.using(alias).values("status").annotate(count=Count("result_id"))
            self.stdout.write(f"\nDatabase: {alias}")
            self.stdout.write("Tasks by Status:")
            for s in stats:
                totals[s["status"]] = totals.get(s["status"], 0) + s["count"]
                color = self.style.SUCCESS if s["status"] == "SUCCESSFUL" else (self.style.ERROR if s["status"] == "FAILED" else self.style.WARNING)
                self.stdout.write(f"  {s['status']:<12}: {color(str(s['count']))}")

            workers = list(Worker.objects.using(alias).all())
            self.stdout.write(f"Active Workers: {len(workers)}")
            for w in workers:
                self.stdout.write(f"  - {w.worker_id} ({w.hostname}) | Queues: {w.queues} | Concurrency: {w.concurrency}")

        if len(aliases) > 1:
            self.stdout.write("\nAggregate Totals:")
            for status, count in sorted(totals.items()):
                color = self.style.SUCCESS if status == "SUCCESSFUL" else (self.style.ERROR if status == "FAILED" else self.style.WARNING)
                self.stdout.write(f"  {status:<12}: {color(str(count))}")


    def get_worker_bin(self):
        worker_bin = getattr(settings, "REPROQ_WORKER_BIN", None) or os.environ.get(
            "REPROQ_WORKER_BIN"
        )
        if worker_bin:
            return worker_bin

        ext = ".exe" if platform.system() == "Windows" else ""
        project_bin = os.path.join(os.getcwd(), ".reproq", "bin", f"reproq{ext}")
        if os.path.exists(project_bin):
            return project_bin

        pkg_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        candidate = os.path.join(pkg_dir, "bin", f"reproq{ext}")
        return candidate if os.path.exists(candidate) else "reproq"

    def get_dsn(self, db_alias: str | None = None):
        alias = db_alias or default_db_alias()
        conn = connections[alias]
        if conn.vendor != "postgresql":
            return None
        from psycopg.conninfo import make_conninfo

        params = conn.get_connection_params()
        for option in ["cursor_factory", "context", "prepare_threshold", "row_factory", "autocommit"]:
            params.pop(option, None)
        return make_conninfo(**params)

    @staticmethod
    def _dsn_target(dsn):
        from psycopg.conninfo import conninfo_to_dict

        parts = conninfo_to_dict(dsn)
        host = parts.get("host", "")
        if not host.startswith("/"):
            host = host.lower()
        return (host, parts.get("hostaddr", ""), parts.get("port") or "5432",
                parts.get("dbname", ""), parts.get("user", ""), parts.get("options", ""),
                parts.get("service", ""), parts.get("servicefile", ""))

    def _find_default_config(self):
        candidates = [
            "reproq.yaml",
            "reproq.yml",
            "reproq.toml",
            ".reproq.yaml",
            ".reproq.yml",
            ".reproq.toml",
        ]
        for name in candidates:
            if os.path.exists(name):
                return name
        return ""

    def _resolve_config_path(self, option_value):
        if option_value:
            return option_value
        env_value = os.environ.get("REPROQ_CONFIG")
        if env_value:
            return env_value
        return self._find_default_config()

    def _resolve_worker_bin(self):
        worker_bin = self.get_worker_bin()
        if os.path.isabs(worker_bin):
            resolved = worker_bin
        else:
            resolved = shutil.which(worker_bin)
        exists = bool(resolved and os.path.exists(resolved))
        return worker_bin, resolved, exists

    def _mask_dsn(self, dsn):
        try:
            from psycopg.conninfo import conninfo_to_dict
            parts = conninfo_to_dict(dsn)
        except Exception:
            return "<invalid>"
        host = quote(parts.get("host", ""), safe=":.,[]")
        if ":" in host and "," not in host and not host.startswith("["):
            host = "[" + host + "]"
        port = ":" + parts["port"] if parts.get("port") else ""
        user = quote(parts.get("user", ""), safe="")
        db = quote(parts.get("dbname", ""), safe="")
        user_part = f"{user}@" if user else ""
        return f"postgresql://{user_part}{host}{port}/{db}"

    def _sha256_file(self, path):
        h = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                h.update(chunk)
        return h.hexdigest()

    @staticmethod
    def _validated_checksum(value):
        if not isinstance(value, str) or re.fullmatch(r"[a-fA-F0-9]{64}", value) is None:
            raise CommandError("Expected a complete 64-character SHA-256 digest.")
        return value.lower()

    def _download_checksum(self, url):
        checksum_url = f"{url}.sha256"
        try:
            with urllib.request.urlopen(checksum_url, timeout=30) as response:
                payload = response.read(4097)
        except Exception:
            return ""
        if len(payload) > 4096:
            raise CommandError("Worker checksum sidecar is too large.")
        try:
            payload = payload.decode().strip()
        except UnicodeDecodeError as error:
            raise CommandError("Worker checksum sidecar is not text.") from error
        if not payload:
            return ""
        return self._validated_checksum(payload.split()[0])

    def _release_checksum(self, tag, asset_name):
        """Use the publisher's asset digest when no checksum sidecar exists."""
        endpoint = "latest" if tag == "latest" else f"tags/{quote(tag, safe='')}"
        request = urllib.request.Request(
            f"https://api.github.com/repos/adpena/reproq-worker/releases/{endpoint}",
            headers={"Accept": "application/vnd.github+json", "User-Agent": "reproq-django"},
        )
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                payload = response.read(1024 * 1024 + 1)
            if len(payload) > 1024 * 1024:
                raise ValueError("Release metadata is too large")
            document = json.loads(payload)
            matches = [asset for asset in document["assets"] if asset["name"] == asset_name]
            if len(matches) != 1 or not isinstance(matches[0].get("digest"), str):
                raise ValueError("Release has no unique digest for this platform")
            algorithm, separator, digest = matches[0]["digest"].partition(":")
            if algorithm != "sha256" or not separator:
                raise ValueError("Release asset has no SHA-256 digest")
            return self._validated_checksum(digest)
        except Exception as error:
            raise CommandError(
                "Cannot verify the downloaded worker. Pin --sha256 or publish a checksum/digest; "
                "nothing was executed or installed."
            ) from error

    def _render_env(self, key, value):
        escaped = str(value).replace('"', '\\"')
        return f'Environment="{key}={escaped}"'

    def _compute_allowed_task_modules(self):
        task_paths, errors = self._discover_task_paths()
        allowed = set()
        for path in task_paths:
            if "." in path:
                allowed.add(path.rsplit(".", 1)[0] + ".")
        return sorted(allowed), sorted(task_paths), errors

    def _discover_task_paths(self):
        import importlib.util
        import pkgutil
        from django.apps import apps

        task_paths = set()
        errors = []

        for app_config in apps.get_app_configs():
            module_name = f"{app_config.name}.tasks"
            try:
                spec = importlib.util.find_spec(module_name)
            except Exception:
                spec = None
            if spec is None:
                continue
            module = self._safe_import(module_name, errors)
            if module:
                self._collect_task_paths(module, task_paths)
            if spec.submodule_search_locations:
                for _, name, _ in pkgutil.walk_packages(
                    spec.submodule_search_locations,
                    prefix=module_name + ".",
                ):
                    submodule = self._safe_import(name, errors)
                    if submodule:
                        self._collect_task_paths(submodule, task_paths)

        return task_paths, errors

    def _safe_import(self, module_name, errors):
        import importlib

        try:
            return importlib.import_module(module_name)
        except Exception as exc:
            errors.append(f"Failed to import {module_name}: {exc}")
            return None

    def _collect_task_paths(self, module, task_paths):
        for value in module.__dict__.values():
            path = self._extract_task_path(value)
            if path:
                task_paths.add(path)

    def _extract_task_path(self, candidate):
        try:
            from django.tasks import Task
        except Exception:
            Task = None

        if Task and isinstance(candidate, Task):
            module_path = getattr(candidate, "module_path", None)
            if isinstance(module_path, str) and module_path:
                return module_path
            for attr in ("path", "name"):
                value = getattr(candidate, attr, None)
                if isinstance(value, str) and "." in value:
                    return value

        module_path = getattr(candidate, "module_path", None)
        if isinstance(module_path, str) and "." in module_path:
            return module_path

        nested = getattr(candidate, "task", None)
        if nested is not None:
            return self._extract_task_path(nested)

        return None

    def _report_allowlist_errors(self, errors):
        for message in errors:
            self.stderr.write(self.style.WARNING(message))

    def _resolve_init_config_path(self, options):
        fmt = options.get("format", "toml")
        suffixes = {"toml": (".toml",), "yaml": (".yaml", ".yml")}
        if fmt not in suffixes:
            raise CommandError("Choose --format toml or --format yaml for the configuration file.")
        if options.get("config"):
            path = os.path.abspath(os.path.expanduser(options["config"]))
        else:
            filename = "reproq.yaml" if fmt == "yaml" else "reproq.toml"
            path = os.path.join(os.getcwd(), filename)
        if os.path.splitext(path)[1].lower() not in suffixes[fmt]:
            allowed = " or ".join(suffixes[fmt])
            raise CommandError(
                f"Config filename must end in {allowed} for --format {fmt}; "
                "select --format toml for .toml or --format yaml for .yaml/.yml."
            )
        if os.path.isdir(path):
            raise CommandError("Config path must name a file, not an existing directory.")
        return path

    def _ensure_config_file(self, path, fmt, force):
        path = os.path.abspath(os.path.expanduser(path))
        if os.path.exists(path) and not force:
            return False
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        template = self._load_config_template(fmt)
        with open(path, "w", encoding="utf-8") as handle:
            handle.write(template)
        return True

    def _load_config_template(self, fmt):
        filename = f"reproq.example.{fmt}"
        try:
            resource = importlib.resources.files("reproq_django.resources").joinpath(filename)
            return resource.read_text(encoding="utf-8")
        except (ModuleNotFoundError, OSError, UnicodeError) as exc:
            raise CommandError(
                f"Bundled ReproQ {fmt} configuration template is unavailable; reinstall reproq-django."
            ) from exc

    def _load_config_file(self, path):
        ext = os.path.splitext(path)[1].lower()
        if ext in (".yaml", ".yml"):
            fmt = "yaml"
            try:
                import yaml
            except Exception as exc:
                raise CommandError(
                    "PyYAML is required to read YAML config files. "
                    "Install pyyaml or use TOML."
                ) from exc
            with open(path, "r", encoding="utf-8") as handle:
                data = yaml.safe_load(handle) or {}
        elif ext == ".toml":
            fmt = "toml"
            with open(path, "rb") as handle:
                data = tomllib.load(handle)
        else:
            raise CommandError(f"Unsupported config extension: {ext}")

        if not isinstance(data, dict):
            raise CommandError("Config file must be a mapping at the top level.")
        if isinstance(data.get("worker"), dict) and "max_attempts_default" in data["worker"]:
            raise CommandError(
                "worker.max_attempts_default is retired; configure MAX_ATTEMPTS on "
                "the Django task backend and reconcile periodic definitions explicitly."
            )
        return data, fmt

    def _write_config_file(self, path, data, fmt):
        if fmt == "yaml":
            try:
                import yaml
            except Exception as exc:
                raise CommandError(
                    "PyYAML is required to write YAML config files. "
                    "Install pyyaml or use TOML."
                ) from exc
            payload = yaml.safe_dump(
                data,
                sort_keys=False,
                default_flow_style=False,
                allow_unicode=False,
            )
        elif fmt == "toml":
            payload = self._toml_dumps(data)
        else:
            raise CommandError(f"Unsupported config format: {fmt}")
        with open(path, "w", encoding="utf-8") as handle:
            handle.write(payload)

    def _toml_dumps(self, data):
        lines = []

        def emit_section(section, prefix):
            simple_items = []
            nested_items = []
            for key, value in section.items():
                if isinstance(value, dict):
                    nested_items.append((key, value))
                else:
                    simple_items.append((key, value))
            if prefix:
                lines.append("")
                lines.append(f"[{'.'.join(prefix)}]")
            for key, value in simple_items:
                lines.append(f"{key} = {self._toml_format(value)}")
            for key, value in nested_items:
                emit_section(value, prefix + [key])

        emit_section(data, [])
        return "\n".join(lines).rstrip() + "\n"

    def _toml_format(self, value):
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, (int, float)):
            return str(value)
        if value is None:
            return '""'
        if isinstance(value, list):
            inner = ", ".join(self._toml_format(item) for item in value)
            return f"[{inner}]"
        return json.dumps(str(value))

    def _default_config(self):
        hostname = platform.node() or "worker"
        worker_id = f"{hostname}-{os.getpid()}"
        return {
            "dsn": "",
            "worker": {
                "dsn": "",
                "worker_id": worker_id,
                "queues": ["default"],
                "allowed_task_modules": [],
                "concurrency": 10,
                "poll_min_backoff": "100ms",
                "poll_max_backoff": "5s",
                "lease_seconds": 300,
                "heartbeat_seconds": 60,
                "reclaim_interval_seconds": 60,
                "python_bin": "python3",
                "executor_module": "reproq_django.executor",
                "payload_mode": "stdin",
                "max_payload_bytes": 1048576,
                "max_stdout_bytes": 1048576,
                "max_stderr_bytes": 1048576,
                "logs_dir": "",
                "exec_timeout": "1h",
                "shutdown_timeout": "30s",
                "priority_aging_factor": 60,
            },
            "beat": {
                "dsn": "",
                "interval": "30s",
            },
            "metrics": {
                "addr": "",
                "port": 0,
                "auth_token": "",
                "allow_cidrs": [],
                "auth_limit": 30,
                "auth_window": "1m",
                "auth_max_entries": 1000,
                "tls_cert": "",
                "tls_key": "",
                "tls_client_ca": "",
            },
        }

    def _effective_config(self, file_config, env_config):
        config = self._default_config()
        sources = {}
        self._set_default_sources(config, sources, "default")
        if file_config:
            self._merge_config(config, file_config, "config", sources)
        if env_config:
            self._merge_config(config, env_config, "env", sources)
        return config, sources

    def _build_env_config(self):
        errors = []
        config = {}

        def set_path(path, value):
            node = config
            for key in path[:-1]:
                node = node.setdefault(key, {})
            node[path[-1]] = value

        def parse_int(value, name, positive=True):
            try:
                parsed = int(value)
            except Exception:
                errors.append(f"Invalid {name}: {value}")
                return None
            if positive and parsed <= 0:
                errors.append(f"Invalid {name}: must be positive")
                return None
            return parsed

        def parse_float(value, name):
            try:
                return float(value)
            except Exception:
                errors.append(f"Invalid {name}: {value}")
                return None

        if os.environ.get("REPROQ_DATABASE_URL"):
            dsn = os.environ["REPROQ_DATABASE_URL"]
            set_path(["dsn"], dsn)
            set_path(["worker", "dsn"], dsn)
            set_path(["beat", "dsn"], dsn)

        if os.environ.get("WORKER_ID"):
            set_path(["worker", "worker_id"], os.environ["WORKER_ID"])

        if os.environ.get("QUEUE_NAMES"):
            set_path(
                ["worker", "queues"],
                self._parse_comma_list(os.environ["QUEUE_NAMES"]) or ["default"],
            )

        if os.environ.get("ALLOWED_TASK_MODULES"):
            set_path(
                ["worker", "allowed_task_modules"],
                self._parse_comma_list(os.environ["ALLOWED_TASK_MODULES"]),
            )

        if os.environ.get("REPROQ_LOGS_DIR"):
            set_path(["worker", "logs_dir"], os.environ["REPROQ_LOGS_DIR"])

        if os.environ.get("PRIORITY_AGING_FACTOR"):
            value = parse_float(os.environ["PRIORITY_AGING_FACTOR"], "PRIORITY_AGING_FACTOR")
            if value is not None:
                set_path(["worker", "priority_aging_factor"], value)

        if os.environ.get("METRICS_ADDR"):
            set_path(["metrics", "addr"], os.environ["METRICS_ADDR"])

        if os.environ.get("METRICS_AUTH_TOKEN"):
            set_path(["metrics", "auth_token"], os.environ["METRICS_AUTH_TOKEN"])

        if os.environ.get("METRICS_ALLOW_CIDRS"):
            set_path(
                ["metrics", "allow_cidrs"],
                self._parse_comma_list(os.environ["METRICS_ALLOW_CIDRS"]),
            )

        if os.environ.get("METRICS_TLS_CERT"):
            set_path(["metrics", "tls_cert"], os.environ["METRICS_TLS_CERT"])

        if os.environ.get("METRICS_TLS_KEY"):
            set_path(["metrics", "tls_key"], os.environ["METRICS_TLS_KEY"])

        if os.environ.get("METRICS_TLS_CLIENT_CA"):
            set_path(["metrics", "tls_client_ca"], os.environ["METRICS_TLS_CLIENT_CA"])

        return config, errors

    def _parse_comma_list(self, value):
        return [item.strip() for item in value.split(",") if item.strip()]

    def _merge_config(self, base, incoming, source, sources, prefix=""):
        for key, value in incoming.items():
            path = f"{prefix}{key}"
            if isinstance(value, dict):
                if key not in base or not isinstance(base.get(key), dict):
                    base[key] = {}
                if value:
                    self._merge_config(base[key], value, source, sources, path + ".")
                elif key not in sources:
                    sources[path] = source
                continue

            if key not in base:
                base[key] = value
                sources[path] = source
                continue

            if value is None:
                continue
            if isinstance(value, str):
                if value == "":
                    continue
                base[key] = value
                sources[path] = source
                continue
            if isinstance(value, list):
                if not value:
                    continue
                base[key] = value
                sources[path] = source
                continue
            base[key] = value
            sources[path] = source

    def _set_default_sources(self, config, sources, source, prefix=""):
        for key, value in config.items():
            path = f"{prefix}{key}"
            if isinstance(value, dict):
                self._set_default_sources(value, sources, source, path + ".")
            else:
                sources[path] = source

    def _apply_settings_dsn(self, config, sources, dsn, source):
        # Empty DSNs inherit; genuine operator values and their source survive.
        # This same effective view is used by runtime preflight/config/doctor.
        if config.get("dsn") is None or config.get("dsn") == "":
            config["dsn"] = dsn
            sources["dsn"] = source
        for scope in ("worker", "beat"):
            scoped = config.setdefault(scope, {})
            if scoped.get("dsn") is None or scoped.get("dsn") == "":
                scoped["dsn"] = config["dsn"]
                sources[scope + ".dsn"] = sources["dsn"]

    def _resolve_effective_dsn(self, config, scope):
        if scope in ("worker", "beat"):
            scoped = config.get(scope, {}).get("dsn")
            if scoped is not None and scoped != "":
                return scoped
        return config.get("dsn")

    def _select_config_view(self, config, mode):
        if mode == "worker":
            return {
                "dsn": config.get("dsn", ""),
                "worker": config.get("worker", {}),
                "metrics": config.get("metrics", {}),
            }
        if mode == "beat":
            return {
                "dsn": config.get("dsn", ""),
                "beat": config.get("beat", {}),
            }
        return config

    def _mask_config(self, config):
        if isinstance(config, dict):
            return {
                key: self._mask_config(value)
                if isinstance(value, (dict, list))
                else self._mask_value(key, value)
                for key, value in config.items()
            }
        if isinstance(config, list):
            return [self._mask_config(value) for value in config]
        return config

    def _mask_value(self, path, value):
        if value is None:
            return value
        lowered = path.lower()
        if "dsn" in lowered and isinstance(value, str):
            return self._mask_dsn(value) if value else value
        if "auth_token" in lowered or lowered.endswith("token"):
            return "<redacted>" if value else value
        return value

    def _flatten_config(self, config, prefix=""):
        items = []
        if isinstance(config, dict):
            for key, value in config.items():
                path = f"{prefix}{key}"
                if isinstance(value, dict):
                    items.extend(self._flatten_config(value, path + "."))
                else:
                    items.append((path, value))
        else:
            items.append((prefix.rstrip("."), config))
        return items

    def _read_logs_uri(self, logs_uri, max_bytes):
        if os.path.exists(logs_uri):
            path = logs_uri
        else:
            from urllib.parse import urlparse
            from urllib.request import url2pathname

            parsed = urlparse(logs_uri)
            if parsed.scheme in ("", "file"):
                path = url2pathname(parsed.path)
            elif parsed.scheme in ("http", "https"):
                with urllib.request.urlopen(logs_uri) as response:
                    data = response.read(max_bytes)
                return data.decode("utf-8", errors="replace")
            else:
                raise CommandError(f"Unsupported logs_uri scheme: {parsed.scheme}")

        if not os.path.exists(path):
            raise CommandError(f"Log path not found: {path}")

        size = os.path.getsize(path)
        read_size = min(size, max_bytes)
        with open(path, "rb") as handle:
            if read_size < size:
                handle.seek(-read_size, os.SEEK_END)
            data = handle.read(read_size)
        return data.decode("utf-8", errors="replace")

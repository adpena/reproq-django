import os
import re
import shlex
import subprocess
import sys
import platform
import shutil
import urllib.request
import tempfile
import hashlib
import json
import tomllib
import importlib.resources
from datetime import timedelta
from django.core.management.base import BaseCommand, CommandError
from django.conf import settings
from django.db import connection
from django.db.models import Q
from django.utils import timezone
from ...models import PeriodicTask, TaskRun

class Command(BaseCommand):
    help = "Unified Reproq management command"

    def add_arguments(self, parser):
        subparsers = parser.add_subparsers(dest="subcommand", required=True)

        # Worker
        worker_parser = subparsers.add_parser("worker", help="Start the Go worker")
        worker_parser.add_argument("--config", type=str, default="", help="Path to reproq config file")
        worker_parser.add_argument("--concurrency", type=int, default=10)
        worker_parser.add_argument("--queues", type=str, default="", help="Comma-separated queue names")
        worker_parser.add_argument("--queue", type=str, default="default", help="Deprecated (use --queues)")
        worker_parser.add_argument("--allowed-task-modules", type=str, default="", help="Comma-separated task module allow-list")
        worker_parser.add_argument("--logs-dir", type=str, default="", help="Directory to persist stdout/stderr logs")
        worker_parser.add_argument("--payload-mode", type=str, default="", help="Payload mode: stdin|file|inline")
        worker_parser.add_argument("--metrics-port", type=int, default=0, help="Port to serve Prometheus metrics")
        worker_parser.add_argument("--metrics-addr", type=str, default="", help="Address to serve health/metrics")
        worker_parser.add_argument("--metrics-auth-token", type=str, default="", help="Bearer token required for health/metrics")
        worker_parser.add_argument("--metrics-allow-cidrs", type=str, default="", help="Comma-separated IP/CIDR allow-list")
        worker_parser.add_argument("--metrics-tls-cert", type=str, default="", help="TLS certificate path for health/metrics")
        worker_parser.add_argument("--metrics-tls-key", type=str, default="", help="TLS private key path for health/metrics")
        worker_parser.add_argument("--metrics-tls-client-ca", type=str, default="", help="Client CA bundle to require mTLS for health/metrics")
        worker_parser.add_argument("--metrics-auth-limit", type=int, default=None, help="Unauthorized request limit per window")
        worker_parser.add_argument("--metrics-auth-window", type=str, default="", help="Rate limit window (e.g. 1m)")
        worker_parser.add_argument("--metrics-auth-max-entries", type=int, default=None, help="Max tracked hosts for auth rate limiting")

        # Beat
        beat_parser = subparsers.add_parser("beat", help="Start the periodic task scheduler (beat)")
        beat_parser.add_argument("--config", type=str, default="", help="Path to reproq config file")
        beat_parser.add_argument("--interval", type=str, default="30s")

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
            help="Remove pg_cron jobs and helper function",
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

        # Migrate
        subparsers.add_parser("migrate-worker", help="Apply Go worker SQL optimizations")

        # Check
        subparsers.add_parser("check", help="Verify configuration")

        # Install
        install_parser = subparsers.add_parser("install", help="Download or build the Go worker binary")
        install_parser.add_argument("--source", type=str, help="Path to reproq-worker source")
        install_parser.add_argument("--build", action="store_true", help="Force building from source")
        install_parser.add_argument("--tag", type=str, default="latest", help="GitHub release tag")

        # Init
        init_parser = subparsers.add_parser("init", help="Bootstrap Reproq in the current project")
        init_parser.add_argument("--config", type=str, default="", help="Path to config file to write")
        init_parser.add_argument("--format", choices=["yaml", "toml"], default="yaml", help="Config format")
        init_parser.add_argument("--force", action="store_true", help="Overwrite existing config file")
        init_parser.add_argument("--skip-install", action="store_true", help="Skip worker binary install")
        init_parser.add_argument("--skip-migrate", action="store_true", help="Skip Django migrations")
        init_parser.add_argument("--skip-worker-migrate", action="store_true", help="Skip worker SQL optimizations")
        init_parser.add_argument("--source", type=str, help="Path to reproq-worker source")
        init_parser.add_argument("--build", action="store_true", help="Force building from source")
        init_parser.add_argument("--tag", type=str, default="latest", help="GitHub release tag")

        # Stats
        subparsers.add_parser("stats", help="Show task execution statistics")
        subparsers.add_parser("status", help="Show task execution statistics (alias)")

        # Stress Test
        stress_parser = subparsers.add_parser("stress-test", help="Enqueue a large number of tasks for benchmarking")
        stress_parser.add_argument("--count", type=int, default=100, help="Number of tasks to enqueue")
        stress_parser.add_argument("--sleep", type=float, default=0, help="Time each task should sleep")
        stress_parser.add_argument("--bulk", action="store_true", help="Use bulk_enqueue")

        # Doctor
        doctor_parser = subparsers.add_parser("doctor", help="Validate configuration, schema, and worker binary")
        doctor_parser.add_argument("--config", type=str, default="", help="Path to reproq config file")
        doctor_parser.add_argument("--strict", action="store_true", help="Exit with error on warnings")

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

        # Logs
        logs_parser = subparsers.add_parser("logs", help="Show task logs from logs_uri")
        logs_parser.add_argument("--id", type=int, required=True, help="Task result_id")
        logs_parser.add_argument("--tail", type=int, default=200, help="Tail N lines from the log file")
        logs_parser.add_argument("--max-bytes", type=int, default=1_000_000, help="Max bytes to read")
        logs_parser.add_argument("--show-path", action="store_true", help="Only print logs_uri path")

        # Cancel
        cancel_parser = subparsers.add_parser("cancel", help="Request cancellation for a task run")
        cancel_parser.add_argument("--id", type=int, required=True, help="Task result_id")

        # Upgrade
        upgrade_parser = subparsers.add_parser("upgrade", help="Upgrade the Go worker binary")
        upgrade_parser.add_argument("--source", type=str, help="Path to reproq-worker source")
        upgrade_parser.add_argument("--build", action="store_true", help="Force building from source")
        upgrade_parser.add_argument("--tag", type=str, default="latest", help="GitHub release tag")
        upgrade_parser.add_argument("--skip-worker-migrate", action="store_true", help="Skip worker SQL optimizations")

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

        prune_successful = subparsers.add_parser(
            "prune-successful",
            help="Delete successful task runs older than a cutoff",
        )
        prune_successful.add_argument(
            "--older-than",
            default="7d",
            help="Delete tasks older than this (e.g., 7d, 30d)",
        )
        prune_successful.add_argument(
            "--limit",
            type=int,
            default=0,
            help="Maximum number of tasks to delete (0 = no limit)",
        )
        prune_successful.add_argument(
            "--dry-run",
            action="store_true",
            help="Show how many tasks would be deleted",
        )

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
            self.run_stats()
        elif subcommand == "status":
            self.run_stats()
        elif subcommand == "stress-test":
            self.run_stress_test(options)
        elif subcommand == "systemd":
            self.run_systemd(options)
        elif subcommand == "install":
            self.run_install(options)
        elif subcommand == "migrate-worker":
            self.run_migrate()
        elif subcommand == "allowlist":
            self.run_allowlist(options)
        elif subcommand == "logs":
            self.run_logs(options)
        elif subcommand == "cancel":
            self.run_cancel(options)
        elif subcommand == "reclaim":
            self.run_reclaim(options)
        elif subcommand == "prune-workers":
            self.run_prune_workers(options)
        elif subcommand == "prune-successful":
            self.run_prune_successful(options)
        elif subcommand == "pg-cron":
            self.run_pg_cron(options)
        elif subcommand in ["worker", "beat"]:
            self.run_worker_or_beat(subcommand, options)

    def run_check(self):
        self.stdout.write("Checking Reproq configuration...")
        failed = False
        worker_bin, resolved_bin, exists = self._resolve_worker_bin()
        self.stdout.write(f"Resolved worker binary: {resolved_bin or worker_bin}")
        try:
            version = subprocess.check_output([worker_bin, "--version"]).decode().strip()
            self.stdout.write(self.style.SUCCESS(f"✅ Worker binary: {version}"))
        except Exception as e:
            self.stderr.write(self.style.ERROR(f"❌ Worker binary invalid: {e}"))
            if resolved_bin:
                self.stderr.write(self.style.ERROR(f"   Resolved path: {resolved_bin}"))
            failed = True

        dsn = self.get_dsn()
        if dsn:
            source = "DATABASE_URL" if os.environ.get("DATABASE_URL") else "DATABASES"
            self.stdout.write(self.style.SUCCESS(f"✅ Database DSN detected ({source})."))
            self.stdout.write(f"DSN: {self._mask_dsn(dsn)}")
        else:
            self.stderr.write(self.style.ERROR("❌ Database DSN not detected."))
            self.stderr.write(
                self.style.ERROR("   Set DATABASE_URL or configure DATABASES with USER/NAME.")
            )
            failed = True

        with connection.cursor() as cursor:
            tables = connection.introspection.table_names(cursor)
            if "task_runs" in tables:
                self.stdout.write(self.style.SUCCESS("✅ Database schema present."))
            else:
                self.stderr.write(self.style.ERROR("❌ Database schema missing (run migrate)."))
                failed = True
        
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
                options.get("format", "yaml"),
                options.get("force", False),
            )
            if created:
                self.stdout.write(self.style.SUCCESS(f"✅ Config written to {config_path}"))
            else:
                self.stdout.write(self.style.WARNING(f"Config already exists at {config_path}"))

        if not options.get("skip_install"):
            self.run_install(options)

        if not options.get("skip_worker_migrate"):
            self.run_migrate()

        if not options.get("skip_migrate"):
            from django.core.management import call_command
            call_command("migrate")

        self.stdout.write(self.style.MIGRATE_HEADING("\n✨ Reproq is ready!"))

    def run_doctor(self, options):
        strict = options.get("strict", False)
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

        config_path = self._resolve_config_path(options.get("config"), True)
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
        if not config_path:
            settings_dsn = self.get_dsn()
            if settings_dsn and not effective.get("dsn"):
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

        worker_bin, resolved_bin, exists = self._resolve_worker_bin()
        if not exists:
            fail(
                "Worker binary not found. Run `python manage.py reproq install` or set "
                "REPROQ_WORKER_BIN."
            )
        else:
            try:
                version = subprocess.check_output([worker_bin, "--version"]).decode().strip()
                self.stdout.write(self.style.SUCCESS(f"✅ Worker binary: {version}"))
            except Exception as exc:
                fail(f"Worker binary failed version check: {exc}")
                if resolved_bin:
                    self.stderr.write(self.style.ERROR(f"Resolved path: {resolved_bin}"))

        try:
            connection.ensure_connection()
            self.stdout.write(self.style.SUCCESS("✅ Database connection ok."))
        except Exception as exc:
            fail(f"Database connection failed: {exc}")
        else:
            with connection.cursor() as cursor:
                tables = set(connection.introspection.table_names(cursor))
            required_tables = {
                "task_runs",
                "periodic_tasks",
                "reproq_workers",
                "rate_limits",
                "workflow_runs",
            }
            missing = sorted(required_tables - tables)
            if missing:
                fail(
                    "Reproq schema missing tables: "
                    f"{', '.join(missing)} (run reproq migrate-worker + migrate)."
                )
            else:
                self.stdout.write(self.style.SUCCESS("✅ Reproq schema present."))

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
        config_path = self._resolve_config_path(options.get("config"), True)
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
        if not config_path:
            settings_dsn = self.get_dsn()
            if settings_dsn and not effective.get("dsn"):
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

        override_path = getattr(settings, "REPROQ_WORKER_BIN", None) or os.environ.get(
            "REPROQ_WORKER_BIN"
        )

        system = platform.system().lower()
        arch = platform.machine().lower()
        if arch == "x86_64": arch = "amd64"
        if arch == "arm64" or arch == "aarch64": arch = "arm64"
        
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

        with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
            tmp_path = tmp_file.name

        success = False
        if not options.get("build"):
            tag = options.get("tag", "latest")
            url = f"https://github.com/adpena/reproq-worker/releases/download/{tag}/{bin_name}"
            if tag == "latest":
                url = f"https://github.com/adpena/reproq-worker/releases/latest/download/{bin_name}"

            self.stdout.write(f"Downloading pre-built binary: {url}")
            try:
                with urllib.request.urlopen(url) as response:
                    with open(tmp_path, 'wb') as f:
                        shutil.copyfileobj(response, f)
                checksum = self._download_checksum(url)
                if checksum:
                    actual = self._sha256_file(tmp_path)
                    if actual != checksum:
                        raise CommandError("Downloaded binary checksum mismatch.")
                    self.stdout.write(self.style.SUCCESS("✅ Checksum verified."))
                else:
                    self.stdout.write(self.style.WARNING("Checksum not found; skipping verification."))
                success = True
            except Exception as e:
                self.stdout.write(self.style.WARNING(f"Download failed: {e}"))

        if not success:
            source_path = options.get("source") or os.path.join(
                os.path.dirname(
                    os.path.dirname(
                        os.path.dirname(
                            os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
                        )
                    )
                ),
                "reproq-worker",
            )
            if os.path.exists(source_path):
                self.stdout.write(f"Building from local source: {source_path}...")
                try:
                    subprocess.run(["go", "build", "-tags", "prod", "-o", tmp_path, "./cmd/reproq"], cwd=source_path, check=True)
                    success = True
                except Exception as e:
                    self.stderr.write(self.style.WARNING(f"Local build failed: {e}"))
            else:
                self.stdout.write(self.style.WARNING(f"Local source not found: {source_path}"))

        if not success:
            self.stderr.write(self.style.ERROR("Failed to install worker."))
            sys.exit(1)

        if system != "windows": os.chmod(tmp_path, 0o755)
        try:
            subprocess.check_output([tmp_path, "--version"])
            shutil.move(tmp_path, target_path)
            self.stdout.write(self.style.SUCCESS(f"Successfully installed to {target_path}"))
        except Exception as e:
            self.stderr.write(self.style.ERROR(f"Verification failed: {e}"))
            os.unlink(tmp_path)
            sys.exit(1)

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

        services = {
            "worker": " ".join(shlex.quote(str(arg)) for arg in worker_args),
            "beat": " ".join(shlex.quote(str(arg)) for arg in beat_args),
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

        for name, cmd in services.items():
            content = f"""[Unit]
Description=Reproq {name.capitalize()} - {project_name}
After=network.target postgresql.service

[Service]
Type=simple
User={user}
Group={group}
WorkingDirectory={cwd}
{env_block}ExecStart={cmd}
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
"""
            service_name = f"reproq-{name}-{project_name}.service"
            with open(service_name, "w") as f: f.write(content)
            self.stdout.write(f"Generated {service_name}")

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

    def run_reclaim(self, options):
        now = timezone.now()
        older_than = self._parse_duration(options["older_than"])
        cutoff = now - older_than

        lease_filter = Q(leased_until__lt=cutoff)
        if options["include_null_lease"]:
            lease_filter |= Q(leased_until__isnull=True)

        queryset = TaskRun.objects.filter(
            status="RUNNING",
            cancel_requested=False,
        ).filter(lease_filter)

        if options["limit"] and options["limit"] > 0:
            ids = list(
                queryset.order_by("leased_until", "result_id")
                .values_list("result_id", flat=True)[: options["limit"]]
            )
            queryset = TaskRun.objects.filter(result_id__in=ids)

        count = queryset.count()
        if options["dry_run"]:
            self.stdout.write(
                self.style.WARNING(f"Dry run: {count} task(s) match reclaim criteria.")
            )
            return

        if count == 0:
            self.stdout.write(self.style.SUCCESS("No expired leases found."))
            return

        action = options["action"]
        if action == "requeue":
            updated = queryset.update(
                status="READY",
                run_after=now,
                leased_until=None,
                leased_by=None,
                started_at=None,
                finished_at=None,
            )
            self.stdout.write(self.style.SUCCESS(f"Requeued {updated} task(s)."))
            return

        failures = 0
        for run in queryset:
            errors = list(run.errors_json or [])
            errors.append(
                {
                    "at": now.isoformat(),
                    "kind": "reclaim",
                    "message": "Lease expired; marking task failed.",
                }
            )
            run.status = "FAILED"
            run.finished_at = now
            run.last_attempted_at = now
            run.leased_until = None
            run.leased_by = None
            run.errors_json = errors
            run.save(
                update_fields=[
                    "status",
                    "finished_at",
                    "last_attempted_at",
                    "leased_until",
                    "leased_by",
                    "errors_json",
                ]
            )
            failures += 1

        self.stdout.write(self.style.SUCCESS(f"Marked {failures} task(s) failed."))

    def run_prune_workers(self, options):
        from reproq_django.models import Worker
        cutoff = timezone.now() - self._parse_duration(options["older_than"])
        queryset = Worker.objects.filter(last_seen_at__lt=cutoff)
        count = queryset.count()
        if options["dry_run"]:
            self.stdout.write(self.style.WARNING(f"Dry run: {count} worker(s) would be deleted."))
            return
        queryset.delete()
        self.stdout.write(self.style.SUCCESS(f"Deleted {count} stale worker(s)."))

    def run_prune_successful(self, options):
        from reproq_django.models import TaskRun
        cutoff = timezone.now() - self._parse_duration(options["older_than"])
        queryset = TaskRun.objects.filter(status="SUCCESSFUL", finished_at__lt=cutoff)
        if options["limit"] and options["limit"] > 0:
            ids = list(
                queryset.order_by("finished_at", "result_id")
                .values_list("result_id", flat=True)[: options["limit"]]
            )
            queryset = TaskRun.objects.filter(result_id__in=ids)
        count = queryset.count()
        if options["dry_run"]:
            self.stdout.write(self.style.WARNING(f"Dry run: {count} task(s) would be deleted."))
            return
        queryset.delete()
        self.stdout.write(self.style.SUCCESS(f"Deleted {count} successful task(s)."))

    def _backfill_task_path(self, batch_size=1000):
        if connection.vendor != "postgresql":
            return

        self.stdout.write("Backfilling task_path in batches...")
        while True:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    WITH candidates AS (
                        SELECT result_id
                        FROM task_runs
                        WHERE task_path IS NULL
                          AND NULLIF(spec_json->>'task_path', '') IS NOT NULL
                        LIMIT %s
                    )
                    UPDATE task_runs
                    SET task_path = spec_json->>'task_path'
                    WHERE result_id IN (SELECT result_id FROM candidates)
                    RETURNING 1;
                    """,
                    [batch_size],
                )
                updated = cursor.fetchall()
            if not updated:
                break

    def run_migrate(self):
        # Go specific optimizations (e.g. creating extensions)
        self.stdout.write("Applying worker-specific optimizations...")
        with connection.cursor() as cursor:
            try:
                cursor.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto;")
                self.stdout.write(self.style.SUCCESS("✅ pgcrypto extension enabled."))
            except Exception as e:
                self.stderr.write(self.style.WARNING(f"⚠️ Could not enable pgcrypto: {e}"))
        with connection.cursor() as cursor:
            tables = set(connection.introspection.table_names(cursor))
        required_tables = {"task_runs", "periodic_tasks", "reproq_workers", "rate_limits", "workflow_runs"}
        missing_tables = sorted(required_tables - tables)
        ensure_pre_statements = [
            """
            ALTER TABLE task_runs
            ADD COLUMN IF NOT EXISTS last_error TEXT;
            """,
            """
            ALTER TABLE task_runs
            ADD COLUMN IF NOT EXISTS failed_at TIMESTAMPTZ;
            """,
            """
            ALTER TABLE task_runs
            ADD COLUMN IF NOT EXISTS task_path TEXT;
            """,
        ]
        ensure_post_statements = [
            """
            CREATE OR REPLACE FUNCTION reproq_task_path_from_spec()
            RETURNS trigger AS $$
            BEGIN
                IF NEW.task_path IS NULL OR NEW.task_path = '' THEN
                    NEW.task_path := NEW.spec_json->>'task_path';
                END IF;
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
            """,
            """
            DROP TRIGGER IF EXISTS trg_task_runs_task_path ON task_runs;
            """,
            """
            CREATE TRIGGER trg_task_runs_task_path
            BEFORE INSERT OR UPDATE OF spec_json, task_path ON task_runs
            FOR EACH ROW
            EXECUTE FUNCTION reproq_task_path_from_spec();
            """,
            """
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_constraint WHERE conname = 'task_runs_task_path_not_empty'
                ) THEN
                    ALTER TABLE task_runs
                    ADD CONSTRAINT task_runs_task_path_not_empty CHECK (task_path IS NOT NULL AND task_path <> '') NOT VALID;
                END IF;
            END $$;
            """,
            """
            CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_task_runs_failed_at
            ON task_runs (failed_at)
            WHERE status = 'FAILED';
            """,
            """
            CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_task_runs_task_path
            ON task_runs (task_path);
            """,
            """
            CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_task_runs_claim_due
            ON task_runs (queue_name, COALESCE(run_after, '-infinity'::timestamptz), priority DESC, enqueued_at ASC)
            WHERE status = 'READY';
            """,
        ]
        if not missing_tables:
            self.stdout.write(self.style.SUCCESS("✅ Reproq schema already present."))
            with connection.cursor() as cursor:
                for statement in ensure_pre_statements:
                    cursor.execute(statement)
            self._backfill_task_path()
            with connection.cursor() as cursor:
                for statement in ensure_post_statements:
                    cursor.execute(statement)
            self.stdout.write(self.style.SUCCESS("✅ Reproq schema updated."))
            return
        self.stdout.write(
            self.style.MIGRATE_HEADING(
                f"Applying Reproq schema (missing: {', '.join(missing_tables)})..."
            )
        )
        statements = [
            """
            CREATE TABLE IF NOT EXISTS task_runs (
                result_id BIGSERIAL PRIMARY KEY,
                backend_alias TEXT NOT NULL DEFAULT 'default',
                queue_name TEXT NOT NULL DEFAULT 'default',
                priority INTEGER NOT NULL DEFAULT 0,
                run_after TIMESTAMPTZ,
                spec_json JSONB NOT NULL,
                spec_hash CHAR(64) NOT NULL,
                task_path TEXT,
                status TEXT NOT NULL DEFAULT 'READY',
                enqueued_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                started_at TIMESTAMPTZ,
                last_attempted_at TIMESTAMPTZ,
                finished_at TIMESTAMPTZ,
                attempts INTEGER NOT NULL DEFAULT 0,
                max_attempts INTEGER NOT NULL DEFAULT 3,
                timeout_seconds INTEGER NOT NULL DEFAULT 900,
                lock_key TEXT,
                worker_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
                return_json JSONB,
                errors_json JSONB NOT NULL DEFAULT '[]'::jsonb,
                last_error TEXT,
                failed_at TIMESTAMPTZ,
                leased_until TIMESTAMPTZ,
                leased_by TEXT,
                logs_uri TEXT,
                artifacts_uri TEXT,
                expires_at TIMESTAMPTZ,
                parent_id BIGINT REFERENCES task_runs(result_id) ON DELETE SET NULL,
                workflow_id UUID,
                wait_count INTEGER NOT NULL DEFAULT 0,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                cancel_requested BOOLEAN NOT NULL DEFAULT FALSE
            );
            """,
            """
            CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_task_runs_claim
            ON task_runs (queue_name, status, priority DESC, enqueued_at ASC)
            WHERE status = 'READY';
            """,
            """
            CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS idx_task_runs_spec_unique
            ON task_runs (spec_hash)
            WHERE status IN ('READY', 'RUNNING');
            """,
            """
            CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_task_runs_lock_key
            ON task_runs (lock_key)
            WHERE status = 'RUNNING';
            """,
            """
            CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_task_runs_parent_id
            ON task_runs (parent_id)
            WHERE status = 'WAITING';
            """,
            """
            CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_task_runs_workflow_id
            ON task_runs (workflow_id);
            """,
            """
            CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_task_runs_failed_at
            ON task_runs (failed_at)
            WHERE status = 'FAILED';
            """,
            """
            CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_task_runs_task_path
            ON task_runs (task_path);
            """,
            """
            CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_task_runs_claim_due
            ON task_runs (queue_name, COALESCE(run_after, '-infinity'::timestamptz), priority DESC, enqueued_at ASC)
            WHERE status = 'READY';
            """,
            """
            CREATE OR REPLACE FUNCTION reproq_task_path_from_spec()
            RETURNS trigger AS $$
            BEGIN
                IF NEW.task_path IS NULL OR NEW.task_path = '' THEN
                    NEW.task_path := NEW.spec_json->>'task_path';
                END IF;
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
            """,
            """
            DROP TRIGGER IF EXISTS trg_task_runs_task_path ON task_runs;
            """,
            """
            CREATE TRIGGER trg_task_runs_task_path
            BEFORE INSERT OR UPDATE OF spec_json, task_path ON task_runs
            FOR EACH ROW
            EXECUTE FUNCTION reproq_task_path_from_spec();
            """,
            """
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_constraint WHERE conname = 'task_runs_task_path_not_empty'
                ) THEN
                    ALTER TABLE task_runs
                    ADD CONSTRAINT task_runs_task_path_not_empty CHECK (task_path IS NOT NULL AND task_path <> '') NOT VALID;
                END IF;
            END $$;
            """,
            """
            CREATE TABLE IF NOT EXISTS periodic_tasks (
                name TEXT PRIMARY KEY,
                cron_expr TEXT NOT NULL,
                task_path TEXT NOT NULL,
                payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                queue_name TEXT NOT NULL DEFAULT 'default',
                priority INTEGER NOT NULL DEFAULT 0,
                max_attempts INTEGER NOT NULL DEFAULT 3,
                last_run_at TIMESTAMPTZ,
                next_run_at TIMESTAMPTZ NOT NULL,
                enabled BOOLEAN NOT NULL DEFAULT TRUE,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """,
            """
            CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_periodic_tasks_next_run
            ON periodic_tasks (next_run_at)
            WHERE enabled = TRUE;
            """,
            """
            CREATE TABLE IF NOT EXISTS reproq_workers (
                worker_id TEXT PRIMARY KEY,
                hostname TEXT,
                concurrency INTEGER,
                queues JSONB NOT NULL DEFAULT '[]'::jsonb,
                started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                version TEXT
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS rate_limits (
                key TEXT PRIMARY KEY,
                tokens_per_second REAL NOT NULL,
                burst_size INTEGER NOT NULL,
                current_tokens REAL NOT NULL,
                last_refilled_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """,
            """
            INSERT INTO rate_limits (key, tokens_per_second, burst_size, current_tokens)
            VALUES ('global', 0, 1, 0)
            ON CONFLICT (key) DO NOTHING;
            """,
            """
            CREATE TABLE IF NOT EXISTS workflow_runs (
                workflow_id UUID PRIMARY KEY,
                expected_count INTEGER NOT NULL,
                success_count INTEGER NOT NULL DEFAULT 0,
                failure_count INTEGER NOT NULL DEFAULT 0,
                callback_result_id BIGINT,
                status TEXT NOT NULL DEFAULT 'RUNNING',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """,
            """
            CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_workflow_runs_callback
            ON workflow_runs (callback_result_id);
            """,
        ]
        with connection.cursor() as cursor:
            for statement in statements:
                cursor.execute(statement)
            for statement in ensure_pre_statements:
                cursor.execute(statement)
        self._backfill_task_path()
        with connection.cursor() as cursor:
            for statement in ensure_post_statements:
                cursor.execute(statement)
        self.stdout.write(self.style.SUCCESS("✅ Reproq schema applied."))

    def run_worker_or_beat(self, cmd, options):
        dsn = self.get_dsn()
        worker_bin, resolved_bin, exists = self._resolve_worker_bin()
        if not exists:
            hint = (
                "Run `python manage.py reproq install` or set REPROQ_WORKER_BIN "
                "to the installed binary path."
            )
            raise CommandError(
                f"Worker binary not found (resolved: {resolved_bin or worker_bin}). {hint}"
            )
        if cmd == "worker":
            explicit_flags = any(
                self._flag_present(flag)
                for flag in [
                    "--config",
                    "--concurrency",
                    "--queues",
                    "--queue",
                    "--allowed-task-modules",
                    "--logs-dir",
                    "--payload-mode",
                    "--metrics-port",
                    "--metrics-addr",
                    "--metrics-auth-token",
                    "--metrics-allow-cidrs",
                    "--metrics-tls-cert",
                    "--metrics-tls-key",
                    "--metrics-tls-client-ca",
                    "--metrics-auth-limit",
                    "--metrics-auth-window",
                    "--metrics-auth-max-entries",
                ]
            )
        else:
            explicit_flags = any(
                self._flag_present(flag)
                for flag in [
                    "--config",
                    "--interval",
                ]
            )

        config_path = self._resolve_config_path(options.get("config"), not explicit_flags)
        use_config = bool(config_path)
        if use_config and not os.path.exists(config_path):
            raise CommandError(f"Config file not found: {config_path}")

        if not dsn and not use_config:
            raise CommandError(
                "DATABASE_URL not set. Reproq worker requires a Postgres DSN."
            )

        self.stdout.write(f"Worker binary: {resolved_bin or worker_bin}")
        if use_config:
            self.stdout.write(f"Config: {config_path}")
        else:
            self.stdout.write(f"DSN: {self._mask_dsn(dsn)}")

        args = [worker_bin, cmd]
        if use_config:
            args.extend(["--config", config_path])
        else:
            args.extend(["--dsn", dsn])

        if cmd == "worker":
            if use_config:
                if self._flag_present("--concurrency"):
                    args.extend(["--concurrency", str(options["concurrency"])])
                if self._flag_present("--queues"):
                    args.extend(["--queues", options.get("queues") or ""])
                elif self._flag_present("--queue"):
                    args.extend(["--queues", options.get("queue", "default")])
                if self._flag_present("--allowed-task-modules"):
                    args.extend(["--allowed-task-modules", options.get("allowed_task_modules") or ""])
                if self._flag_present("--logs-dir"):
                    args.extend(["--logs-dir", options.get("logs_dir") or ""])
                if self._flag_present("--payload-mode"):
                    args.extend(["--payload-mode", options.get("payload_mode") or ""])
                if self._flag_present("--metrics-port"):
                    args.extend(["--metrics-port", str(options["metrics_port"])])
                if self._flag_present("--metrics-addr"):
                    args.extend(["--metrics-addr", options.get("metrics_addr") or ""])
                if self._flag_present("--metrics-auth-token"):
                    args.extend(["--metrics-auth-token", options.get("metrics_auth_token") or ""])
                if self._flag_present("--metrics-allow-cidrs"):
                    args.extend(["--metrics-allow-cidrs", options.get("metrics_allow_cidrs") or ""])
                if self._flag_present("--metrics-auth-limit"):
                    args.extend(["--metrics-auth-limit", str(options["metrics_auth_limit"])])
                if self._flag_present("--metrics-auth-window"):
                    args.extend(["--metrics-auth-window", options.get("metrics_auth_window") or ""])
                if self._flag_present("--metrics-auth-max-entries"):
                    args.extend(["--metrics-auth-max-entries", str(options["metrics_auth_max_entries"])])
                if self._flag_present("--metrics-tls-cert"):
                    args.extend(["--metrics-tls-cert", options.get("metrics_tls_cert") or ""])
                if self._flag_present("--metrics-tls-key"):
                    args.extend(["--metrics-tls-key", options.get("metrics_tls_key") or ""])
                if self._flag_present("--metrics-tls-client-ca"):
                    args.extend(["--metrics-tls-client-ca", options.get("metrics_tls_client_ca") or ""])
            else:
                args.extend(["--concurrency", str(options["concurrency"])])
                queues = options.get("queues") or ""
                if queues:
                    args.extend(["--queues", queues])
                else:
                    args.extend(["--queues", options.get("queue", "default")])
                if options.get("allowed_task_modules"):
                    args.extend(["--allowed-task-modules", options["allowed_task_modules"]])
                elif os.environ.get("ALLOWED_TASK_MODULES"):
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
                if options.get("logs_dir"):
                    args.extend(["--logs-dir", options["logs_dir"]])
                if options.get("payload_mode"):
                    args.extend(["--payload-mode", options["payload_mode"]])
                if options.get("metrics_port"):
                    args.extend(["--metrics-port", str(options["metrics_port"])])
                if options.get("metrics_addr"):
                    args.extend(["--metrics-addr", options["metrics_addr"]])
                if options.get("metrics_auth_token"):
                    args.extend(["--metrics-auth-token", options["metrics_auth_token"]])
                if options.get("metrics_allow_cidrs"):
                    args.extend(["--metrics-allow-cidrs", options["metrics_allow_cidrs"]])
                if options.get("metrics_tls_cert"):
                    args.extend(["--metrics-tls-cert", options["metrics_tls_cert"]])
                if options.get("metrics_tls_key"):
                    args.extend(["--metrics-tls-key", options["metrics_tls_key"]])
                if options.get("metrics_tls_client_ca"):
                    args.extend(["--metrics-tls-client-ca", options["metrics_tls_client_ca"]])
                if options.get("metrics_auth_limit") is not None:
                    args.extend(["--metrics-auth-limit", str(options["metrics_auth_limit"])])
                if options.get("metrics_auth_window"):
                    args.extend(["--metrics-auth-window", options["metrics_auth_window"]])
                if options.get("metrics_auth_max_entries") is not None:
                    args.extend(["--metrics-auth-max-entries", str(options["metrics_auth_max_entries"])])
        elif cmd == "beat":
            if use_config:
                if self._flag_present("--interval"):
                    args.extend(["--interval", options["interval"]])
            else:
                args.extend(["--interval", options["interval"]])
            
        self.stdout.write(f"Starting {cmd}...")
        try:
            subprocess.run(args, check=True)
        except KeyboardInterrupt:
            pass
        except Exception as e:
            self.stderr.write(self.style.ERROR(f"Error running {cmd}: {e}"))

    def run_pg_cron(self, options):
        if connection.vendor != "postgresql":
            raise CommandError("pg-cron requires a PostgreSQL database.")

        install = options.get("install")
        remove = options.get("remove")
        if install and remove:
            raise CommandError("Choose either --install or --remove (not both).")
        if not install and not remove:
            install = True

        prefix = (options.get("prefix") or "reproq_periodic").strip()
        dry_run = options.get("dry_run")

        with connection.cursor() as cursor:
            tables = set(connection.introspection.table_names(cursor))
            if "periodic_tasks" not in tables:
                raise CommandError("Missing periodic_tasks table. Run migrations first.")
            self._ensure_pg_cron_extensions(cursor, dry_run)
            supports_named = self._pg_cron_supports_named_jobs(cursor)
            if remove:
                self._unschedule_pg_cron_jobs(cursor, prefix, supports_named, dry_run)
                self._drop_pg_cron_function(cursor, dry_run)
                return

            self._ensure_pg_cron_function(cursor, dry_run)
            self._unschedule_pg_cron_jobs(cursor, prefix, supports_named, dry_run)
            self._schedule_pg_cron_jobs(cursor, prefix, supports_named, dry_run)

    def _ensure_pg_cron_extensions(self, cursor, dry_run):
        if dry_run:
            self.stdout.write("dry-run: ensure extensions pgcrypto, pg_cron")
            return
        try:
            cursor.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto;")
            cursor.execute("CREATE EXTENSION IF NOT EXISTS pg_cron;")
        except Exception as exc:
            raise CommandError(
                "Failed to enable pg_cron. Ensure it is available and listed in "
                "shared_preload_libraries on your Postgres server."
            ) from exc

    def _pg_cron_supports_named_jobs(self, cursor):
        cursor.execute(
            """
            SELECT COUNT(*)
            FROM pg_proc p
            JOIN pg_namespace n ON p.pronamespace = n.oid
            WHERE n.nspname = 'cron' AND p.proname = 'schedule' AND p.pronargs = 3
            """
        )
        return cursor.fetchone()[0] > 0

    def _drop_pg_cron_function(self, cursor, dry_run):
        if dry_run:
            self.stdout.write("dry-run: drop function reproq_enqueue_periodic_task")
            return
        cursor.execute("DROP FUNCTION IF EXISTS reproq_enqueue_periodic_task(TEXT);")

    def _ensure_pg_cron_function(self, cursor, dry_run):
        sql = """
            CREATE OR REPLACE FUNCTION reproq_enqueue_periodic_task(task_name TEXT)
            RETURNS VOID
            LANGUAGE plpgsql
            AS $$
            DECLARE
                task_row periodic_tasks%ROWTYPE;
                args_json JSONB;
                spec JSONB;
                spec_hash TEXT;
            BEGIN
                SELECT * INTO task_row
                FROM periodic_tasks
                WHERE name = task_name AND enabled = TRUE;

                IF NOT FOUND THEN
                    RETURN;
                END IF;

                args_json := COALESCE(task_row.payload_json, '[]'::jsonb);
                spec := jsonb_build_object(
                    'task_path', task_row.task_path,
                    'args', args_json,
                    'kwargs', '{}'::jsonb,
                    'periodic_name', task_row.name,
                    'scheduled_at', to_char(NOW() AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
                );
                spec_hash := encode(digest(spec::text, 'sha256'), 'hex');

                INSERT INTO task_runs (
                    backend_alias,
                    queue_name,
                    priority,
                    run_after,
                    spec_json,
                    spec_hash,
                    status,
                    enqueued_at,
                    attempts,
                    max_attempts,
                    timeout_seconds,
                    wait_count,
                    worker_ids,
                    errors_json,
                    cancel_requested,
                    created_at,
                    updated_at
                )
                SELECT
                    'default',
                    COALESCE(task_row.queue_name, 'default'),
                    COALESCE(task_row.priority, 0),
                    NOW(),
                    spec,
                    spec_hash,
                    'READY',
                    NOW(),
                    0,
                    COALESCE(NULLIF(task_row.max_attempts, 0), 3),
                    900,
                    0,
                    '[]'::jsonb,
                    '[]'::jsonb,
                    FALSE,
                    NOW(),
                    NOW()
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM task_runs
                    WHERE task_runs.spec_hash = spec_hash
                      AND status IN ('READY', 'RUNNING')
                );

                UPDATE periodic_tasks
                SET last_run_at = NOW(),
                    updated_at = NOW()
                WHERE name = task_row.name;
            END;
            $$;
        """
        if dry_run:
            self.stdout.write("dry-run: create function reproq_enqueue_periodic_task")
            return
        cursor.execute(sql)

    def _unschedule_pg_cron_jobs(self, cursor, prefix, supports_named, dry_run):
        rows = []
        if supports_named:
            cursor.execute(
                "SELECT jobid, jobname FROM cron.job WHERE jobname LIKE %s",
                [prefix + "%"],
            )
            rows = cursor.fetchall()
        else:
            cursor.execute(
                "SELECT jobid, command FROM cron.job WHERE command LIKE %s",
                ["SELECT reproq_enqueue_periodic_task(%"],
            )
            rows = cursor.fetchall()

        if not rows:
            self.stdout.write("No existing pg_cron jobs to remove.")
            return

        for jobid, label in rows:
            if dry_run:
                self.stdout.write(f"dry-run: unschedule job {label} ({jobid})")
                continue
            cursor.execute("SELECT cron.unschedule(%s);", [jobid])
        self.stdout.write(f"Removed {len(rows)} pg_cron job(s).")

    def _schedule_pg_cron_jobs(self, cursor, prefix, supports_named, dry_run):
        tasks = list(
            PeriodicTask.objects.filter(enabled=True).values(
                "name",
                "cron_expr",
            )
        )
        if not tasks:
            self.stdout.write("No enabled periodic tasks found; nothing to schedule.")
            return

        used_names = set()
        for task in tasks:
            name = task["name"]
            cron_expr = task["cron_expr"]
            if not cron_expr:
                self.stdout.write(f"Skipping periodic task with empty cron_expr: {name}")
                continue
            jobname = self._pg_cron_job_name(prefix, name)
            while jobname in used_names:
                jobname = self._pg_cron_job_name(
                    prefix,
                    f"{name}-{hashlib.sha256(jobname.encode('utf-8')).hexdigest()[:4]}",
                )
            used_names.add(jobname)
            escaped = name.replace("'", "''")
            command = f"SELECT reproq_enqueue_periodic_task('{escaped}');"
            if dry_run:
                self.stdout.write(
                    f"dry-run: schedule {jobname} {cron_expr} -> {command}"
                )
                continue
            if supports_named:
                cursor.execute(
                    "SELECT cron.schedule(%s, %s, %s);",
                    [jobname, cron_expr, command],
                )
            else:
                cursor.execute(
                    "SELECT cron.schedule(%s, %s);",
                    [cron_expr, command],
                )
        self.stdout.write(f"Scheduled {len(tasks)} pg_cron job(s).")

    def _pg_cron_job_name(self, prefix, name):
        cleaned = re.sub(r"[^a-zA-Z0-9_]+", "_", name).strip("_")
        if not cleaned:
            cleaned = hashlib.sha256(name.encode("utf-8")).hexdigest()[:8]
        base = f"{prefix}_{cleaned}" if prefix else cleaned
        if len(base) <= 63:
            return base
        suffix = hashlib.sha256(base.encode("utf-8")).hexdigest()[:8]
        return f"{base[:54]}_{suffix}"

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
            config_path = self._resolve_config_path(options.get("config"), True)
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
        try:
            run = TaskRun.objects.get(result_id=result_id)
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
        result_id = options["id"]
        try:
            run = TaskRun.objects.get(result_id=result_id)
        except TaskRun.DoesNotExist as exc:
            raise CommandError(f"Task run {result_id} not found.") from exc

        if run.cancel_requested:
            self.stdout.write(self.style.WARNING("Cancel already requested."))
        else:
            run.cancel_requested = True
            run.save(update_fields=["cancel_requested"])
            self.stdout.write(self.style.SUCCESS("Cancel requested."))

        if run.status in ("SUCCESSFUL", "FAILED", "CANCELLED"):
            self.stdout.write(
                self.style.WARNING(
                    f"Task is already {run.status}; worker may not cancel it."
                )
            )

    def run_stats(self):
        from reproq_django.models import TaskRun, Worker
        from django.db.models import Count

        self.stdout.write(self.style.MIGRATE_HEADING("📊 Reproq Statistics"))
        
        stats = TaskRun.objects.values("status").annotate(count=Count("result_id"))
        self.stdout.write("\nTasks by Status:")
        for s in stats:
            color = self.style.SUCCESS if s["status"] == "SUCCESSFUL" else (self.style.ERROR if s["status"] == "FAILED" else self.style.WARNING)
            self.stdout.write(f"  {s['status']:<12}: {color(str(s['count']))}")

        workers = Worker.objects.all()
        self.stdout.write(f"\nActive Workers: {len(workers)}")
        for w in workers:
            self.stdout.write(f"  - {w.worker_id} ({w.hostname}) | Queues: {w.queues} | Concurrency: {w.concurrency}")

    def run_stress_test(self, options):
        from reproq_django.tasks import debug_noop_task
        import time
        import django.tasks as django_tasks

        count = options["count"]
        sleep = options["sleep"]
        bulk = options["bulk"]

        self.stdout.write(self.style.MIGRATE_HEADING(f"🚀 Enqueueing {count} tasks (sleep={sleep}s, bulk={bulk})..."))
        start = time.time()

        def _resolve_backend():
            if hasattr(django_tasks, "tasks"):
                try:
                    return django_tasks.tasks["default"]
                except Exception:
                    pass
            if hasattr(django_tasks, "get_task_backend"):
                try:
                    return django_tasks.get_task_backend("default")
                except Exception:
                    pass
            if hasattr(debug_noop_task, "get_backend"):
                try:
                    return debug_noop_task.get_backend()
                except Exception:
                    pass
            backend = getattr(debug_noop_task, "backend", None)
            if backend and not isinstance(backend, str):
                return backend
            return None

        if bulk:
            backend = _resolve_backend()
            if backend is None:
                self.stderr.write(
                    self.style.WARNING(
                        "Bulk enqueue unavailable; falling back to per-task enqueue."
                    )
                )
                bulk = False
            else:
                tasks_data = [
                    (debug_noop_task, (), {"sleep_seconds": sleep})
                    for _ in range(count)
                ]
                backend.bulk_enqueue(tasks_data)

        if not bulk:
            for _ in range(count):
                debug_noop_task.enqueue(sleep_seconds=sleep)

        duration = time.time() - start
        self.stdout.write(self.style.SUCCESS(f"✅ Enqueued {count} tasks in {duration:.2f}s ({count/duration:.1f} tasks/sec)"))
        self.stdout.write("Run 'python manage.py reproq worker' to process them.")

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

    def get_dsn(self):
        env_dsn = os.environ.get("DATABASE_URL")
        if env_dsn: return env_dsn
        db_conf = settings.DATABASES.get("default", {})
        user = db_conf.get("USER")
        name = db_conf.get("NAME")
        if not user or not name: return None
        return f"postgres://{user}:{db_conf.get('PASSWORD', '')}@{db_conf.get('HOST', 'localhost')}:{db_conf.get('PORT', '5432')}/{name}"

    def _flag_present(self, flag):
        for arg in sys.argv:
            if arg == flag or arg.startswith(flag + "="):
                return True
        return False

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

    def _resolve_config_path(self, option_value, allow_default):
        if option_value:
            return option_value
        env_value = os.environ.get("REPROQ_CONFIG")
        if env_value:
            return env_value
        if allow_default:
            return self._find_default_config()
        return ""

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
            from urllib.parse import urlparse
        except Exception:
            return "<unavailable>"
        try:
            parsed = urlparse(dsn)
        except Exception:
            return "<invalid>"
        if not parsed.scheme:
            return "<invalid>"
        host = parsed.hostname or ""
        port = f":{parsed.port}" if parsed.port else ""
        user = parsed.username or ""
        db = parsed.path.lstrip("/") if parsed.path else ""
        user_part = f"{user}@" if user else ""
        return f"{parsed.scheme}://{user_part}{host}{port}/{db}"

    def _sha256_file(self, path):
        h = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                h.update(chunk)
        return h.hexdigest()

    def _download_checksum(self, url):
        checksum_url = f"{url}.sha256"
        try:
            with urllib.request.urlopen(checksum_url) as response:
                payload = response.read().decode().strip()
        except Exception:
            return ""
        if not payload:
            return ""
        return payload.split()[0]

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
        if options.get("config"):
            return os.path.abspath(os.path.expanduser(options["config"]))
        fmt = options.get("format", "yaml")
        filename = "reproq.yaml" if fmt == "yaml" else "reproq.toml"
        return os.path.join(os.getcwd(), filename)

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
        candidates = [os.path.join(os.getcwd(), filename)]
        repo_root = os.path.abspath(
            os.path.join(os.path.dirname(os.path.abspath(__file__)), "../../../../")
        )
        candidates.append(os.path.join(repo_root, filename))
        for candidate in candidates:
            if os.path.exists(candidate):
                with open(candidate, "r", encoding="utf-8") as handle:
                    return handle.read()
        try:
            resource = importlib.resources.files("reproq_django.resources").joinpath(filename)
            return resource.read_text(encoding="utf-8")
        except Exception:
            return self._fallback_config_template(fmt)

    def _fallback_config_template(self, fmt):
        if fmt == "toml":
            return (
                'dsn = "postgres://user:pass@localhost:5432/reproq?sslmode=disable"\n'
                "\n"
                "[worker]\n"
                "queues = [\"default\"]\n"
                "concurrency = 10\n"
                "\n"
                "[beat]\n"
                "interval = \"30s\"\n"
            )
        return (
            'dsn: "postgres://user:pass@localhost:5432/reproq?sslmode=disable"\n'
            "worker:\n"
            "  queues:\n"
            "    - default\n"
            "  concurrency: 10\n"
            "beat:\n"
            "  interval: \"30s\"\n"
        )

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
                "max_attempts_default": 3,
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

        if os.environ.get("DATABASE_URL"):
            dsn = os.environ["DATABASE_URL"]
            set_path(["dsn"], dsn)
            set_path(["worker", "dsn"], dsn)
            set_path(["beat", "dsn"], dsn)

        if os.environ.get("WORKER_ID"):
            set_path(["worker", "worker_id"], os.environ["WORKER_ID"])

        if os.environ.get("QUEUE_NAMES"):
            set_path(
                ["worker", "queues"],
                self._parse_comma_list(os.environ["QUEUE_NAMES"]),
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
        config["dsn"] = dsn
        config.setdefault("worker", {})["dsn"] = dsn
        config.setdefault("beat", {})["dsn"] = dsn
        sources["dsn"] = source
        sources["worker.dsn"] = source
        sources["beat.dsn"] = source

    def _resolve_effective_dsn(self, config, scope):
        if scope == "worker":
            return config.get("worker", {}).get("dsn") or config.get("dsn")
        if scope == "beat":
            return config.get("beat", {}).get("dsn") or config.get("dsn")
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

import os
import re
import subprocess
import sys
import platform
import shutil
import urllib.request
import tempfile
from datetime import timedelta
from django.core.management.base import BaseCommand, CommandError
from django.conf import settings
from django.db import connection
from django.db.models import Q
from django.utils import timezone
from ...models import TaskRun

class Command(BaseCommand):
    help = "Unified Reproq management command"

    def add_arguments(self, parser):
        subparsers = parser.add_subparsers(dest="subcommand", required=True)

        # Worker
        worker_parser = subparsers.add_parser("worker", help="Start the Go worker")
        worker_parser.add_argument("--concurrency", type=int, default=10)
        worker_parser.add_argument("--queue", type=str, default="default")
        worker_parser.add_argument("--metrics-port", type=int, default=0, help="Port to serve Prometheus metrics")

        # Beat
        beat_parser = subparsers.add_parser("beat", help="Start the periodic task scheduler (beat)")
        beat_parser.add_argument("--interval", type=str, default="30s")

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
        subparsers.add_parser("init", help="Bootstrap Reproq in the current project")

        # Stats
        subparsers.add_parser("stats", help="Show task execution statistics")

        # Stress Test
        stress_parser = subparsers.add_parser("stress-test", help="Enqueue a large number of tasks for benchmarking")
        stress_parser.add_argument("--count", type=int, default=100, help="Number of tasks to enqueue")
        stress_parser.add_argument("--sleep", type=float, default=0, help="Time each task should sleep")
        stress_parser.add_argument("--bulk", action="store_true", help="Use bulk_enqueue")

        # systemd
        systemd_parser = subparsers.add_parser("systemd", help="Generate systemd service files")
        systemd_parser.add_argument("--user", type=str, help="User to run as")
        systemd_parser.add_argument("--group", type=str, help="Group to run as")
        systemd_parser.add_argument("--concurrency", type=int, default=10)

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
        elif subcommand == "stats":
            self.run_stats()
        elif subcommand == "stress-test":
            self.run_stress_test(options)
        elif subcommand == "systemd":
            self.run_systemd(options)
        elif subcommand == "install":
            self.run_install(options)
        elif subcommand == "migrate-worker":
            self.run_migrate()
        elif subcommand == "reclaim":
            self.run_reclaim(options)
        elif subcommand == "prune-workers":
            self.run_prune_workers(options)
        elif subcommand == "prune-successful":
            self.run_prune_successful(options)
        elif subcommand in ["worker", "beat"]:
            self.run_worker_or_beat(subcommand, options)

    def run_check(self):
        self.stdout.write("Checking Reproq configuration...")
        worker_bin, resolved_bin, exists = self._resolve_worker_bin()
        self.stdout.write(f"Resolved worker binary: {resolved_bin or worker_bin}")
        try:
            version = subprocess.check_output([worker_bin, "--version"]).decode().strip()
            self.stdout.write(self.style.SUCCESS(f"✅ Worker binary: {version}"))
        except Exception as e:
            self.stderr.write(self.style.ERROR(f"❌ Worker binary invalid: {e}"))
            if resolved_bin:
                self.stderr.write(self.style.ERROR(f"   Resolved path: {resolved_bin}"))

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

        with connection.cursor() as cursor:
            tables = connection.introspection.table_names(cursor)
            if "task_runs" in tables:
                self.stdout.write(self.style.SUCCESS("✅ Database schema present."))
            else:
                self.stderr.write(self.style.ERROR("❌ Database schema missing (run migrate)."))
        
        self.stdout.write(self.style.SUCCESS("\n✨ Configuration looks good!"))

    def run_init(self, options):
        self.stdout.write(self.style.MIGRATE_HEADING("🚀 Bootstrapping Reproq..."))
        if "reproq_django" not in settings.INSTALLED_APPS:
            self.stderr.write(self.style.ERROR("❌ 'reproq_django' not found in INSTALLED_APPS."))
        else:
            self.stdout.write(self.style.SUCCESS("✅ 'reproq_django' is in INSTALLED_APPS."))

        self.run_install(options)
        self.stdout.write(self.style.MIGRATE_HEADING("\n✨ Reproq is ready!"))

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
                    subprocess.run(["go", "build", "-o", tmp_path, "./cmd/reproq"], cwd=source_path, check=True)
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
        
        services = {
            "worker": f"{python_bin} {manage_py} reproq worker --concurrency {options['concurrency']}",
            "beat": f"{python_bin} {manage_py} reproq beat"
        }

        for name, cmd in services.items():
            content = f"""[Unit]
Description=Reproq {name.capitalize()} - {project_name}
After=network.target postgresql.service

[Service]
Type=simple
User={user}
Group={group}
WorkingDirectory={cwd}
ExecStart={cmd}
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
        if not missing_tables:
            self.stdout.write(self.style.SUCCESS("✅ Reproq schema already present."))
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
            CREATE INDEX IF NOT EXISTS idx_task_runs_claim
            ON task_runs (queue_name, status, priority DESC, enqueued_at ASC)
            WHERE status = 'READY';
            """,
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_task_runs_spec_unique
            ON task_runs (spec_hash)
            WHERE status IN ('READY', 'RUNNING');
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_task_runs_lock_key
            ON task_runs (lock_key)
            WHERE status = 'RUNNING';
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_task_runs_parent_id
            ON task_runs (parent_id)
            WHERE status = 'WAITING';
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_task_runs_workflow_id
            ON task_runs (workflow_id);
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
            CREATE INDEX IF NOT EXISTS idx_periodic_tasks_next_run
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
            CREATE INDEX IF NOT EXISTS idx_workflow_runs_callback
            ON workflow_runs (callback_result_id);
            """,
        ]
        with connection.cursor() as cursor:
            for statement in statements:
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
        if not dsn:
            raise CommandError(
                "DATABASE_URL not set. Reproq worker requires a Postgres DSN."
            )
        self.stdout.write(f"Worker binary: {resolved_bin or worker_bin}")
        self.stdout.write(f"DSN: {self._mask_dsn(dsn)}")
        args = [worker_bin, cmd, "--dsn", dsn]
        if cmd == "worker":
            args.extend(["--concurrency", str(options["concurrency"])])
            if options.get("metrics_port"):
                args.extend(["--metrics-port", str(options["metrics_port"])])
        elif cmd == "beat":
            args.extend(["--interval", options["interval"]])
            
        self.stdout.write(f"Starting {cmd}...")
        try:
            subprocess.run(args, check=True)
        except KeyboardInterrupt:
            pass
        except Exception as e:
            self.stderr.write(self.style.ERROR(f"Error running {cmd}: {e}"))

    def run_stats(self):
        from reproq_django.models import TaskRun, Worker
        from django.db.models import Count
        import json

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

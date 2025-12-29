import os
import subprocess
import sys
import platform
import shutil
import urllib.request
import tempfile
from django.core.management.base import BaseCommand, CommandError
from django.conf import settings
from django.db import connection

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
        elif subcommand in ["worker", "beat"]:
            self.run_worker_or_beat(subcommand, options)

    def run_check(self):
        self.stdout.write("Checking Reproq configuration...")
        worker_bin, resolved_bin, exists = self._resolve_worker_bin()
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
        self.stdout.write(f"Target path: {target_path}")

        with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
            tmp_path = tmp_file.name

        success = False
        if not options.get("build"):
            tag = options.get("tag", "latest")
            url = f"https://github.com/adpena/reproq-worker/releases/download/{tag}/{bin_name}"
            if tag == "latest":
                url = f"https://github.com/adpena/reproq-worker/releases/latest/download/{bin_name}"

            self.stdout.write(f"Downloading pre-built binary for {system}/{arch}...")
            try:
                with urllib.request.urlopen(url) as response:
                    with open(tmp_path, 'wb') as f:
                        shutil.copyfileobj(response, f)
                success = True
            except Exception as e:
                self.stdout.write(self.style.WARNING(f"Download failed: {e}"))

        if not success:
            source_path = options.get("source") or os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))), "reproq-worker")
            if os.path.exists(source_path):
                self.stdout.write(f"Building from local source: {source_path}...")
                try:
                    subprocess.run(["go", "build", "-o", tmp_path, "./cmd/reproq"], cwd=source_path, check=True)
                    success = True
                except:
                    pass

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

    def run_migrate(self):
        # Go specific optimizations (e.g. creating extensions)
        self.stdout.write("Applying worker-specific optimizations...")
        with connection.cursor() as cursor:
            try:
                cursor.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto;")
                self.stdout.write(self.style.SUCCESS("✅ pgcrypto extension enabled."))
            except Exception as e:
                self.stderr.write(self.style.WARNING(f"⚠️ Could not enable pgcrypto: {e}"))

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
        from django.tasks import tasks
        import time

        count = options["count"]
        sleep = options["sleep"]
        bulk = options["bulk"]

        self.stdout.write(self.style.MIGRATE_HEADING(f"🚀 Enqueueing {count} tasks (sleep={sleep}s, bulk={bulk})..."))
        start = time.time()

        if bulk:
            backend = tasks["default"]
            tasks_data = [(debug_noop_task, (), {"sleep_seconds": sleep}) for _ in range(count)]
            backend.bulk_enqueue(tasks_data)
        else:
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

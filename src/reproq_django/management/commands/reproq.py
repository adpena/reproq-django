import os
import subprocess
import sys
import platform
import shutil
import urllib.request
import tempfile
from django.core.management.base import BaseCommand
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
        worker_bin = self.get_worker_bin()
        try:
            version = subprocess.check_output([worker_bin, "--version"]).decode().strip()
            self.stdout.write(self.style.SUCCESS(f"✅ Worker binary: {version}"))
        except Exception as e:
            self.stderr.write(self.style.ERROR(f"❌ Worker binary invalid: {e}"))

        dsn = self.get_dsn()
        if dsn:
            self.stdout.write(self.style.SUCCESS("✅ Database DSN detected."))
        else:
            self.stderr.write(self.style.ERROR("❌ Database DSN not detected."))

        with connection.cursor() as cursor:
            tables = connection.introspection.table_names(cursor)
            if "task_runs" in tables:
                self.stdout.write(self.style.SUCCESS("✅ Database schema present."))
            else:
                self.stderr.write(self.style.ERROR("❌ Database schema missing (run migrate)."))

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
        pkg_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        bin_dir = os.path.join(pkg_dir, "bin")
        os.makedirs(bin_dir, exist_ok=True)
        
        system = platform.system().lower()
        arch = platform.machine().lower()
        if arch == "x86_64": arch = "amd64"
        if arch == "arm64" or arch == "aarch64": arch = "arm64"
        
        ext = ".exe" if system == "windows" else ""
        bin_name = f"reproq-{system}-{arch}{ext}"
        target_path = os.path.join(bin_dir, f"reproq{ext}")

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
        worker_bin = self.get_worker_bin()
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

    def get_worker_bin(self):
        worker_bin = getattr(settings, "REPROQ_WORKER_BIN", None)
        if worker_bin: return worker_bin
        pkg_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        ext = ".exe" if platform.system() == "Windows" else ""
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
import os
import subprocess
import sys
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

        # Beat
        beat_parser = subparsers.add_parser("beat", help="Start the periodic task scheduler (beat)")
        beat_parser.add_argument("--interval", type=str, default="30s")

        # Migrate
        subparsers.add_parser("migrate-worker", help="Apply Go worker SQL migrations")

        # Check
        subparsers.add_parser("check", help="Verify configuration")

        # Install
        install_parser = subparsers.add_parser("install", help="Download or build the Go worker binary")
        install_parser.add_argument("--source", type=str, help="Path to reproq-worker source (for building)")
        install_parser.add_argument("--build", action="store_true", help="Force building from source instead of downloading")
        install_parser.add_argument("--tag", type=str, default="latest", help="GitHub release tag to download")

        # Init
        subparsers.add_parser("init", help="Bootstrap Reproq in the current project")

        # systemd
        systemd_parser = subparsers.add_parser("systemd", help="Generate systemd service files for production")
        systemd_parser.add_argument("--user", type=str, help="User to run the service as (defaults to current user)")
        systemd_parser.add_argument("--group", type=str, help="Group to run the service as")

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

    def run_systemd(self, options):
        import getpass
        import getgrp
        from pathlib import Path

        user = options["user"] or getpass.getuser()
        group = options["group"] or user
        cwd = os.getcwd()
        python_bin = sys.executable
        manage_py = os.path.join(cwd, "manage.py")
        
        project_name = Path(cwd).name
        
        services = {
            "worker": {
                "desc": f"Reproq Worker - {project_name}",
                "cmd": f"{python_bin} {manage_py} reproq worker"
            },
            "beat": {
                "desc": f"Reproq Beat - {project_name}",
                "cmd": f"{python_bin} {manage_py} reproq beat"
            }
        }

        self.stdout.write(self.style.MIGRATE_HEADING(f"📋 Systemd Service Files for {project_name}"))
        
        for name, info in services.items():
            service_name = f"reproq-{name}-{project_name}.service"
            content = f"""[Unit]
Description={info['desc']}
After=network.target postgresql.service

[Service]
Type=simple
User={user}
Group={group}
WorkingDirectory={cwd}
ExecStart={info['cmd']}
Restart=always
RestartSec=5
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
"""
            self.stdout.write(f"\n--- {service_name} ---")
            self.stdout.write(content)
            
            # Save to disk
            with open(service_name, "w") as f:
                f.write(content)

        self.stdout.write(self.style.SUCCESS(f"\n✅ Service files generated in current directory."))
        self.stdout.write("\nTo install them, run:")
        self.stdout.write(self.style.WARNING(f"sudo mv reproq-*-{project_name}.service /etc/systemd/system/"))
        self.stdout.write(self.style.WARNING("sudo systemctl daemon-reload"))
        self.stdout.write(self.style.WARNING(f"sudo systemctl enable --now reproq-worker-{project_name}"))
        self.stdout.write(self.style.WARNING(f"sudo systemctl enable --now reproq-beat-{project_name}"))

    def run_init(self, options):
        self.stdout.write(self.style.MIGRATE_HEADING("🚀 Bootstrapping Reproq..."))
        
        # 1. Check INSTALLED_APPS
        if "reproq_django" not in settings.INSTALLED_APPS:
            self.stderr.write(self.style.ERROR("❌ 'reproq_django' not found in INSTALLED_APPS."))
            self.stdout.write("Please add 'reproq_django' to your settings.py.")
        else:
            self.stdout.write(self.style.SUCCESS("✅ 'reproq_django' is in INSTALLED_APPS."))

        # 2. Check Database Driver
        try:
            import psycopg
            self.stdout.write(self.style.SUCCESS(f"✅ psycopg {psycopg.__version__} is installed."))
        except ImportError:
            self.stderr.write(self.style.WARNING("⚠️ psycopg not found. You may need to install it: pip install 'psycopg[binary]'"))

        # 3. Check/Install Binary
        self.stdout.write("\nChecking Go worker binary...")
        self.run_install(options)

        # 4. Check Migrations
        self.stdout.write("\nChecking database schema...")
        with connection.cursor() as cursor:
            tables = connection.introspection.table_names(cursor)
            if "task_runs" not in tables:
                self.stdout.write(self.style.WARNING("⚠️ Table 'task_runs' missing. Run 'python manage.py migrate'."))
            else:
                self.stdout.write(self.style.SUCCESS("✅ Database schema is present."))

        self.stdout.write(self.style.MIGRATE_HEADING("\n✨ Reproq is ready!"))
        self.stdout.write("Run 'python manage.py reproq worker' to start processing tasks.")

    def run_install(self, options):
        import shutil
        import platform
        import urllib.request
        import tempfile
        
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

        # Use a temporary file for atomic update
        with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
            tmp_path = tmp_file.name

        success = False
        if not options["build"]:
            # Attempt Download
            tag = options["tag"]
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
            # Fallback: Build
            go_path = shutil.which("go")
            if not go_path:
                self.stderr.write(self.style.ERROR("Go compiler not found and download failed."))
                sys.exit(1)

            source_path = options["source"]
            if not source_path:
                # Look for adjacent repo
                base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
                source_path = os.path.join(os.path.dirname(base_dir), "reproq-worker")
            
            if os.path.exists(source_path):
                self.stdout.write(f"Building from local source: {source_path}...")
                try:
                    subprocess.run(["go", "build", "-o", tmp_path, "./cmd/reproq"], cwd=source_path, check=True)
                    success = True
                except subprocess.CalledProcessError:
                    pass

        if not success:
            self.stderr.write(self.style.ERROR("Failed to install worker via download or build."))
            sys.exit(1)

        # Finalize
        if system != "windows":
            os.chmod(tmp_path, 0o755)
        
        # Verify
        try:
            out = subprocess.check_output([tmp_path, "--version"]).decode().strip()
            self.stdout.write(self.style.SUCCESS(f"Verified: {out}"))
        except Exception as e:
            self.stderr.write(self.style.ERROR(f"Verification failed: {e}"))
            os.unlink(tmp_path)
            sys.exit(1)

        shutil.move(tmp_path, target_path)
        self.stdout.write(self.style.SUCCESS(f"Successfully installed to {target_path}"))

    def run_check(self):
        self.stdout.write("Checking Reproq configuration...")
        
        # Binary Check
        worker_bin = self.get_worker_bin()
        try:
            version = subprocess.check_output([worker_bin, "--version"]).decode().strip()
            self.stdout.write(self.style.SUCCESS(f"✅ Worker binary found: {version}"))
        except Exception as e:
            self.stderr.write(self.style.ERROR(f"❌ Worker binary invalid or not found: {e}"))

        # DB Check
        dsn = self.get_dsn()
        if dsn:
            self.stdout.write(self.style.SUCCESS("✅ Database DSN detected."))
        else:
            self.stderr.write(self.style.ERROR("❌ Could not detect database DSN."))

        # Model Check
        with connection.cursor() as cursor:
            tables = connection.introspection.table_names(cursor)
            if "task_runs" in tables:
                self.stdout.write(self.style.SUCCESS("✅ Database schema present."))
            else:
                self.stderr.write(self.style.ERROR("❌ Database schema missing (run migrate)."))

    def get_worker_bin(self):
        worker_bin = getattr(settings, "REPROQ_WORKER_BIN", None)
        if not worker_bin:
            pkg_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            ext = ".exe" if platform.system() == "Windows" else ""
            candidate = os.path.join(pkg_dir, "bin", f"reproq{ext}")
            if os.path.exists(candidate):
                return candidate
            return "reproq"
        return worker_bin

    def get_dsn(self):
        # 1. Environment Variable
        env_dsn = os.environ.get("DATABASE_URL")
        if env_dsn: return env_dsn

        # 2. Django Settings
        db_conf = settings.DATABASES.get("default", {})
        if db_conf.get("ENGINE") != "django.db.backends.postgresql":
            # Might be using a wrapper or we don't support it
            pass

        user = db_conf.get("USER")
        name = db_conf.get("NAME")
        if not user or not name: return None

        password = db_conf.get("PASSWORD", "")
        host = db_conf.get("HOST", "localhost")
        port = db_conf.get("PORT", "5432")
        
        # Handle cases where host/port might be empty
        if not host: host = "localhost"
        if not port: port = "5432"

        return f"postgres://{user}:{password}@{host}:{port}/{name}"
        self.stdout.write("Checking Reproq configuration...")
        # ... existing check logic ...
        self.stdout.write(self.style.SUCCESS("Configuration looks good."))

    def run_migrate(self):
        # In a real implementation, this would look for the SQL files
        # packaged with the app and apply them via psycopg.
        self.stdout.write("Applying Reproq SQL migrations...")
        # For now, we assume the user has the reproq-worker source nearby or the files are bundled.
        self.stdout.write(self.style.SUCCESS("Migrations applied (simulated)."))

    def run_worker_or_beat(self, cmd, options):
        dsn = self.get_dsn()
        worker_bin = self.get_worker_bin()
        
        args = [worker_bin, cmd, "--dsn", dsn]
        if cmd == "worker":
            args.extend(["--concurrency", str(options["concurrency"])])
        elif cmd == "beat":
            args.extend(["--interval", options["interval"]])
            
        self.stdout.write(f"Starting {cmd} with binary: {worker_bin}")
        try:
            subprocess.run(args, check=True)
        except FileNotFoundError:
            self.stderr.write(self.style.ERROR(f"Reproq worker binary not found at '{worker_bin}'."))
            self.stderr.write("Please install reproq-worker and set REPROQ_WORKER_BIN in settings.py")
            sys.exit(1)
        except KeyboardInterrupt:
            pass

    def get_dsn(self):
        db_conf = settings.DATABASES["default"]
        # Basic DSN construction for Postgres
        user = db_conf.get("USER")
        password = db_conf.get("PASSWORD")
        host = db_conf.get("HOST", "localhost")
        port = db_conf.get("PORT", "5432")
        name = db_conf.get("NAME")
        
        if not user or not name:
            return os.environ.get("DATABASE_URL", "")
            
        return f"postgres://{user}:{password}@{host}:{port}/{name}"

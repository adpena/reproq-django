from django.core.management.base import BaseCommand
from django.db import connection
from django.conf import settings

class Command(BaseCommand):
    help = "Validates Reproq Django setup"

    def handle(self, *args, **options):
        self.stdout.write("Checking Django setup...")
        # django.setup() is already called by management command runner
        
        self.stdout.write("Checking database connectivity...")
        try:
            connection.ensure_connection()
        except Exception as e:
            self.stderr.write(f"Database connection failed: {e}")
            sys.exit(1)

        self.stdout.write("Checking task_runs table...")
        with connection.cursor() as cursor:
            table_names = connection.introspection.table_names(cursor)
            if "task_runs" not in table_names:
                self.stderr.write("Table 'task_runs' not found. Did you run 'python manage.py migrate'?")
                return

        self.stdout.write("Checking executor module...")
        from django.utils.module_loading import import_string
        try:
            import_string("reproq_django.executor.execute")
        except Exception as e:
            self.stderr.write(f"Executor module not found or broken: {e}")
            return

        self.stdout.write(self.style.SUCCESS("OK: Reproq Django is correctly configured."))
        self.stdout.write("\nWorker command example:")
        self.stdout.write(self.style.WARNING("reproq-worker --backend postgres --conn \"postgres://user:pass@localhost:5432/dbname\" --executor \"python -m reproq_django.executor --payload-stdin\""))

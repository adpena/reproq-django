from django.db import migrations

class Migration(migrations.Migration):
    atomic = False
    dependencies = [
        ('reproq_django', '0001_task_runs'),
    ]

    operations = [
        migrations.RunSQL(
            sql="""
            CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS task_runs_dedupe_active
            ON task_runs (spec_hash)
            WHERE status IN ('READY', 'RUNNING');
            """,
            reverse_sql="""
            DROP INDEX IF EXISTS task_runs_dedupe_active;
            """
        ),
    ]

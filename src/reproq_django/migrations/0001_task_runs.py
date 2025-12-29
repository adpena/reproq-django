from django.db import migrations, models

class Migration(migrations.Migration):
    initial = True
    dependencies = []

    operations = [
        migrations.RunSQL(
            sql="""
            CREATE TABLE IF NOT EXISTS task_runs (
              result_id         VARCHAR(63) PRIMARY KEY,
              backend_alias     TEXT NOT NULL,
              queue_name        TEXT NOT NULL,
              priority          SMALLINT NOT NULL CHECK (priority BETWEEN -100 AND 100),
              run_after         TIMESTAMPTZ NULL,

              spec_json         JSONB NOT NULL,
              spec_hash         CHAR(64) NOT NULL,

              status            TEXT NOT NULL CHECK (status IN ('READY','RUNNING','FAILED','SUCCESSFUL')),
              enqueued_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
              started_at        TIMESTAMPTZ NULL,
              last_attempted_at TIMESTAMPTZ NULL,
              finished_at       TIMESTAMPTZ NULL,

              attempts          INT NOT NULL DEFAULT 0,
              worker_ids        TEXT[] NOT NULL DEFAULT '{}',

              return_json       JSONB NULL,
              errors_json       JSONB NOT NULL DEFAULT '[]'::jsonb,

              leased_until      TIMESTAMPTZ NULL,
              leased_by         TEXT NULL,

              logs_uri          TEXT NULL,
              artifacts_uri     TEXT NULL,

              created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
              updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
            );

            CREATE INDEX IF NOT EXISTS task_runs_ready_idx
              ON task_runs (queue_name, priority DESC, enqueued_at)
              WHERE status = 'READY';

            CREATE INDEX IF NOT EXISTS task_runs_sched_idx
              ON task_runs (run_after)
              WHERE status = 'READY' AND run_after IS NOT NULL;

            CREATE INDEX IF NOT EXISTS task_runs_spec_hash_idx
              ON task_runs (spec_hash);
            """,
            reverse_sql="""
            DROP TABLE IF EXISTS task_runs;
            """,
        ),
    ]
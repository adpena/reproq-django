# Deployment Guide

Reproq is designed for stability in production environments. Follow these guidelines for a robust setup.

## 0. Deployment Options

**Option A (Recommended): Separate worker + scheduler processes**
- Run `python manage.py reproq worker` and either `python manage.py reproq beat` or `python manage.py reproq pg-cron --install`.
- Use a supervisor (systemd, supervisor, or separate container/services) so they restart automatically.

**Option B: Single-service (web + worker + scheduler)**
- Run the worker and beat in the same service as your web process (or use `pg-cron` and omit beat).
- Simpler to deploy, but less reliable: background processes are not supervised and can die silently.

Example single-service start command:
```bash
/bin/bash -lc "
python manage.py reproq worker --concurrency 5 &
python manage.py reproq beat --interval 30s &
exec gunicorn myproj.wsgi:application --workers=1 --timeout=120
"
```

Only one `beat` instance should run per database.

For a full command reference and examples, see `docs/cli.md`.

## 1. Systemd Configuration (Recommended)

Reproq provides an automated way to generate systemd service files. This is the preferred method for Linux servers.

```bash
python manage.py reproq systemd --concurrency 20
```

To bake metrics settings into the unit files:
```bash
python manage.py reproq systemd --metrics-addr 127.0.0.1:9090 --metrics-allow-cidrs "127.0.0.1/32"
```

You can also provide an environment file:
```bash
python manage.py reproq systemd --env-file /etc/reproq-worker.env
```

This will output two service files:
- `reproq-worker.service`: Manages the task execution nodes.
- `reproq-beat.service`: Manages the periodic task scheduler.

### Manual Installation
Copy the generated files to `/etc/systemd/system/` and enable them:

```bash
sudo cp *.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now reproq-worker
sudo systemctl enable --now reproq-beat
```

## 2. Environment Variables

The Go worker needs access to your database. It respects the following:

- `DATABASE_URL`: Standard PostgreSQL DSN (e.g., `postgres://user:pass@host:5432/dbname`). Optional if you provide `--dsn` or a config file with `dsn`.
- `WORKER_ID`: Unique name for the worker node (defaults to hostname).
- `PYTHONPATH`: Ensure your Django project is importable.
- `REPROQ_WORKER_BIN`: Optional explicit path to the worker binary. If set, `python manage.py reproq install` writes the binary to this path.
  If unset, the default install target is `./.reproq/bin/reproq`.
- `METRICS_TLS_CERT`: Optional TLS certificate path for health/metrics.
- `METRICS_TLS_KEY`: Optional TLS private key path for health/metrics.
- `METRICS_TLS_CLIENT_CA`: Optional client CA bundle to require mTLS for health/metrics.
- `ALLOWED_TASK_MODULES`: Optional allow-list for task module prefixes. If unset, `python manage.py reproq worker` auto-configures it from discovered task modules.
- `REPROQ_LOGS_DIR`: Optional directory to persist worker stdout/stderr logs (updates `task_runs.logs_uri`).

If `DATABASE_URL` is not set, `python manage.py reproq worker` derives a DSN from `settings.DATABASES["default"]` using `USER`, `PASSWORD`, `HOST`, `PORT`, and `NAME`.

Worker binary resolution order:
1. `REPROQ_WORKER_BIN` (setting or env)
2. `./.reproq/bin/reproq` (installed by `reproq install`)
3. `reproq_django/bin/reproq` (packaged fallback)
4. `PATH`

### Schema Compatibility
Reproq Django uses JSONB columns for worker metadata (`task_runs.worker_ids` and
`reproq_workers.queues`). If you previously applied the legacy reproq-worker SQL
migrations that created array columns, apply
`migrations/000013_convert_worker_arrays_to_jsonb.up.sql` from the reproq-worker
repo before starting the worker.

## 2a. Metrics & Health Hardening
If you enable metrics (via `--metrics-port` or `--metrics-addr`), secure the endpoint:
- Bind to localhost or a private interface (for example `--metrics-addr 127.0.0.1:9090`).
- Set `METRICS_AUTH_TOKEN` to require a bearer token.
- Optionally set `METRICS_ALLOW_CIDRS` to restrict access by IP or CIDR.

## 3. Worker Concurrency

The number of concurrent tasks a single worker process can handle is configurable.
- **CPU-bound tasks**: Set concurrency to `number of cores`.
- **IO-bound tasks**: Set concurrency higher (e.g., `20-50`).

```bash
python manage.py reproq worker --concurrency 50
```

Queue selection uses `--queues` (comma-separated). The legacy `--queue` flag remains for compatibility but is deprecated.

## 4. Periodic Tasks (Beat or pg_cron)

**CRITICAL**: Only run **one scheduler** per database. Running multiple schedulers will result in tasks being scheduled multiple times.

### Option A: Beat
Start beat as a dedicated process (recommended when pg_cron is unavailable):
```bash
python manage.py reproq beat --interval 30s
```

### Option B: pg_cron
If your database supports `pg_cron`, install schedules with:
```bash
python manage.py reproq pg-cron --install
```
Remove schedules with:
```bash
python manage.py reproq pg-cron --remove
```

Create schedules in Django Admin or via the ORM:
```python
from django.utils import timezone
from reproq_django.models import PeriodicTask

PeriodicTask.objects.update_or_create(
    name="Nightly cleanup",
    defaults={
        "cron_expr": "0 2 * * *",
        "task_path": "myapp.tasks.nightly_cleanup",
        "queue_name": "maintenance",
        "next_run_at": timezone.now(),
        "enabled": True,
    },
)
```

To run a schedule immediately, set `next_run_at` to `timezone.now()` or call the task's `enqueue()` method directly.

## 4a. Reclaiming Orphaned Tasks

If a worker dies while holding a lease, tasks can remain stuck in `RUNNING`.
Use the reclaim command to requeue or fail those stale tasks:

```bash
python manage.py reproq reclaim --older-than 5m --action requeue
```

## 5. Reverse Proxy & Admin

The Reproq Admin uses standard Django Admin templates. Ensure your static files are collected and served correctly:

```bash
python manage.py collectstatic
```

## 6. Render Deployment Note

When deploying on Render, run migrations during the pre-deploy step in this order:

```bash
python manage.py reproq migrate-worker
python manage.py migrate --noinput
```

This ensures Postgres extensions are enabled before Django applies its migrations.

After migrations complete, enqueue any deploy-time tasks (such as deploy
notifications or periodic task seeds). Enqueuing before schema setup can
fail silently or create partial task records.

If you run worker + beat in the same service as the web process, avoid
enqueueing deploy-time tasks in a pre-deploy hook. Older worker processes
can still claim the task with outdated environment settings. Prefer a
post-start enqueue guarded by a deploy identifier (for example,
`RENDER_DEPLOY_ID`) so the task runs once per deploy.

## 7. Troubleshooting Runbook

**Tasks stuck in RUNNING**
- Reclaim expired leases: `python manage.py reproq reclaim --older-than 5m --action requeue`
- If tasks should fail instead: `--action fail`
- Verify workers are heartbeating (`python manage.py reproq status`)

**Allowlist errors or missing tasks**
- Auto-generate: `python manage.py reproq allowlist --write --config reproq.yaml`
- For local dev only, `ALLOWED_TASK_MODULES=*` disables validation.

**Schema mismatch or missing columns**
- Run `python manage.py reproq migrate-worker` and re-check `python manage.py reproq doctor`.
- If you used legacy array-based worker migrations, apply the JSONB conversion migration from the reproq-worker repo.
 - If you upgraded from a version that created the task_path guard as `NOT VALID`, validate it after backfill: `ALTER TABLE task_runs VALIDATE CONSTRAINT task_runs_task_path_not_empty;`

**DSN or config precedence confusion**
- Use `python manage.py reproq config --explain` to see the winning values.
- `--dsn` overrides `DATABASE_URL`, and `DATABASE_URL` is optional when a config file or flags supply the DSN.

**No logs available**
- Ensure `REPROQ_LOGS_DIR` (or `--logs-dir`) is set on the worker.
- Inspect logs via `python manage.py reproq logs --id <result_id>`.

# Deploying Reproq to Render

This guide explains how to configure your Render environment to run Reproq Worker and Beat processes alongside your Django web application.

## 1. Build Configuration
The Reproq Go worker binary is not bundled in the repository. You must download it during the Render build phase.

Update your `render.yaml` (or the **Build Command** in the Render Dashboard):

```bash
# Add this line to your build command
python manage.py reproq install
```

Example `render.yaml` build command:
```yaml
buildCommand: |
  uv sync --upgrade
  uv run python manage.py reproq install  # <--- Fetch the Go binary
  uv run python manage.py collectstatic --noinput
```

Recommended migration step (web service preDeploy):
```yaml
preDeployCommand: |
  uv run python manage.py reproq migrate-worker
  uv run python manage.py migrate --noinput
```

## 1a. Deploy-Time Task Enqueues (Recommended Ordering)
If you enqueue deploy-time tasks (like deploy notifications), prefer doing so
after the worker starts. Pre-deploy hooks can still be serviced by old worker
processes with outdated environment settings. On Render, you can guard the
enqueue using `RENDER_DEPLOY_ID` so it only runs once per deploy.

## 1b. Single-Service Supervisor (Cost-Saving)
If you want to keep a single Render service, Reproq Django ships a lightweight
supervisor script that restarts worker/beat when they exit.

Example `startCommand`:
```yaml
startCommand: >
  /bin/bash -lc "
  export REPROQ_PRESTART_CMD='uv run python manage.py reproq check';
  export REPROQ_PRESTART_INTERVAL_SECONDS='5';
  export REPROQ_PRESTART_MAX_WAIT_SECONDS='120';
  export REPROQ_WEB_CMD='uv run gunicorn myproj.wsgi:application --workers=${WEB_CONCURRENCY:-1}';
  render_start=$(uv run python -c 'import importlib.resources as r; print(r.files("reproq_django.resources").joinpath("render_start.sh"))');
  bash \"$render_start\"
  "
```

Optional overrides:
- `REPROQ_WORKER_CMD`: Full worker command (string).
- `REPROQ_BEAT_CMD`: Full beat command (string). Leave unset to use the default;
  set it to an empty string to disable beat.
- `REPROQ_RESTART_DELAY_SECONDS`: Restart delay (default 5).

This is less isolated than dedicated worker services, but it avoids extra
service costs while still providing auto-restart.

## 2. Worker Service
Background tasks require a separate service on Render. We recommend adding a **Background Worker** service.

Add this to your `render.yaml`:

```yaml
  - type: worker
    name: reproq-worker
    runtime: python
    buildCommand: uv sync --upgrade && uv run python manage.py reproq install
    startCommand: uv run python manage.py reproq worker
    envVars:
      - key: DATABASE_URL
        fromDatabase:
          name: your-db-name
          property: connectionString
```

## 3. Periodic Tasks: Beat or pg_cron
If you use periodic tasks, choose **one** scheduler:

### Option A: Beat Service
Run exactly one instance of the `beat` process. You can run this as another background worker:

```yaml
  - type: worker
    name: reproq-beat
    runtime: python
    buildCommand: uv sync --upgrade && uv run python manage.py reproq install
    startCommand: uv run python manage.py reproq beat
    envVars:
      - key: DATABASE_URL
        fromDatabase:
          name: your-db-name
          property: connectionString
```

### Option B: pg_cron
If your Postgres supports `pg_cron`, install schedules in-app (after migrations):

```bash
uv run python manage.py reproq pg-cron --install
```

To remove schedules:
```bash
uv run python manage.py reproq pg-cron --remove
```

Recommended Render pre-deploy hook when using pg_cron:
```yaml
preDeployCommand: |
  uv run python manage.py reproq migrate-worker
  uv run python manage.py migrate --noinput
  uv run python manage.py reproq pg-cron --install --if-supported
```

If `pg_cron` is not available, the command will fail. Verify support with:
```sql
SELECT * FROM pg_available_extensions WHERE name = 'pg_cron';
```

### Seeding Periodic Tasks
You can create schedules in Django Admin or seed them in code using `PeriodicTask`:
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

## 4. Environment Variables
Ensure the following variables are set in your Render environment:
- `DATABASE_URL`: Required unless you supply `--dsn` or a config file with `dsn`.
- `REPROQ_WORKER_BIN`: (Optional) Defaults to `src/reproq_django/bin/reproq`.
- `ALLOWED_TASK_MODULES`: (Optional) Allow-list for task module prefixes. If unset, `python manage.py reproq worker` auto-configures it from discovered task modules.
- `REPROQ_LOGS_DIR`: (Optional) Directory to persist worker stdout/stderr logs (updates `task_runs.logs_uri`).
- `METRICS_AUTH_TOKEN`: (Optional) Bearer token for health/metrics/events and TUI auth signing.
- `METRICS_ALLOW_CIDRS`: (Optional) Comma-separated IP/CIDR allow-list for health/metrics.
- `METRICS_TLS_CERT`: (Optional) TLS certificate path for health/metrics.
- `METRICS_TLS_KEY`: (Optional) TLS private key path for health/metrics.
- `METRICS_TLS_CLIENT_CA`: (Optional) Client CA bundle to require mTLS for health/metrics.

## 5. Memory Considerations
Reproq Worker is extremely light. It typically uses less than 20MB of RAM, making it perfect for Render's **Free Tier** or **Starter** plans.

For Python/Django memory tuning on small instances:
- Keep `WEB_CONCURRENCY=1` and `REPROQ_CONCURRENCY=1` to avoid over-committing RAM.
- Use `--max-requests` + jitter to recycle Gunicorn workers (limits fragmentation over long uptimes).
- Avoid `--preload` unless you have headroom; preload can spike RSS on small plans.
- Enable `LOW_MEMORY_MODE=1` to disable metrics/events/health endpoints when needed.
  When `LOW_MEMORY_MODE=1` is set, the supervisor will skip running beat so you
  can rely on pg_cron instead.

# Reproq Django 🚀

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Documentation](https://img.shields.io/badge/Docs-View%20Online-indigo)](https://adpena.github.io/reproq-django/)

**Deterministic Background Tasks for Django 6.0+ powered by Go.**

Reproq is a production-grade tasks backend that combines the ease of Django with the performance and reliability of the [Reproq Worker](https://github.com/adpena/reproq-worker), a high-performance execution engine written in Go.

---

## 🤝 Relationship with Reproq Worker

Reproq is split into two specialized components:
1. **Reproq Django (this repo)**: Task definition, enqueueing, results, and the Admin dashboard.
2. **[Reproq Worker](https://github.com/adpena/reproq-worker)**: A Go binary that claims and executes tasks from Postgres.

## Key Features

- **Postgres-Only**: Uses `SKIP LOCKED` for high-performance claiming.
- **Deterministic**: Each task has a `spec_hash` for deduplication.
- **Django Native**: Implements the Django 6.0 Tasks API.
- **Periodic Tasks**: Built-in scheduler with Django models.
- **Monitoring**: TaskRuns + Workers in Django Admin.

## ✅ Compatibility

| Component | Supported |
| :--- | :--- |
| Django | 6.x (`Django>=6.0,<7.0`) |
| Python | 3.12+ (dev/CI uses 3.12.x) |
| Reproq Worker | Latest release (install/upgrade via `python manage.py reproq install` / `upgrade`) |

Use `python manage.py reproq doctor` to verify schema, worker binary, and DSN alignment.

---

## ⚡ Quickstart

### 1. Install
```bash
uv pip install reproq-django
# or: pip install reproq-django
```

### 2. Configure Settings
Add `reproq_django` to your `INSTALLED_APPS` and configure the `TASKS` backend:

```python
INSTALLED_APPS = [
    ...,
    "reproq_django",
]

TASKS = {
    "default": {
        "BACKEND": "reproq_django.backend.ReproqBackend",
        "OPTIONS": {
            "DEDUP_ACTIVE": True,
            "TIMEOUT_SECONDS": 900,
            "MAX_ATTEMPTS": 3,
        }
    }
}
```

### 3. Define a Task
Use the Django 6.0 Tasks API with a `@task` decorator:

```python
from django.tasks import task

@task(queue_name="default", priority=0)
def send_welcome_email(user_id: int) -> str:
    # business logic here
    return f"Email sent to {user_id}"
```

### 4. Bootstrap (Recommended)
Bootstrap writes a config file, installs the worker binary, and runs both migration steps:
```bash
python manage.py reproq init
```
Use `--format toml`, `--skip-install`, `--skip-migrate`, or `--skip-worker-migrate` if you need a lighter touch.

### 5. Install the Worker (Standalone)
If you want only the worker binary:
```bash
python manage.py reproq install
```
*This command detects your OS/Architecture and fetches the correct pre-built binary from GitHub. No Go installation required!*

### 6. Run Migrations
```bash
python manage.py reproq migrate-worker
python manage.py migrate
```
*Note: `migrate-worker` applies necessary Postgres optimizations (indexes, extensions) that Django migrations cannot handle.*
It also backfills `task_path` in batches and ensures the `task_runs_task_path_not_empty` check exists; new installs validate it immediately, while older installs can validate it later if desired.

### 7. Start the Worker
```bash
python manage.py reproq worker
```

### 8. (Optional) Start Beat for Periodic Tasks
Run exactly one beat process per database if you use periodic tasks:
```bash
python manage.py reproq beat
```

---

## 🧰 Management Commands

Run `python manage.py reproq <subcommand>` to manage the worker and day-to-day ops.
Full reference (examples + exit codes): `docs/cli.md`.

- **Bootstrap**: `init` writes `reproq.yaml`/`reproq.toml`, installs the worker, and runs migrations.
- **Config**: `config --explain` prints the effective config and its precedence.
- **Doctor**: `doctor --strict` validates DSN, schema, worker binary, and allowlist.
- **Upgrade**: `upgrade` fetches the latest worker release and optionally runs `migrate-worker`.
- **Allowlist**: `allowlist --write --config reproq.yaml` populates `allowed_task_modules`.
- **Ops**: `status`/`stats`, `logs --id <result_id>`, `cancel --id <result_id>`.

---

## ✅ Why Reproq (vs Celery, RQ, Huey)

Reproq is built for teams who want deterministic background tasks without adding Redis or RabbitMQ.

- **No extra broker**: Uses Postgres only; no Redis/RabbitMQ to provision.
- **Deterministic deduping**: Identical tasks can be coalesced safely.
- **Django-native**: Implements the Django 6.0 Tasks API end-to-end.
- **Operationally lean**: One database + one Go worker binary.

If you need complex routing, multi-broker support, or huge existing Celery ecosystems, Celery may still fit better. Reproq prioritizes clarity, determinism, and low operational overhead.

---

## 📚 API Reference

Reproq fully implements the Django 6.0 Tasks API.

### Defining Tasks
Use the standard `@task` decorator. Reproq respects `queue_name` and `priority`.

```python
from django.tasks import task

@task(queue_name="high-priority", priority=100)
def send_welcome_email(user_id):
    # logic here
    return f"Email sent to {user_id}"
```

### Enqueuing Tasks
Use `.enqueue()` to dispatch tasks. Reproq supports additional arguments via `kwargs`.

```python
# Standard Enqueue
result = send_welcome_email.enqueue(123)

# Scheduled Execution (run_after)
from datetime import timedelta
result = send_welcome_email.using(run_after=timedelta(minutes=10)).enqueue(123)

# Concurrency Control (lock_key)
# Ensure only one task with this key runs at a time
result = send_welcome_email.enqueue(123, lock_key=f"user_123_sync")
```

**Supported `enqueue` kwargs (Reproq extensions):**
- `run_after`: `datetime` or `timedelta`. Delays execution.
- `lock_key`: `str`. Prevents multiple tasks with the same key from being in `RUNNING` state simultaneously.
- `priority`: `int`. Overrides the task's default priority for this enqueue only.

**Reserved kwargs:** `run_after`, `lock_key`, and `priority` are treated as scheduling metadata and are removed from task kwargs. If your task needs parameters with these names, rename them.

**Note on Priority:** Task priority is set at definition time via `@task(priority=...)` and can be overridden per call via `enqueue(priority=...)`.

### Async Contexts (ASGI)
If you are in an async view or task producer, use `aenqueue()` to avoid blocking:

```python
result = await send_welcome_email.aenqueue(123)
```

In sync/Wsgi code, continue to use `enqueue()`.

### Bulk Enqueuing
For high-throughput scenarios, use `bulk_enqueue` to insert thousands of tasks in a single query.

```python
from django.tasks import tasks
from datetime import timedelta

backend = tasks["default"]
jobs = []

for i in range(1000):
    # (task_func, args, kwargs)
    jobs.append((
        send_welcome_email,
        (i,),
        {"lock_key": f"user_{i}", "run_after": timedelta(seconds=i)}
    ))

backend.bulk_enqueue(jobs)
```

---

## ⏰ Periodic Tasks

Reproq stores schedules in the `PeriodicTask` model. The scheduler is the
`python manage.py reproq beat` process. Run exactly one beat per database.

### Create a Schedule (Admin or ORM)
You can manage schedules in the Django Admin under "Reproq Django" or via code.

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

### Seed Schedules in Code (post_migrate)
This pattern keeps schedules in sync across environments without migrations.

```python
from django.apps import AppConfig
from django.db import connections
from django.db.models.signals import post_migrate
from django.utils import timezone
from reproq_django.models import PeriodicTask

def _setup_periodic_tasks(**kwargs):
    using = kwargs.get("using")
    connection = connections[using]
    if "periodic_tasks" not in connection.introspection.table_names():
        return

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

class MyAppConfig(AppConfig):
    name = "myapp"

    def ready(self) -> None:
        post_migrate.connect(_setup_periodic_tasks, sender=self)
```

### Run Now (Ad Hoc)
Use the ORM to force a schedule to run, or enqueue the task directly.

```python
from django.utils import timezone
from reproq_django.models import PeriodicTask
from myapp.tasks import nightly_cleanup

PeriodicTask.objects.filter(name="Nightly cleanup").update(
    next_run_at=timezone.now()
)

# Or bypass the schedule
nightly_cleanup.enqueue()
```

### Notes
- `cron_expr` uses standard 5-field cron syntax: `min hour day month weekday`.
- `task_path` must be the full Python import path for the task.
- `queue_name` is optional; when set, the worker must listen on that queue.
- Ensure the task module is allowlisted when `ALLOWED_TASK_MODULES` is used.

---

## ⚙️ Configuration

Configure Reproq behavior via the `TASKS` setting in `settings.py`.

```python
TASKS = {
    "default": {
        "BACKEND": "reproq_django.backend.ReproqBackend",
        "OPTIONS": {
            # Deduplication (Default: True)
            # If True, enqueuing a task with the exact same arguments as a 
            # READY/RUNNING task will return the existing result_id.
            "DEDUP_ACTIVE": True,

            # Execution Timeout (Default: 900)
            # Max seconds a task can run before being killed by the worker.
            "TIMEOUT_SECONDS": 900,

            # Retry Limit (Default: 3)
            # Max number of attempts for a task.
            "MAX_ATTEMPTS": 3,

            # Expiry (Optional)
            # If set, tasks not picked up by this time will be marked expired.
            "EXPIRES_IN": timedelta(hours=24),
            
            # Provenance (Optional)
            # Metadata stored with the task for auditing.
            "CODE_REF": "git-sha-or-version",
            "PIP_LOCK_HASH": "hash-of-dependencies",
        }
    }
}
```

Worker config files (`reproq.yaml`/`reproq.toml`) are optional. Precedence is:
defaults < config file < env vars < CLI flags. `--dsn` overrides `DATABASE_URL`, and
`DATABASE_URL` is optional when flags or a config file are provided.

---

## 🧪 Development

We standardize on Python 3.12.x for local development.

```bash
bash scripts/dev_bootstrap.sh
uv run pytest
```
Always run tests with `uv run pytest` so dependencies and settings stay consistent.

---

## ⛓ Workflows (Chains & Groups)

Reproq supports complex task dependencies.

### Chains (Sequential)
Execute tasks one after another. If a task fails, the chain stops.

```python
from reproq_django.workflows import chain

# task_a -> task_b -> task_c
c = chain(
    (task_a, (1,), {}),
    task_b, # no args
    (task_c, (), {"param": "val"})
)
results = c.enqueue()
# results[0] is READY, results[1..] are WAITING
```

### Groups (Parallel)
Execute tasks in parallel.

```python
from reproq_django.workflows import group

g = group(
    (resize_image, ("img1.jpg",), {}),
    (resize_image, ("img2.jpg",), {}),
)
results = g.enqueue()
```

### Chords (Group + Callback)
Run a callback once a group finishes.

```python
from reproq_django.workflows import chord

group_results, callback_result = chord(
    (resize_image, ("img1.jpg",), {}),
    (resize_image, ("img2.jpg",), {}),
    callback=notify_done,
).enqueue()
```
The callback runs only after all group tasks succeed. Failed tasks leave the callback waiting.
Failures mark the workflow as failed and the callback will not run.

---

## 🔁 Retries & Backoff

Retries are managed by the Go worker. When a task fails and attempts remain, it is re-queued with an **exponential backoff**:

- Base delay: 30s
- Backoff: `2^attempt` (attempt starts at 1)
- Cap: 1 hour

The worker updates `run_after` on the failed task, and the backend will only claim it after that timestamp.

---

## 🚦 Rate Limiting

Reproq Worker enforces token bucket limits stored in the `rate_limits` table. You can manage these from Django Admin or via the worker CLI.

Keys:
- `queue:<queue_name>` limits a specific queue.
- `task:<task_path>` limits a specific task (overrides queue/global).
- `global` is a fallback when no task/queue limit exists.

Example:
```bash
reproq limit set --key queue:default --rate 5 --burst 10
```
Defaults: global rate limiting is disabled until you set a positive rate.

---

## 🧰 Worker CLI Ops

The Go worker binary includes operational commands you can run directly:

```bash
# Request cancellation of a running task
reproq cancel --dsn "..." --id 12345

# Inspect failed tasks
reproq triage list --dsn "..." --limit 50
```

---

## 🖥 Management Commands

The `python manage.py reproq` command is your Swiss Army knife.

| Subcommand | Description |
| :--- | :--- |
| `init` | Bootstraps Reproq in the current project. |
| `worker` | Starts the Go worker. Flags: `--config`, `--concurrency` (default 10), `--queues`, `--allowed-task-modules`, `--logs-dir`, `--payload-mode`, `--metrics-port`, `--metrics-addr`, `--metrics-auth-token`, `--metrics-allow-cidrs`, `--metrics-tls-cert`, `--metrics-tls-key`, `--metrics-tls-client-ca`. Auto-configures allow-list when unset (unless config file is used). |
| `beat` | Starts the scheduler. Flags: `--config`, `--interval` (default 30s). |
| `install` | Downloads/builds the worker binary. |
| `upgrade` | Upgrades the worker binary and optionally runs `migrate-worker`. |
| `migrate-worker` | Applies essential SQL schema optimizations (indexes, extensions). |
| `check` | Validates binary path, DB connection, and schema health. |
| `doctor` | Validates DSN, schema, worker binary, and allowlist; `--strict` fails on warnings. |
| `config` | Prints effective worker/beat config; use `--explain` for precedence. |
| `allowlist` | Prints `ALLOWED_TASK_MODULES` or writes them to a config file with `--write`. |
| `logs` | Prints logs for a task run using `logs_uri`. |
| `cancel` | Requests cancellation of a task run by result ID. |
| `reclaim` | Requeue or fail tasks with expired leases. |
| `prune-workers` | Delete workers not seen recently. |
| `prune-successful` | Delete successful task runs older than a cutoff. |
| `stats` / `status` | Shows task counts by status and active workers. |
| `systemd` | Generates systemd service files for production. |
| `stress-test` | Enqueues dummy tasks for benchmarking. |

---

## 📊 Stats API (JSON)

When you include `reproq_django.urls` in your project, `GET /stats/` returns
JSON task counts, per-queue task counts, worker records, and periodic task
schedules. Access is granted to staff sessions or an API token.

## 🧭 TUI Integration

Set `REPROQ_TUI_SECRET` to enable the TUI login flow and signed JWT access.
If you do not want to expose SSE, set `REPROQ_TUI_DISABLE_EVENTS=1` to omit
the `/reproq/tui/events/` stream from the TUI config payload.

Configure a token via `METRICS_AUTH_TOKEN` (settings or env). The stats endpoint
also accepts signed TUI JWTs (when `REPROQ_TUI_SECRET` is set).

Example:

```bash
curl -H "Authorization: Bearer $METRICS_AUTH_TOKEN" https://your-app/reproq/stats/
```

You may also send `X-Reproq-Token: <token>`.

---

## 🧾 Worker/Beat Config Files

The Go worker/beat support YAML/TOML config files. `manage.py reproq worker` and `manage.py reproq beat`
will load a config file when `--config` or `REPROQ_CONFIG` is set. If no worker/beat flags are provided,
they also look for `reproq.yaml`, `reproq.yml`, `reproq.toml`, `.reproq.yaml`, `.reproq.yml`, or `.reproq.toml`
in the current working directory. CLI flags override config values; environment variables override config values too.
`--dsn` always overrides `DATABASE_URL`, and `DATABASE_URL` is optional when a config file or flags are provided.

See `reproq.example.yaml` and `reproq.example.toml` for full templates.

Queue selection uses `--queues` (comma-separated). The legacy `--queue` flag remains for compatibility but is deprecated.

---

## ⚖️ Scaling: Workers vs Concurrency

- **Increase concurrency** (`--concurrency`): more goroutines in a single worker process; best for I/O-heavy tasks with minimal overhead.
- **Run multiple workers**: separate processes/hosts; best for CPU-heavy workloads and fault isolation.

Rule of thumb: start with 1-2 workers per host and tune `--concurrency` to available CPU cores and workload type.

---

## 🔍 Admin Dashboard

Reproq integrates deeply with the Django Admin.

- **Task Runs**: View all tasks. Filter by status, queue, or lease state.
- **Actions**:
    - **Replay**: Select tasks to re-enqueue them (creates a new copy).
    - **Retry Failed**: Reset failed tasks to READY.
    - **Cancel**: Request cancellation of running/ready tasks.
- **Workers**: Monitor active worker nodes, their concurrency, and last heartbeat.
- **Periodic Tasks**: Create, enable/disable, and edit cron schedules. Set `next_run_at` to run a job immediately.
- **Status Note**: Non-standard statuses like `WAITING` or `CANCELLED` map to `PENDING`/`CANCELLED` when supported by Django's `TaskResultStatus`. The original value is always available via `raw_status`.

---

## 🚀 Production Deployment

### Recommended Setup (Systemd)
Generate service files to run the worker and beat processes as background daemons.

```bash
python manage.py reproq systemd --user myuser --concurrency 20
```

This generates `reproq-worker.service` and `reproq-beat.service`. Copy them to `/etc/systemd/system/` and enable them.
You can pass metrics flags (for example `--metrics-addr 127.0.0.1:9090`) or use `--env-file` to load `METRICS_AUTH_TOKEN` and `METRICS_ALLOW_CIDRS`.

### Env Vars
The Go worker relies on standard environment variables:
- `DATABASE_URL`: `postgres://user:pass@host:5432/db` (optional if you provide `--dsn` or a config file)
- `WORKER_ID`: (Optional) Unique name for the node.
- `REPROQ_WORKER_BIN`: (Optional) Path to the binary if not using `manage.py reproq install`.
- `REPROQ_CONFIG`: (Optional) Path to a YAML/TOML worker/beat config file.
- `ALLOWED_TASK_MODULES`: (Optional) Comma-separated task module allow-list for the worker. If unset, `manage.py reproq worker` auto-configures it from discovered task modules.
- `REPROQ_LOGS_DIR`: (Optional) Directory to persist worker stdout/stderr logs (updates `task_runs.logs_uri`).
- `METRICS_AUTH_TOKEN`: (Optional) Bearer token for `/metrics` and `/healthz`.
- `METRICS_ALLOW_CIDRS`: (Optional) Comma-separated IP/CIDR allow-list for metrics/health.
- `METRICS_AUTH_LIMIT`: (Optional) Max unauthorized requests per window (default 30).
- `METRICS_AUTH_WINDOW`: (Optional) Rate limit window (default 1m).
- `METRICS_AUTH_MAX_ENTRIES`: (Optional) Max tracked hosts for auth rate limiting (default 1000).
- `METRICS_TLS_CERT`: (Optional) TLS certificate path for health/metrics.
- `METRICS_TLS_KEY`: (Optional) TLS private key path for health/metrics.
- `METRICS_TLS_CLIENT_CA`: (Optional) Client CA bundle to require mTLS for health/metrics.

If `DATABASE_URL` is not set, `manage.py reproq worker` derives a DSN from `settings.DATABASES["default"]`.

Worker binary resolution order:
1. `REPROQ_WORKER_BIN` (setting or env)
2. `./.reproq/bin/reproq` (installed by `reproq install`)
3. `reproq_django/bin/reproq` (packaged fallback)
4. `PATH`

---

## 🤝 Contributing

Reproq is split into two repos:
- **Reproq Django**: This repo (Python/Django logic).
- **Reproq Worker**: The Go execution engine.

Issues and PRs are welcome in both!

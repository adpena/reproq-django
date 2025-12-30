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

---

## ⚡ Quickstart

### 1. Install
```bash
pip install reproq-django
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

### 3. Install the Worker
Reproq can automatically download or build the Go binary for you:
```bash
python manage.py reproq install
```
*This command detects your OS/Architecture and fetches the correct pre-built binary from GitHub. No Go installation required!*

### 4. Run Migrations
```bash
python manage.py reproq migrate-worker
python manage.py migrate
```
*Note: `migrate-worker` applies necessary Postgres optimizations (indexes, extensions) that Django migrations cannot handle.*

### 5. Start the Worker
```bash
python manage.py reproq worker
```

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

**Reserved kwargs:** `run_after` is treated as scheduling metadata and is removed from task kwargs. If your task needs a parameter named `run_after`, rename it.

**Note on Priority:** The task priority is set at definition time via `@task(priority=...)`. Overriding priority via `enqueue(priority=...)` is currently **not supported**.

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

            # Retry Limit (Default: 5)
            # Max number of attempts for a task.
            "MAX_ATTEMPTS": 5,

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
Execute tasks in parallel. Groups do not provide a "chord" callback yet; they are a convenience for enqueueing multiple independent tasks at once.

```python
from reproq_django.workflows import group

g = group(
    (resize_image, ("img1.jpg",), {}),
    (resize_image, ("img2.jpg",), {}),
)
results = g.enqueue()
```

---

## 🔁 Retries & Backoff

Retries are managed by the Go worker. When a task fails and attempts remain, it is re-queued with an **exponential backoff**:

- Base delay: 30s
- Backoff: `2^attempt` (attempt starts at 1)
- Cap: 1 hour

The worker updates `run_after` on the failed task, and the backend will only claim it after that timestamp.

---

## 🖥 Management Commands

The `python manage.py reproq` command is your Swiss Army knife.

| Subcommand | Description |
| :--- | :--- |
| `init` | Bootstraps Reproq in the current project. |
| `worker` | Starts the Go worker. Flags: `--concurrency` (default 10), `--queue`. |
| `beat` | Starts the scheduler. Flags: `--interval` (default 30s). |
| `install` | Downloads/builds the worker binary. |
| `migrate-worker` | Applies essential SQL schema optimizations (indexes, extensions). |
| `check` | Validates binary path, DB connection, and schema health. |
| `reclaim` | Requeue or fail tasks with expired leases. |
| `stats` | Shows task counts by status and active workers. |
| `systemd` | Generates systemd service files for production. |
| `stress-test` | Enqueues dummy tasks for benchmarking. |
| `reproq_health` | Health check for DB, workers, and queues. |

---

## 🔍 Admin Dashboard

Reproq integrates deeply with the Django Admin.

- **Task Runs**: View all tasks. Filter by status, queue, or lease state.
- **Actions**:
    - **Replay**: Select tasks to re-enqueue them (creates a new copy).
    - **Retry Failed**: Reset failed tasks to READY.
    - **Cancel**: Request cancellation of running/ready tasks.
- **Workers**: Monitor active worker nodes, their concurrency, and last heartbeat.
- **Periodic Tasks**: Create and manage cron schedules via the UI.
- **Status Note**: Non-standard statuses like `WAITING` or `CANCELLED` are available via `raw_status` on results.

---

## 🚀 Production Deployment

### Recommended Setup (Systemd)
Generate service files to run the worker and beat processes as background daemons.

```bash
python manage.py reproq systemd --user myuser --concurrency 20
```

This generates `reproq-worker.service` and `reproq-beat.service`. Copy them to `/etc/systemd/system/` and enable them.

### Env Vars
The Go worker relies on standard environment variables:
- `DATABASE_URL`: `postgres://user:pass@host:5432/db`
- `WORKER_ID`: (Optional) Unique name for the node.
- `REPROQ_WORKER_BIN`: (Optional) Path to the binary if not using `manage.py reproq install`.

---

## 🤝 Contributing

Reproq is split into two repos:
- **Reproq Django**: This repo (Python/Django logic).
- **Reproq Worker**: The Go execution engine.

Issues and PRs are welcome in both!

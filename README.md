# Reproq Django 🚀

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Documentation](https://img.shields.io/badge/Docs-View%20Online-indigo)](https://adpena.github.io/reproq-django/)

**Deterministic Background Tasks for Django 6.0+ powered by Go.**

Reproq is a production-grade tasks backend that combines the ease of Django with the performance and reliability of the [Reproq Worker](https://github.com/adpena/reproq-worker), a high-performance execution engine written in Go.

---

## 🤝 Relationship with Reproq Worker

Reproq is split into two specialized components:
1. **Reproq Django (this repo)**: The "Brain." It provides the Django 6.0 Tasks API, handles task definition, enqueuing, results, and the Admin dashboard.
2. **[Reproq Worker](https://github.com/adpena/reproq-worker)**: The "Muscle." A standalone Go binary that polls the database and executes tasks with extreme efficiency and reliability.

---

## Key Features

- **Postgres-Only**: Uses `SKIP LOCKED` for high-performance, atomic task claiming.
- **Deterministic**: Every task has a `spec_hash` (SHA256). Identical tasks can be automatically de-duplicated.
- **Strict Isolation**: The Go worker invokes Django tasks via a strict JSON-over-stdin protocol.
- **Django Native**: Implements the Django 6.0 `BaseTaskBackend` API.
- **Periodic Tasks**: Built-in support for cron-like scheduling managed via Django models.
- **Monitoring**: Beautiful Django Admin integration to view, retry, and replay tasks.
- **Worker Heartbeats**: Track active worker nodes and their health in real-time.
- **Exponential Backoff**: Automatic retry delays that scale with failure count.

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
python manage.py migrate
python manage.py reproq migrate-worker
```

### 5. Start the Worker
```bash
python manage.py reproq worker
```

---

## 🛠 Usage

### Defining and Enqueueing Tasks
Use the standard Django 6.0 `@task` decorator:

```python
from django.tasks import task

@task(queue_name="high-priority")
def send_welcome_email(user_id):
    # logic here
    return f"Email sent to {user_id}"

# Enqueue for background execution
result = send_welcome_email.enqueue(123)

# Wait for result (optional)
result.wait(timeout=5)
print(result.result) # "Email sent to 123"
```

### Periodic Tasks (Cron)
Define periodic tasks in the Django Admin or via the `PeriodicTask` model...

---

## advanced Features

### Concurrency Control (Lock Key)
Prevent multiple tasks from operating on the same resource simultaneously:

```python
@task
def process_user(user_id):
    pass

# Ensure only one task for 'user_123' runs at a time
process_user.enqueue(123, lock_key="user_123")
```

### Workflows (Chains & Groups)
Execute tasks in sequence or parallel:

```python
from reproq_django.workflows import chain, group

# Sequential execution
chain(task_a, task_b, task_c).enqueue()

# Parallel execution
group(task_1, task_2, task_3).enqueue()
```

---

## 🖥 Management Commands

The `reproq` command is your primary tool for managing the task system.

| Command | Description |
| :--- | :--- |
| `python manage.py reproq worker` | Starts the Go worker to process tasks. |
| `python manage.py reproq beat` | Starts the Go scheduler for periodic tasks. |
| `python manage.py reproq check` | Validates your settings and database connectivity. |
| `python manage.py reproq systemd` | Generates systemd service files for production. |
| `python manage.py reproq migrate-worker` | Applies Go-specific SQL optimizations. |

---

## 🚀 Production Deployment

Reproq is designed to be easily managed in production using `systemd`.

1. **Generate Service Files**:
   ```bash
   python manage.py reproq systemd
   ```
2. **Install & Start**:
   Follow the on-screen instructions to move the files to `/etc/systemd/system/` and enable them. This ensures your worker and beat processes auto-restart on failure and start automatically on boot.

---

## 🔍 Monitoring & Management

Reproq comes with a powerful Django Admin interface.

1. **Task List**: View all tasks, their status, queues, and execution times.
2. **Details**: See the exact arguments (`spec_json`), the return value, or the full traceback if it failed.
3. **Replay**: Select any task and use the "Replay selected tasks" action. This creates a fresh copy of the task and enqueues it, perfect for manual retries after fixing a bug.
4. **Periodic Tasks**: Manage your cron schedules directly from the UI.

---

## ⚙️ Advanced Configuration

You can customize the backend behavior in `settings.py`:

```python
TASKS = {
    "default": {
        "BACKEND": "reproq_django.backend.ReproqBackend",
        "OPTIONS": {
            "DEDUP_ACTIVE": True,      # Coalesce identical tasks if already READY/RUNNING
            "TIMEOUT_SECONDS": 900,     # Max execution time before worker kills the task
            "MAX_ATTEMPTS": 5,          # Retries for failed tasks
        }
    }
}

# Path to the Go binary (if not in PATH)
REPROQ_WORKER_BIN = "/usr/local/bin/reproq"
```

---

## 📋 Checklist for Production

- [ ] Ensure `DATABASE_URL` is set if not using Django's default DB configuration.
- [ ] Set `REPROQ_WORKER_BIN` if the binary is in a custom location.
- [ ] Configure `MAX_ATTEMPTS` and `TIMEOUT_SECONDS` for your workload.
- [ ] Set up a process supervisor (like Systemd or Supervisor) for `python manage.py reproq worker`.
- [ ] If using periodic tasks, ensure `python manage.py reproq beat` is also running (only one instance needed).

## 🤝 Contributing & Feedback

Reproq is an open-source project and we love contributions! 

- **Found a bug?** Open an [issue](https://github.com/adpena/reproq-django/issues).
- **Want a feature?** Let's discuss it in the issues or submit a PR.
- **Questions?** Feel free to reach out or start a discussion.

We are specifically looking for feedback on developer experience (DX) and performance in high-scale environments.

---

## 📜 Documentation & Guides

- [Architecture Overview](docs/architecture.md)
- [Deployment Guide](docs/deployment.md)
- [Handling Large Payloads](docs/payloads.md)
- [Deterministic Tasks Guide](docs/determinism.md)

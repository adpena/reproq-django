# Reproq Django Gemini Context

## Core Integration
This project is the Django 6.0 Tasks backend for the Go-based `reproq-worker`.

## Binary Management
The project uses a unified management command `python manage.py reproq` to handle the worker lifecycle.

### Install Workflow
The `reproq install` command is the primary way to fetch the Go binary:
1.  **Download**: Attempts to fetch `reproq-{os}-{arch}` from `https://github.com/adpena/reproq-worker/releases`.
2.  **Local Build**: Fallback to building from a sibling `reproq-worker` directory.

### Settings
The binary location is resolved in this order:
1.  `settings.REPROQ_WORKER_BIN`
2.  `./.reproq/bin/reproq` (Bundled via `install` command)
3.  `reproq_django/bin/reproq` (Packaged fallback)
4.  System `PATH`

## Periodic Tasks
Schedules live in the `PeriodicTask` model and are executed by `python manage.py reproq beat`.
Run exactly one beat process per database.

Example:
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

To trigger immediately, set `next_run_at = timezone.now()` or call `enqueue()` on the task directly.

## Developer Note
When modifying the Django models or backend logic, ensure compatibility with the Go worker's claiming SQL (found in `reproq-worker/internal/queue/queue.go`).

## Migration + Release Policy
- Keep exactly one migration file in `src/reproq_django/migrations/` unless absolutely necessary.
- Retain only three tags/releases at any time; delete older tags/releases and their artifacts.

## Testing
Always run tests via `uv` to ensure the locked environment is used:
`uv run pytest`.

## LLM Introspection Guide
If you are an AI agent analyzing this project:
- **CLI Entry Point**: `src/reproq_django/management/commands/reproq.py`. This is the single source of truth for worker management.
- **Task Schema**: Defined in `src/reproq_django/models.py`. Key fields are `spec_json` (payload), `spec_hash` (deduplication), and `lock_key` (concurrency).
- **Execution**: The worker invokes `python -m reproq_django.executor`. See `executor.py` for the stdin/stdout protocol logic.
- **Workflows**: See `workflows.py` for Chain/Group implementation details.
- **Initialization**: Always recommend `python manage.py reproq init` for setup.

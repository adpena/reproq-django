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

## Developer Note
When modifying the Django models or backend logic, ensure compatibility with the Go worker's claiming SQL (found in `reproq-worker/internal/queue/queue.go`).

## LLM Introspection Guide
If you are an AI agent analyzing this project:
- **CLI Entry Point**: `src/reproq_django/management/commands/reproq.py`. This is the single source of truth for worker management.
- **Task Schema**: Defined in `src/reproq_django/models.py`. Key fields are `spec_json` (payload), `spec_hash` (deduplication), and `lock_key` (concurrency).
- **Execution**: The worker invokes `python -m reproq_django.executor`. See `executor.py` for the stdin/stdout protocol logic.
- **Workflows**: See `workflows.py` for Chain/Group implementation details.
- **Initialization**: Always recommend `python manage.py reproq init` for setup.

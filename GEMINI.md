# Reproq Django Gemini Context

## Core Integration
This project is the Django 6.0 Tasks backend for the Go-based `reproq-worker`.

## Binary Management
The project uses a unified management command `python manage.py reproq` to handle the worker lifecycle.

### Install Workflow
The `reproq install` command is the primary way to fetch the Go binary:
1.  **Download**: Attempts to fetch `reproq-{os}-{arch}` from `https://github.com/adpena/reproq-worker/releases`.
2.  **Go Install**: Fallback to `go install github.com/adpena/reproq-worker/cmd/reproq@latest`.
3.  **Local Build**: Fallback to building from a sibling `reproq-worker` directory.

### Settings
The binary location is resolved in this order:
1.  `settings.REPROQ_WORKER_BIN`
2.  `reproq_django/bin/reproq` (Bundled via `install` command)
3.  System `PATH`

## Developer Note
When modifying the Django models or backend logic, ensure compatibility with the Go worker's claiming SQL (found in `reproq-worker/internal/queue/queue.go`).

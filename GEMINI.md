# Reproq Django Gemini Context

## Core Integration
This project is the Django 6.0 Tasks backend for the Go-based `reproq-worker`.

## Binary Management
The project uses a unified management command `python manage.py reproq` to handle the worker lifecycle.

### Install Workflow
The installer requires an explicit or published platform digest, verifies the
bytes before executing them, probes schema2/executor-protocol2 capabilities and
atomically replaces the installed worker only after success. Downloads never
fall back to source. Source builds require explicit `--build --source PATH` and
use the worker repository's canonical reproducible release builder/verifier.

### Settings
The binary location is resolved in this order:
1.  `settings.REPROQ_WORKER_BIN`
2.  `./.reproq/bin/reproq` (Bundled via `install` command)
3.  `reproq_django/bin/reproq` (Packaged fallback)
4.  System `PATH`

## Periodic Tasks
Use `register_periodic` or `@recurring` plus explicit `reproq sync-recurring`.
Do not create runnable definitions with raw ORM writes or post_migrate hooks.
The SDK canonical serializer owns specs for direct, bulk and periodic tasks.
Registration preserves existing operator enable/next/last state unless explicitly
reset. Each definition is assigned to Go or pg_cron. Go beat and one-shot schedule
share one scheduler; native jobs call the migration-owned occurrence primitive.
Native cron.timezone must match the definition's zone. Native execution-minute
identity/catch-up differs from Go nominal-tick coalescing; see README.

## Runtime Contract
The worker and SDK must agree on schema2/executor protocol2 before claims.
Every claim has a fresh UUID lease token; RUNNING owner/token/unexpired DB lease
fence heartbeat, completion and Python metadata writes. Task logs go to stderr;
stdout is one complete bounded UTF-8 JSON envelope. Invalid/oversized results
fail, without silent truncation. Delivery is at least once: task side effects
need application idempotency/transactions; token fencing is not exactly-once
external execution. Foreground commands replace the Django parent process and
preserve the worker exit status. Queue credentials use REPROQ_DATABASE_URL; business DATABASE_URL remains unchanged for Django tasks.

Native TaskResult/TaskContext classes, immutability, JSON args/kwargs,
refresh-return-None and cumulative worker history define the public contract.
Rich model/timedelta access is lazy and explicit decoded_args/decoded_kwargs;
async callers await adecoded_args()/adecoded_kwargs(). Refresh never hydrates
business models. Failures preserve qualified exception identity and bounded
frames, with explicit diagnostic-budget markers. Resettable budget_attempt is
separate from native attempts. TaskContext.from_claim accepts
an immutable ExecutionClaim; only current v1 owned/unexpired rows can execute.

## Developer Note
When modifying the Django models or backend logic, ensure compatibility with the Go worker's claiming SQL (found in `reproq-worker/internal/queue/queue.go`).

## Multi-Database Queues
Queue routing is controlled by `REPROQ_QUEUE_DATABASES` and `REPROQ_DEFAULT_DB_ALIAS`.
Multiple queue databases require prefixed result IDs; their aliases are percent
encoded and decoded canonically. Native IDs stay below64characters; unsupported
alias lengths are rejected before producer writes. Disabling the prefix for
multiple databases is rejected rather than guessing the result's database.

### Observability & Heatmaps
- Surface rich statistics via `reproq_stats_api` to support TUI dashboards.
- Track task failure heatmaps (top failing task paths) and queue depth trends.
- Support OIDC/OAuth flows for TUI authentication if managed headers are used.

## Migration + Release Policy
- Preserve published migration bytes and append reviewed forward migrations.
  Committed Django migrations own all six queue tables and their PostgreSQL DDL.
  Legacy adoption requires full semantic catalog/history verification and an
  explicit audit receipt. Workers require the migration-owned schema contract;
  they must not create, repair, or migrate queue tables themselves.
- Retain only three tags/releases at any time; delete older tags/releases and their artifacts.

## Testing
Always run tests via `uv` to ensure the locked environment is used:
`uv run pytest`.

## Render Env Var Safety
The Render API `PUT /services/{id}/env-vars` replaces the full env var set.
Always list the current env vars first, compare against `render.yaml`, and only
then apply changes so you do not wipe required keys.

## LLM Introspection Guide
If you are an AI agent analyzing this project:
- **CLI Entry Point**: `src/reproq_django/management/commands/reproq.py`. This is the single source of truth for worker management.
- **Task Schema**: Defined in `src/reproq_django/models.py`. Key fields are `spec_json` (payload), `spec_hash` (deduplication), and `lock_key` (concurrency).
- **Execution**: The worker invokes `python -m reproq_django.executor`. See `executor.py` for the stdin/stdout protocol logic.
- **Workflows**: See `workflows.py` for Chain/Group implementation details.
- **Initialization**: Always recommend `python manage.py reproq init` for setup.

## Task Schema Additions
Recent additions include:
- `concurrency_key`/`concurrency_limit` in `task_runs` and `periodic_tasks`.
- `metadata_json` for task progress metadata.
- `reproq_queue_controls` for queue pause/resume.

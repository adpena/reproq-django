# Reproq Django 🚀

[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)
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
| Django | 6.x; Python 3.12–3.14 requires `Django>=6.0.9,!=6.1.0,!=6.1.1,<7.0`, and Python 3.15 requires `Django>=6.1.2,<7.0` |
| Python | 3.12–3.15, checked locally with isolated locked environments |
| Reproq Worker | Release tested with this SDK (install/upgrade via `python manage.py reproq install` / `upgrade`) |

Use `python manage.py reproq doctor` to verify schema, worker binary, and DSN alignment.
The Go process reads its queue connection from `REPROQ_DATABASE_URL` or explicit
configuration/flags. Generic `DATABASE_URL` remains Django's business connection;
the SDK derives queue connections from the selected Django alias and preserves
that business environment for task execution. Queue names must route to the
selected alias through `REPROQ_QUEUE_DATABASES`.

Python 3.15 is validated by this project's checks against published Django 6.1.2
and `psycopg[binary]` 3.3.6. As of October 28, 2026, Django's upstream
[6.1.3 release notes](https://github.com/django/django/blob/5e109ba2895f0db9679c03a1b23f0b40c0b9310f/docs/releases/6.1.3.txt)
plan official Python 3.15 support for that release. The current lock uses released
packages, with no prerelease or source pins. Earlier Python versions use the
[patched Django 6.0 branch](https://www.djangoproject.com/weblog/2026/oct/06/security-releases/).
The explicit 6.1.0 and 6.1.1 exclusions prevent an installed affected higher
minor release from satisfying the older-Python security floor. They cover
known published versions, rather than promising security for future releases.
Package builds require `setuptools>=84.1.0` and `wheel>=0.46.2`.

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
Bootstrap writes a config file, installs the worker binary, and applies committed
Django migrations. Queue schema changes have one owner: this package's migrations.
```bash
python manage.py reproq init
```
Use `--format toml` or `--skip-install` as needed. `--skip-migrate` skips the
project-wide invocation; `--skip-worker-migrate` skips the separate ReproQ-only
invocation. Both invocations use the same committed Django migrations. Use both
flags when migration execution must be deferred.

### 5. Install the Worker (Standalone)
If you want only the worker binary:
```bash
python manage.py reproq install
```
*This command detects your OS/Architecture and fetches the correct pre-built binary from GitHub. No Go installation required!*

Downloads must match the publisher's checksum sidecar or GitHub release-asset
SHA-256 digest before the binary is made executable or its version is queried.
For reproducible deployment, pin both a release tag and the digest of the
binary for the target OS and architecture:

~~~bash
python manage.py reproq install --tag v0.0.145 --sha256 <platform-binary-sha256>
~~~

The same options work with init and upgrade. An explicit digest disables
download-to-source fallback. Missing or mismatched integrity metadata stops
installation and preserves the existing binary; it never triggers a source
build. Explicit --build still supports local development and can also verify
an expected digest. Successful installation replaces the binary atomically.

Run the complete locked local lint and test suite with make check. The optional
hosted CI workflow is manual and calls the same command.
Release builds are also manual: they require an existing reviewed tag matching
both package version declarations, run the complete locked checks, and create
a draft release. Existing releases and their assets are never overwritten by
that workflow. Local builds use uv build after make check; publishing and tag
retention remain separate reviewed actions.

### 6. Run Migrations
```bash
python manage.py migrate
```

The historical `reproq migrate-worker --database <alias>` command now delegates to
the same Django migrations for ReproQ. It does not execute another SQL schema or
repair tables. The standalone Go worker requires the supported migration-owned
schema contract before claiming tasks or scheduling work.

Stop legacy workers and drain all `RUNNING` tasks before the forward contract
migration. Its validated lease constraint makes old claim/completion SQL fail
closed. The migration preserves historical task JSON and hashes, adds nullable
canonical-spec fields with `spec_version=0`, and leaves canonical schedule
reconciliation to an explicit later operation. Schema rollback requires a
verified database backup; this migration deliberately refuses a destructive
reverse operation.

Existing databases with legacy Go-created tables or a partially applied Django
squash require explicit adoption before ordinary migration. Inspect the selected
database first:

```bash
python manage.py reproq adopt-schema --database <alias> --inspect
```

Adoption accepts only checked-in semantic catalog profiles and exact supported
historical ledger sets. It checks column types/nullability/defaults, constraints
and validation state, indexes, triggers/functions and sequence ownership. Unknown
or partial schemas fail without changing migration history. The exact historical
Django baseline plus the Go baseline's auxiliary tables and eight additional
indexes is also supported. If the baseline and its replacement records are
already coherent, use ordinary forward Django migration without re-adopting
history. The physical catalog and forward data checks still apply; a migration
record alone never certifies the schema. After reviewing the
inspection, supply its exact catalog digest and a new audit file:

```bash
python manage.py reproq adopt-schema --database <alias> \
  --expected-catalog-sha256 <inspected-sha256> --audit-output <new-audit-file.json>
python manage.py reproq migrate-worker --database <alias>
```

Adoption reconciles the replacement markers covered by the published squash;
it does not invent code for its never-committed third replacement member. The
receipt records before/after history and catalog identity. Adoption requires an
outermost autocommit boundary; it refuses nested or manually managed transactions.
An interrupted `prepared`, `unconfirmed`, or `commit_unknown` receipt requires
independent ledger verification. Losing a COMMIT acknowledgement does not prove
rollback. Receipt contents are fsynced and directory entries are flushed where
the platform supports it; database and filesystem commits are separate resources.
There is no
`--fake` or table-existence shortcut. The known raw Go profile's unvalidated
task-path check requires valid existing rows and is validated by the forward
Django migration.

The currently qualified PostgreSQL catalog profiles use the `public` schema.
Other queue namespaces require their own qualified catalog support; the guard
fails closed rather than treating a migration record as schema proof. The
forward migration also verifies all four core tables and any preexisting
auxiliary tables before establishing the new contract.
Adoption, migration and runtime checks bind all queue, marker and history
relations to ordinary permanent public tables. Temporary objects and shadow
schemas cannot substitute for the validated tables, even on a public-first path.

### 7. Start the Worker
```bash
python manage.py reproq worker
```

### 8. (Optional) Schedule Periodic Tasks
You have two options (choose one):

**Option A: Run Beat (one process per database)**
```bash
python manage.py reproq beat
```

**Option B: Use Postgres-native scheduling (pg_cron)**
```bash
python manage.py reproq pg-cron --install
```
`pg_cron` requires the Postgres extension to be enabled (see `docs/deployment.md`).

---

## 🧰 Management Commands

Run `python manage.py reproq <subcommand>` to manage the worker and day-to-day ops.
Full reference (examples + exit codes): `docs/cli.md`.

- **Bootstrap**: `init` writes `reproq.toml`, installs the worker, and runs migrations. Explicit YAML configuration requires the optional PyYAML reader in the Django environment.
- **Config**: `config --explain` prints the effective config and its precedence.
- **Doctor**: `doctor --strict` validates DSN, schema, worker binary, and allowlist.
- **Upgrade**: `upgrade` installs the release tested with this SDK and optionally applies the committed queue migrations.
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

You can also annotate a task with a concurrency limit:

```python
from reproq_django.concurrency import limits_concurrency

@limits_concurrency(lambda user_id: f"user:{user_id}", to=1)
@task(queue_name="high-priority", priority=100)
def send_welcome_email(user_id):
    return f"Email sent to {user_id}"
```

### Enqueuing Tasks
Use Django's standard task API. Every enqueue keyword is a function argument;
ReproQ does not reserve names such as priority, run_after or concurrency_key.
Delivery options belong on the task, backend configuration or decorators:

```python
from datetime import timedelta
from django.utils import timezone

result = send_welcome_email.enqueue(123)
result = send_welcome_email.using(
    priority=42,
    run_after=timezone.now() + timedelta(minutes=10),
).enqueue(123)
```

Use `@limits_concurrency(lambda user_id: f"user:{user_id}", to=2)` for a shared
key, as shown above. Named task function attributes can supply custom lock/key
options; they never consume function kwargs. Direct, bulk and periodic enqueue
use the same typed arguments/spec builder. Strict integer budgets and optional
string keys are validated before database writes; bool/float coercion is rejected.
Concurrency capacity is the strongest positive candidate/active limit for a
shared key. A zero contributes no limit but does not bypass an active key's cap.

The former delivery-in-function-kwargs extension is retired. Migrate delivery
priority/run_after to Task.using and shared concurrency to the decorator; leave
same-named function parameters intact.

### Async Contexts (ASGI)
If you are in an async view or task producer, use `aenqueue()` to avoid blocking:

```python
result = await send_welcome_email.aenqueue(123)
```

In sync/Wsgi code, continue to use `enqueue()`.

### Task Context + Metadata
If your task needs context (result id, attempt, metadata), use `takes_context=True`.

```python
from django.tasks import task

@task(takes_context=True)
def process_upload(context, upload_id):
    context.metadata["stage"] = "starting"
    context.save_metadata()
    # work...
    context.metadata["stage"] = "done"
    context.save_metadata()
```

Metadata is stored in `task_runs.metadata_json` and surfaced in task results.

### Bulk Enqueuing
Use `bulk_enqueue` for batches of jobs. It groups database inserts and uses the
same task options, argument codec and canonical spec builder as `enqueue`.
Choose a batch size that fits the producer's memory budget.

```python
from django.tasks import task_backends
from datetime import timedelta
from django.utils import timezone

backend = task_backends["default"]
jobs = []
scheduled_at = timezone.now()

for i in range(1000):
    # (task_func, args, kwargs)
    jobs.append((
        send_welcome_email.using(run_after=scheduled_at + timedelta(seconds=i)),
        (i,),
        {},
    ))

backend.bulk_enqueue(jobs)
```

---

## ⏰ Periodic Tasks

Register source intent through the SDK, then run a scheduler. Migrations do not
seed or reset operator schedules. Direct ORM creation leaves a legacy/unassigned
row that requires explicit reconciliation; it cannot silently become runnable.

```python
from myapp.tasks import nightly_cleanup
from reproq_django.recurring import register_periodic

register_periodic(
    name="Nightly cleanup",
    schedule="CRON_TZ=America/Chicago 0 2 * * *",
    task=nightly_cleanup,
    queue_name="maintenance",
    scheduler_provider="go",  # or "pg_cron" on a configured database
)
```

The same canonical SDK serializer handles direct, bulk and periodic task specs,
including typed arguments, context, retry/timeout budget, routing and provenance.
Registration preserves an existing definition's enabled, next-run and last-run
state. Resetting schedule state or assigning another provider is explicit. The
admin permits pausing/resuming definitions; canonical spec fields are read-only.

Each definition belongs to one provider. Long-running Go beat and one-shot Go
scheduling use the same implementation:

```bash
python manage.py reproq beat
# Or invoke this from an external timer; do not also run the long-running beat:
python manage.py reproq schedule
```

Go coalesces an overdue definition to one occurrence at its stored nominal
`next_run_at`, then advances beyond the database clock. It does not invent every
missed tick. The occurrence identity `(periodic_name, scheduled_for)` remains
unique after completion or failure, while separate occurrences may overlap
subject to the configured concurrency key/limit.

The optional native provider uses the same migration-owned PostgreSQL enqueue
primitive and the exact stored SDK bytes. It needs an administrator-installed
`pg_cron` extension and its named-job API; the SDK does not create extensions or
functions. Assign definitions to `pg_cron`, then reconcile only their owned jobs:

```bash
python manage.py reproq pg-cron --install --dry-run
python manage.py reproq pg-cron --install
python manage.py reproq pg-cron --remove
```

`--if-supported` skips only an unavailable extension/database capability. It does
not hide invalid definitions or mismatched zones. The portable grammar uses ASCII numbers/names, lists, ascending ranges and positive wildcard/range steps; Weekdays are0–6 (Sunday0). L/#/wrapping ranges, scalar steps and macros are rejected. Bare cron expressions mean UTC;
`CRON_TZ`/`TZ` selects a named zone. Native `cron.timezone` must match that zone
(UTC/GMT aliases are accepted). Go supports per-definition zones; pg_cron uses
its database server's configured zone. Native occurrences are identified by the
UTC minute when their SQL starts, not an asserted nominal tick or a replay of
missed ticks. Native scheduler delays/catch-up follow pg_cron; the SDK does not
synthesize a misleading `next_run_at` for it. Inspect native job run history to
verify actual execution. See [deployment](docs/deployment.md) for prerequisites.

Before upgrading a database with the old native helper, explicitly remove its
known owned jobs with the reviewed SDK command while legacy schedulers are
stopped. Forward migration0003 owns retiring that exact legacy helper and
installing the shared primitive; unknown functions/dependencies/jobs fail closed.

Code-defined registrations can use `@recurring` with Django's standard `@task`:

```python
from django.tasks import task
from reproq_django.recurring import recurring

@recurring(schedule="CRON_TZ=America/Chicago 0 9 * * *", key="daily_report", args=(42,))
@task(queue_name="maintenance")
def send_report(account_id):
    ...
```

Run `python manage.py reproq sync-recurring` explicitly after imports/deployment.
Named legacy rows can be canonicalized with `reconcile-periodic --name NAME`
(`--dry-run` previews with transaction rollback). Neither runs on `post_migrate`.
To run a task immediately, call its standard `enqueue()` method; this does not
rewrite its recurring schedule or pretend to be a scheduled occurrence.

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
defaults < config file < env vars < CLI flags. Django management commands derive
an empty DSN from the selected queue database alias and validate explicit
file/environment targets against it. The standalone Go CLI also accepts `--dsn`.

### Multi-Database Queues
Route specific queues to different Django database aliases.

```python
DATABASES = {
    "default": {...},
    "queues": {...},
}

REPROQ_QUEUE_DATABASES = {
    "high-priority": "queues",
    "bulk-*": "queues",  # glob patterns supported
}
```

Use `REPROQ_DEFAULT_DB_ALIAS` to change the fallback alias (defaults to
`"default"`). When multiple queue databases are configured, task result IDs are
prefixed with a canonically percent-encoded database alias (`queues:123`).
Multiple databases cannot disable this prefix. Unsupported encoded alias
lengths are rejected before producer writes, keeping native result IDs below
64 characters without truncation or guessed routing.
To route all Reproq tables to a non-default alias in ORM reads/writes (admin,
health checks, etc.), add the router:
```python
DATABASE_ROUTERS = ["reproq_django.db_router.ReproqRouter"]
```

Run one worker/beat per database:

```bash
python manage.py reproq worker --database queues --queues high-priority,bulk-1
python manage.py reproq beat --database queues
```

Use `REPROQ_STATS_DATABASES=["*"]` to aggregate stats across all queue databases.

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

Automatic retries keep dependents waiting until the task succeeds or exhausts
its budget. Terminal workflow members cannot be manually reset in place: that
would break dependency order or aggregate counters. Enqueue a fresh workflow to
recover the workflow, or explicitly replay one task as a detached new task.

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
| `worker` | Starts the Go worker. Flags: `--config`, `--concurrency` (default 10), `--queues`, `--allowed-task-modules`, `--logs-dir`, `--payload-mode`, `--metrics-port`, `--metrics-addr`, `--metrics-auth-token`, `--metrics-allow-cidrs`, `--metrics-tls-cert`, `--metrics-tls-key`, `--metrics-tls-client-ca`, `--database`. Auto-configures allow-list when unset (unless config file is used). |
| `beat` | Starts the scheduler. Flags: `--config`, `--interval` (default 30s), `--once`, `--database`. |
| `schedule` | Enqueue due periodic tasks once and exit (alias for `beat --once`). |
| `pg-cron` | Syncs Postgres-native schedules. Flags: `--install` (default), `--remove`, `--prefix`, `--dry-run`, `--database`. |
| `install` | Downloads/builds the worker binary. |
| `upgrade` | Upgrades the worker binary and optionally runs `migrate-worker`. |
| `migrate-worker` | Applies committed ReproQ Django migrations on the selected database alias. |
| `adopt-schema` | Inspects or explicitly adopts a fully verified known legacy PostgreSQL schema. |
| `check` | Validates binary path, DB connection, and schema health. |
| `doctor` | Validates DSN, schema, worker binary, and allowlist; `--strict` fails on warnings. |
| `config` | Prints effective worker/beat config; use `--explain` for precedence. |
| `allowlist` | Prints `ALLOWED_TASK_MODULES` or writes them to a config file with `--write`. |
| `sync-recurring` | Sync code-defined recurring schedules into `periodic_tasks`. |
| `pause-queue` | Pause a queue (prevents new claims). |
| `resume-queue` | Resume a paused queue. |
| `logs` | Prints logs for a task run using `logs_uri` (supports aliased result IDs). |
| `cancel` | Requests cancellation of a task run by result ID (supports aliased result IDs). |
| `reclaim` | Requeue or fail tasks with expired leases (`--database` / `--all-databases`). |
| `prune-workers` | Delete workers not seen recently (`--database` / `--all-databases`). |
| `prune` | Delete terminal task history by status/age in bounded batches (`--database` / `--all-databases`). |
| `stats` / `status` | Shows task counts by status and active workers (`--database` / `--all-databases`). |
| `systemd` | Generates systemd service files for production. |

---

## 📊 Stats API (JSON)

When you include `reproq_django.urls` in your project, `GET /stats/` returns
JSON task counts, per-queue task counts, worker records, and periodic task
schedules. Access is granted to staff sessions, a signed TUI JWT, or
`METRICS_AUTH_TOKEN` as a bearer token.
Additional fields include `worker_health`, `queue_controls`, `scheduler`, and
per-database rollups when `REPROQ_STATS_DATABASES` is set.

## 🧭 TUI Integration

Set `METRICS_AUTH_TOKEN` to enable the TUI login flow and sign TUI JWTs. This
token is also accepted as a bearer token for `/reproq/stats/` and is forwarded
to the worker metrics proxy endpoints. If you do not want to expose SSE, set
`REPROQ_TUI_DISABLE_EVENTS=1` to omit the `/reproq/tui/events/` stream from the
TUI config payload.
If `METRICS_AUTH_TOKEN` is unset, the TUI auth endpoints return 404.

Set `LOW_MEMORY_MODE=1` to disable the worker proxy endpoints
(`/reproq/tui/metrics/`, `/reproq/tui/healthz/`, `/reproq/tui/events/`). The
endpoints return a 503 with a short hint while low-memory mode is enabled, and
events are omitted from the TUI config payload.

Set `REPROQ_MEMORY_LOG_INTERVAL=60s` to emit periodic memory logs from the
Django process (includes RSS on Linux).

Pass the TUI JWT in the `Authorization: Bearer ...` header when accessing
these endpoints programmatically.

---

## 🧾 Worker/Beat Config Files

The Go worker/beat support YAML/TOML config files. `manage.py reproq worker` and `manage.py reproq beat`
will load a config file when `--config` or `REPROQ_CONFIG` is set. Otherwise,
they look for `reproq.yaml`, `reproq.yml`, `reproq.toml`, `.reproq.yaml`, `.reproq.yml`, or `.reproq.toml`
in the current working directory, even when other flags are present. Actual CLI
options override environment and config values. Empty queue DSNs inherit from
the selected Django queue database; explicit targets must match that alias.

The canonical templates are [TOML](src/reproq_django/resources/reproq.example.toml)
and [YAML](src/reproq_django/resources/reproq.example.yaml). `reproq init` reads
these packaged resources; files in the current directory cannot override them.

Queue selection uses `--queues` (comma-separated). The worker's legacy `--queue`
flag has been retired; queue-control commands retain their positional queue name.

---

## ⚖️ Scaling: Workers vs Concurrency

- **Increase concurrency** (`--concurrency`): more goroutines in a single worker process; best for I/O-heavy tasks with minimal overhead.
- **Run multiple workers**: separate processes/hosts; best for CPU-heavy workloads and fault isolation.

Start with one task slot on a small shared web instance. Measure combined web,
Go, Python executor and database connection peaks under representative work;
reserve headroom before increasing concurrency. Each active Django task has its
own Python process, so a small Go footprint does not make a memory-heavy task
cheap. Move heavy ETL to a separate process/host when it exceeds the web budget.

---

## 🔍 Admin Dashboard

Reproq integrates deeply with the Django Admin.

- **Task Runs**: View all tasks. Filter by status, queue, or lease state.
- **Actions**:
    - **Replay**: Select tasks to re-enqueue them (creates a new copy).
    - **Retry Failed**: Reset failed tasks to READY.
    - **Cancel**: Request cancellation of running/ready tasks.
- **Workers**: Monitor active worker nodes, their concurrency, and last heartbeat.
- **Periodic Tasks**: Inspect registered definitions and enable/disable them. Register or change execution and schedule intent through the explicit SDK/management API; the admin does not create definitions or edit cron/next-run state.
- **Status Note**: `WAITING` projects to Django's `READY`; `CANCELLED` projects to `FAILED`. The original value remains available via `raw_status`.
- **Arguments**: Native `args`/`kwargs` contain JSON transport values. Explicit `decoded_args`/`decoded_kwargs` extensions restore timedeltas and saved models from their recorded source database on first access. Async callers use `await result.adecoded_args()` or `await result.adecoded_kwargs()` to keep model lookups off the event loop. A refresh invalidates the decoded cache without querying business models. If a referenced model was deleted, only decoded access raises; JSON arguments, status and successful return values remain inspectable.

Results and contexts inherit Django's frozen, slotted native classes.
`refresh()` and `arefresh()` update the result and return `None`. Native
`attempts` and `context.attempt` derive from the complete `worker_ids` history,
including the current worker as in Django's built-in backends. The separate
`budget_attempt` extension is ReproQ's resettable retry-budget counter. Context
metadata is an explicit mutable mapping; save it with the fenced
`save_metadata()` or `asave_metadata()` method rather than replacing the mapping.
Native `result.task.run_after` preserves original enqueue intent. The distinct
`eligible_after` extension reports mutable queue eligibility for periodic slots,
retry backoff or replay. Canonical text supplies v1 arguments; a JSONB projection
cannot preserve every source detail, including the sign of negative zero.

Task failures retain their qualified Python exception class and bounded stack
frames in Django's native `TaskError` records. Diagnostics omit source lines,
locals and chained details; an explicit marker explains when a message or stack
exceeds the budget. Worker or protocol failures without a recoverable Python
exception use `reproq_django.protocol.WorkerExecutionError`. `raw_errors` retains
the original worker records for inspection.

Use the unified `reproq check`, `reproq doctor` and `reproq worker` commands.
The standalone `reproq_check`, `reproq_health` and `reproq_init_executor`
commands, generated executor stubs and HTTP/CLI stress-test entry points have
been retired. The canonical executor is `python -m reproq_django.executor`.

Timedeltas and saved model references use explicit typed envelopes. Other JSON
normalization follows DjangoJSONEncoder: Decimal, UUID and date/time values
arrive as strings; tuples arrive as lists. Do not depend on those Python types
surviving a queue round trip. Pass explicit task-domain representations when
their type or precision is part of the task contract.

---

## 🚀 Production Deployment

### Recommended Setup (Systemd)
Generate service files to run the worker and beat processes as background daemons.

```bash
python manage.py reproq systemd --user myuser --concurrency 20
```

This generates `reproq-worker.service` and `reproq-beat.service`. Copy them to `/etc/systemd/system/` and enable them.
You can pass metrics flags (for example `--metrics-addr 127.0.0.1:9090`) or use `--env-file` to load `METRICS_AUTH_TOKEN` and `METRICS_ALLOW_CIDRS`.

To use cron-style scheduling instead of a persistent beat process, generate a timer:

```bash
python manage.py reproq systemd --schedule --schedule-on-calendar "*-*-* *:*:00"
```

Set `REPROQ_SCHEDULER_MODE=cron` in your EnvironmentFile and enable the timer to ensure
only one scheduler runs.

### Env Vars
The Go worker relies on standard environment variables:
- `REPROQ_DATABASE_URL`: `postgres://user:pass@host:5432/db` (optional if you provide `--dsn` or a config file)
- `WORKER_ID`: (Optional) Unique name for the node.
- `REPROQ_WORKER_BIN`: (Optional) Path to the binary if not using `manage.py reproq install`.
- `REPROQ_CONFIG`: (Optional) Path to a YAML/TOML worker/beat config file.
- `ALLOWED_TASK_MODULES`: (Optional) Comma-separated task module allow-list for the worker. If unset, `manage.py reproq worker` auto-configures it from discovered task modules.
- `REPROQ_LOGS_DIR`: (Optional) Directory to persist worker stdout/stderr logs (updates `task_runs.logs_uri`).
- `REPROQ_MEMORY_LOG_INTERVAL`: (Optional) Log memory stats at the given interval (e.g., `60s`).
- `METRICS_AUTH_TOKEN`: (Optional) Bearer token for `/metrics`, `/healthz`, and `/events`.
- `METRICS_ALLOW_CIDRS`: (Optional) Comma-separated IP/CIDR allow-list for metrics/health.
- `METRICS_TLS_CERT`: (Optional) TLS certificate path for health/metrics.
- `METRICS_TLS_KEY`: (Optional) TLS private key path for health/metrics.
- `METRICS_TLS_CLIENT_CA`: (Optional) Client CA bundle to require mTLS for health/metrics.

The SDK derives the queue DSN from the selected Django queue alias; namespaced environment/configuration must match it. Business DATABASE_URL is preserved for task execution.

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

## License

Apache License 2.0. See [LICENSE](LICENSE).

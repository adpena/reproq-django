# Reproq CLI Reference

This page documents `uv run python manage.py reproq <subcommand>`. Use `--help` for the full option list.

## Exit Codes
- `0`: Success.
- `1`: Runtime or configuration error (for example `CommandError`, install failure).
- `2`: Invalid CLI usage (argparse validation).

## init
Bootstrap a project by writing a config file, installing the worker, and running migrations.
TOML is the default and uses Python's standard library reader. Explicit
`--format yaml` requires PyYAML in the Django environment. Generated config
files omit the optional `worker.python_bin` so they remain portable.

```bash
uv run python manage.py reproq init --format toml
```

If you already have a reviewed local worker, set `REPROQ_WORKER_BIN` and use
`--skip-install`. See the [bootstrap guide](../README.md#4-bootstrap-recommended).

Useful flags: `--skip-install`, `--skip-migrate`, `--skip-worker-migrate`, `--force`, `--tag`.

## worker
Start the Go worker process.

Use `--python-bin` to select the executor explicitly. Otherwise, the SDK uses
`worker.python_bin` from the config file when set, or the interpreter selected
by `uv run`. See the [executor configuration contract](../README.md#executor-python-selection-sdk-01531--worker-v00145)
for the required worker capability and precedence.

```bash
uv run python manage.py reproq worker --concurrency 20 --queues default,high
```

Notes:
- `--config` loads a YAML/TOML config file.
- Empty queue DSNs derive from the selected Django queue database alias.
  Explicit config/environment targets must match that alias. `DATABASE_URL`
  remains the business connection inherited by Python tasks. `--dsn` is a
  standalone Go option, not a Django management option.
- Use `--database` to pick a Django database alias when you route queues to multiple databases.
- If `ALLOWED_TASK_MODULES` is unset, the worker auto-configures it from discovered tasks.

## beat
Start the periodic task scheduler. Run exactly one beat process per database.

```bash
uv run python manage.py reproq beat --interval 30s
```

One-shot mode (for crontab or low-memory environments):

```bash
uv run python manage.py reproq beat --once
```

## schedule
Cron-friendly alias for `beat --once`.

```bash
uv run python manage.py reproq schedule
```

## pg-cron
Sync Postgres-native schedules via `pg_cron` (optional).

```bash
uv run python manage.py reproq pg-cron --install
```

Remove schedules:
```bash
uv run python manage.py reproq pg-cron --remove
```

If you want deployments to succeed even when `pg_cron` is unavailable, use:
```bash
uv run python manage.py reproq pg-cron --install --if-supported
```

Use `--database` to target a specific database alias.

## install
Download (or build) the worker binary.

```bash
uv run python manage.py reproq install
```

Install, init, and upgrade default to the worker release tested with this SDK.
SDK 0.1.532 selects v0.0.145. An explicit `--tag` selects a different release
subject to the SDK's compatibility checks. See the [pinned installation
instructions](../README.md#5-download-the-compatible-worker) for platform
digests and source-build behavior.

Useful flags: `--tag`, `--sha256`, `--build`, `--source`.

## migrate-worker
Apply the committed Django migrations to the selected queue database. This
command does not run an independent schema initializer or repair tables.

```bash
uv run python manage.py reproq migrate-worker
```

## check
Validate the worker binary path, database connection, and schema health.

```bash
uv run python manage.py reproq check
```

## doctor
Validate DSN, schema, worker binary, and allowlist.

```bash
uv run python manage.py reproq doctor --strict
```

Example output:
```
🔎 Reproq Doctor
✅ Worker binary: 0.0.145
✅ Database connection ok.
✅ Reproq schema present.
✨ Doctor completed with no blocking issues.
```

## config
Show the effective config and explain precedence.

```bash
uv run python manage.py reproq config --mode all --explain
```

Example output (trimmed):
```
Config file: reproq.yaml (yaml)
Precedence: defaults < config file < env vars < CLI flags
{
  "worker": { ... },
  "beat": { ... }
}
```

## upgrade
Install the worker release tested with this SDK (or build from source) and
optionally apply the committed queue migrations. Pin a compatible tag and
platform digest for reproducible deployment.

```bash
uv run python manage.py reproq upgrade
```

Useful flags: `--source`, `--build`, `--skip-worker-migrate`.

## status / stats
Show task counts by status and active workers.

```bash
uv run python manage.py reproq status
```

Use `--database` to scope to one alias or `--all-databases` to aggregate.

Example output:
```
📊 Reproq Statistics
Tasks by Status:
  READY       : 12
  RUNNING     : 3
  SUCCESSFUL  : 104
  FAILED      : 1
Active Workers: 2
```

## logs
Fetch the logs for a task run using `logs_uri`.

```bash
uv run python manage.py reproq logs --id 1234 --tail 200
uv run python manage.py reproq logs --id 1234 --show-path
```

`--tail` controls how many lines to show and `--max-bytes` caps the read size.
Aliased IDs (`queues:123`) are supported when multiple databases are configured.

## cancel
Request cancellation for a task run. `READY` / `WAITING` work becomes `CANCELLED`
atomically. Cancellation of `RUNNING` work is cooperative and observed on a worker
heartbeat. Once cancellation is recorded, a later success acknowledgment is
rejected.
Repeated or terminal cancellation leaves history unchanged.

```bash
uv run python manage.py reproq cancel --id 1234
```

An exact ID selects the cancellation root; affected dependencies and workflow
callbacks can also be finalized. A completed command acknowledges the request,
not that running work has stopped. Inspect the current native task state to
confirm the outcome. If the command fails or its acknowledgment is lost, inspect
that state before another operator request; do not retry automatically. See the
worker's [current cancellation contract](https://github.com/adpena/reproq-worker#cancel).

Aliased IDs (`queues:123`) and `--database` select a Django database alias, not a
`TASKS` backend alias. If both are supplied, their database aliases must agree.

## allowlist
Compute the module allowlist from installed task modules.

```bash
uv run python manage.py reproq allowlist --show-tasks
uv run python manage.py reproq allowlist --write --config reproq.yaml
```

Use `--format plain` to print the comma-separated list without `ALLOWED_TASK_MODULES=`.

## reclaim
Requeue or fail tasks with expired leases.

```bash
uv run python manage.py reproq reclaim --older-than 5m --action requeue
```

Use `--database` or `--all-databases` for multi-DB setups.

## prune-workers
Delete workers not seen recently.

```bash
uv run python manage.py reproq prune-workers --older-than 10m
```

Use `--database` or `--all-databases` for multi-DB setups.

## prune
Delete terminal task history by status and age. Accepted statuses are SUCCESSFUL,
FAILED and CANCELLED. Active and unknown statuses and negative limits are rejected.
The former `prune-successful` command is retired; use `--statuses SUCCESSFUL`.

```bash
uv run python manage.py reproq prune --statuses SUCCESSFUL,FAILED --older-than 30d
```

Use `--database` or `--all-databases` for multi-DB setups. PostgreSQL cleanup locks
bounded batches and skips locked rows, preserving tasks concurrently retried or
claimed. Cleanup retains terminal records with active direct children or active
members of the same workflow. `--limit 0` processes all currently eligible history in batches of 1000;
`--dry-run` reports current eligibility, which may change before a later deletion.
The Django command delegates to the compatible Go worker's `prune history`
operation. It requires `operations.history_prune=1`; older binaries are rejected
before the request. One canonical queue runtime owns locking and deletion.

## sync-recurring
Sync code-defined recurring tasks into `periodic_tasks`.

```bash
uv run python manage.py reproq sync-recurring
```

## pause-queue / resume-queue
Pause or resume a queue (prevents new claims while paused).

```bash
uv run python manage.py reproq pause-queue maintenance
uv run python manage.py reproq resume-queue maintenance
```

## systemd
Generate worker and beat service files for systemd 251 or newer. Use `--schedule`
to emit a cron-style timer and service, and set `REPROQ_SCHEDULER_MODE=cron` in
your environment file to activate them. If provided, `--env-file` names one
required file. Relative paths are resolved against the project directory.

```bash
uv run python manage.py reproq systemd --user myuser --concurrency 20
# Or generate a schedule timer (default: every minute)
uv run python manage.py reproq systemd --schedule --schedule-on-calendar "*-*-* *:*:00"
```

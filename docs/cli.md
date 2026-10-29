# Reproq CLI Reference

This page documents `python manage.py reproq <subcommand>`. Use `--help` for the full option list.

## Exit Codes
- `0`: Success.
- `1`: Runtime or configuration error (for example `CommandError`, install failure).
- `2`: Invalid CLI usage (argparse validation).

## init
Bootstrap a project by writing a config file, installing the worker, and running migrations.
TOML is the default and uses Python's standard library reader. Explicit
`--format yaml` requires PyYAML in the Django environment.

```bash
python manage.py reproq init --format toml
```

Example output:
```
🚀 Bootstrapping Reproq...
✅ Config written to reproq.toml
✅ Reproq schema applied.
✨ Reproq is ready!
```

Useful flags: `--skip-install`, `--skip-migrate`, `--skip-worker-migrate`, `--force`, `--tag`.

## worker
Start the Go worker process.

```bash
python manage.py reproq worker --concurrency 20 --queues default,high
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
python manage.py reproq beat --interval 30s
```

One-shot mode (for crontab or low-memory environments):

```bash
python manage.py reproq beat --once
```

## schedule
Cron-friendly alias for `beat --once`.

```bash
python manage.py reproq schedule
```

## pg-cron
Sync Postgres-native schedules via `pg_cron` (optional).

```bash
python manage.py reproq pg-cron --install
```

Remove schedules:
```bash
python manage.py reproq pg-cron --remove
```

If you want deployments to succeed even when `pg_cron` is unavailable, use:
```bash
python manage.py reproq pg-cron --install --if-supported
```

Use `--database` to target a specific database alias.

## install
Download (or build) the worker binary.

```bash
python manage.py reproq install
```

Useful flags: `--tag`, `--build`, `--source`.

## migrate-worker
Apply the committed Django migrations to the selected queue database. This
command does not run an independent schema initializer or repair tables.

```bash
python manage.py reproq migrate-worker
```

## check
Validate the worker binary path, database connection, and schema health.

```bash
python manage.py reproq check
```

## doctor
Validate DSN, schema, worker binary, and allowlist.

```bash
python manage.py reproq doctor --strict
```

Example output:
```
🔎 Reproq Doctor
✅ Worker binary: 0.0.129
✅ Database connection ok.
✅ Reproq schema present.
✨ Doctor completed with no blocking issues.
```

## config
Show the effective config and explain precedence.

```bash
python manage.py reproq config --mode all --explain
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
Fetch the latest worker binary (or build from source) and optionally run `migrate-worker`.

```bash
python manage.py reproq upgrade --tag latest
```

Useful flags: `--source`, `--build`, `--skip-worker-migrate`.

## status / stats
Show task counts by status and active workers.

```bash
python manage.py reproq status
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
python manage.py reproq logs --id 1234 --tail 200
python manage.py reproq logs --id 1234 --show-path
```

`--tail` controls how many lines to show and `--max-bytes` caps the read size.
Aliased IDs (`queues:123`) are supported when multiple databases are configured.

## cancel
Request cancellation for a running task. The worker enforces cancellation on the next heartbeat.

```bash
python manage.py reproq cancel --id 1234
```

Aliased IDs (`queues:123`) are supported when multiple databases are configured.

## allowlist
Compute the module allowlist from installed task modules.

```bash
python manage.py reproq allowlist --show-tasks
python manage.py reproq allowlist --write --config reproq.yaml
```

Use `--format plain` to print the comma-separated list without `ALLOWED_TASK_MODULES=`.

## reclaim
Requeue or fail tasks with expired leases.

```bash
python manage.py reproq reclaim --older-than 5m --action requeue
```

Use `--database` or `--all-databases` for multi-DB setups.

## prune-workers
Delete workers not seen recently.

```bash
python manage.py reproq prune-workers --older-than 10m
```

Use `--database` or `--all-databases` for multi-DB setups.

## prune
Delete terminal task history by status and age. Accepted statuses are SUCCESSFUL,
FAILED and CANCELLED. Active and unknown statuses and negative limits are rejected.
The former `prune-successful` command is retired; use `--statuses SUCCESSFUL`.

```bash
python manage.py reproq prune --statuses SUCCESSFUL,FAILED --older-than 30d
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
python manage.py reproq sync-recurring
```

## pause-queue / resume-queue
Pause or resume a queue (prevents new claims while paused).

```bash
python manage.py reproq pause-queue maintenance
python manage.py reproq resume-queue maintenance
```

## systemd
Generate systemd service files for worker and beat. Use `--schedule` to emit a
cron-style schedule timer/service and set `REPROQ_SCHEDULER_MODE=cron` in your
environment file to activate it.

```bash
python manage.py reproq systemd --user myuser --concurrency 20
# Or generate a schedule timer (default: every minute)
python manage.py reproq systemd --schedule --schedule-on-calendar "*-*-* *:*:00"
```

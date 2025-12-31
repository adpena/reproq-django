# Reproq CLI Reference

This page documents `python manage.py reproq <subcommand>`. Use `--help` for the full option list.

## Exit Codes
- `0`: Success.
- `1`: Runtime or configuration error (for example `CommandError`, install failure).
- `2`: Invalid CLI usage (argparse validation).

## init
Bootstrap a project by writing a config file, installing the worker, and running migrations.

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
- `--dsn` overrides `DATABASE_URL` (and `DATABASE_URL` is optional if a config file is supplied).
- If `ALLOWED_TASK_MODULES` is unset, the worker auto-configures it from discovered tasks.

## beat
Start the periodic task scheduler. Run exactly one beat process per database.

```bash
python manage.py reproq beat --interval 30s
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

## install
Download (or build) the worker binary.

```bash
python manage.py reproq install
```

Useful flags: `--tag`, `--build`, `--source`.

## migrate-worker
Apply the worker schema helpers and indexes that Django migrations cannot express.

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

## cancel
Request cancellation for a running task. The worker enforces cancellation on the next heartbeat.

```bash
python manage.py reproq cancel --id 1234
```

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

## prune-workers
Delete workers not seen recently.

```bash
python manage.py reproq prune-workers --older-than 10m
```

## prune-successful
Delete successful task runs older than a cutoff.

```bash
python manage.py reproq prune-successful --older-than 30d
```

## systemd
Generate systemd service files for worker and beat.

```bash
python manage.py reproq systemd --user myuser --concurrency 20
```

## stress-test
Enqueue sample tasks for benchmarking throughput.

```bash
python manage.py reproq stress-test --count 500
```

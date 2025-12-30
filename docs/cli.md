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

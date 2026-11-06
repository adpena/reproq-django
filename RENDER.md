# Deploying ReproQ on Render

Use a locked Django application, a reviewed compatible Go worker release and
committed Django migrations. The SDK verifies the worker digest before executing
the downloaded binary, then checks schema and executor-protocol capabilities.
Keep the worker tag and platform digest in deployment configuration; upgrade
them together with the reviewed SDK lockfile.

## Build and schema

After setting `REPROQ_WORKER_TAG` and `REPROQ_WORKER_SHA256` to the reviewed
published release and platform digest:

```yaml
buildCommand: |
  uv sync --locked
  uv run python manage.py reproq install --tag "$REPROQ_WORKER_TAG" --sha256 "$REPROQ_WORKER_SHA256"
  uv run python manage.py collectstatic --noinput
preDeployCommand: uv run python manage.py migrate --noinput
```

For a separate queue database, also run
`uv run python manage.py reproq migrate-worker --database <alias>`. This delegates
to committed Django migrations; there is no separate Go SQL initializer.

A legacy queue needs an explicit cutover: stop old producers and workers, resolve
active tasks, inspect the catalog and migration history, and use the guarded
adoption procedure in the [README](README.md). Do not fake migrations or relabel
historical specifications as current.

## Process layout

Run the web process and a supervised worker, starting with one task slot:

```bash
uv run python manage.py reproq worker --concurrency 1
```

Separate services provide independent resource budgets and scaling. For a shared
web service, use the packaged [supervisor](src/reproq_django/resources/render_start.sh)
and [deployment guide](docs/deployment.md); do not launch unsupervised shell
background jobs. `REPROQ_CONCURRENCY` is a supervisor input that becomes a worker
CLI flag, rather than a native Go environment setting.

Run the supervisor inside the selected project environment, for example through
one outer UV invocation. Its default prestart, worker and beat commands reuse
the absolute interpreter in VIRTUAL_ENV, including through login shells. Without
an active environment it resolves the interpreter once with UV. A broken active
environment fails rather than silently selecting another one. Custom commands
are passed through unchanged; UV is unnecessary when every effective command is
custom. Empty/unset prestart and worker commands retain their defaults, while an
explicitly disabled beat command still omits beat. A custom web command should
use the selected interpreter's absolute path to run the Gunicorn module, rather
than nesting UV or depending on a login shell's PATH.

SDK 0.1.532 and later provide `REPROQ_WORKER_ENABLED` to explicitly control
the supervisor's worker process.
Unset means enabled; accepted values are `true`/`1` and `false`/`0`, with
case-insensitive words. Empty or unknown values fail before prestart or process
launch without logging the supplied value. When enabled, an empty or unset
`REPROQ_WORKER_CMD` still selects the default worker command. When disabled,
even a custom worker command is not run. Scheduler selection is independent.

For a web-only process, set `REPROQ_WORKER_ENABLED=false` and
`REPROQ_SCHEDULER_MODE=off`. After successful prestart, the supervisor executes
the web command directly, without worker or beat restart supervisors. These
settings do not stop actors in other services or disable database-native jobs.
The default prestart remains `manage.py reproq check`, including worker binary,
configuration and queue-schema checks; there is no `check --skip-binary` option.
A deployment that deliberately has no worker binary can supply its existing
custom-prestart contract, for example
`REPROQ_PRESTART_CMD='exec /absolute/venv/bin/python manage.py check'`.
That Django system check does not establish queue schema or worker readiness;
perform any additional application/release checks in the same reviewed command.
`REPROQ_PROCESS_ROLE` labels logs and does not enable or disable actors.
Supervisor status messages omit prestart-command and scheduler-mode values.
Custom prestart commands run with stdout and stderr discarded. Use the
supervisor's status messages to track their exit and retry behavior.

The Django worker passes the selected interpreter to Go's executor through
`--python-bin`. See the [executor configuration contract](README.md#executor-python-selection-sdk-01531--worker-v00145)
for explicit overrides and the capability that prestart and doctor verify.

Select the Django queue database with `--database` and configure queue routing
through `REPROQ_QUEUE_DATABASES`. The management command derives an empty queue
DSN from that alias and validates explicit file/environment targets against it.
`REPROQ_DATABASE_URL` belongs to the Go queue. Preserve the application's
`DATABASE_URL` for Django business data; tasks may use a different database.
Use `uv run python manage.py reproq config --explain` to inspect precedence and
`uv run python manage.py reproq doctor --strict` for preflight checks.

## Periodic tasks

Register definitions through `register_periodic` or `@recurring` plus an explicit
`reproq sync-recurring`. See [periodic tasks](README.md#-periodic-tasks) for source
options and provider assignment. Raw ORM schedule inserts and admin schedule
editing are not registration interfaces.

Choose a provider for each definition:

- Go: one supervised `reproq beat` process per queue database, or invoke
  `reproq schedule` externally for one-shot scheduling.
- PostgreSQL: where the database provides a compatible `pg_cron` extension,
  reconcile explicitly with `reproq pg-cron --install`. Its timezone must match
  the definition. This removes a persistent beat process, while execution still
  needs a worker.

The packaged supervisor uses `REPROQ_SCHEDULER_MODE` to select its beat process:
`beat` runs it, while `off` and `pg_cron` omit it. An unset mode keeps the existing
default beat process. An explicit disabled `REPROQ_BEAT_CMD` still disables beat;
a custom command runs only when the selected mode permits beat. Choosing
`pg_cron` does not register schedules or install native jobs automatically.

Migration note: `LOW_MEMORY_MODE` no longer overrides scheduler selection. If a
deployment relied on that flag to stop beat, set `REPROQ_SCHEDULER_MODE=off` or
select `pg_cron` after its explicit reconciliation. An explicit `beat` selection
now remains active even with `LOW_MEMORY_MODE=1`. The flag's existing optional
UI/proxy behavior is unchanged; it is not a scheduler provider setting.

`--if-supported` reports a skip when native scheduling is unavailable; it does
not start another scheduler. Review provider ownership before removing jobs or
switching providers. Enqueue deployment tasks only after compatible workers are
ready, with application idempotency keyed to the deployment.

## Capacity

Budget the combined web, Go worker, active Python executors, caches, buffers and
database connections. Measure representative peak memory and tail latency;
an idle worker's RSS does not establish task capacity. Start shared deployments
with one web worker and one task slot, then increase concurrency only after
measuring headroom. Move expensive ETL to a separate service when it exceeds
the shared budget. Configure Gunicorn recycling and task timeouts for the actual
workload rather than relying on a plan name or a generic memory claim.

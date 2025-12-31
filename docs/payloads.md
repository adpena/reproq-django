# Handling Large Payloads

Reproq uses JSON for task arguments and results. While convenient, there are best practices to follow when dealing with large amounts of data.

## 1. Pass IDs, Not Objects

Avoid passing large Django model instances or large dictionaries as task arguments. Instead, pass the primary key and fetch the object inside the task.

**Bad:**
```python
@task
def process_user(user_obj): # user_obj could be huge
    pass
```

**Good:**
```python
@task
def process_user(user_id):
    user = User.objects.get(pk=user_id)
    # process user
```

## 2. Result Size Limits

The Go worker captures `stdout` from the Python executor. To prevent memory exhaustion, there is a default limit (usually 1MB) on the captured output. 

If your task needs to return a large amount of data, consider:
- Writing the data to a file or cloud storage (S3) and returning the URL/path.
- Saving the data to a specialized Result model in your database.

If you set `REPROQ_LOGS_DIR` (or `--logs-dir`), the worker persists `stdout`/`stderr` to disk and stores the path in `task_runs.logs_uri`.

Example:
```python
from django.tasks import task

@task
def export_report(report_id):
    report_url = upload_report(report_id)
    return {"report_url": report_url}
```

## 3. Serialization

Reproq computes `spec_hash` using canonical JSON with Django's JSON encoder. It sorts keys to enforce determinism while safely encoding types like `Decimal` and `UUID` as strings. Prefer simple JSON-serializable inputs whenever possible.
Datetime values are encoded to ISO 8601 strings by Django's JSON encoder.

## 4. Executor Settings Resolution

The executor (`python -m reproq_django.executor`) resolves settings in this order:
- `--settings` CLI flag (manual runs only).
- `spec["django"]["settings_module"]` (if provided in the payload).
- `DJANGO_SETTINGS_MODULE` environment variable (typical production setup).

The Go worker does not pass `--settings`, so ensure `DJANGO_SETTINGS_MODULE` is set in the worker environment if your payload does not include a settings override.

## 5. Payload Transport Modes

The Go worker defaults to passing payloads over stdin. Avoid inline payload mode in production because it exposes payload data in process arguments. Production builds of the worker (`-tags prod`) reject `--payload-mode inline`.

Payload modes map to executor flags:
- `stdin` → `--payload-stdin`
- `file` → `--payload-file`
- `inline` → `--payload-json`

## 6. Stdout/Stderr Handling

The executor writes only the JSON result envelope to stdout. Task `print()` output is captured internally and suppressed, with a short notice sent to stderr. The worker captures stdout/stderr (up to its configured limits) and can persist them to `logs_uri` when `REPROQ_LOGS_DIR` is set.

## 7. Worker/Executor Contract

The Go worker invokes `python -m reproq_django.executor` and sends a canonical JSON spec via stdin (or a file). The executor returns a JSON result envelope on stdout.

### Spec Payload (input)
Common fields in the spec JSON:
- `v`: Integer schema version (currently `1`).
- `task_path`: Python import path for the task (for example `myapp.tasks.send_email`).
- `args`: Positional arguments array.
- `kwargs`: Keyword arguments object.
- `takes_context`: Boolean; when true, the executor passes a context dict as the first arg.
- `queue_name`: Queue name (string).
- `priority`: Integer priority.
- `lock_key`: Optional string for concurrency control.
- `run_after`: ISO 8601 timestamp or `null`.
- `exec.timeout_seconds`: Max execution seconds.
- `exec.max_attempts`: Max attempts before final failure.
- `provenance.code_ref`: Optional code version identifier.
- `provenance.pip_lock_hash`: Optional lockfile hash.
- `django.settings_module`: Optional override for `DJANGO_SETTINGS_MODULE`.

Example payload:
```json
{
  "v": 1,
  "task_path": "myapp.tasks.send_email",
  "args": [123],
  "kwargs": {"welcome": true},
  "takes_context": false,
  "queue_name": "default",
  "priority": 0,
  "lock_key": null,
  "run_after": null,
  "exec": {"timeout_seconds": 900, "max_attempts": 3},
  "provenance": {"code_ref": "v1.2.3", "pip_lock_hash": "sha256:..."},
  "django": {"settings_module": "myproject.settings"}
}
```

### Result Envelope (output)
Success:
```json
{"ok": true, "return": {"status": "sent"}}
```

Failure:
```json
{"ok": false, "exception_class": "ValueError", "message": "Bad input", "traceback": "..."}
```

Notes:
- `traceback` is included when available.
- The executor exits non-zero on failure; the worker translates this into a `FAILED` task run.

## 8. logs_uri and `reproq logs`

When `REPROQ_LOGS_DIR` (or `--logs-dir`) is set, the worker persists stdout/stderr to a file and stores the path in `task_runs.logs_uri`. The default file format is:

```
STDOUT:
<stdout text>
STDERR:
<stderr text>
```

The `logs_uri` value can be a local file path or an HTTP/HTTPS URL. Use:

```bash
python manage.py reproq logs --id 1234 --tail 200
python manage.py reproq logs --id 1234 --show-path
```

Use `--max-bytes` to cap the read size. If `logs_uri` is empty, no logs were captured or persisted.

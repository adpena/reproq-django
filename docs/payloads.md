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

## 3. Serialization

Reproq computes `spec_hash` using canonical JSON with Django's JSON encoder. It sorts keys to enforce determinism while safely encoding types like `Decimal` and `UUID` as strings. Prefer simple JSON-serializable inputs whenever possible.

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

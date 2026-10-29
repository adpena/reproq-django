# Deterministic Tasks & Deduplication

Reproq features a powerful deterministic execution model based on **Specification Hashing**.

## What is Spec Hashing?

When you enqueue a task, Reproq generates a SHA256 hash of the "specification":
- Task name (e.g., `myapp.tasks.send_email`)
- Arguments (`args`)
- Keyword arguments (`kwargs`)
- Queue name
- Priority
- Lock key (if provided)
- Concurrency key/limit (if provided)
- Run-after timestamp
- Execution settings (timeout, max attempts)
- Provenance metadata (when configured)

This hash is stored in the `spec_hash` column.

## Automatic Deduplication

In your `TASKS` settings, you can enable `DEDUP_ACTIVE`:

```python
TASKS = {
    "default": {
        "BACKEND": "reproq_django.backend.ReproqBackend",
        "OPTIONS": {
            "DEDUP_ACTIVE": True,
        }
    }
}
```

When `DEDUP_ACTIVE` is True, Reproq will check if a task with the **exact same specification** is already in a `READY` or `RUNNING` state. If it is, the new enqueue request will simply return the existing task instead of creating a duplicate.

Example:
```python
result_1 = send_welcome_email.enqueue(123)
result_2 = send_welcome_email.enqueue(123)

# With DEDUP_ACTIVE enabled, both results refer to the same task run.
assert result_1.id == result_2.id
```

### Benefits
- **Idempotency**: Prevents accidental double-clicks or double-triggers from spawning redundant work.
- **Resource Efficiency**: Saves database space and worker cycles.
- **Simplified Logic**: You don't need to manually check if a job is already running before enqueuing it.

## Forcing a Run

If you want to bypass deduplication for a specific call, change a non-functional argument (like a timestamp) or disable dedup for that backend.

Example with a deploy identifier:
```python
import uuid

deploy_id = str(uuid.uuid4())
notify_deploy_success.enqueue(deploy_id=deploy_id)
```

Scheduling and concurrency options also participate in the spec hash. Use
Django's task options and ReproQ's concurrency decorator:
```python
from datetime import timedelta
from django.tasks import task
from django.utils import timezone
from reproq_django.concurrency import limits_concurrency

send_welcome_email.using(
    run_after=timezone.now() + timedelta(minutes=5),
).enqueue(123)

@limits_concurrency(lambda user_id: f"user:{user_id}", to=2)
@task
def refresh_user(user_id):
    return update_user(user_id)

refresh_user.enqueue(123)
```

## Inspecting a Spec Hash
You can look up the canonical hash for a task run in the database:

```python
from reproq_django.models import TaskRun

task_run = TaskRun.objects.get(result_id=result_1.id)
print(task_run.spec_hash)
```

ReproQ stores versioned canonical JSON with sorted keys and its typed argument
codec, then hashes the exact UTF-8 bytes. The codec preserves values such as
`Decimal`, `UUID`, datetimes and source-bound saved model references. Literal
marker dictionaries remain ordinary user data.

### Note on `lock_key`
`lock_key` is included in the canonical spec hash. Tasks that differ only by `lock_key` will **not** deduplicate when `DEDUP_ACTIVE` is enabled.

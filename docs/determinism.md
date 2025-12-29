# Deterministic Tasks & Deduplication

Reproq features a powerful deterministic execution model based on **Specification Hashing**.

## What is Spec Hashing?

When you enqueue a task, Reproq generates a SHA256 hash of the "specification":
- Task name (e.g., `myapp.tasks.send_email`)
- Arguments (`args`)
- Keyword arguments (`kwargs`)
- Queue name
- Lock key (if any)

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

### Benefits
- **Idempotency**: Prevents accidental double-clicks or double-triggers from spawning redundant work.
- **Resource Efficiency**: Saves database space and worker cycles.
- **Simplified Logic**: You don't need to manually check if a job is already running before enqueuing it.

## Forcing a Run

If you want to bypass deduplication for a specific call, you can change a non-functional argument (like a timestamp) or use a unique `lock_key`.

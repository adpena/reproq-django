from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, timedelta
from typing import Any

from django.db import connection, transaction
from django.utils import timezone
from django.tasks.backends.base import BaseTaskBackend
from django.tasks.exceptions import TaskResultDoesNotExist

def _canonical_json(obj: Any) -> str:
    # Stable serialization to compute spec_hash.
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)

def _make_result_id() -> str:
    # Must be < 64 chars (Django Tasks constraint).
    return uuid.uuid4().hex  # 32 chars

def _normalize_run_after(run_after: Any) -> datetime | None:
    if run_after is None:
        return None
    if isinstance(run_after, timedelta):
        return timezone.now() + run_after
    if isinstance(run_after, datetime):
        if run_after.tzinfo is None:
            raise ValueError("run_after must be timezone-aware")
        return run_after
    raise TypeError("run_after must be datetime, timedelta, or None")

class ReproqBackend(BaseTaskBackend):
    """
    Django 6.0 Tasks backend that stores enqueued tasks + results in Postgres table `task_runs`.
    A separate Go worker service claims and executes runs.
    """

    supports_defer = True
    supports_priority = True
    supports_get_result = True
    supports_async_task = False  # set True only if you explicitly support coroutine tasks

    def enqueue(self, task, args, kwargs):
        # Django validates JSON round-trip in Task.enqueue(); still validate task shape.
        self.validate_task(task)

        run_after_dt = _normalize_run_after(task.run_after)

        spec = {
            "v": 1,
            "task_path": f"{task.func.__module__}.{task.func.__name__}",
            "args": args,
            "kwargs": kwargs,
            "queue_name": task.queue_name,
            "priority": task.priority,
            "run_after": run_after_dt.isoformat() if run_after_dt else None,
            "django": {
                # Optional: include settings module or project name
                "settings_module": None,
            },
            "exec": {
                # Go worker decides how to run; keep placeholders for determinism.
                "mode": "python_module",
                "entrypoint": "myproject.task_executor",
                "timeout_seconds": int(self.options.get("TIMEOUT_SECONDS", 900)),
                "max_attempts": int(self.options.get("MAX_ATTEMPTS", 5)),
            },
            "provenance": {
                "code_ref": self.options.get("CODE_REF"),
                "pip_lock_hash": self.options.get("PIP_LOCK_HASH"),
            },
        }

        spec_str = _canonical_json(spec)
        spec_hash = hashlib.sha256(spec_str.encode("utf-8")).hexdigest()
        result_id = _make_result_id()

        with transaction.atomic():
            with connection.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO task_runs (
                      result_id, backend_alias, queue_name, priority, run_after,
                      spec_json, spec_hash, status, enqueued_at, errors_json
                    )
                    VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s, 'READY', now(), '[]'::jsonb)
                    """,
                    [result_id, task.backend, task.queue_name, task.priority, run_after_dt, spec_str, spec_hash],
                )

        return task.get_result(result_id)

    def get_result(self, result_id: str):
        with connection.cursor() as cur:
            cur.execute(
                """
                SELECT status, enqueued_at, started_at, last_attempted_at, finished_at,
                       backend_alias, errors_json, return_json, attempts, worker_ids
                FROM task_runs
                WHERE result_id = %s
                """,
                [result_id],
            )
            row = cur.fetchone()

        if not row:
            raise TaskResultDoesNotExist(result_id)

        (status, enq, started, last, finished,
         backend_alias, errors_json, return_json, attempts, worker_ids) = row

        # Django’s concrete TaskResult class may vary; this approach works if TaskResult
        # can be instantiated. If not, we’ll replace with a small proxy later.
        from django.tasks.results import TaskResult

        return TaskResult(
            id=result_id,
            status=status,
            enqueued_at=enq,
            started_at=started,
            last_attempted_at=last,
            finished_at=finished,
            backend=backend_alias,
            errors=errors_json,
            _return_value=return_json,
            _attempts=attempts,
            _worker_ids=worker_ids,
        )

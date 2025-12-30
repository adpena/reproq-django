from __future__ import annotations
import hashlib
import json
import uuid
from datetime import datetime, timedelta
from typing import Any

from django.db import connection, transaction, IntegrityError
from django.tasks.backends.base import BaseTaskBackend
from django.utils import timezone
from asgiref.sync import sync_to_async

from .models import TaskRun
from .proxy import TaskResultProxy

def _canonical_json(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)

def _normalize_run_after(run_after: Any) -> datetime | None:
    if run_after is None:
        return None
    if isinstance(run_after, timedelta):
        return timezone.now() + run_after
    if isinstance(run_after, datetime):
        if timezone.is_naive(run_after):
            raise ValueError("run_after must be timezone-aware")
        return run_after
    raise TypeError("run_after must be datetime, timedelta, or None")

class ReproqBackend(BaseTaskBackend):
    supports_defer = True
    supports_priority = True
    supports_get_result = True
    supports_async_task = True

    def enqueue(self, task, args, kwargs) -> TaskResultProxy:
        self.validate_task(task)
        run_after_dt = _normalize_run_after(kwargs.pop("run_after", task.run_after))

        spec = {
            "v": 1,
            "task_path": task.module_path,
            "args": args,
            "kwargs": kwargs,
            "takes_context": getattr(task, "takes_context", False),
            "queue_name": task.queue_name or "default",
            "priority": task.priority or 0,
            "run_after": run_after_dt.isoformat() if run_after_dt else None,
            "exec": {
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
        dedup = self.options.get("DEDUP_ACTIVE", True)
        
        # Lock Key
        lock_key = kwargs.pop("lock_key", getattr(task, "lock_key", None))

        # TTL / Expiry
        expires_in = self.options.get("EXPIRES_IN")
        expires_at = None
        if expires_in:
            expires_at = timezone.now() + expires_in

        def _try_insert():
            run = TaskRun.objects.create(
                backend_alias=self.alias,
                queue_name=spec["queue_name"],
                priority=spec["priority"],
                run_after=run_after_dt,
                spec_json=spec,
                spec_hash=spec_hash,
                status="READY",
                errors_json=[],
                max_attempts=spec["exec"]["max_attempts"],
                timeout_seconds=spec["exec"]["timeout_seconds"],
                expires_at=expires_at,
                lock_key=lock_key
            )
            return run.result_id

        if not dedup:
            return self.get_result(_try_insert())

        existing = TaskRun.objects.filter(
            spec_hash=spec_hash, status__in=["READY", "RUNNING"]
        ).first()
        if existing:
            return self.get_result(existing.result_id)

        try:
            return self.get_result(_try_insert())
        except IntegrityError:
            # Conflict on spec_hash for active status (due to unique index in Go migrations)
            # We try to find the existing one.
            row = TaskRun.objects.filter(spec_hash=spec_hash, status__in=["READY", "RUNNING"]).first()
            if row:
                return self.get_result(row.result_id)
            
            # If no row found, maybe it just finished. Try inserting again.
            try:
                return self.get_result(_try_insert())
            except IntegrityError:
                raise RuntimeError(f"Failed to enqueue or find duplicate for spec_hash {spec_hash}")

    def get_result(self, result_id: int | str) -> TaskResultProxy:
        return TaskResultProxy(str(result_id), self)

    def bulk_enqueue(self, tasks_data: list[tuple[Task, tuple, dict]]) -> list[TaskResultProxy]:
        """
        Enqueue multiple tasks in a single database transaction and query.
        """
        runs = []
        for task, args, kwargs in tasks_data:
            run_after_dt = _normalize_run_after(kwargs.pop("run_after", task.run_after))
            lock_key = kwargs.pop("lock_key", getattr(task, "lock_key", None))
            
            spec = {
                "v": 1,
                "task_path": task.module_path,
                "args": args,
                "kwargs": kwargs,
                "takes_context": getattr(task, "takes_context", False),
                "queue_name": task.queue_name or "default",
                "priority": task.priority or 0,
                "run_after": run_after_dt.isoformat() if run_after_dt else None,
                "exec": {
                    "timeout_seconds": int(self.options.get("TIMEOUT_SECONDS", 900)),
                    "max_attempts": int(self.options.get("MAX_ATTEMPTS", 5)),
                },
            }
            spec_str = _canonical_json(spec)
            spec_hash = hashlib.sha256(spec_str.encode("utf-8")).hexdigest()

            runs.append(TaskRun(
                backend_alias=self.alias,
                queue_name=spec["queue_name"],
                priority=spec["priority"],
                run_after=run_after_dt,
                spec_json=spec,
                spec_hash=spec_hash,
                status="READY",
                max_attempts=spec["exec"]["max_attempts"],
                timeout_seconds=spec["exec"]["timeout_seconds"],
                lock_key=lock_key
            ))

        created = TaskRun.objects.bulk_create(runs)
        return [TaskResultProxy(str(run.result_id), self) for run in created]

    async def aenqueue(self, task, args, kwargs) -> TaskResultProxy:
        return await sync_to_async(self.enqueue, thread_sensitive=True)(
            task, args, kwargs
        )

    async def aget_result(self, result_id: str) -> TaskResultProxy:
        return TaskResultProxy(result_id, self)

from __future__ import annotations
from datetime import datetime, timedelta
from typing import Any

from django.tasks import Task
from django.db import IntegrityError
from django.tasks.backends.base import BaseTaskBackend
from django.utils import timezone
from asgiref.sync import sync_to_async

from .models import TaskRun
from .proxy import TaskResultProxy
from .serialization import normalize_and_hash

def _chunked(values: list[str], size: int = 1000):
    for idx in range(0, len(values), size):
        yield values[idx : idx + size]

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

        # Lock Key
        lock_key = kwargs.pop("lock_key", getattr(task, "lock_key", None))
        priority = kwargs.pop("priority", task.priority)
        if priority is None:
            priority = 0

        spec = {
            "v": 1,
            "task_path": task.module_path,
            "args": args,
            "kwargs": kwargs,
            "takes_context": getattr(task, "takes_context", False),
            "queue_name": task.queue_name or "default",
            "priority": priority,
            "lock_key": lock_key,
            "run_after": run_after_dt.isoformat() if run_after_dt else None,
            "exec": {
                "timeout_seconds": int(self.options.get("TIMEOUT_SECONDS", 900)),
                "max_attempts": int(self.options.get("MAX_ATTEMPTS", 3)),
            },
            "provenance": {
                "code_ref": self.options.get("CODE_REF"),
                "pip_lock_hash": self.options.get("PIP_LOCK_HASH"),
            },
        }

        spec_normalized, spec_hash = normalize_and_hash(spec)
        dedup = self.options.get("DEDUP_ACTIVE", True)

        # TTL / Expiry
        expires_in = self.options.get("EXPIRES_IN")
        expires_at = None
        if expires_in:
            expires_at = timezone.now() + expires_in

        def _try_insert():
            run = TaskRun.objects.create(
                backend_alias=self.alias,
                queue_name=spec["queue_name"],
                priority=priority,
                run_after=run_after_dt,
                spec_json=spec_normalized,
                task_path=spec_normalized.get("task_path"),
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
        if not tasks_data:
            return []

        dedup = self.options.get("DEDUP_ACTIVE", True)
        expires_in = self.options.get("EXPIRES_IN")
        expires_at = None
        if expires_in:
            expires_at = timezone.now() + expires_in
        entries = []
        spec_hashes = set()

        for task, args, kwargs in tasks_data:
            self.validate_task(task)
            safe_kwargs = dict(kwargs)
            run_after_dt = _normalize_run_after(
                safe_kwargs.pop("run_after", task.run_after)
            )
            lock_key = safe_kwargs.pop("lock_key", getattr(task, "lock_key", None))
            priority = safe_kwargs.pop("priority", task.priority)
            if priority is None:
                priority = 0

            spec = {
                "v": 1,
                "task_path": task.module_path,
                "args": args,
                "kwargs": safe_kwargs,
                "takes_context": getattr(task, "takes_context", False),
                "queue_name": task.queue_name or "default",
                "priority": priority,
                "lock_key": lock_key,
                "run_after": run_after_dt.isoformat() if run_after_dt else None,
                "exec": {
                    "timeout_seconds": int(self.options.get("TIMEOUT_SECONDS", 900)),
                    "max_attempts": int(self.options.get("MAX_ATTEMPTS", 3)),
                },
                "provenance": {
                    "code_ref": self.options.get("CODE_REF"),
                    "pip_lock_hash": self.options.get("PIP_LOCK_HASH"),
                },
            }

            spec_normalized, spec_hash = normalize_and_hash(spec)
            entries.append(
                {
                    "spec": spec_normalized,
                    "spec_hash": spec_hash,
                    "run_after_dt": run_after_dt,
                    "lock_key": lock_key,
                    "priority": priority,
                    "expires_at": expires_at,
                }
            )
            spec_hashes.add(spec_hash)

        if not dedup:
            runs = [
                TaskRun(
                    backend_alias=self.alias,
                    queue_name=entry["spec"]["queue_name"],
                    priority=entry["priority"],
                    run_after=entry["run_after_dt"],
                    spec_json=entry["spec"],
                    task_path=entry["spec"].get("task_path"),
                    spec_hash=entry["spec_hash"],
                    status="READY",
                    max_attempts=entry["spec"]["exec"]["max_attempts"],
                    timeout_seconds=entry["spec"]["exec"]["timeout_seconds"],
                    lock_key=entry["lock_key"],
                    expires_at=entry["expires_at"],
                )
                for entry in entries
            ]
            created = TaskRun.objects.bulk_create(runs, batch_size=1000)
            return [TaskResultProxy(str(run.result_id), self) for run in created]

        result_ids: dict[str, int] = {}
        spec_hash_list = list(spec_hashes)
        for chunk in _chunked(spec_hash_list):
            existing = TaskRun.objects.filter(
                spec_hash__in=chunk, status__in=["READY", "RUNNING"]
            ).values_list("spec_hash", "result_id")
            for spec_hash, result_id in existing:
                result_ids[spec_hash] = result_id

        runs = []
        seen = set(result_ids.keys())
        for entry in entries:
            spec_hash = entry["spec_hash"]
            if spec_hash in seen:
                continue
            runs.append(
                TaskRun(
                    backend_alias=self.alias,
                    queue_name=entry["spec"]["queue_name"],
                    priority=entry["priority"],
                    run_after=entry["run_after_dt"],
                    spec_json=entry["spec"],
                    task_path=entry["spec"].get("task_path"),
                    spec_hash=spec_hash,
                    status="READY",
                    max_attempts=entry["spec"]["exec"]["max_attempts"],
                    timeout_seconds=entry["spec"]["exec"]["timeout_seconds"],
                    lock_key=entry["lock_key"],
                    expires_at=entry["expires_at"],
                )
            )
            seen.add(spec_hash)

        if runs:
            TaskRun.objects.bulk_create(runs, ignore_conflicts=True, batch_size=1000)
            run_hashes = [run.spec_hash for run in runs]
            for chunk in _chunked(run_hashes):
                created = TaskRun.objects.filter(
                    spec_hash__in=chunk,
                    status__in=["READY", "RUNNING"],
                ).values_list("spec_hash", "result_id")
                for spec_hash, result_id in created:
                    result_ids[spec_hash] = result_id

        results = []
        for entry in entries:
            spec_hash = entry["spec_hash"]
            result_id = result_ids.get(spec_hash)
            if result_id is None:
                run = TaskRun.objects.create(
                    backend_alias=self.alias,
                    queue_name=entry["spec"]["queue_name"],
                    priority=entry["priority"],
                    run_after=entry["run_after_dt"],
                    spec_json=entry["spec"],
                    task_path=entry["spec"].get("task_path"),
                    spec_hash=spec_hash,
                    status="READY",
                    max_attempts=entry["spec"]["exec"]["max_attempts"],
                    timeout_seconds=entry["spec"]["exec"]["timeout_seconds"],
                    lock_key=entry["lock_key"],
                    expires_at=entry["expires_at"],
                )
                result_id = run.result_id
                result_ids[spec_hash] = result_id
            results.append(TaskResultProxy(str(result_id), self))

        return results

    async def aenqueue(self, task, args, kwargs) -> TaskResultProxy:
        return await sync_to_async(self.enqueue, thread_sensitive=True)(
            task, args, kwargs
        )

    async def aget_result(self, result_id: str) -> TaskResultProxy:
        return TaskResultProxy(result_id, self)

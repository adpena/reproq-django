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
from .serialization import encode_args_kwargs, normalize_and_hash, normalize_json
from .db import format_result_id, parse_result_id, resolve_queue_db
from .signals import task_enqueued

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

    def _allowed_queues(self) -> list[str] | None:
        queues = (getattr(self, "params", None) or {}).get("QUEUES")
        if queues is None:
            return None
        if isinstance(queues, (list, tuple)):
            return [str(q) for q in queues if str(q)]
        return None

    def _validate_queue(self, queue_name: str):
        allowed = self._allowed_queues()
        if allowed is None or len(allowed) == 0:
            return
        if queue_name not in allowed:
            try:
                from django.tasks.exceptions import InvalidTaskError
            except Exception:
                raise ValueError(f"Unknown queue {queue_name!r}")
            raise InvalidTaskError(f"Unknown queue {queue_name!r}")

    def _resolve_concurrency(self, task, args, kwargs):
        raw_key = kwargs.pop("concurrency_key", getattr(task, "concurrency_key", None))
        limit = kwargs.pop("concurrency_limit", getattr(task, "concurrency_limit", None))
        key = raw_key
        if callable(raw_key):
            key = raw_key(*args, **kwargs)
        if isinstance(key, str) and not key.strip():
            key = None
        try:
            limit_val = int(limit) if limit is not None else 0
        except Exception:
            limit_val = 0
        if limit_val < 0:
            limit_val = 0
        return key, limit_val

    def _result_proxy(self, result_id: int | str, db_alias: str | None = None) -> TaskResultProxy:
        if db_alias is None:
            db_alias, raw_id = parse_result_id(result_id)
        else:
            raw_id = str(result_id)
        return TaskResultProxy(
            format_result_id(raw_id, db_alias),
            self,
            db_alias=db_alias,
            raw_id=raw_id,
        )

    def enqueue(self, task, args, kwargs) -> TaskResultProxy:
        self.validate_task(task)
        run_after_dt = _normalize_run_after(kwargs.pop("run_after", task.run_after))

        # Lock Key
        lock_key = kwargs.pop("lock_key", getattr(task, "lock_key", None))
        priority = kwargs.pop("priority", task.priority)
        if priority is None:
            priority = 0
        queue_name = task.queue_name or "default"
        self._validate_queue(queue_name)
        concurrency_key, concurrency_limit = self._resolve_concurrency(task, args, kwargs)
        encoded_args, encoded_kwargs = encode_args_kwargs(args, kwargs)

        spec = {
            "v": 1,
            "task_path": task.module_path,
            "args": encoded_args,
            "kwargs": encoded_kwargs,
            "takes_context": getattr(task, "takes_context", False),
            "queue_name": queue_name,
            "priority": priority,
            "lock_key": lock_key,
            "concurrency_key": concurrency_key,
            "concurrency_limit": concurrency_limit,
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
        db_alias = resolve_queue_db(queue_name)

        # TTL / Expiry
        expires_in = self.options.get("EXPIRES_IN")
        expires_at = None
        if expires_in:
            expires_at = timezone.now() + expires_in

        def _try_insert():
            run = TaskRun.objects.using(db_alias).create(
                backend_alias=self.alias,
                queue_name=queue_name,
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
                lock_key=lock_key,
                concurrency_key=concurrency_key,
                concurrency_limit=concurrency_limit,
            )
            return run.result_id

        if not dedup:
            result = self._result_proxy(_try_insert(), db_alias=db_alias)
            task_enqueued.send(sender=self.__class__, task_result=result, spec=spec_normalized)
            return result

        existing = TaskRun.objects.using(db_alias).filter(
            spec_hash=spec_hash, status__in=["READY", "RUNNING"]
        ).first()
        if existing:
            result = self._result_proxy(existing.result_id, db_alias=db_alias)
            task_enqueued.send(sender=self.__class__, task_result=result, spec=spec_normalized)
            return result

        try:
            result = self._result_proxy(_try_insert(), db_alias=db_alias)
            task_enqueued.send(sender=self.__class__, task_result=result, spec=spec_normalized)
            return result
        except IntegrityError:
            # Conflict on spec_hash for active status (due to unique index in Go migrations)
            # We try to find the existing one.
            row = TaskRun.objects.using(db_alias).filter(
                spec_hash=spec_hash, status__in=["READY", "RUNNING"]
            ).first()
            if row:
                result = self._result_proxy(row.result_id, db_alias=db_alias)
                task_enqueued.send(sender=self.__class__, task_result=result, spec=spec_normalized)
                return result
            
            # If no row found, maybe it just finished. Try inserting again.
            try:
                result = self._result_proxy(_try_insert(), db_alias=db_alias)
                task_enqueued.send(sender=self.__class__, task_result=result, spec=spec_normalized)
                return result
            except IntegrityError:
                raise RuntimeError(f"Failed to enqueue or find duplicate for spec_hash {spec_hash}")

    def get_result(self, result_id: int | str) -> TaskResultProxy:
        return self._result_proxy(result_id)

    def save_metadata(self, result_id: int | str, metadata: dict[str, Any]) -> None:
        db_alias, raw_id = parse_result_id(result_id)
        payload = normalize_json(metadata or {})
        TaskRun.objects.using(db_alias).filter(result_id=raw_id).update(
            metadata_json=payload,
            updated_at=timezone.now(),
        )

    async def asave_metadata(self, result_id: int | str, metadata: dict[str, Any]) -> None:
        await sync_to_async(self.save_metadata, thread_sensitive=True)(result_id, metadata)

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

        grouped: dict[str, list[dict[str, Any]]] = {}
        ordered: list[dict[str, Any]] = []

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
            queue_name = task.queue_name or "default"
            self._validate_queue(queue_name)
            concurrency_key, concurrency_limit = self._resolve_concurrency(task, args, safe_kwargs)
            encoded_args, encoded_kwargs = encode_args_kwargs(args, safe_kwargs)

            spec = {
                "v": 1,
                "task_path": task.module_path,
                "args": encoded_args,
                "kwargs": encoded_kwargs,
                "takes_context": getattr(task, "takes_context", False),
                "queue_name": queue_name,
                "priority": priority,
                "lock_key": lock_key,
                "concurrency_key": concurrency_key,
                "concurrency_limit": concurrency_limit,
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
            db_alias = resolve_queue_db(queue_name)
            entry = {
                "spec": spec_normalized,
                "spec_hash": spec_hash,
                "run_after_dt": run_after_dt,
                "lock_key": lock_key,
                "concurrency_key": concurrency_key,
                "concurrency_limit": concurrency_limit,
                "priority": priority,
                "expires_at": expires_at,
                "db_alias": db_alias,
            }
            ordered.append(entry)
            grouped.setdefault(db_alias, []).append(entry)

        results_by_entry: dict[int, TaskResultProxy] = {}
        for db_alias, entries in grouped.items():
            result_ids: dict[str, int] = {}
            spec_hashes = {entry["spec_hash"] for entry in entries}
            if dedup and spec_hashes:
                for chunk in _chunked(list(spec_hashes)):
                    existing = TaskRun.objects.using(db_alias).filter(
                        spec_hash__in=chunk, status__in=["READY", "RUNNING"]
                    ).values_list("spec_hash", "result_id")
                    for spec_hash, result_id in existing:
                        result_ids[spec_hash] = result_id

            runs = []
            seen = set(result_ids.keys())
            for entry in entries:
                spec_hash = entry["spec_hash"]
                if dedup and spec_hash in seen:
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
                        concurrency_key=entry["concurrency_key"],
                        concurrency_limit=entry["concurrency_limit"],
                        expires_at=entry["expires_at"],
                    )
                )
                seen.add(spec_hash)

            if runs:
                TaskRun.objects.using(db_alias).bulk_create(
                    runs, ignore_conflicts=True, batch_size=1000
                )
                run_hashes = [run.spec_hash for run in runs]
                for chunk in _chunked(run_hashes):
                    created = TaskRun.objects.using(db_alias).filter(
                        spec_hash__in=chunk,
                        status__in=["READY", "RUNNING"],
                    ).values_list("spec_hash", "result_id")
                    for spec_hash, result_id in created:
                        result_ids[spec_hash] = result_id

            for entry in entries:
                spec_hash = entry["spec_hash"]
                result_id = result_ids.get(spec_hash)
                if result_id is None:
                    run = TaskRun.objects.using(db_alias).create(
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
                        concurrency_key=entry["concurrency_key"],
                        concurrency_limit=entry["concurrency_limit"],
                        expires_at=entry["expires_at"],
                    )
                    result_id = run.result_id
                proxy = self._result_proxy(result_id, db_alias=db_alias)
                task_enqueued.send(sender=self.__class__, task_result=proxy, spec=entry["spec"])
                results_by_entry[id(entry)] = proxy

        return [results_by_entry[id(entry)] for entry in ordered]

    async def aenqueue(self, task, args, kwargs) -> TaskResultProxy:
        return await sync_to_async(self.enqueue, thread_sensitive=True)(
            task, args, kwargs
        )

    async def aget_result(self, result_id: str) -> TaskResultProxy:
        return self._result_proxy(result_id)

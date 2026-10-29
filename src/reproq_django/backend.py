from __future__ import annotations
from datetime import datetime, timedelta
from typing import Any

from django.tasks import Task
from django.db import IntegrityError, connections, transaction
from django.tasks.backends.base import BaseTaskBackend
from django.utils import timezone
from asgiref.sync import sync_to_async

from .models import TaskRun
from .proxy import TaskResultProxy
from .specs import build_spec, spec_fields
from .db import format_result_id, parse_result_id, resolve_queue_db, validate_result_id_routing
from .signals import task_enqueued
from .schema_contract import require_schema_contract
from .concurrency import task_option

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

    def _notify_enqueued(self, result, spec, alias):
        transaction.on_commit(
            lambda: task_enqueued.send(sender=self.__class__, task_result=result, spec=spec),
            using=alias,
        )

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
        raw_key = task_option(task, "concurrency_key")
        limit = task_option(task, "concurrency_limit")
        key = raw_key
        if callable(raw_key):
            key = raw_key(*args, **kwargs)
        return key, limit if limit is not None else 0

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

    def _prepare(self, task, args, kwargs):
        self.validate_task(task)
        kwargs = dict(kwargs)
        run_after_dt = _normalize_run_after(task.run_after)
        lock_key = task_option(task, "lock_key")
        priority = task.priority
        if priority is None:
            priority = 0
        queue_name = task.queue_name or "default"
        self._validate_queue(queue_name)
        concurrency_key, concurrency_limit = self._resolve_concurrency(task, args, kwargs)
        spec = build_spec(
            task_path=task.module_path, args=args, kwargs=kwargs, backend_alias=self.alias,
            takes_context=getattr(task, "takes_context", False),
            queue_name=queue_name, priority=priority, lock_key=lock_key,
            concurrency_key=concurrency_key, concurrency_limit=concurrency_limit,
            run_after=run_after_dt,
            timeout_seconds=self.options.get("TIMEOUT_SECONDS", 900),
            max_attempts=self.options.get("MAX_ATTEMPTS", 3),
            code_ref=self.options.get("CODE_REF"), pip_lock_hash=self.options.get("PIP_LOCK_HASH"),
        )
        expires_in = self.options.get("EXPIRES_IN")
        return {
            **spec_fields(spec),
            "backend_alias": self.alias, "queue_name": queue_name,
            "priority": priority, "run_after": run_after_dt,
            "task_path": spec.payload["task_path"], "status": "READY", "errors_json": [],
            "max_attempts": spec.payload["exec"]["max_attempts"],
            "timeout_seconds": spec.payload["exec"]["timeout_seconds"],
            "expires_at": timezone.now() + expires_in if expires_in else None,
            "lock_key": lock_key, "concurrency_key": concurrency_key,
            "concurrency_limit": concurrency_limit,
            "dedupe_active": bool(self.options.get("DEDUP_ACTIVE", True)),
        }

    def enqueue(self, task, args, kwargs) -> TaskResultProxy:
        entry = self._prepare(task, args, kwargs)
        spec_normalized, spec_hash = entry["spec_json"], entry["spec_hash"]
        dedup = self.options.get("DEDUP_ACTIVE", True)
        db_alias = resolve_queue_db(entry["queue_name"])
        validate_result_id_routing(db_alias)
        require_schema_contract(connections[db_alias])

        def _try_insert():
            return self._insert_prepared(entry, db_alias)

        if not dedup:
            result = self._result_proxy(_try_insert(), db_alias=db_alias)
            self._notify_enqueued(result, spec_normalized, db_alias)
            return result

        existing = TaskRun.objects.using(db_alias).filter(
            spec_hash=spec_hash, status__in=["READY", "RUNNING"],
            dedupe_active=True, periodic_name__isnull=True,
        ).first()
        if existing:
            result = self._result_proxy(existing.result_id, db_alias=db_alias)
            self._notify_enqueued(result, spec_normalized, db_alias)
            return result

        try:
            result = self._result_proxy(_try_insert(), db_alias=db_alias)
            self._notify_enqueued(result, spec_normalized, db_alias)
            return result
        except IntegrityError:
            # The migration-owned active-spec constraint arbitrates enqueue races.
            # We try to find the existing one.
            row = TaskRun.objects.using(db_alias).filter(
                spec_hash=spec_hash, status__in=["READY", "RUNNING"],
                dedupe_active=True, periodic_name__isnull=True,
            ).first()
            if row:
                result = self._result_proxy(row.result_id, db_alias=db_alias)
                self._notify_enqueued(result, spec_normalized, db_alias)
                return result
            
            # If no row found, maybe it just finished. Try inserting again.
            try:
                result = self._result_proxy(_try_insert(), db_alias=db_alias)
                self._notify_enqueued(result, spec_normalized, db_alias)
                return result
            except IntegrityError:
                raise RuntimeError(f"Failed to enqueue or find duplicate for spec_hash {spec_hash}")

    def get_result(self, result_id: int | str) -> TaskResultProxy:
        result = self._result_proxy(result_id)
        result.refresh()
        return result

    def bulk_enqueue(self, tasks_data: list[tuple[Task, tuple, dict]]) -> list[TaskResultProxy]:
        if not tasks_data:
            return []
        grouped = {}
        ordered = []
        for task, args, kwargs in tasks_data:
            entry = self._prepare(task, args, kwargs)
            alias = resolve_queue_db(entry["queue_name"])
            if alias not in grouped:
                validate_result_id_routing(alias)
            ordered.append((alias, entry))
            grouped.setdefault(alias, []).append(entry)

        results = {}
        for alias, entries in grouped.items():
            require_schema_contract(connections[alias])
            manager = TaskRun.objects.using(alias)
            if not self.options.get("DEDUP_ACTIVE", True):
                # Equal specs remain distinct task runs when callers disable
                # deduplication. Never reconstruct these IDs from their hashes.
                runs = manager.bulk_create([TaskRun(**entry) for entry in entries], batch_size=1000)
                for entry, run in zip(entries, runs, strict=True):
                    results[id(entry)] = self._result_proxy(run.result_id, db_alias=alias)
                continue

            by_hash = {}
            for entry in entries:
                by_hash.setdefault(entry["spec_hash"], entry)
            known = {}
            for chunk in _chunked(list(by_hash)):
                known.update(manager.filter(
                    spec_hash__in=chunk, status__in=["READY", "RUNNING"],
                    dedupe_active=True, periodic_name__isnull=True,
                ).values_list("spec_hash", "result_id"))
            pending = [TaskRun(**entry) for key, entry in by_hash.items() if key not in known]
            if pending:
                manager.bulk_create(pending, ignore_conflicts=True, batch_size=1000)
                for chunk in _chunked([run.spec_hash for run in pending]):
                    known.update(manager.filter(
                        spec_hash__in=chunk, status__in=["READY", "RUNNING"],
                        dedupe_active=True, periodic_name__isnull=True,
                    ).values_list("spec_hash", "result_id"))
            for entry in entries:
                result_id = known.get(entry["spec_hash"])
                if result_id is None:
                    # A competing run may have completed between insert and
                    # lookup. The ordinary enqueue path resolves that race.
                    result_id = self._insert_prepared(entry, alias)
                results[id(entry)] = self._result_proxy(result_id, db_alias=alias)

        for alias, entry in ordered:
            self._notify_enqueued(results[id(entry)], entry["spec_json"], alias)
        return [results[id(entry)] for _, entry in ordered]

    def _insert_prepared(self, entry, alias):
        from django.db import transaction
        try:
            with transaction.atomic(using=alias):
                return TaskRun.objects.using(alias).create(**entry).result_id
        except IntegrityError:
            row = TaskRun.objects.using(alias).filter(
                spec_hash=entry["spec_hash"], status__in=["READY", "RUNNING"],
                dedupe_active=True, periodic_name__isnull=True,
            ).first()
            if row is not None and entry["dedupe_active"]:
                return row.result_id
            raise

    async def aenqueue(self, task, args, kwargs) -> TaskResultProxy:
        result = await sync_to_async(self.enqueue, thread_sensitive=True)(
            task, args, kwargs
        )
        await result.arefresh()
        return result

    async def aget_result(self, result_id: str) -> TaskResultProxy:
        result = self._result_proxy(result_id)
        await result.arefresh()
        return result

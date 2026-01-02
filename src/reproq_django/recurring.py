from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from django.conf import settings
from django.db import connections
from django.utils import timezone

from .db import resolve_queue_db
from .serialization import encode_args_kwargs


@dataclass(frozen=True)
class RecurringTask:
    key: str
    schedule: str
    task_path: str
    args: list
    kwargs: dict
    queue_name: str
    priority: int
    max_attempts: int
    concurrency_key: str | None
    concurrency_limit: int
    enabled: bool


_registry: list[RecurringTask] = []


def recurring(
    *,
    schedule: str,
    key: str,
    args: tuple | list | None = None,
    kwargs: dict | None = None,
    queue_name: str | None = None,
    priority: int = 0,
    max_attempts: int = 3,
    enabled: bool = True,
):
    def decorator(task_obj):
        task_path = _resolve_task_path(task_obj)
        if not task_path:
            raise ValueError("Unable to resolve task_path for recurring task")
        args_list = list(args) if args is not None else []
        kwargs_dict = dict(kwargs) if kwargs is not None else {}
        resolved_queue = queue_name or getattr(task_obj, "queue_name", None) or "default"
        concurrency_key = getattr(task_obj, "concurrency_key", None)
        concurrency_limit = getattr(task_obj, "concurrency_limit", 0) or 0
        definition = RecurringTask(
            key=key,
            schedule=schedule,
            task_path=task_path,
            args=args_list,
            kwargs=kwargs_dict,
            queue_name=resolved_queue,
            priority=priority,
            max_attempts=max_attempts,
            concurrency_key=concurrency_key,
            concurrency_limit=int(concurrency_limit),
            enabled=enabled,
        )
        _registry.append(definition)
        existing = getattr(task_obj, "_reproq_recurring", None)
        if existing is None:
            setattr(task_obj, "_reproq_recurring", [definition])
        else:
            existing.append(definition)
        return task_obj
    return decorator


def get_recurring_tasks() -> list[RecurringTask]:
    return list(_registry)


def sync_recurring_tasks(*, using: str | None = None, clear_missing: bool = False) -> int:
    from .models import PeriodicTask

    tasks = get_recurring_tasks()
    if not tasks:
        return 0

    targets = {resolve_queue_db(task.queue_name) for task in tasks}
    if using:
        targets = {using}

    total = 0
    for db_alias in targets:
        if not _periodic_table_exists(db_alias):
            continue
        alias_tasks = [task for task in tasks if resolve_queue_db(task.queue_name) == db_alias]
        keys = set()
        for task in alias_tasks:
            keys.add(task.key)
            encoded_args, encoded_kwargs = encode_args_kwargs(tuple(task.args), task.kwargs)
            PeriodicTask.objects.using(db_alias).update_or_create(
                name=task.key,
                defaults={
                    "cron_expr": task.schedule,
                    "task_path": task.task_path,
                    "payload_json": {"args": encoded_args, "kwargs": encoded_kwargs},
                    "queue_name": task.queue_name,
                    "priority": task.priority,
                    "max_attempts": task.max_attempts,
                    "concurrency_key": task.concurrency_key,
                    "concurrency_limit": task.concurrency_limit,
                    "next_run_at": _next_run_at(task.schedule),
                    "enabled": task.enabled,
                },
            )
            total += 1
        if clear_missing:
            PeriodicTask.objects.using(db_alias).exclude(name__in=keys).update(enabled=False)
    return total


def maybe_sync_recurring_tasks(sender, **kwargs):
    if not getattr(settings, "REPROQ_RECURRING_AUTOSYNC", True):
        return
    using = kwargs.get("using") or "default"
    sync_recurring_tasks(using=using)


def _periodic_table_exists(using: str) -> bool:
    connection = connections[using]
    with connection.cursor() as cursor:
        return "periodic_tasks" in connection.introspection.table_names(cursor)


def _resolve_task_path(task_obj: Any) -> str | None:
    for attr in ("module_path", "path", "name"):
        value = getattr(task_obj, attr, None)
        if isinstance(value, str) and "." in value:
            return value
    module = getattr(task_obj, "__module__", None)
    name = getattr(task_obj, "__name__", None)
    if module and name:
        return f"{module}.{name}"
    nested = getattr(task_obj, "task", None)
    if nested is not None:
        return _resolve_task_path(nested)
    return None


def _next_run_at(expr: str) -> datetime:
    now = timezone.now()
    try:
        from croniter import croniter
    except Exception:
        return now
    return croniter(expr, now).get_next(datetime)

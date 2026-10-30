from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from django.db import connections, transaction
from django.utils import timezone

from .db import resolve_queue_db, validate_result_id_routing
from .schema_contract import require_schema_contract
from .specs import SPEC_VERSION, build_spec
from .concurrency import task_option


@dataclass(frozen=True)
class RecurringTask:
    key: str
    schedule: str
    task_path: str
    args: list
    kwargs: dict
    queue_name: str
    priority: int | None
    max_attempts: int | None
    concurrency_key: str | None
    concurrency_limit: int
    enabled: bool
    task_object: Any


_registry: list[RecurringTask] = []


def periodic_definition_status(spec_version, scheduler_provider):
    """Registration markers only; this does not assert scheduler/worker health."""
    if spec_version == SPEC_VERSION and scheduler_provider in {'go', 'pg_cron'}:
        return 'registered'
    return 'reconciliation_required'


def recurring(
    *,
    schedule: str,
    key: str,
    args: tuple | list | None = None,
    kwargs: dict | None = None,
    queue_name: str | None = None,
    priority: int | None = None,
    max_attempts: int | None = None,
    enabled: bool = True,
):
    def decorator(task_obj):
        task_path = _resolve_task_path(task_obj)
        if not task_path:
            raise ValueError("Unable to resolve task_path for recurring task")
        args_list = list(args) if args is not None else []
        kwargs_dict = dict(kwargs) if kwargs is not None else {}
        resolved_queue = queue_name or getattr(task_obj, "queue_name", None) or "default"
        concurrency_key = task_option(task_obj, "concurrency_key")
        concurrency_limit = task_option(task_obj, "concurrency_limit", 0)
        if concurrency_limit is None:
            concurrency_limit = 0
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
            concurrency_limit=concurrency_limit,
            enabled=enabled,
            task_object=task_obj,
        )
        _registry.append(definition)
        return task_obj
    return decorator


def get_recurring_tasks() -> list[RecurringTask]:
    return list(_registry)


def register_periodic(*, name, schedule, task, args=(), kwargs=None,
                      using=None, queue_name=None, priority=None, max_attempts=None,
                      timeout_seconds=None, lock_key=None, concurrency_key=None,
                      concurrency_limit=None, enabled=True, next_run_at=None,
                      reset_schedule_state=False, scheduler_provider=None):
    """Register canonical source intent while preserving existing operator state."""
    from django.tasks import task_backends
    from django.conf import settings
    from django.utils.module_loading import import_string
    from .models import PeriodicTask

    task_object = import_string(task) if isinstance(task, str) else task
    task_path = task if isinstance(task, str) else _resolve_task_path(task_object)
    alias = getattr(task_object, "backend", "default")
    options = task_backends[alias].options
    queue = queue_name or getattr(task_object, "queue_name", None) or "default"
    routed_alias = resolve_queue_db(queue)
    db_alias = using or routed_alias
    if db_alias != routed_alias:
        raise ValueError('Explicit periodic database must match REPROQ_QUEUE_DATABASES routing for its queue')
    validate_result_id_routing(db_alias)
    require_schema_contract(connections[db_alias])
    if not isinstance(name, str) or not name.strip():
        raise ValueError("Periodic definition name is required")
    if scheduler_provider is not None and scheduler_provider not in {"go", "pg_cron"}:
        raise ValueError("Scheduler provider must be go or pg_cron")
    planned_next = _next_run_at(schedule)
    computed_next = next_run_at or planned_next
    if timezone.is_naive(computed_next):
        raise ValueError("Periodic next_run_at must be timezone-aware")
    key = concurrency_key if concurrency_key is not None else task_option(task_object, "concurrency_key")
    if callable(key):
        key = key(*args, **(kwargs or {}))
    limit = concurrency_limit if concurrency_limit is not None else task_option(task_object, "concurrency_limit", 0)
    priority = priority if priority is not None else getattr(task_object, "priority", 0)
    limit = 0 if limit is None else limit
    spec = build_spec(
        task_path=task_path, args=args, kwargs=kwargs, backend_alias=alias,
        takes_context=getattr(task_object, "takes_context", False),
        queue_name=queue, priority=priority,
        lock_key=lock_key if lock_key is not None else task_option(task_object, "lock_key"),
        concurrency_key=key, concurrency_limit=limit,
        max_attempts=max_attempts if max_attempts is not None else options.get("MAX_ATTEMPTS", 3),
        timeout_seconds=timeout_seconds if timeout_seconds is not None else options.get("TIMEOUT_SECONDS", 900),
        code_ref=options.get("CODE_REF"), pip_lock_hash=options.get("PIP_LOCK_HASH"),
    )
    fields = {"cron_expr": schedule, "task_path": task_path,
              "payload_json": {"args": spec.payload["args"], "kwargs": spec.payload["kwargs"]},
              "queue_name": queue, "priority": priority,
              "max_attempts": spec.payload["exec"]["max_attempts"],
              "concurrency_key": key, "concurrency_limit": limit,
              "canonical_spec_text": spec.text, "canonical_spec_hash": spec.sha256,
              "spec_version": spec.version}
    configured_provider = str(getattr(settings, "REPROQ_SCHEDULER_MODE", "go")).lower()
    default_provider = "pg_cron" if configured_provider == "pg_cron" else "go"
    with transaction.atomic(using=db_alias):
        row, created = PeriodicTask.objects.using(db_alias).select_for_update().get_or_create(
            name=name, defaults={**fields, "next_run_at": computed_next, "enabled": enabled,
                                 "scheduler_provider": scheduler_provider or default_provider}
        )
        if not created:
            for key, value in fields.items():
                setattr(row, key, value)
            update_fields = list(fields) + ["updated_at"]
            if scheduler_provider is not None or row.scheduler_provider == "unassigned":
                row.scheduler_provider = scheduler_provider or default_provider
                update_fields.append("scheduler_provider")
            if reset_schedule_state:
                row.enabled = enabled
                row.next_run_at = computed_next
                update_fields += ["enabled", "next_run_at"]
            row.save(using=db_alias, update_fields=update_fields)
    return row


def sync_recurring_tasks(*, using: str | None = None, reset_schedule_state=False) -> int:
    tasks = get_recurring_tasks()
    keys = set()
    total = 0
    for definition in tasks:
        alias = resolve_queue_db(definition.queue_name)
        if using and using != alias:
            continue
        identity = (alias, definition.key)
        if identity in keys:
            raise ValueError(f"Duplicate recurring definition: {definition.key}")
        keys.add(identity)
    for definition in tasks:
        alias = resolve_queue_db(definition.queue_name)
        if using and using != alias:
            continue
        register_periodic(
            name=definition.key, schedule=definition.schedule, task=definition.task_object,
            args=definition.args, kwargs=definition.kwargs, using=alias,
            queue_name=definition.queue_name, priority=definition.priority,
            max_attempts=definition.max_attempts, concurrency_key=definition.concurrency_key,
            concurrency_limit=definition.concurrency_limit, enabled=definition.enabled,
            reset_schedule_state=reset_schedule_state,
        )
        total += 1
    return total


def periodic_arguments(payload, *, spec_version=0):
    from .serialization import TYPE_MARKER, decode_args_kwargs

    if isinstance(payload, list):
        args, kwargs = payload, {}
    elif isinstance(payload, dict):
        if "args" in payload or "kwargs" in payload:
            args, kwargs = payload.get("args", []), payload.get("kwargs", {})
            if not isinstance(args, list) or not isinstance(kwargs, dict):
                raise ValueError("Legacy periodic payload requires array args and object kwargs")
        else:
            args, kwargs = [], payload
    else:
        raise ValueError("Periodic payload must be an array or object")
    if spec_version == 1:
        return decode_args_kwargs(args, kwargs)
    if spec_version != 0:
        raise ValueError('Unknown periodic spec version; reconcile original source intent explicitly')

    def ambiguous(value):
        if isinstance(value, dict):
            return TYPE_MARKER in value or any(ambiguous(item) for item in value.values())
        return isinstance(value, list) and any(ambiguous(item) for item in value)

    if ambiguous(args) or ambiguous(kwargs):
        raise ValueError('Legacy typed/literal markers are ambiguous; register original Python source intent explicitly')
    return args, kwargs


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


def parse_schedule(expr: str):
    from zoneinfo import ZoneInfo

    if not isinstance(expr, str):
        raise ValueError('Periodic schedule must be a string')
    parts = expr.split()
    zone = ZoneInfo("UTC")
    if parts and parts[0].startswith(("CRON_TZ=", "TZ=")):
        zone = ZoneInfo(parts.pop(0).split("=", 1)[1])
    if len(parts) != 5:
        raise ValueError("Periodic schedules require five cron fields and an optional CRON_TZ prefix")
    schedule = " ".join(parts)
    months = {name: i for i, name in enumerate('JAN FEB MAR APR MAY JUN JUL AUG SEP OCT NOV DEC'.split(), 1)}
    weekdays = {name: i for i, name in enumerate('SUN MON TUE WED THU FRI SAT'.split())}
    for field, low, high, names in zip(parts, (0, 0, 1, 1, 0), (59, 23, 31, 12, 6), ({}, {}, {}, months, weekdays)):
        _validate_cron_field(field, low, high, names)
    return schedule, zone


def _validate_cron_field(field, low, high, names):
    def number(value):
        if not value.isascii():
            raise ValueError('Portable cron fields require ASCII tokens')
        if value.upper() in names:
            return names[value.upper()]
        if not value.isdecimal() or not low <= int(value) <= high:
            raise ValueError('Cron value is outside the portable field range')
        return int(value)

    for component in field.split(','):
        pieces = component.split('/')
        if len(pieces) > 2:
            raise ValueError('Invalid portable cron step')
        base = pieces[0]
        if len(pieces) == 2:
            step = pieces[1]
            if not step.isascii() or not step.isdecimal() or not 1 <= int(step) <= high - low + 1:
                raise ValueError('Cron step must be positive and within its field span')
            if base != '*' and '-' not in base:
                raise ValueError('Portable cron steps require a wildcard or explicit ascending range')
        if base == '*':
            continue
        endpoints = base.split('-')
        if len(endpoints) > 2 or number(endpoints[0]) > number(endpoints[-1]):
            raise ValueError('Portable cron ranges must be ascending; L/#/wrapping extensions are unsupported')


def _next_run_at(expr: str) -> datetime:
    from croniter import croniter

    schedule, zone = parse_schedule(expr)
    return croniter(schedule, timezone.now().astimezone(zone)).get_next(datetime)

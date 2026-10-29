from datetime import timedelta
from decimal import Decimal
from uuid import uuid4

import pytest
from django.core.management import call_command
from django.tasks import Task
from django.utils import timezone

from reproq_django.backend import ReproqBackend
from reproq_django.concurrency import limits_concurrency
from reproq_django.models import PeriodicTask, TaskRun
from reproq_django.recurring import recurring, register_periodic
from reproq_django.serialization import spec_hash_for


def owned_task(context, *args, **kwargs):
    return kwargs


def keyword_task(priority, run_after, lock_key, concurrency_key, concurrency_limit):
    return priority, run_after, lock_key, concurrency_key, concurrency_limit


@pytest.fixture(autouse=True)
def installed_schema():
    from reproq_django import recurring as recurring_module
    initial_registry = list(recurring_module._registry)
    call_command("migrate", verbosity=0)
    TaskRun.objects.all().delete()
    PeriodicTask.objects.all().delete()
    yield
    recurring_module._registry[:] = initial_registry
    TaskRun.objects.all().delete()
    PeriodicTask.objects.all().delete()
    for name in ("concurrency_key", "concurrency_limit"):
        if hasattr(owned_task, name):
            delattr(owned_task, name)


def make_task():
    return Task(func=owned_task, priority=0, queue_name="default", backend="default",
                run_after=None, takes_context=True)


def test_single_bulk_and_recurring_use_the_same_complete_spec_identity():
    backend = ReproqBackend(alias="default", params={"QUEUES": ["default"]})
    task = make_task()
    arguments = (Decimal("1.25"), uuid4(), timedelta(seconds=17), "Texas 😀")
    first = backend.enqueue(task, arguments, {"zero": 0, "false": False})
    bulk = backend.bulk_enqueue([(task, arguments, {"zero": 0, "false": False})])[0]
    recurring_row = register_periodic(name="identity", schedule="0 0 * * *", task=task,
                                     args=arguments, kwargs={"zero": 0, "false": False})
    run = TaskRun.objects.get(pk=first.result_id)
    assert bulk.id == first.id
    assert run.spec_version == recurring_row.spec_version == 1
    assert run.canonical_spec_text == recurring_row.canonical_spec_text
    assert run.spec_hash == recurring_row.canonical_spec_hash == spec_hash_for(run.spec_json)
    assert run.spec_json["takes_context"] is True
    assert run.spec_json["provenance"] == {"code_ref": None, "pip_lock_hash": None}


def test_disabled_dedupe_returns_distinct_bulk_runs_with_truthful_equal_hashes():
    backend = ReproqBackend(alias="default", params={"QUEUES": ["default"], "OPTIONS": {"DEDUP_ACTIVE": False}})
    task = make_task()
    results = backend.bulk_enqueue([(task, (), {}), (task, (), {})])
    assert results[0].id != results[1].id
    rows = list(TaskRun.objects.order_by("pk"))
    assert rows[0].spec_hash == rows[1].spec_hash
    assert all(not row.dedupe_active for row in rows)
    assert backend.enqueue(task, (), {}).id not in {result.id for result in results}


def test_native_function_keywords_mean_the_same_for_every_enqueue_provider():
    task = Task(func=keyword_task, priority=0, queue_name='default', backend='default', run_after=None)
    keywords = {'priority': 7, 'run_after': 'a function value', 'lock_key': 'value',
                'concurrency_key': 'another value', 'concurrency_limit': 23}
    assert task.call(**keywords) == tuple(keywords.values())
    backend = ReproqBackend(alias='default', params={'QUEUES': ['default']})
    single = backend.enqueue(task, (), keywords)
    bulk = backend.bulk_enqueue([(task, (), keywords)])[0]
    periodic = register_periodic(name='native-keywords', schedule='0 0 * * *', task=task, kwargs=keywords)
    row = TaskRun.objects.get(pk=single.result_id)
    assert bulk.id == single.id
    assert row.spec_json['kwargs'] == keywords
    assert row.priority == 0 and row.concurrency_limit == 0
    assert row.canonical_spec_text == periodic.canonical_spec_text
    assert row.spec_hash == periodic.canonical_spec_hash


def test_recurring_registration_preserves_operator_state_unless_explicitly_reset():
    next_run = timezone.now() + timedelta(days=3)
    last_run = timezone.now() - timedelta(days=1)
    row = register_periodic(name="operator-owned", schedule="0 0 * * *", task=make_task())
    PeriodicTask.objects.filter(pk=row.pk).update(enabled=False, next_run_at=next_run, last_run_at=last_run)
    row = register_periodic(name=row.name, schedule="CRON_TZ=America/Chicago 0 0 * * *", task=make_task())
    assert row.enabled is False and row.next_run_at == next_run and row.last_run_at == last_run
    row = register_periodic(name=row.name, schedule=row.cron_expr, task=make_task(), reset_schedule_state=True)
    assert row.enabled is True and row.next_run_at != next_run and row.last_run_at == last_run


def test_decorators_support_frozen_django_tasks_and_using_copies():
    task = limits_concurrency("dataset", to=2)(make_task())
    decorated = recurring(schedule="0 0 * * *", key="frozen-django")(task)
    backend = ReproqBackend(alias="default", params={"QUEUES": ["default"]})
    result = backend.enqueue(decorated.using(priority=4), (), {})
    run = TaskRun.objects.get(pk=result.result_id)
    assert (run.concurrency_key, run.concurrency_limit, run.priority) == ("dataset", 2, 4)


@pytest.mark.parametrize("limit", [0, -1, True, "2"])
def test_invalid_concurrency_limit_is_not_silently_unlimited(limit):
    with pytest.raises(ValueError):
        limits_concurrency("dataset", to=limit)


def test_periodic_priority_override_respects_native_django_bounds_before_writes():
    from django.tasks.base import TASK_MAX_PRIORITY
    with pytest.raises(ValueError,match='priority'):
        register_periodic(name='invalid-priority',schedule='0 0 * * *',task=make_task(),priority=TASK_MAX_PRIORITY+1)
    assert not PeriodicTask.objects.exists()

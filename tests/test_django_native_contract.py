import asyncio
from dataclasses import FrozenInstanceError, fields, replace
from uuid import uuid4
from datetime import timedelta

import pytest
from asgiref.sync import sync_to_async
from django.conf import settings
from django.core.management import call_command
from django.db import connections
from django.tasks import (
    Task,
    TaskContext as DjangoTaskContext,
    TaskResult,
    TaskResultStatus,
)
from django.tasks.exceptions import TaskResultMismatch
from django.tasks import task as django_task
from django.test import override_settings
from django.utils import timezone

from reproq_django.backend import ReproqBackend
from reproq_django.context import ExecutionClaim, TaskContext
from reproq_django.db import format_result_id, parse_result_id
from reproq_django.models import PeriodicTask, TaskRun
from reproq_django.recurring import register_periodic


class OwnedParserError(ValueError):
    pass


@django_task(takes_context=True)
def current_declaration(context=None, *args):
    return {"user_argument_preserved": context == "user", "extra_arguments": len(args)}


current_declaration = replace(
    current_declaration, run_after=timezone.now() + timedelta(days=1)
)


def native_task(*args, **kwargs):
    return kwargs


def another_task():
    pass


@pytest.fixture
def file_queue(tmp_path):
    alias = "owned_native_queue"
    config = {
        **connections["default"].settings_dict,
        "NAME": str(tmp_path / "queue.sqlite3"),
    }
    assert alias not in settings.DATABASES
    settings.DATABASES[alias] = config
    connections.databases[alias] = config
    try:
        call_command("migrate", database=alias, verbosity=0)
        with override_settings(REPROQ_QUEUE_DATABASES={"default": alias}):
            yield alias
    finally:

        async def close_thread_connections():
            await sync_to_async(connections.close_all, thread_sensitive=True)()

        asyncio.run(close_thread_connections())
        connections[alias].close()
        del connections[alias]
        settings.DATABASES.pop(alias, None)
        connections.databases.pop(alias, None)


def task():
    return Task(
        func=native_task,
        backend="default",
        queue_name="default",
        priority=0,
        run_after=None,
    )


def test_public_producer_and_result_are_native_frozen_slotted_types(file_queue):
    result = task().enqueue(0, False, literal={"key": "value"})
    assert isinstance(result, TaskResult) and not hasattr(result, "__dict__")
    assert result.args == [0, False] and result.kwargs == {"literal": {"key": "value"}}
    for name, value in [
        ("id", "other"),
        ("backend", "other"),
        ("status", TaskResultStatus.FAILED),
    ]:
        with pytest.raises(FrozenInstanceError):
            setattr(result, name, value)
    assert result.refresh() is None


def test_native_snapshot_refresh_reads_result_errors_and_successful_false_value(
    file_queue,
):
    result = task().enqueue()
    result.refresh()
    snapshot = TaskResult(
        **{
            field.name: getattr(result, field.name)
            for field in fields(TaskResult)
            if field.init
        }
    )
    TaskRun.objects.using(file_queue).filter(pk=result.result_id).update(
        status="SUCCESSFUL",
        return_json=False,
        worker_ids=["same-worker", "same-worker"],
        attempts=1,
    )
    assert snapshot.refresh() is None
    assert snapshot.return_value is False and snapshot.is_finished
    assert snapshot.attempts == 2 and snapshot.errors == []
    result.refresh()
    assert result.attempts == 2 and result.budget_attempt == 1


def test_async_producer_get_and_native_snapshot_refresh(file_queue):
    async def inspect():
        result = await task().aenqueue(0, False)
        assert isinstance(result, TaskResult)
        assert await result.arefresh() is None
        again = await task().aget_result(result.id)
        assert isinstance(again, TaskResult) and again.args == [0, False]
        snapshot = TaskResult(
            **{
                field.name: getattr(again, field.name)
                for field in fields(TaskResult)
                if field.init
            }
        )
        assert await snapshot.arefresh() is None
        assert snapshot.status is TaskResultStatus.READY

    asyncio.run(inspect())


def test_cross_backend_lookup_preserves_the_origin_backend(file_queue):
    result = task().enqueue()
    other = ReproqBackend(alias="other", params={}).get_result(result.id)
    assert other.backend == other.task.backend == "default"
    wrong = Task(
        func=another_task,
        backend="default",
        priority=0,
        queue_name="default",
        run_after=None,
    )
    with pytest.raises(TaskResultMismatch):
        wrong.get_result(result.id)


def test_context_inherits_native_history_attempt_and_core_immutability(file_queue):
    result = task().enqueue()
    token = uuid4()
    TaskRun.objects.using(file_queue).filter(pk=result.result_id).update(
        status="RUNNING",
        attempts=1,
        worker_ids=["worker", "worker"],
        leased_by="worker",
        lease_token=token,
        leased_until=timezone.now() + timedelta(minutes=1),
    )
    context = TaskContext.from_claim(
        ExecutionClaim(
            result_id=result.result_id,
            budget_attempt=1,
            expected_task_path=task().module_path,
            db_alias=file_queue,
            worker_id="worker",
            lease_token=str(token),
        )
    )
    assert isinstance(context, DjangoTaskContext) and not hasattr(context, "__dict__")
    assert (
        context.attempt == context.task_result.attempts == 2
        and context.budget_attempt == 1
    )
    with pytest.raises(FrozenInstanceError):
        context.task_result = result
    # Match the installed native class: 3.12's slotted frozen dataclass raises
    # TypeError for this property while newer interpreters raise FrozenInstanceError.
    native = DjangoTaskContext(task_result=context.task_result)
    with pytest.raises((FrozenInstanceError, TypeError)) as native_error:
        native.attempt = 0
    with pytest.raises(type(native_error.value)):
        context.attempt = 0
    context.metadata["progress"] = 0
    context.save_metadata()
    assert TaskRun.objects.using(file_queue).get(pk=result.result_id).metadata_json == {
        "progress": 0
    }


def test_alias_encoding_is_invertible_and_unknown_prefix_does_not_guess():
    alias = "queue:é%"
    with override_settings(
        DATABASES={**settings.DATABASES, alias: settings.DATABASES["default"]}
    ):
        encoded = format_result_id(1, alias)
        assert encoded == "queue%3A%C3%A9%25:1" and parse_result_id(encoded) == (
            alias,
            "1",
        )
        assert len(encoded) < 64
        with pytest.raises(ValueError, match="Noncanonical"):
            parse_result_id("queue%3a%C3%A9%25:1")
        with pytest.raises(ValueError, match="unknown"):
            parse_result_id("not-configured:1")


@pytest.mark.parametrize("producer", ["single", "bulk", "periodic"])
def test_long_alias_configuration_rejects_before_any_producer_write(
    file_queue, producer
):
    alias = "queue" + ("x" * 60)
    with override_settings(
        DATABASES={**settings.DATABASES, alias: settings.DATABASES[file_queue]},
        REPROQ_QUEUE_DATABASES={"default": alias},
    ):
        with pytest.raises(ValueError, match="shorter"):
            if producer == "single":
                task().enqueue()
            elif producer == "bulk":
                task().get_backend().bulk_enqueue([(task(), (), {})])
            else:
                register_periodic(name="too-long", schedule="0 0 * * *", task=task())
    assert not TaskRun.objects.using(file_queue).exists()
    assert not PeriodicTask.objects.using(file_queue).exists()


def test_multiple_databases_cannot_disable_result_id_routing(file_queue):
    with override_settings(REPROQ_RESULT_ID_WITH_ALIAS=False):
        with pytest.raises(ValueError, match="unambiguous"):
            task().enqueue()
    assert not TaskRun.objects.using(file_queue).exists()


def test_explicit_database_identity_survives_a_lookup_outside_queue_routes(file_queue):
    result = task().enqueue()
    with override_settings(REPROQ_QUEUE_DATABASES={}):
        inspected = task().get_result(result.id)
        assert inspected.id == result.id
        assert task().get_result(inspected.id).db_alias == file_queue


def test_canonical_source_preserves_negative_zero_even_if_json_projection_changes(
    file_queue,
):
    import math

    result = task().enqueue(-0.0)
    row = TaskRun.objects.using(file_queue).get(pk=result.result_id)
    projected = row.spec_json
    projected["args"] = [0.0]  # PostgreSQL JSONB does this to negative zero.
    TaskRun.objects.using(file_queue).filter(pk=row.pk).update(spec_json=projected)
    restored = task().get_result(result.id)
    assert math.copysign(1, restored.args[0]) == -1
    assert math.copysign(1, restored.decoded_args[0]) == -1


def test_invalid_native_canonical_source_remains_inspectable_but_cannot_supply_arguments(
    file_queue,
):
    import hashlib
    from django.tasks.base import TASK_MAX_PRIORITY
    from reproq_django.serialization import canonical_json

    result = task().enqueue()
    row = TaskRun.objects.using(file_queue).get(pk=result.result_id)
    payload = row.spec_json
    payload["priority"] = TASK_MAX_PRIORITY + 1
    source = canonical_json(payload)
    TaskRun.objects.using(file_queue).filter(pk=row.pk).update(
        canonical_spec_text=source,
        spec_hash=hashlib.sha256(source.encode()).hexdigest(),
        priority=payload["priority"],
    )
    inspected = task().get_backend().get_result(result.id)
    assert inspected.status is TaskResultStatus.READY
    with pytest.raises(ValueError, match="priority"):
        _ = inspected.args


@pytest.mark.parametrize("delivery", ["periodic", "retry", "replay"])
def test_operational_eligibility_does_not_rewrite_native_initial_intent(
    file_queue, delivery
):
    initial = None if delivery == "periodic" else timezone.now() - timedelta(hours=1)
    source_task = task() if initial is None else task().using(run_after=initial)
    result = source_task.enqueue()
    eligible = None if delivery == "replay" else timezone.now() - timedelta(seconds=5)
    token = uuid4()
    changes = {
        "status": "RUNNING",
        "run_after": eligible,
        "attempts": 1,
        "worker_ids": ["worker"],
        "leased_by": "worker",
        "lease_token": token,
        "leased_until": timezone.now() + timedelta(minutes=1),
    }
    if delivery == "periodic":
        changes.update(
            periodic_name="owned-periodic",
            scheduled_for=eligible,
            periodic_provider="go",
        )
    TaskRun.objects.using(file_queue).filter(pk=result.result_id).update(**changes)
    context = TaskContext.from_claim(
        ExecutionClaim(
            result_id=result.result_id,
            budget_attempt=1,
            expected_task_path=source_task.module_path,
            db_alias=file_queue,
            worker_id="worker",
            lease_token=str(token),
        )
    )
    assert context.task_result.task.run_after == initial
    assert context.task_result.eligible_after == eligible


def test_source_snapshot_clears_current_declaration_defaults_and_drives_execution(
    file_queue,
):
    import json
    from types import SimpleNamespace
    from reproq_django.executor import _run_task

    source_task = replace(current_declaration, run_after=None, takes_context=False)
    result = source_task.enqueue("user")
    assert result.task.run_after is None and result.task.takes_context is False
    token = uuid4()
    TaskRun.objects.using(file_queue).filter(pk=result.result_id).update(
        status="RUNNING",
        attempts=1,
        worker_ids=["worker"],
        leased_by="worker",
        lease_token=token,
        leased_until=timezone.now() + timedelta(minutes=1),
    )
    row = TaskRun.objects.using(file_queue).get(pk=result.result_id)
    arguments = SimpleNamespace(
        settings=None,
        task_path=None,
        result_id=result.result_id,
        attempt=1,
        worker_id="worker",
        lease_token=token,
        max_output_bytes=4096,
    )
    response = json.loads(_run_task(arguments, row.spec_json))
    assert response["ok"] is True
    assert response["return"] == {"user_argument_preserved": True, "extra_arguments": 0}


def test_native_error_inspection_preserves_qualified_custom_class_and_task_frame(
    file_queue,
):
    import json
    from reproq_django.protocol import failure_envelope

    def parse_owned():
        raise OwnedParserError("Owned parser failed")

    try:
        parse_owned()
    except OwnedParserError as error:
        envelope = json.loads(failure_envelope(error, 4096))
    result = task().enqueue()
    TaskRun.objects.using(file_queue).filter(pk=result.result_id).update(
        status="FAILED",
        errors_json=[{"kind": "app_error", **envelope}],
    )
    result.refresh()
    failure = result.errors[0]
    assert failure.exception_class is OwnedParserError
    assert (
        "parse_owned" in failure.traceback
        and "Owned parser failed" in failure.traceback
    )


def test_non_python_worker_failure_has_explicit_type_and_unavailable_traceback(
    file_queue,
):
    from reproq_django.protocol import WorkerExecutionError

    result = task().enqueue()
    TaskRun.objects.using(file_queue).filter(pk=result.result_id).update(
        status="FAILED",
        errors_json=[{"kind": "infra_error", "message": "owned process timeout"}],
    )
    result.refresh()
    assert result.errors[0].exception_class is WorkerExecutionError
    assert result.errors[0].traceback.startswith("Traceback unavailable.")


def test_retired_id_override_cannot_create_ambiguous_identity(file_queue):
    with pytest.raises(TypeError, match="include_alias"):
        format_result_id(1, file_queue, include_alias=False)

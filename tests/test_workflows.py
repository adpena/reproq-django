"""Canonical atomic producers; runtime terminal propagation is tested in Go."""

from datetime import timedelta
import hashlib
from unittest.mock import patch

import pytest
from django.conf import settings
from django.core.management import call_command
from django.db import IntegrityError, connections, transaction
from django.db.models.query import QuerySet
from django.tasks import Task, TaskResult, TaskResultStatus
from django.test import override_settings
from django.test.utils import CaptureQueriesContext
from django.utils import timezone

from reproq_django.models import QueueControl, TaskRun, WorkflowRun
from reproq_django.signals import task_enqueued
from reproq_django.specs import read_task_spec
from reproq_django import workflows
from reproq_django.workflows import Chain, Group, chain, group, chord


def workflow_task(*args, **kwargs):
    return {"args": args, "kwargs": kwargs}


@pytest.fixture
def workflow_databases(tmp_path):
    aliases = ("owned_wf", "owned_other")
    for alias in aliases:
        assert alias not in settings.DATABASES
        config = {**connections["default"].settings_dict,
                  "NAME": str(tmp_path / (alias + ".sqlite3"))}
        settings.DATABASES[alias] = config
        connections.databases[alias] = config
    tasks = {"default": {"BACKEND": "reproq_django.backend.ReproqBackend"},
             "workflow": {"BACKEND": "reproq_django.backend.ReproqBackend",
                          "QUEUES": ["default", "q", "test-queue"],
                          "OPTIONS": {"TIMEOUT_SECONDS": 42, "MAX_ATTEMPTS": 2,
                                      "CODE_REF": "owned-workflow-source",
                                      "PIP_LOCK_HASH": "b" * 64,
                                      "EXPIRES_IN": timedelta(minutes=10)}}}
    try:
        with override_settings(TASKS=tasks, REPROQ_DEFAULT_DB_ALIAS=aliases[0],
                               REPROQ_QUEUE_DATABASES={"q": aliases[0], "test-queue": aliases[1]}):
            for alias in aliases:
                call_command("migrate", "reproq_django", database=alias, verbosity=0)
            yield aliases
    finally:
        for alias in aliases:
            connections[alias].close()
            del connections[alias]
            settings.DATABASES.pop(alias, None)
            connections.databases.pop(alias, None)


def task(*, queue="q", **options):
    return Task(func=workflow_task, backend="workflow", queue_name=queue,
                priority=7, run_after=None, **options)


def rows(alias, results):
    return [TaskRun.objects.using(alias).get(pk=result.result_id) for result in results]


@pytest.mark.parametrize("factory", [Chain, Group])
def test_empty_chain_and_group_do_not_access_database(workflow_databases, factory):
    alias, _ = workflow_databases
    with CaptureQueriesContext(connections[alias]) as queries:
        assert factory().enqueue() == []
    assert not queries


def test_chain_uses_returned_ids_for_identical_nodes_and_canonical_native_results(workflow_databases):
    alias, _ = workflow_databases
    node = task()
    workflow = chain((node, (5,), {}), (node, (5,), {}), (node, (5,), {}))
    result = workflow.enqueue()
    stored = rows(alias, result)
    assert len({row.pk for row in stored}) == 3
    assert len({row.spec_hash for row in stored}) == 1
    assert [row.parent_id for row in stored] == [None, stored[0].pk, stored[1].pk]
    assert [row.status for row in stored] == ["READY", "WAITING", "WAITING"]
    assert [row.wait_count for row in stored] == [0, 1, 1]
    assert not WorkflowRun.objects.using(alias).exists()
    for native, row in zip(result, stored, strict=True):
        assert isinstance(native, TaskResult) and native.backend == "workflow"
        assert native.status is TaskResultStatus.READY and native.args == [5]
        assert native.decoded_args == [5]
        assert native.task.get_result(native.id).id == native.id
        assert row.workflow_id == workflow.workflow_id and row.dedupe_active is False
        assert row.spec_version == 1 and len(row.spec_hash) == 64
        assert hashlib.sha256(row.canonical_spec_text.encode()).hexdigest() == row.spec_hash
        assert read_task_spec(row).payload == row.spec_json


def test_group_preserves_shared_preparation_options_and_keyword_names(workflow_databases, monkeypatch):
    alias, _ = workflow_databases
    monkeypatch.setattr(workflow_task, "lock_key", "execution-mutex", raising=False)
    monkeypatch.setattr(workflow_task, "concurrency_key", "execution-group", raising=False)
    monkeypatch.setattr(workflow_task, "concurrency_limit", 2, raising=False)
    deadline = timezone.now() + timedelta(minutes=2)
    node = task().using(priority=11, run_after=deadline)
    arguments = {"priority": 42, "run_after": timedelta(seconds=17),
                 "lock_key": "ordinary argument", "concurrency_key": "ordinary key"}
    result = group((node, (), arguments), (node, (), arguments)).enqueue()
    for native, row in zip(result, rows(alias, result), strict=True):
        assert native.decoded_kwargs == arguments
        assert row.parent_id is None and row.wait_count == 0 and row.status == "READY"
        assert row.priority == 11 and row.run_after == deadline
        assert row.lock_key == "execution-mutex" and row.concurrency_key == "execution-group"
        assert row.concurrency_limit == 2 and row.timeout_seconds == 42 and row.max_attempts == 2
        assert row.expires_at is not None
        spec = read_task_spec(row).payload
        assert spec["backend_alias"] == "workflow"
        assert spec["provenance"] == {"code_ref": "owned-workflow-source", "pip_lock_hash": "b" * 64}
    assert not WorkflowRun.objects.using(alias).exists()


@pytest.mark.parametrize("count", [0, 1, 3])
def test_chord_initial_state_matches_go_callback_counter_contract(workflow_databases, count):
    alias, _ = workflow_databases
    node = task()
    workflow = chord(*[(node, ("same",), {})] * count, callback=(node, ("same",), {}))
    members, callback = workflow.enqueue()
    stored = rows(alias, [*members, callback])
    state = WorkflowRun.objects.using(alias).get(workflow_id=workflow.workflow_id)
    assert len({row.pk for row in stored}) == count + 1
    assert len({row.spec_hash for row in stored}) == 1
    assert state.expected_count == count and state.success_count == state.failure_count == 0
    assert state.callback_result_id == stored[-1].pk
    assert state.status == ("RUNNING" if count else "WAITING_CALLBACK")
    assert stored[-1].status == ("WAITING" if count else "READY")
    assert stored[-1].wait_count == count
    assert all(row.status == "READY" and row.wait_count == 0 for row in stored[:-1])
    assert all(row.parent_id is None and row.dedupe_active is False for row in stored)
    assert callback.raw_status == stored[-1].status


@pytest.mark.parametrize("factory", [chain, group, lambda *nodes: chord(*nodes, callback=task())])
def test_invalid_later_node_fails_before_any_database_access(workflow_databases, factory):
    alias, other = workflow_databases
    with CaptureQueriesContext(connections[alias]) as queries:
        with pytest.raises(ValueError, match="string keys"):
            factory(task(), (task(), ({1: "ambiguous key"},), {})).enqueue()
    assert not queries
    assert TaskRun.objects.using(alias).count() == TaskRun.objects.using(other).count() == 0


@pytest.mark.parametrize("kind", ["chain", "group", "chord"])
def test_cross_database_routes_fail_before_any_write(workflow_databases, kind):
    alias, other = workflow_databases
    if kind == "chord":
        workflow = chord(task(), callback=task(queue="test-queue"))
    else:
        workflow = {"chain": chain, "group": group}[kind](task(), task(queue="test-queue"))
    with CaptureQueriesContext(connections[alias]) as first, CaptureQueriesContext(connections[other]) as second:
        with pytest.raises(ValueError, match="same database"):
            workflow.enqueue()
    assert not first and not second


@pytest.mark.parametrize("bad", [(None, (), {}), ("not a task",), (None, (), []), object()])
def test_malformed_nodes_refuse_without_partial_insertion(workflow_databases, bad):
    alias, _ = workflow_databases
    with pytest.raises((TypeError, ValueError)):
        group(task(), bad).enqueue()
    assert not TaskRun.objects.using(alias).exists()


def test_chain_wiring_failure_rolls_back_all_nodes_and_notifications(workflow_databases):
    alias, _ = workflow_databases
    notifications = []
    def receiver(**kwargs):
        notifications.append(kwargs["task_result"])
    task_enqueued.connect(receiver, weak=False)
    try:
        with patch("django.db.models.query.QuerySet.bulk_update", side_effect=RuntimeError("owned wiring failure")):
            with pytest.raises(RuntimeError, match="wiring failure"):
                chain(task(), task(), task()).enqueue()
        assert not TaskRun.objects.using(alias).exists()
        assert not WorkflowRun.objects.using(alias).exists() and notifications == []
    finally:
        task_enqueued.disconnect(receiver)


def test_chord_summary_failure_rolls_back_headers_and_callback(workflow_databases):
    alias, _ = workflow_databases
    with patch.object(WorkflowRun, "save", side_effect=RuntimeError("owned summary failure")):
        with pytest.raises(RuntimeError, match="summary failure"):
            chord(task(), task(), callback=task()).enqueue()
    assert not TaskRun.objects.using(alias).exists() and not WorkflowRun.objects.using(alias).exists()


def test_repeated_chord_identity_conflict_is_atomic(workflow_databases):
    alias, _ = workflow_databases
    workflow = chord(task(), callback=task())
    members, callback = workflow.enqueue()
    ids = {members[0].result_id, callback.result_id}
    with pytest.raises(IntegrityError):
        workflow.enqueue()
    assert set(str(pk) for pk in TaskRun.objects.using(alias).values_list("pk", flat=True)) == ids
    assert WorkflowRun.objects.using(alias).count() == 1


def test_notifications_follow_outer_commit_and_outer_rollback_discards_everything(workflow_databases):
    alias, _ = workflow_databases
    events = []
    def receiver(**kwargs):
        result = kwargs["task_result"]
        assert isinstance(result, TaskResult)
        events.append((result.id, kwargs["spec"], TaskRun.objects.using(alias).count()))
    task_enqueued.connect(receiver, weak=False)
    try:
        with pytest.raises(RuntimeError):
            with transaction.atomic(using=alias):
                chord(task(), callback=task()).enqueue()
                assert events == []
                raise RuntimeError("owned caller rollback")
        assert not TaskRun.objects.using(alias).exists() and events == []
        with transaction.atomic(using=alias):
            members, callback = chord(task(), callback=task()).enqueue()
            assert events == []
        assert [event[0] for event in events] == [members[0].id, callback.id]
        assert all(event[2] == 2 for event in events)
    finally:
        task_enqueued.disconnect(receiver)


@pytest.mark.parametrize("factory", [chain, group])
def test_bulk_node_insertion_and_chain_wiring_avoid_per_node_writes(workflow_databases, factory):
    alias, _ = workflow_databases
    with CaptureQueriesContext(connections[alias]) as queries:
        result = factory(*[task()] * 12).enqueue()
    mutations = [q["sql"] for q in queries if q["sql"].lstrip().startswith(("INSERT", "UPDATE"))]
    assert len(result) == 12 and len(mutations) < 6
    assert TaskRun.objects.using(alias).count() == 12


def test_repeated_model_nodes_do_not_query_business_source_until_decoded_access(workflow_databases):
    alias, source_alias = workflow_databases
    original = QueueControl.objects.using(source_alias).create(queue_name="source-model", reason="business")
    node = task()
    with CaptureQueriesContext(connections[source_alias]) as producer_queries:
        result = group(*[(node, (original,), {})] * 4).enqueue()
        assert all(native.args[0]["database"] == source_alias for native in result)
        assert all(native.status is TaskResultStatus.READY for native in result)
    model_selects = [q["sql"] for q in producer_queries
                     if q["sql"].lstrip().startswith("SELECT") and QueueControl._meta.db_table in q["sql"]]
    assert model_selects == [], f"producer issued {len(model_selects)} hidden model SELECTs"
    with CaptureQueriesContext(connections[source_alias]) as decoded_queries:
        decoded = result[0].decoded_args[0]
        assert decoded._state.db == source_alias and decoded.reason == "business"
        assert result[0].decoded_args[0] is decoded
    assert len(decoded_queries) == 1
    assert TaskRun.objects.using(alias).count() == 4


def test_chain_wiring_bounds_orm_expression_batches_before_bulk_update(workflow_databases):
    alias, _ = workflow_databases
    count = workflows.BATCH_SIZE + 8
    calls = []
    original = QuerySet.bulk_update
    def bounded_update(queryset, objects, fields, batch_size=None):
        calls.append(len(objects))
        assert len(objects) <= workflows.BATCH_SIZE, f"ORM received {len(objects)} parent expressions at once"
        return original(queryset, objects, fields, batch_size=batch_size)
    with patch.object(QuerySet, "bulk_update", bounded_update):
        with CaptureQueriesContext(connections[alias]) as queries:
            results = chain(*[task()] * count).enqueue()
    assert calls == [workflows.BATCH_SIZE, 7]
    assert len(results) == count
    stored = list(TaskRun.objects.using(alias).order_by("pk").values_list("pk", "parent_id", "wait_count"))
    assert stored[0][1:] == (None, 0)
    assert all(stored[index][1:] == (stored[index - 1][0], 1) for index in range(1, count))
    updates = [q["sql"] for q in queries if q["sql"].lstrip().startswith("UPDATE")]
    assert 1 < len(updates) < count // 10

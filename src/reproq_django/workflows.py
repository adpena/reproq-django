"""Atomic workflow producers using the same canonical task preparation as enqueue.

Chain/Group IDs group their nodes; only Chord has aggregate callback counters.
The Go runtime owns dependency release, terminal transitions and callback state.
"""

from dataclasses import dataclass
from uuid import uuid4

from django.db import NotSupportedError, connections, transaction
from django.tasks import Task, task_backends

from .backend import ReproqBackend
from .db import resolve_queue_db, validate_result_id_routing
from .models import TaskRun, WorkflowRun
from .schema_contract import require_schema_contract


BATCH_SIZE = 1000


@dataclass(frozen=True, slots=True)
class _PreparedNode:
    backend: ReproqBackend
    fields: dict
    alias: str


def _prepare_nodes(items):
    """Validate every task, native option and route before opening a write block."""
    nodes = []
    for item in items:
        if isinstance(item, tuple):
            if len(item) != 3:
                raise ValueError("Workflow nodes require (task, args, kwargs)")
            task, args, kwargs = item
        else:
            task, args, kwargs = item, (), {}
        if not isinstance(task, Task):
            raise TypeError("Workflow nodes must contain Django Task objects")
        if not isinstance(args, (tuple, list)) or not isinstance(kwargs, dict):
            raise TypeError("Workflow arguments require an array and keyword mapping")
        backend = task_backends[task.backend]
        if not isinstance(backend, ReproqBackend):
            raise TypeError("Workflow tasks require a ReproqBackend")
        fields = backend._prepare(task, args, kwargs)
        alias = resolve_queue_db(fields["queue_name"])
        validate_result_id_routing(alias)
        nodes.append(_PreparedNode(backend, fields, alias))
    if nodes and any(node.alias != nodes[0].alias for node in nodes):
        raise ValueError("All workflow tasks must target the same database")
    return nodes


def _insert_nodes(nodes, workflow_id, *, sequential=False, callback=False):
    if not nodes:
        return []
    alias = nodes[0].alias
    connection = connections[alias]
    require_schema_contract(connection)
    if not connection.features.can_return_rows_from_bulk_insert:
        raise NotSupportedError("Workflow enqueue requires bulk-insert returned IDs")
    runs = []
    for index, node in enumerate(nodes):
        waiting = index > 0 if sequential else callback and index == len(nodes) - 1 and index > 0
        runs.append(TaskRun(**{**node.fields, "workflow_id": workflow_id,
                               "dedupe_active": False,
                               "status": "WAITING" if waiting else "READY",
                               "wait_count": (1 if sequential else index) if waiting else 0}))
    manager = TaskRun.objects.using(alias)
    with transaction.atomic(using=alias):
        manager.bulk_create(runs, batch_size=BATCH_SIZE)
        if any(run.pk is None for run in runs):
            raise RuntimeError("Workflow insert did not return every task ID")
        if sequential and len(runs) > 1:
            # Django builds CASE/WHEN expressions for the entire supplied list
            # before its own SQL batching. Bound that input as well.
            for start in range(1, len(runs), BATCH_SIZE):
                children = runs[start:start + BATCH_SIZE]
                for index, child in enumerate(children, start):
                    child.parent_id = runs[index - 1].pk
                manager.bulk_update(children, ["parent"], batch_size=BATCH_SIZE)
        if callback:
            count = len(runs) - 1
            WorkflowRun.objects.using(alias).create(
                workflow_id=workflow_id, expected_count=count,
                success_count=0, failure_count=0, callback_result_id=runs[-1].pk,
                status="RUNNING" if count else "WAITING_CALLBACK",
            )
        results = [node.backend._result_proxy(run.pk, db_alias=alias)._bind_run(run, prime_arguments=False)
                   for node, run in zip(nodes, runs, strict=True)]
        for node, result in zip(nodes, results, strict=True):
            node.backend._notify_enqueued(result, node.fields["spec_json"], alias)
    return results


class Chain:
    """Explicit task arguments, executed in direct-parent success order.

    Terminal-parent failure handling belongs to Go. Enqueue does not propagate
    task return values or implement a competing Python state machine.
    """

    def __init__(self, *tasks):
        self.tasks = tasks
        self.workflow_id = uuid4()

    def enqueue(self):
        return _insert_nodes(_prepare_nodes(self.tasks), self.workflow_id, sequential=True)


class Group:
    """Independent nodes inserted atomically, without an aggregate callback row."""

    def __init__(self, *tasks):
        self.tasks = tasks
        self.workflow_id = uuid4()

    def enqueue(self):
        return _insert_nodes(_prepare_nodes(self.tasks), self.workflow_id)


class Chord:
    """Headers plus a callback, released by the Go workflow counter authority."""

    def __init__(self, tasks, callback):
        self.tasks = tuple(tasks)
        self.callback = callback
        self.workflow_id = uuid4()

    def enqueue(self):
        nodes = _prepare_nodes((*self.tasks, self.callback))
        results = _insert_nodes(nodes, self.workflow_id, callback=True)
        return results[:-1], results[-1]


def chain(*tasks):
    return Chain(*tasks)


def group(*tasks):
    return Group(*tasks)


def chord(*tasks, callback):
    return Chord(tasks, callback)

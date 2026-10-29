from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from asgiref.sync import sync_to_async
from django.db import connections, transaction
from django.db.models.functions import Now
from django.tasks import TaskContext as DjangoTaskContext, task_backends

from .db import format_result_id
from .models import TaskRun
from .proxy import TaskResultProxy
from .schema_contract import require_schema_contract
from .serialization import normalize_json
from .specs import SPEC_VERSION


class LeaseLostError(RuntimeError):
    """This executor no longer owns a live claim on the task."""


class _LeaseNow(Now):
    def as_postgresql(self, compiler, connection, **extra_context):
        return self.as_sql(
            compiler, connection, template="CLOCK_TIMESTAMP()", **extra_context
        )


@dataclass(frozen=True, slots=True, kw_only=True)
class ExecutionClaim:
    result_id: str
    budget_attempt: int
    expected_task_path: str | None
    db_alias: str
    worker_id: str
    lease_token: str = field(repr=False)

    def __post_init__(self):
        if type(self.budget_attempt) is not int or self.budget_attempt < 1:
            raise ValueError("A positive retry-budget attempt is required")
        if not self.worker_id or not self.lease_token:
            raise LeaseLostError("A worker owner and lease token are required")


def _owned_run(claim):
    require_schema_contract(connections[claim.db_alias])
    return TaskRun.objects.using(claim.db_alias).filter(
        result_id=claim.result_id,
        attempts=claim.budget_attempt,
        spec_version=SPEC_VERSION,
        status="RUNNING",
        cancel_requested=False,
        leased_by=claim.worker_id,
        lease_token=claim.lease_token,
        leased_until__gt=_LeaseNow(),
    )


def _load_owned(claim):
    try:
        run = _owned_run(claim).get()
    except TaskRun.DoesNotExist as error:
        raise LeaseLostError(
            "Task lease expired or belongs to a newer claim"
        ) from error
    if (
        claim.expected_task_path is not None
        and run.task_path != claim.expected_task_path
    ):
        raise ValueError("Task context path disagrees with the claimed row")
    if not isinstance(run.metadata_json, dict):
        raise ValueError("Task metadata must be a JSON object")
    return run


@dataclass(frozen=True, slots=True, kw_only=True)
class TaskContext(DjangoTaskContext):
    """Native immutable context with explicit fenced metadata extensions."""

    _claim: ExecutionClaim = field(repr=False)
    metadata: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_claim(cls, claim: ExecutionClaim):
        run = _load_owned(claim)
        result = TaskResultProxy(
            format_result_id(run.pk, claim.db_alias),
            task_backends[run.backend_alias],
            db_alias=claim.db_alias,
            raw_id=str(run.pk),
        )._bind_run(run, prime_arguments=True)
        result._source()
        return cls(task_result=result, _claim=claim, metadata=dict(run.metadata_json))

    @property
    def result_id(self):
        return self._claim.result_id

    @property
    def db_alias(self):
        return self._claim.db_alias

    @property
    def worker_id(self):
        return self._claim.worker_id

    @property
    def lease_token(self):
        return self._claim.lease_token

    @property
    def budget_attempt(self):
        return self._claim.budget_attempt

    @property
    def task_path(self):
        return self.task_result.data.task_path

    @property
    def queue_name(self):
        return self.task_result.data.queue_name

    @property
    def priority(self):
        return self.task_result.data.priority

    def load_metadata(self):
        run = _load_owned(self._claim)
        self.task_result._bind_run(run, prime_arguments=True)
        self.task_result._source()
        self.metadata.clear()
        self.metadata.update(run.metadata_json)

    def save_metadata(self):
        payload = normalize_json(self.metadata)
        require_schema_contract(connections[self.db_alias])
        with transaction.atomic(using=self.db_alias):
            # PostgreSQL can evaluate even volatile WHERE before an unchanged
            # tuple lock wait. Lock first, then check the fresh clock and claim.
            locked = (
                TaskRun.objects.using(self.db_alias)
                .filter(result_id=self.result_id)
                .select_for_update()
                .values_list("result_id", flat=True)
                .first()
            )
            if locked is None:
                raise LeaseLostError("Task no longer exists")
            changed = _owned_run(self._claim).update(
                metadata_json=payload, updated_at=_LeaseNow()
            )
            if changed != 1:
                raise LeaseLostError("Task lease expired or belongs to a newer claim")

    async def asave_metadata(self):
        await sync_to_async(self.save_metadata, thread_sensitive=True)()

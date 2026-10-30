from __future__ import annotations
import time
import asyncio
from datetime import datetime
from dataclasses import replace
from typing import Any, Self, TYPE_CHECKING
from django.tasks import TaskResult, TaskResultStatus
from django.tasks.base import TaskError
from django.tasks.exceptions import TaskResultDoesNotExist
from asgiref.sync import sync_to_async

from .db import parse_result_id

if TYPE_CHECKING:
    from .backend import ReproqBackend


class TaskResultProxy(TaskResult):
    """
    Native frozen result with a lazy queue-row cache and explicit rich arguments.
    """

    __slots__ = (
        "_db_alias",
        "_raw_id",
        "_data",
        "_argument_run",
        "_decoded_arguments",
        "_argument_error",
        "_payload",
        "_source_error",
    )

    def __init__(
        self,
        result_id: str,
        backend: ReproqBackend,
        db_alias: str | None = None,
        raw_id: str | None = None,
    ):
        alias, resolved_id = parse_result_id(raw_id or result_id)
        for name, value in {
            "id": result_id,
            "backend": backend.alias,
            "_db_alias": db_alias or alias,
            "_raw_id": resolved_id,
            "_data": None,
            "_argument_run": None,
            "_decoded_arguments": None,
            "_argument_error": None,
            "_payload": None,
            "_source_error": None,
            "_return_value": None,
        }.items():
            object.__setattr__(self, name, value)

    @property
    def db_alias(self):
        return self._db_alias

    def _bind_run(self, run, *, prime_arguments: bool = False) -> Self:
        """Bind cheap native state; hydrate rich values only on explicit demand."""
        object.__setattr__(self, "_data", run)
        object.__setattr__(self, "backend", run.backend_alias)
        object.__setattr__(
            self,
            "_return_value",
            run.return_json if run.status == "SUCCESSFUL" else None,
        )
        object.__setattr__(self, "_argument_run", None)
        object.__setattr__(self, "_decoded_arguments", None)
        object.__setattr__(self, "_argument_error", None)
        object.__setattr__(self, "_source_error", None)
        object.__setattr__(self, "_payload", None)
        if prime_arguments:
            self._prime_arguments()
        return self

    def _source(self) -> dict[str, Any]:
        from .specs import SPEC_VERSION, read_task_spec

        run = self.data
        if self._payload is not None:
            return self._payload
        if self._source_error is not None:
            raise self._source_error
        try:
            if run.spec_version == SPEC_VERSION:
                payload = read_task_spec(run).payload
            elif run.spec_version == 0:
                # Explicit historical inspection only. The executor context
                # rejects these rows; no source or model database is invented.
                payload = run.spec_json
            else:
                raise ValueError("Unsupported stored task source version")
            if not isinstance(payload, dict):
                raise ValueError("Stored task source must be a JSON object")
            object.__setattr__(self, "_payload", payload)
        except (ValueError, TypeError, UnicodeError) as error:
            object.__setattr__(self, "_source_error", error.with_traceback(None))
            raise
        return payload

    def _prime_arguments(self) -> None:
        from .serialization import decode_args_kwargs

        object.__setattr__(self, "_argument_run", self.data)
        try:
            object.__setattr__(
                self, "_decoded_arguments", decode_args_kwargs(self.args, self.kwargs)
            )
        except Exception as error:
            # A historical model may have been deleted. Its decode failure must
            # not hide task status, return values or the original references.
            object.__setattr__(self, "_argument_error", error.with_traceback(None))

    def _arguments(self) -> tuple[list[Any], dict[str, Any]]:
        run = self.data
        if self._argument_run is not run:
            self._prime_arguments()
        if self._argument_error is not None:
            raise self._argument_error
        return self._decoded_arguments

    async def adecoded_args(self) -> list[Any]:
        return (await sync_to_async(self._arguments, thread_sensitive=True)())[0]

    async def adecoded_kwargs(self) -> dict[str, Any]:
        return (await sync_to_async(self._arguments, thread_sensitive=True)())[1]

    def refresh(self):
        from django.db import connections
        from .models import TaskRun
        from .schema_contract import require_schema_contract

        require_schema_contract(connections[self.db_alias])
        try:
            run = TaskRun.objects.using(self.db_alias).get(result_id=self._raw_id)
        except TaskRun.DoesNotExist:
            raise TaskResultDoesNotExist(self.id)
        self._bind_run(run)

    async def arefresh(self):
        await sync_to_async(self.refresh, thread_sensitive=True)()

    @property
    def data(self):
        if self._data is None:
            self.refresh()
        return self._data

    @property
    def result_id(self) -> str:
        return self._raw_id

    @property
    def task(self):
        from django.tasks import Task
        from django.utils.module_loading import import_string

        spec = self._source()
        run_after = (
            (
                datetime.fromisoformat(spec["run_after"])
                if spec.get("run_after")
                else None
            )
            if self.data.spec_version == 1
            else self.data.run_after
        )
        task_path = spec.get("task_path") or self.data.task_path
        target = import_string(task_path)
        if isinstance(target, Task):
            return replace(
                target,
                priority=self.data.priority,
                queue_name=self.data.queue_name,
                run_after=run_after,
                backend=self.data.backend_alias,
                takes_context=bool(spec.get("takes_context")),
            )
        return Task(
            func=target,
            priority=self.data.priority,
            queue_name=self.data.queue_name,
            run_after=run_after,
            backend=self.data.backend_alias,
            takes_context=bool(spec.get("takes_context")),
        )

    @property
    def args(self):
        return self._source().get("args", [])

    @property
    def kwargs(self):
        return self._source().get("kwargs", {})

    @property
    def decoded_args(self):
        return self._arguments()[0]

    @property
    def decoded_kwargs(self):
        return self._arguments()[1]

    @property
    def status(self) -> TaskResultStatus:
        raw = self.data.status
        try:
            return TaskResultStatus(raw)
        except ValueError as exc:
            mapped = _map_status(raw)
            if mapped is not None:
                return mapped
            raise RuntimeError(
                f"Task {self.id} has non-standard status {raw!r}. "
                "Use raw_status to inspect it."
            ) from exc

    @property
    def raw_status(self) -> str:
        return self.data.status

    @property
    def enqueued_at(self):
        return self.data.enqueued_at

    @property
    def started_at(self):
        return self.data.started_at

    @property
    def last_attempted_at(self):
        return self.data.last_attempted_at

    @property
    def finished_at(self):
        return self.data.finished_at

    @property
    def budget_attempt(self):
        return self.data.attempts

    @property
    def eligible_after(self):
        return self.data.run_after

    @property
    def worker_ids(self):
        return self.data.worker_ids

    @property
    def raw_errors(self):
        return self.data.errors_json

    @property
    def errors(self):
        import builtins

        errors = []
        for error in self.raw_errors:
            from .protocol import WorkerExecutionError
            from .serialization import DeserializationError

            name = error.get("exception_class")
            if error.get("kind") == "infra_error" or not name:
                path = f"{WorkerExecutionError.__module__}.{WorkerExecutionError.__qualname__}"
            elif isinstance(name, str) and "." in name:
                path = name
            else:
                candidate = (
                    getattr(builtins, name, None) if isinstance(name, str) else None
                )
                if not isinstance(candidate, type) or not issubclass(
                    candidate, BaseException
                ):
                    raise DeserializationError(
                        "Original exception class path is unavailable; inspect raw_errors"
                    )
                path = f"builtins.{name}"
            errors.append(
                TaskError(
                    exception_class_path=path,
                    traceback=error.get("traceback")
                    or (
                        "Traceback unavailable. Worker diagnostic: "
                        + (error.get("message") or "No traceback supplied.")
                    ),
                )
            )
        return errors

    @property
    def metadata(self):
        return self.data.metadata_json or {}

    def wait(
        self, timeout: float = None, poll_interval: float = 0.5
    ) -> TaskResultProxy:
        start_time = time.monotonic()
        while True:
            self.refresh()
            if self.is_finished:
                return self
            if timeout is not None and (time.monotonic() - start_time) >= timeout:
                raise TimeoutError(f"Timed out waiting for task {self.id}")
            time.sleep(poll_interval)

    async def await_result(
        self, timeout: float = None, poll_interval: float = 0.5
    ) -> TaskResultProxy:
        start_time = time.monotonic()
        while True:
            await self.arefresh()
            if self.is_finished:
                return self
            if timeout is not None and (time.monotonic() - start_time) >= timeout:
                raise TimeoutError(f"Timed out waiting for task {self.id}")
            await asyncio.sleep(poll_interval)

    def __repr__(self):
        return f"<TaskResultProxy id={self.id} status={self._data.status if self._data else 'UNKNOWN'}>"


def _map_status(raw_status: str) -> TaskResultStatus | None:
    if raw_status == "WAITING":
        return TaskResultStatus.READY
    if raw_status == "CANCELLED":
        return TaskResultStatus.FAILED
    return None

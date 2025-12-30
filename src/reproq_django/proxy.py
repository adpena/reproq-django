from __future__ import annotations
import time
import asyncio
from typing import Any, TYPE_CHECKING
from django.tasks import TaskResultStatus
from django.tasks.exceptions import TaskResultDoesNotExist
from django.utils import timezone
from asgiref.sync import sync_to_async

if TYPE_CHECKING:
    from .backend import ReproqBackend

class TaskResultProxy:
    """
    A robust proxy for Django Task results that avoids undocumented internals.
    """
    def __init__(self, result_id: str, backend: ReproqBackend):
        self.id = result_id
        self.backend = backend
        self._data = None

    def refresh(self):
        from .models import TaskRun
        try:
            self._data = TaskRun.objects.get(result_id=self.id)
        except TaskRun.DoesNotExist:
            raise TaskResultDoesNotExist(self.id)
        return self

    async def arefresh(self):
        return await sync_to_async(self.refresh, thread_sensitive=True)()

    @property
    def data(self):
        if self._data is None:
            self.refresh()
        return self._data

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
    def enqueued_at(self): return self.data.enqueued_at
    @property
    def started_at(self): return self.data.started_at
    @property
    def last_attempted_at(self): return self.data.last_attempted_at
    @property
    def finished_at(self): return self.data.finished_at
    @property
    def attempts(self): return self.data.attempts
    @property
    def worker_ids(self): return self.data.worker_ids
    @property
    def errors(self): return self.data.errors_json

    @property
    def result(self) -> Any:
        if self.status != TaskResultStatus.SUCCESSFUL:
            if self.status == TaskResultStatus.FAILED:
                raise RuntimeError(f"Task {self.id} failed with errors: {self.errors}")
            raise RuntimeError(f"Task {self.id} has status {self.status}, result not available.")
        return self.data.return_json

    def _terminal_statuses(self) -> set[TaskResultStatus]:
        statuses = {TaskResultStatus.SUCCESSFUL, TaskResultStatus.FAILED}
        if "CANCELLED" in TaskResultStatus.__members__:
            statuses.add(TaskResultStatus.CANCELLED)
        return statuses

    def wait(self, timeout: float = None, poll_interval: float = 0.5) -> TaskResultProxy:
        start_time = time.time()
        terminal_statuses = self._terminal_statuses()
        while True:
            self.refresh()
            if self.status in terminal_statuses:
                return self
            if timeout and (time.time() - start_time) > timeout:
                raise TimeoutError(f"Timed out waiting for task {self.id}")
            time.sleep(poll_interval)

    async def await_result(self, timeout: float = None, poll_interval: float = 0.5) -> TaskResultProxy:
        start_time = time.time()
        terminal_statuses = self._terminal_statuses()
        while True:
            await self.arefresh()
            if self.status in terminal_statuses:
                return self
            if timeout and (time.time() - start_time) > timeout:
                raise TimeoutError(f"Timed out waiting for task {self.id}")
            await asyncio.sleep(poll_interval)

    def __repr__(self):
        return f"<TaskResultProxy id={self.id} status={self._data.status if self._data else 'UNKNOWN'}>"


def _map_status(raw_status: str) -> TaskResultStatus | None:
    if raw_status in ("READY", "WAITING") and "PENDING" in TaskResultStatus.__members__:
        return TaskResultStatus.PENDING
    if raw_status == "CANCELLED" and "CANCELLED" in TaskResultStatus.__members__:
        return TaskResultStatus.CANCELLED
    return None

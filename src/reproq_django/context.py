from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from asgiref.sync import sync_to_async
from django.utils import timezone

from .models import TaskRun
from .serialization import normalize_json


@dataclass
class TaskContext:
    result_id: str
    attempt: int
    task_path: str | None
    queue_name: str | None
    priority: int | None
    db_alias: str
    metadata: dict[str, Any] = field(default_factory=dict)

    def save_metadata(self):
        payload = normalize_json(self.metadata or {})
        TaskRun.objects.using(self.db_alias).filter(result_id=self.result_id).update(
            metadata_json=payload,
            updated_at=timezone.now(),
        )

    async def asave_metadata(self):
        return await sync_to_async(self.save_metadata, thread_sensitive=True)()

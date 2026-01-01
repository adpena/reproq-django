from __future__ import annotations

from typing import Callable


def limits_concurrency(key: str | Callable[..., str], to: int = 1):
    """
    Decorator to limit concurrent executions sharing a key.
    """
    def decorator(task_obj):
        _apply_concurrency(task_obj, key, to)
        return task_obj
    return decorator


def _apply_concurrency(task_obj, key: str | Callable[..., str], limit: int):
    for candidate in (task_obj, getattr(task_obj, "task", None)):
        if candidate is None:
            continue
        try:
            setattr(candidate, "concurrency_key", key)
            setattr(candidate, "concurrency_limit", int(limit))
        except Exception:
            continue

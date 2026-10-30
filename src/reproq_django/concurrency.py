from __future__ import annotations

from typing import Callable


def limits_concurrency(key: str | Callable[..., str], to: int = 1):
    """
    Decorator to limit concurrent executions sharing a key.
    """
    if not callable(key) and (not isinstance(key, str) or not key.strip()):
        raise ValueError("Concurrency key must be a nonempty string or callable")
    if isinstance(to, bool) or not isinstance(to, int) or to <= 0:
        raise ValueError("Concurrency limit must be a positive integer")

    def decorator(task_obj):
        # Django Task is frozen and slotted. Keep custom options on its Python
        # function so both decorator orders and Task.using() preserve them.
        target = getattr(task_obj, "func", task_obj)
        setattr(target, "concurrency_key", key)
        setattr(target, "concurrency_limit", to)
        return task_obj
    return decorator


def task_option(task, name, default=None):
    return getattr(task, name, getattr(getattr(task, "func", None), name, default))

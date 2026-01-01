from __future__ import annotations

from .db import default_db_alias, queue_db_aliases


def _is_reproq_model(model) -> bool:
    try:
        return model._meta.app_label == "reproq_django"
    except Exception:
        return False


class ReproqRouter:
    """Route Reproq tables to the configured queue database alias."""

    def db_for_read(self, model, **hints):
        if not _is_reproq_model(model):
            return None
        instance = hints.get("instance")
        if instance is not None and getattr(instance._state, "db", None):
            return instance._state.db
        return default_db_alias()

    def db_for_write(self, model, **hints):
        if not _is_reproq_model(model):
            return None
        instance = hints.get("instance")
        if instance is not None and getattr(instance._state, "db", None):
            return instance._state.db
        return default_db_alias()

    def allow_relation(self, obj1, obj2, **hints):
        if _is_reproq_model(obj1.__class__) and _is_reproq_model(obj2.__class__):
            db1 = getattr(obj1._state, "db", None)
            db2 = getattr(obj2._state, "db", None)
            if db1 and db2:
                return db1 == db2
            return True
        return None

    def allow_migrate(self, db, app_label, model_name=None, **hints):
        if app_label != "reproq_django":
            return None
        return db in queue_db_aliases()

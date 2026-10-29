from __future__ import annotations

import fnmatch
from typing import Tuple
from urllib.parse import quote, unquote

from django.conf import settings
from django.db import DEFAULT_DB_ALIAS

# Django's TaskResult contract requires IDs shorter than 64 characters.
RESULT_ID_MAX_LENGTH = 63


def default_db_alias() -> str:
    return getattr(settings, "REPROQ_DEFAULT_DB_ALIAS", DEFAULT_DB_ALIAS)


def queue_database_map() -> dict[str, str]:
    mapping = getattr(settings, "REPROQ_QUEUE_DATABASES", None)
    if mapping is None:
        return {}
    if not isinstance(mapping, dict):
        raise ValueError("REPROQ_QUEUE_DATABASES must be a dict of queue->db_alias")
    return mapping


def resolve_queue_db(queue_name: str | None) -> str:
    if not queue_name:
        queue_name = "default"
    mapping = queue_database_map()
    if queue_name in mapping:
        return mapping[queue_name]
    for key, alias in mapping.items():
        if _is_glob(key) and fnmatch.fnmatchcase(queue_name, key):
            return alias
    return default_db_alias()


def queue_db_aliases() -> list[str]:
    aliases = {default_db_alias()}
    for alias in queue_database_map().values():
        aliases.add(alias)
    return sorted(aliases)


def should_prefix_result_ids() -> bool:
    default = default_db_alias()
    multiple = any(alias != default for alias in queue_database_map().values())
    override = getattr(settings, "REPROQ_RESULT_ID_WITH_ALIAS", None)
    if override is not None:
        if type(override) is not bool or (multiple and not override):
            raise ValueError(
                "Multiple queue databases require unambiguous prefixed result IDs"
            )
        return override
    return multiple


def validate_result_id_routing(db_alias: str):
    """Reject unsupported ID configuration before a producer writes a row."""
    if not isinstance(db_alias, str) or db_alias not in settings.DATABASES:
        raise ValueError("Queue database alias is not configured")
    include_alias = should_prefix_result_ids() or db_alias != default_db_alias()
    if include_alias:
        from django.db.backends.base.operations import BaseDatabaseOperations

        lower, upper = BaseDatabaseOperations.integer_field_ranges["BigAutoField"]
        max_digits = max(len(str(lower)), len(str(upper)))
        if len(quote(db_alias, safe="")) + 1 + max_digits > RESULT_ID_MAX_LENGTH:
            raise ValueError(
                "Encoded queue database alias cannot fit Django TaskResult IDs; use a shorter configured alias"
            )


def format_result_id(result_id: int | str, db_alias: str) -> str:
    include_alias = should_prefix_result_ids() or db_alias != default_db_alias()
    validate_result_id_routing(db_alias)
    result_str = str(result_id)
    if include_alias and db_alias:
        result_str = f"{quote(db_alias, safe='')}:{result_str}"
    if len(result_str) > RESULT_ID_MAX_LENGTH:
        raise ValueError("Task result ID exceeds the native Django length contract")
    return result_str


def parse_result_id(value: int | str) -> Tuple[str, str]:
    if isinstance(value, int):
        return default_db_alias(), str(value)
    raw = str(value)
    if ":" in raw:
        prefix, result_id = raw.split(":", 1)
        alias = unquote(prefix, errors="strict")
        if quote(alias, safe="") != prefix or not result_id:
            raise ValueError("Noncanonical or incomplete task result ID")
        if alias in getattr(settings, "DATABASES", {}):
            return alias, result_id
        raise ValueError("Task result ID names an unknown database alias")
    return default_db_alias(), raw


def _is_glob(value: str) -> bool:
    return any(char in value for char in ("*", "?", "["))
